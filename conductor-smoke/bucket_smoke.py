#!/usr/bin/env python3
"""Bucket fan-out smoke: N workflows of the same name all idle on the same
WAIT_FOR_WEBHOOK matches, single signed event arrives, all N complete.

Exercises the InMemoryWebhookTaskService bucket logic from a production angle:
when multiple task instances share the same registration hash, one matched
event must dispatch to every bucket member.

Why this matters: the per-verifier smoke (smoke.py) starts a single workflow
per verifier and fires one event at it. That validates the verifier path but
not what happens when many tasks are stacked on the same matcher. The unit
test InMemoryWebhookTaskServiceTest.multiple_tasks_sameHash_bucketed covers
this in-process; bucket_smoke.py proves it at the queue/worker level.

Uses HMAC_BASED only — the verifier-cross-cut is smoke.py's job.

Run:
  python conductor-smoke/bucket_smoke.py --n 20
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import httpx


def workflow_def(name: str) -> Dict[str, Any]:
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "wait",
                "taskReferenceName": "wait",
                "type": "WAIT_FOR_WEBHOOK",
                "inputParameters": {"matches": {"$.event": "fanout"}},
            }
        ],
        "inputParameters": [],
        "outputParameters": {},
        "schemaVersion": 2,
        "restartable": True,
        "ownerEmail": "smoke@conductor.local",
        "timeoutPolicy": "ALERT_ONLY",
        "timeoutSeconds": 120,
    }


def register_workflow_def(conductor_url: str, wf: Dict[str, Any]) -> None:
    r = httpx.put(
        conductor_url.rstrip("/") + "/api/metadata/workflow",
        json=[wf],
        timeout=10.0,
    )
    r.raise_for_status()


def register_webhook(conductor_url: str, name: str, wf_name: str, secret_b64: str) -> str:
    r = httpx.post(
        conductor_url.rstrip("/") + "/api/metadata/webhook",
        json={
            "name": name,
            "verifier": "HMAC_BASED",
            "headerKey": "X-Sig",
            "secretKey": "X-Sig",
            "secretValue": secret_b64,
            "sourcePlatform": "bucket-smoke",
            "receiverWorkflowNamesToVersions": {wf_name: 1},
            "workflowsToStart": {},
            "urlVerified": False,
        },
        timeout=10.0,
    )
    r.raise_for_status()
    return r.json()["id"]


def start_workflow(conductor_url: str, name: str) -> str:
    r = httpx.post(
        conductor_url.rstrip("/") + f"/api/workflow/{name}",
        json={},
        timeout=10.0,
    )
    r.raise_for_status()
    return r.text.strip().strip('"')


def get_workflow_status(conductor_url: str, workflow_id: str) -> str:
    r = httpx.get(
        conductor_url.rstrip("/") + f"/api/workflow/{workflow_id}",
        params={"includeTasks": "false"},
        timeout=5.0,
    )
    r.raise_for_status()
    return r.json().get("status", "UNKNOWN")


def fire_event(
    emitter_url: str,
    bearer: Optional[str],
    conductor_url: str,
    webhook_id: str,
    secret_b64: str,
) -> int:
    headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}
    r = httpx.post(
        emitter_url.rstrip("/") + "/fire",
        json={
            "target_url": conductor_url,
            "webhook_id": webhook_id,
            "verifier": "HMAC_BASED",
            "secret": secret_b64,
            "header_name": "X-Sig",
            "payload": {"event": "fanout"},
        },
        headers=headers,
        timeout=20.0,
    )
    r.raise_for_status()
    return r.json()["status_code"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conductor-url", default=os.environ.get("CONDUCTOR_URL", "http://localhost:7001"))
    ap.add_argument("--emitter-url", default=os.environ.get("EMITTER_URL", "http://localhost:8765"))
    ap.add_argument("--bearer", default=os.environ.get("EMITTER_TOKEN"))
    ap.add_argument("--n", type=int, default=20, help="Number of workflow instances to stack on the same matcher.")
    ap.add_argument("--timeout", type=float, default=60.0, help="Per-workflow completion timeout (s).")
    args = ap.parse_args()

    run_id = uuid.uuid4().hex[:8]
    wf_name = f"bucket-{run_id}"
    webhook_name = f"bucket-cfg-{run_id}"
    secret_b64 = base64.b64encode(secrets.token_bytes(32)).decode("ascii")

    print(f"bucket fan-out smoke run_id={run_id} N={args.n}")
    print(f"  conductor={args.conductor_url}  emitter={args.emitter_url}")

    register_workflow_def(args.conductor_url, workflow_def(wf_name))
    webhook_id = register_webhook(args.conductor_url, webhook_name, wf_name, secret_b64)

    print(f"  starting {args.n} workflows concurrently...")
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(args.n, 16)) as ex:
        ids = list(ex.map(lambda _: start_workflow(args.conductor_url, wf_name), range(args.n)))
    print(f"  {len(ids)} workflows started in {time.monotonic() - t0:.2f}s")

    # Settle so all N WAIT_FOR_WEBHOOK tasks are registered before the event lands.
    time.sleep(1.0)

    print("  firing single event...")
    sc = fire_event(args.emitter_url, args.bearer, args.conductor_url, webhook_id, secret_b64)
    if sc >= 300:
        print(f"  emitter delivered to conductor but got {sc}; aborting", file=sys.stderr)
        return 1

    print(f"  polling for all {args.n} workflows to reach COMPLETED...")
    t_fire = time.monotonic()
    pending = set(ids)
    completed: Dict[str, float] = {}
    nonterminal_other: Dict[str, str] = {}
    while pending and (time.monotonic() - t_fire) < args.timeout:
        # Bulk poll — sequential is fine for moderate N; if N gets large this could parallelize.
        for wid in list(pending):
            try:
                status = get_workflow_status(args.conductor_url, wid)
            except Exception as e:
                print(f"    poll error {wid}: {e}", file=sys.stderr)
                continue
            if status == "COMPLETED":
                completed[wid] = time.monotonic() - t_fire
                pending.discard(wid)
            elif status in ("FAILED", "TERMINATED", "TIMED_OUT"):
                nonterminal_other[wid] = status
                pending.discard(wid)
        if pending:
            time.sleep(0.25)

    elapsed = time.monotonic() - t_fire
    print(f"\n  results after {elapsed:.2f}s:")
    print(f"    COMPLETED: {len(completed)}/{args.n}")
    if nonterminal_other:
        print(f"    non-COMPLETED terminal: {len(nonterminal_other)} → {nonterminal_other}")
    if pending:
        print(f"    still pending (last known: RUNNING): {len(pending)} ids={sorted(pending)[:5]}...")
    if completed:
        times = sorted(completed.values())
        print(f"    completion latency: min={times[0]:.2f}s  median={times[len(times)//2]:.2f}s  max={times[-1]:.2f}s")

    ok = len(completed) == args.n
    print(f"\n  {'PASS' if ok else 'FAIL'}: {len(completed)}/{args.n} completed from a single event")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
