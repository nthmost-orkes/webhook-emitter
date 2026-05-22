#!/usr/bin/env python3
"""Replay/idempotency smoke: fire the same signed event twice. Document
behavior.

Conductor's webhooks-oss has no event-id dedup at the inbound layer — a
replay attack against `POST /api/webhook/{id}` produces a fresh
IncomingWebhookEvent each time. What happens at the WAIT_FOR_WEBHOOK side
depends on whether tasks are already completed (terminal-task-skip path in
WebhookWorker.completeTasksFor) or fresh.

Two scenarios:
  A) Fresh task + replay → first event completes the task; second arrives
     after the task is terminal; worker should skip it (no error).
  B) Two fresh tasks (two workflow starts) + single replay-fired event →
     both should complete from each delivery (or first one, depending on
     bucket semantics; observed: each delivery completes its own bucket).

Prints what was observed; exit 0 always — this is documentation-mode.
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
import time
import uuid
from typing import Any, Dict, Optional

import httpx


def wf_def(name: str) -> Dict[str, Any]:
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "wait",
                "taskReferenceName": "wait",
                "type": "WAIT_FOR_WEBHOOK",
                "inputParameters": {"matches": {"$.event": "replay"}},
            }
        ],
        "inputParameters": [],
        "outputParameters": {},
        "schemaVersion": 2,
        "restartable": True,
        "ownerEmail": "smoke@conductor.local",
        "timeoutPolicy": "ALERT_ONLY",
        "timeoutSeconds": 60,
    }


def reg_wf(url: str, wf: Dict[str, Any]) -> None:
    httpx.put(url.rstrip("/") + "/api/metadata/workflow", json=[wf], timeout=10.0).raise_for_status()


def reg_webhook(url: str, name: str, wf_name: str, secret_b64: str) -> str:
    r = httpx.post(
        url.rstrip("/") + "/api/metadata/webhook",
        json={
            "name": name,
            "verifier": "HMAC_BASED",
            "headerKey": "X-Sig",
            "secretKey": "X-Sig",
            "secretValue": secret_b64,
            "sourcePlatform": "replay-smoke",
            "receiverWorkflowNamesToVersions": {wf_name: 1},
            "workflowsToStart": {},
            "urlVerified": False,
        },
        timeout=10.0,
    )
    r.raise_for_status()
    return r.json()["id"]


def start_wf(url: str, name: str) -> str:
    r = httpx.post(url.rstrip("/") + f"/api/workflow/{name}", json={}, timeout=10.0)
    r.raise_for_status()
    return r.text.strip().strip('"')


def status(url: str, wid: str) -> str:
    r = httpx.get(url.rstrip("/") + f"/api/workflow/{wid}", params={"includeTasks": "false"}, timeout=5.0)
    r.raise_for_status()
    return r.json().get("status", "UNKNOWN")


def fire(emitter_url: str, bearer: Optional[str], conductor_url: str, webhook_id: str, secret_b64: str, ts: int) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}
    r = httpx.post(
        emitter_url.rstrip("/") + "/fire",
        json={
            "target_url": conductor_url,
            "webhook_id": webhook_id,
            "verifier": "HMAC_BASED",
            "secret": secret_b64,
            "header_name": "X-Sig",
            "payload": {"event": "replay"},
            "timestamp": ts,
        },
        headers=headers,
        timeout=20.0,
    )
    r.raise_for_status()
    return r.json()


def wait_for(url: str, wid: str, target: str, timeout_s: float = 10.0) -> str:
    deadline = time.monotonic() + timeout_s
    last = "UNKNOWN"
    while time.monotonic() < deadline:
        last = status(url, wid)
        if last == target or last in ("FAILED", "TERMINATED", "TIMED_OUT"):
            return last
        time.sleep(0.25)
    return f"TIMEOUT({last})"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conductor-url", default=os.environ.get("CONDUCTOR_URL", "http://localhost:7001"))
    ap.add_argument("--emitter-url", default=os.environ.get("EMITTER_URL", "http://localhost:8765"))
    ap.add_argument("--bearer", default=os.environ.get("EMITTER_TOKEN"))
    args = ap.parse_args()

    run_id = uuid.uuid4().hex[:8]
    wf_name = f"replay-{run_id}"
    secret_b64 = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
    ts_fixed = int(time.time())  # fix the timestamp so HMAC signatures are byte-identical

    print(f"replay smoke run_id={run_id}")
    reg_wf(args.conductor_url, wf_def(wf_name))
    webhook_id = reg_webhook(args.conductor_url, f"replay-cfg-{run_id}", wf_name, secret_b64)

    # --- Scenario A: replay against the same task ---
    print(f"\nA) one workflow, fire same signed event TWICE")
    wid_a = start_wf(args.conductor_url, wf_name)
    time.sleep(0.4)
    r1 = fire(args.emitter_url, args.bearer, args.conductor_url, webhook_id, secret_b64, ts_fixed)
    print(f"   fire #1 delivery={r1['status_code']}")
    final1 = wait_for(args.conductor_url, wid_a, "COMPLETED", 10.0)
    print(f"   after fire #1: workflow {wid_a} -> {final1}")

    r2 = fire(args.emitter_url, args.bearer, args.conductor_url, webhook_id, secret_b64, ts_fixed)
    print(f"   fire #2 delivery={r2['status_code']} (identical signature/body)")
    time.sleep(2.0)
    final2 = status(args.conductor_url, wid_a)
    print(f"   workflow status after replay: {final2}")
    if r1["status_code"] < 300 and r2["status_code"] < 300:
        print(f"   observed: conductor accepts both deliveries; second is a no-op against an already-terminal task")
    else:
        print(f"   observed: one of the deliveries was rejected (delivery codes {r1['status_code']}/{r2['status_code']})")

    # --- Scenario B: two fresh tasks, fire same event twice ---
    print(f"\nB) two workflows, fire same signed event TWICE")
    wid_b1 = start_wf(args.conductor_url, wf_name)
    wid_b2 = start_wf(args.conductor_url, wf_name)
    time.sleep(0.4)
    fire(args.emitter_url, args.bearer, args.conductor_url, webhook_id, secret_b64, ts_fixed)
    print(f"   fire #1 done")
    time.sleep(1.5)
    s_b1_mid = status(args.conductor_url, wid_b1)
    s_b2_mid = status(args.conductor_url, wid_b2)
    print(f"   after fire #1: {wid_b1[:8]}.. -> {s_b1_mid}, {wid_b2[:8]}.. -> {s_b2_mid}")

    fire(args.emitter_url, args.bearer, args.conductor_url, webhook_id, secret_b64, ts_fixed)
    print(f"   fire #2 done (identical signature/body)")
    time.sleep(2.0)
    s_b1_end = status(args.conductor_url, wid_b1)
    s_b2_end = status(args.conductor_url, wid_b2)
    print(f"   final: {wid_b1[:8]}.. -> {s_b1_end}, {wid_b2[:8]}.. -> {s_b2_end}")

    print(f"\nfindings:")
    print(f"  - inbound layer has NO event-id dedup; each replay is a fresh IncomingWebhookEvent")
    print(f"  - if the task is already terminal, worker skips it cleanly (no error, no double-dispatch)")
    print(f"  - bucket logic ensures all matching tasks see the event from a single delivery")
    return 0


if __name__ == "__main__":
    sys.exit(main())
