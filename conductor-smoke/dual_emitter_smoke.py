#!/usr/bin/env python3
"""Concurrent dual-emitter smoke: two emitters on different hosts fire
identically-signed events at the same conductor at the same instant.

Tests two things:
  1. The worker handles concurrent inbound events without races.
  2. Body fidelity across different network paths — if anything between
     emitter and conductor mangles the body (proxy re-serialization,
     content-length re-encoding, TLS termination quirks), the HMAC
     verification would fail at conductor's end even though the emitter
     produced a byte-correct signature.

Each fire targets a fresh workflow instance so we can observe both
independently. Uses a barrier (threading.Event) to start both threads at
the same instant.
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional, Tuple

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
                "inputParameters": {"matches": {"$.event": "dual"}},
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
            "sourcePlatform": "dual-smoke",
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


def fire(emitter_url: str, bearer: Optional[str], conductor_url: str, webhook_id: str, secret_b64: str, label: str) -> Tuple[str, int, float]:
    headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}
    t0 = time.monotonic()
    r = httpx.post(
        emitter_url.rstrip("/") + "/fire",
        json={
            "target_url": conductor_url,
            "webhook_id": webhook_id,
            "verifier": "HMAC_BASED",
            "secret": secret_b64,
            "header_name": "X-Sig",
            "payload": {"event": "dual", "via": label},
        },
        headers=headers,
        timeout=20.0,
    )
    r.raise_for_status()
    return label, r.json()["status_code"], time.monotonic() - t0


def wait_for(url: str, wid: str, timeout_s: float = 15.0) -> str:
    deadline = time.monotonic() + timeout_s
    last = "UNKNOWN"
    while time.monotonic() < deadline:
        last = status(url, wid)
        if last in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
            return last
        time.sleep(0.25)
    return f"TIMEOUT({last})"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conductor-url", default=os.environ.get("CONDUCTOR_URL", "http://localhost:7001"))
    ap.add_argument("--emitter-a", required=True, help="First emitter URL.")
    ap.add_argument("--emitter-b", required=True, help="Second emitter URL.")
    ap.add_argument("--bearer", default=os.environ.get("EMITTER_TOKEN"))
    args = ap.parse_args()

    run_id = uuid.uuid4().hex[:8]
    wf_name = f"dual-{run_id}"
    secret_b64 = base64.b64encode(secrets.token_bytes(32)).decode("ascii")

    print(f"dual-emitter smoke run_id={run_id}")
    print(f"  conductor={args.conductor_url}")
    print(f"  emitter-a={args.emitter_a}")
    print(f"  emitter-b={args.emitter_b}")
    reg_wf(args.conductor_url, wf_def(wf_name))
    webhook_id = reg_webhook(args.conductor_url, f"dual-cfg-{run_id}", wf_name, secret_b64)

    # One fresh workflow instance per emitter — independently observable.
    wid_a = start_wf(args.conductor_url, wf_name)
    wid_b = start_wf(args.conductor_url, wf_name)
    print(f"  workflow A={wid_a[:12]}.. via {args.emitter_a}")
    print(f"  workflow B={wid_b[:12]}.. via {args.emitter_b}")
    time.sleep(0.5)

    # Barrier: both threads wait, then fire simultaneously.
    barrier = threading.Event()

    def fire_with_barrier(emitter_url: str, label: str):
        barrier.wait()
        return fire(emitter_url, args.bearer, args.conductor_url, webhook_id, secret_b64, label)

    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_a = ex.submit(fire_with_barrier, args.emitter_a, "a")
        fut_b = ex.submit(fire_with_barrier, args.emitter_b, "b")
        time.sleep(0.1)  # both threads parked at barrier.wait()
        barrier.set()    # release at the same instant
        (la, sa, da) = fut_a.result()
        (lb, sb, db) = fut_b.result()

    print(f"  fired concurrently: a={sa} ({da:.2f}s)  b={sb} ({db:.2f}s)")
    if sa >= 300 or sb >= 300:
        print(f"  delivery failure detected — aborting")
        return 1

    final_a = wait_for(args.conductor_url, wid_a)
    final_b = wait_for(args.conductor_url, wid_b)
    print(f"\n  workflow A: {final_a}")
    print(f"  workflow B: {final_b}")

    ok = final_a == "COMPLETED" and final_b == "COMPLETED"
    print(f"\n  {'PASS' if ok else 'FAIL'}: both workflows completed from concurrent dual-emitter fires")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
