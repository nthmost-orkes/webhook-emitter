#!/usr/bin/env python3
"""Multi-verifier same-workflow smoke: 3 webhook configs (HMAC, signature, header)
all bound to the same workflow as receivers, fired from each in turn.

Tests that conductor's matcher recomputation (InMemoryWebhookDAO.getMatchers)
handles overlapping `receiverWorkflowNamesToVersions` — i.e. multiple webhook
configs reference the same workflow and each correctly dispatches to a fresh
WAIT_FOR_WEBHOOK task instance.

Three configs * one workflow + start fresh instance per fire = 3 COMPLETED
workflows from 3 verifier-different events.
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx


@dataclass
class Cfg:
    verifier: str
    secret: str
    header_key: str = "X-Sig"
    header_value: str = ""
    config_headers: Dict[str, str] = field(default_factory=dict)


def workflow_def(name: str) -> Dict[str, Any]:
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "wait",
                "taskReferenceName": "wait",
                "type": "WAIT_FOR_WEBHOOK",
                "inputParameters": {"matches": {"$.event": "multi"}},
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


def register_workflow_def(conductor_url: str, wf: Dict[str, Any]) -> None:
    r = httpx.put(conductor_url.rstrip("/") + "/api/metadata/workflow", json=[wf], timeout=10.0)
    r.raise_for_status()


def register_webhook(conductor_url: str, name: str, wf_name: str, cfg: Cfg) -> str:
    payload = {
        "name": name,
        "verifier": cfg.verifier,
        "headerKey": cfg.header_key,
        "secretKey": cfg.header_key,
        "secretValue": cfg.secret,
        "sourcePlatform": "multi-smoke",
        "receiverWorkflowNamesToVersions": {wf_name: 1},
        "workflowsToStart": {},
        "urlVerified": False,
    }
    if cfg.config_headers:
        payload["headers"] = cfg.config_headers
    r = httpx.post(conductor_url.rstrip("/") + "/api/metadata/webhook", json=payload, timeout=10.0)
    r.raise_for_status()
    return r.json()["id"]


def start_workflow(conductor_url: str, name: str) -> str:
    r = httpx.post(conductor_url.rstrip("/") + f"/api/workflow/{name}", json={}, timeout=10.0)
    r.raise_for_status()
    return r.text.strip().strip('"')


def get_status(conductor_url: str, wid: str) -> str:
    r = httpx.get(conductor_url.rstrip("/") + f"/api/workflow/{wid}", params={"includeTasks": "false"}, timeout=5.0)
    r.raise_for_status()
    return r.json().get("status", "UNKNOWN")


def fire(emitter_url: str, bearer: Optional[str], conductor_url: str, webhook_id: str, cfg: Cfg) -> int:
    headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}
    r = httpx.post(
        emitter_url.rstrip("/") + "/fire",
        json={
            "target_url": conductor_url,
            "webhook_id": webhook_id,
            "verifier": cfg.verifier,
            "secret": cfg.secret,
            "header_name": cfg.header_key,
            "header_value": cfg.header_value,
            "payload": {"event": "multi"},
        },
        headers=headers,
        timeout=20.0,
    )
    r.raise_for_status()
    return r.json()["status_code"]


def poll(conductor_url: str, wid: str, timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    last = "UNKNOWN"
    while time.monotonic() < deadline:
        last = get_status(conductor_url, wid)
        if last in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
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
    wf_name = f"multi-{run_id}"

    # Three different verifier configs, all pointing at the same workflow.
    hmac_val = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
    sig_val = secrets.token_hex(16)
    hdr_val = secrets.token_hex(8)
    configs = [
        ("hmac", Cfg("HMAC_BASED", hmac_val)),
        ("sig", Cfg("SIGNATURE_BASED", sig_val)),
        ("hdr", Cfg("HEADER_BASED", "", header_value=hdr_val, config_headers={"X-Sig": hdr_val})),
    ]

    print(f"multi-verifier smoke run_id={run_id} wf={wf_name}")
    register_workflow_def(args.conductor_url, workflow_def(wf_name))

    # Register all three webhooks pointing at the same workflow.
    webhook_ids: List[str] = []
    for label, cfg in configs:
        wid = register_webhook(args.conductor_url, f"multi-cfg-{label}-{run_id}", wf_name, cfg)
        webhook_ids.append(wid)
        print(f"  registered webhook[{label}] ({cfg.verifier}) -> {wid}")

    print(f"\n  firing from each, each spawns a fresh workflow instance:")
    results = []
    for (label, cfg), webhook_id in zip(configs, webhook_ids):
        workflow_id = start_workflow(args.conductor_url, wf_name)
        time.sleep(0.4)
        t0 = time.monotonic()
        sc = fire(args.emitter_url, args.bearer, args.conductor_url, webhook_id, cfg)
        final = poll(args.conductor_url, workflow_id, timeout_s=15.0)
        dt = time.monotonic() - t0
        ok = sc < 300 and final == "COMPLETED"
        marker = "PASS" if ok else "FAIL"
        print(f"    [{marker}] {cfg.verifier:<16} workflow={workflow_id} delivery={sc} final={final} ({dt:.2f}s)")
        results.append(ok)

    passed = sum(results)
    print(f"\n  summary: {passed}/{len(results)} passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
