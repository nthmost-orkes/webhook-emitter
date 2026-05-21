#!/usr/bin/env python3
"""End-to-end smoke harness: webhook-emitter → conductor → WAIT_FOR_WEBHOOK completion.

For each verifier scheme, the harness:
  1. Registers a workflow def containing a WAIT_FOR_WEBHOOK task whose matches
     pin `$.event == "smoke"`.
  2. Registers a webhook config bound to that workflow as a `receiverWorkflows`
     target. Secrets are generated fresh per run.
  3. Starts the workflow → conductor enqueues the WAIT_FOR_WEBHOOK task, which
     idles waiting for a matching event.
  4. Fires a signed event via webhook-emitter's POST /fire with body `{"event": "smoke"}`.
  5. Polls the workflow status until COMPLETED (success) or TIMED_OUT (failure).

Prints a per-verifier pass/fail table at the end. Exits non-zero if any verifier failed.

Run against a local conductor-server-lite:
  ./gradlew :conductor-server-lite:bootRun   # in another terminal
  webhook-emitter --port 8765                # in another terminal
  python conductor-smoke/smoke.py
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
from typing import Any, Dict, List, Optional, Tuple

import httpx


@dataclass
class VerifierCase:
    name: str
    secret_value: str
    # Header carrying the signature. Vendor schemes pin specific names; configurable schemes use X-Sig.
    header_key: str = "X-Sig"
    # Literal value for HEADER_BASED only.
    header_value: str = ""
    # For HEADER_BASED: WebhookConfig.headers map the verifier compares against.
    config_headers: Dict[str, str] = field(default_factory=dict)
    # SLACK_BASED uses urlVerified=true to bypass the URL-handshake challenge step.
    url_verified: bool = False


def hmac_case() -> VerifierCase:
    # HMACVerifier base64-decodes the configured secret; emitter expects base64 too.
    raw = secrets.token_bytes(32)
    return VerifierCase("HMAC_BASED", base64.b64encode(raw).decode("ascii"))


def signature_case() -> VerifierCase:
    return VerifierCase("SIGNATURE_BASED", secrets.token_hex(16))


def header_case() -> VerifierCase:
    # HeaderBasedVerifier iterates webhookConfig.headers and requires each one
    # to be present with the matching value on the incoming event.
    val = secrets.token_hex(8)
    return VerifierCase(
        "HEADER_BASED",
        secret_value="",
        header_value=val,
        config_headers={"X-Sig": val},
    )


def slack_case() -> VerifierCase:
    # SlackVerifier short-circuits on urlVerified=true; we drop the `challenge`
    # field from the body so IncomingWebhookService treats it as a real event
    # (challenge present → handshake mode → no dispatch).
    return VerifierCase("SLACK_BASED", secrets.token_hex(16), url_verified=True)


def stripe_case() -> VerifierCase:
    # StripeVerifier reconstructs a Stripe Event from the body and reads
    # event.getApiVersion() — body must include api_version: YYYY-MM-DD.
    return VerifierCase("STRIPE", secrets.token_hex(16))


def twitter_case() -> VerifierCase:
    # TwitterVerifier (extends SignatureBasedVerifier) reads the header named by
    # WebhookConfig.headerKey. The emitter sends `x-twitter-webhooks-signature`.
    return VerifierCase(
        "TWITTER",
        secrets.token_hex(16),
        header_key="x-twitter-webhooks-signature",
    )


ALL_CASES = {
    "HMAC_BASED": hmac_case,
    "SIGNATURE_BASED": signature_case,
    "HEADER_BASED": header_case,
    "SLACK_BASED": slack_case,
    "STRIPE": stripe_case,
    "TWITTER": twitter_case,
    # SENDGRID requires ECDSA keypair generation; skipped by default.
}


@dataclass
class Result:
    verifier: str
    ok: bool
    detail: str
    duration_s: float = 0.0


def workflow_def(name: str, task_ref: str) -> Dict[str, Any]:
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": task_ref,
                "taskReferenceName": task_ref,
                "type": "WAIT_FOR_WEBHOOK",
                "inputParameters": {"matches": {"$.event": "smoke"}},
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


def webhook_config_payload(
    name: str, workflow_name: str, case: VerifierCase
) -> Dict[str, Any]:
    payload = {
        "name": name,
        "verifier": case.name,
        "headerKey": case.header_key,
        "secretKey": case.header_key,
        "secretValue": case.secret_value,
        "sourcePlatform": "smoke",
        "receiverWorkflowNamesToVersions": {workflow_name: 1},
        "workflowsToStart": {},
        "urlVerified": case.url_verified,
    }
    if case.config_headers:
        payload["headers"] = case.config_headers
    return payload


def register_workflow_def(conductor_url: str, wf: Dict[str, Any]) -> None:
    # PUT /api/metadata/workflow takes a List<WorkflowDef> and upserts.
    # We use a per-run unique name, but PUT is idempotent regardless.
    r = httpx.put(
        conductor_url.rstrip("/") + "/api/metadata/workflow",
        json=[wf],
        timeout=10.0,
    )
    r.raise_for_status()


def register_webhook(
    conductor_url: str, payload: Dict[str, Any]
) -> str:
    r = httpx.post(
        conductor_url.rstrip("/") + "/api/metadata/webhook",
        json=payload,
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


def fire_via_emitter(
    emitter_url: str,
    bearer: Optional[str],
    conductor_url: str,
    webhook_id: str,
    case: VerifierCase,
    body: Dict[str, Any],
) -> Tuple[int, str]:
    headers = {}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    r = httpx.post(
        emitter_url.rstrip("/") + "/fire",
        json={
            "target_url": conductor_url,
            "webhook_id": webhook_id,
            "verifier": case.name,
            "secret": case.secret_value,
            "header_name": case.header_key,
            "header_value": case.header_value,
            "payload": body,
        },
        headers=headers,
        timeout=20.0,
    )
    r.raise_for_status()
    fr = r.json()
    return fr["status_code"], fr["response_body"]


def poll_completion(
    conductor_url: str, workflow_id: str, timeout_s: float
) -> str:
    """Poll until workflow reaches a terminal state or timeout. Returns final status."""
    deadline = time.monotonic() + timeout_s
    last = "UNKNOWN"
    while time.monotonic() < deadline:
        last = get_workflow_status(conductor_url, workflow_id)
        if last in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
            return last
        time.sleep(0.25)
    return f"TIMED_OUT_AFTER_{timeout_s}s ({last})"


def run_one(
    case: VerifierCase,
    conductor_url: str,
    emitter_url: str,
    bearer: Optional[str],
    run_id: str,
) -> Result:
    started = time.monotonic()
    suffix = case.name.lower().replace("_", "-")
    wf_name = f"smoke-{suffix}-{run_id}"
    webhook_name = f"smoke-cfg-{suffix}-{run_id}"

    try:
        register_workflow_def(conductor_url, workflow_def(wf_name, "wait"))
        webhook_id = register_webhook(
            conductor_url, webhook_config_payload(webhook_name, wf_name, case)
        )
        workflow_id = start_workflow(conductor_url, wf_name)

        # Brief settle so the WAIT_FOR_WEBHOOK task is registered in the
        # InMemoryWebhookTaskService before the event arrives. The matcher
        # is recomputed per-event so this is belt-and-suspenders, but
        # the worker only picks up events from the queue.
        time.sleep(0.5)

        body: Dict[str, Any] = {"event": "smoke"}
        if case.name == "STRIPE":
            # StripeVerifier reads event.getApiVersion(); body must be a
            # Stripe-Event-shaped object with api_version (YYYY-MM-DD prefix).
            body = {
                "id": f"evt_{run_id}",
                "object": "event",
                "api_version": "2024-06-20",
                "type": "smoke.fired",
                "data": {"object": {"event": "smoke"}},
                "event": "smoke",
            }
        sc, resp = fire_via_emitter(
            emitter_url, bearer, conductor_url, webhook_id, case, body
        )
        if sc >= 300:
            return Result(
                case.name,
                ok=False,
                detail=f"emitter delivered to conductor but got {sc}: {resp[:200]}",
                duration_s=time.monotonic() - started,
            )

        final = poll_completion(conductor_url, workflow_id, timeout_s=20.0)
        ok = final == "COMPLETED"
        return Result(
            case.name,
            ok=ok,
            detail=f"workflow={workflow_id} final={final}",
            duration_s=time.monotonic() - started,
        )
    except httpx.HTTPStatusError as e:
        return Result(
            case.name,
            ok=False,
            detail=f"{e.request.method} {e.request.url} -> {e.response.status_code}: {e.response.text[:200]}",
            duration_s=time.monotonic() - started,
        )
    except Exception as e:
        return Result(
            case.name,
            ok=False,
            detail=f"{type(e).__name__}: {e}",
            duration_s=time.monotonic() - started,
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conductor-url", default=os.environ.get("CONDUCTOR_URL", "http://localhost:7001"))
    ap.add_argument("--emitter-url", default=os.environ.get("EMITTER_URL", "http://localhost:8765"))
    ap.add_argument("--bearer", default=os.environ.get("EMITTER_TOKEN"),
                    help="Bearer token for webhook-emitter (if it requires auth).")
    ap.add_argument("--verifiers", default=",".join(ALL_CASES.keys()),
                    help="Comma-separated subset to run.")
    args = ap.parse_args()

    selected = [v.strip() for v in args.verifiers.split(",") if v.strip()]
    unknown = [v for v in selected if v not in ALL_CASES]
    if unknown:
        print(f"unknown verifiers: {unknown}; supported: {sorted(ALL_CASES)}", file=sys.stderr)
        return 2

    # Sanity ping both services first.
    try:
        httpx.get(args.conductor_url.rstrip("/") + "/api/admin/config", timeout=3.0).raise_for_status()
    except Exception as e:
        print(f"conductor not reachable at {args.conductor_url}: {e}", file=sys.stderr)
        return 3
    try:
        httpx.get(args.emitter_url.rstrip("/") + "/healthz", timeout=3.0).raise_for_status()
    except Exception as e:
        print(f"webhook-emitter not reachable at {args.emitter_url}: {e}", file=sys.stderr)
        return 3

    run_id = uuid.uuid4().hex[:8]
    print(f"smoke run_id={run_id} conductor={args.conductor_url} emitter={args.emitter_url}")
    print(f"verifiers: {selected}\n")

    results: List[Result] = []
    for name in selected:
        case = ALL_CASES[name]()
        print(f"  → {name} ...", end=" ", flush=True)
        r = run_one(case, args.conductor_url, args.emitter_url, args.bearer, run_id)
        results.append(r)
        print(f"{'PASS' if r.ok else 'FAIL'} ({r.duration_s:.2f}s)")
        if not r.ok:
            print(f"      detail: {r.detail}")

    print("\n" + "=" * 60)
    passed = sum(1 for r in results if r.ok)
    print(f"summary: {passed}/{len(results)} verifiers passed")
    for r in results:
        marker = "PASS" if r.ok else "FAIL"
        print(f"  [{marker}] {r.verifier:<16} {r.duration_s:.2f}s  {r.detail}")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
