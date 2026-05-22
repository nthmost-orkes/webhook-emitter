"""Conductor-specific scenario endpoints — convenience wrappers that bundle
the full register-workflow → register-webhook → start-workflow → fire-event →
poll-completion cycle into a single POST.

Kept in a separate module from `emitter.py` because conductor coupling is
optional: the emitter itself is a generic signed-webhook fire service.
Mounted at `/scenarios/...` if the import succeeds; failure to import is
non-fatal — `emitter.py` falls back to the generic surface.

End users (dashboards, CI jobs, ad-hoc curl) can POST one thing and get
back a yes/no plus phase timings, without writing a multi-step driver
script.
"""

from __future__ import annotations

import base64
import logging
import secrets
import time
import uuid
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from signers import SIGNERS, SigningContext, sign

log = logging.getLogger("webhook-emitter.scenarios")

router = APIRouter(prefix="/scenarios", tags=["scenarios"])


class WaitForWebhookRequest(BaseModel):
    conductor_url: str = Field(..., description="Base URL of the conductor server.")
    verifier: str = Field("HMAC_BASED", description=f"One of: {sorted(SIGNERS)}")
    secret: Optional[str] = Field(
        None,
        description="Verifier secret. HMAC_BASED expects base64-encoded key bytes; "
        "others raw. If omitted, generated fresh per call.",
    )
    matches: Dict[str, Any] = Field(
        default_factory=lambda: {"$.event": "scenario"},
        description="JSONPath → expected value matches for the WAIT_FOR_WEBHOOK task.",
    )
    payload: Optional[Dict[str, Any]] = Field(
        None,
        description="Body of the fired event. If omitted, derived from matches "
        "(top-level fields only).",
    )
    config_headers: Optional[Dict[str, str]] = Field(
        None,
        description="WebhookConfig.headers — required for HEADER_BASED, ignored otherwise.",
    )
    timeout_s: float = Field(20.0, description="How long to wait for COMPLETED.")
    url_verified: bool = Field(
        False,
        description="Set true to pre-mark the webhook as URL-verified — required for SLACK_BASED.",
    )


class WaitForWebhookResponse(BaseModel):
    ok: bool
    workflow_name: str
    workflow_id: Optional[str] = None
    webhook_id: Optional[str] = None
    final_status: str
    delivery_status_code: Optional[int] = None
    duration_s: float
    phases: Dict[str, float]
    detail: Optional[str] = None


def _payload_from_matches(matches: Dict[str, Any]) -> Dict[str, Any]:
    """Derive a body that satisfies top-level $.field matches.

    Nested paths fall through — caller should pass `payload` explicitly for them.
    """
    out: Dict[str, Any] = {}
    for path, value in matches.items():
        if isinstance(value, str) and value.startswith("$"):
            continue  # dynamic value; skip
        if path.startswith("$.") and "." not in path[2:]:
            out[path[2:]] = value
    return out


def _workflow_def(name: str, matches: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "wait",
                "taskReferenceName": "wait",
                "type": "WAIT_FOR_WEBHOOK",
                "inputParameters": {"matches": matches},
            }
        ],
        "inputParameters": [],
        "outputParameters": {},
        "schemaVersion": 2,
        "restartable": True,
        "ownerEmail": "scenario@webhook-emitter",
        "timeoutPolicy": "ALERT_ONLY",
        "timeoutSeconds": 120,
    }


def _default_secret(verifier: str) -> str:
    if verifier == "HMAC_BASED":
        return base64.b64encode(secrets.token_bytes(32)).decode("ascii")
    return secrets.token_hex(16)


def _vendor_header_key(verifier: str) -> str:
    """For schemes that pin a vendor header, return that name; else X-Sig."""
    return {
        "TWITTER": "x-twitter-webhooks-signature",
    }.get(verifier, "X-Sig")


# Lazy import to avoid circular dep with emitter.py at module-load time.
def _require_token():
    from emitter import require_token  # type: ignore

    return require_token


@router.post("/wait-for-webhook", response_model=WaitForWebhookResponse)
def wait_for_webhook(
    req: WaitForWebhookRequest, _=Depends(_require_token()),
) -> WaitForWebhookResponse:
    if req.verifier not in SIGNERS:
        raise HTTPException(400, f"unknown verifier '{req.verifier}'")

    run_id = uuid.uuid4().hex[:8]
    wf_name = f"scenario-{run_id}"
    cfg_name = f"scenario-cfg-{run_id}"
    secret = req.secret or _default_secret(req.verifier)
    header_key = _vendor_header_key(req.verifier)
    payload = req.payload if req.payload is not None else _payload_from_matches(req.matches)
    phases: Dict[str, float] = {}
    t_total = time.monotonic()

    cb = req.conductor_url.rstrip("/")
    workflow_id: Optional[str] = None
    webhook_id: Optional[str] = None
    delivery_status_code: Optional[int] = None
    detail: Optional[str] = None

    try:
        with httpx.Client(timeout=10.0) as client:
            t = time.monotonic()
            r = client.put(cb + "/api/metadata/workflow", json=[_workflow_def(wf_name, req.matches)])
            r.raise_for_status()
            phases["register_wf"] = time.monotonic() - t

            t = time.monotonic()
            webhook_payload = {
                "name": cfg_name,
                "verifier": req.verifier,
                "headerKey": header_key,
                "secretKey": header_key,
                "secretValue": secret,
                "sourcePlatform": "scenario",
                "receiverWorkflowNamesToVersions": {wf_name: 1},
                "workflowsToStart": {},
                "urlVerified": req.url_verified,
            }
            if req.config_headers:
                webhook_payload["headers"] = req.config_headers
            r = client.post(cb + "/api/metadata/webhook", json=webhook_payload)
            r.raise_for_status()
            webhook_id = r.json()["id"]
            phases["register_webhook"] = time.monotonic() - t

            t = time.monotonic()
            r = client.post(cb + f"/api/workflow/{wf_name}", json={})
            r.raise_for_status()
            workflow_id = r.text.strip().strip('"')
            phases["start_workflow"] = time.monotonic() - t

            # Brief settle so the WAIT_FOR_WEBHOOK task is registered.
            time.sleep(0.4)

            # Sign + deliver inline (same logic as POST /fire, but no HTTP hop).
            import json as _json
            body_bytes = _json.dumps(payload, separators=(",", ":"), sort_keys=False).encode("utf-8")
            sig_headers = sign(
                req.verifier,
                SigningContext(
                    secret=secret,
                    body_bytes=body_bytes,
                    header_name=header_key,
                    header_value=req.config_headers.get(header_key, "") if req.config_headers else "",
                ),
            )
            headers = {"Content-Type": "application/json", **sig_headers}
            t = time.monotonic()
            r = client.post(cb + f"/api/webhook/{webhook_id}", content=body_bytes, headers=headers)
            phases["fire"] = time.monotonic() - t
            delivery_status_code = r.status_code
            if r.status_code >= 300:
                detail = f"delivery {r.status_code}: {r.text[:200]}"
                return WaitForWebhookResponse(
                    ok=False, workflow_name=wf_name, workflow_id=workflow_id,
                    webhook_id=webhook_id, final_status="DELIVERY_FAILED",
                    delivery_status_code=delivery_status_code,
                    duration_s=time.monotonic() - t_total, phases=phases, detail=detail,
                )

            t = time.monotonic()
            deadline = t + req.timeout_s
            final_status = "UNKNOWN"
            while time.monotonic() < deadline:
                s = client.get(cb + f"/api/workflow/{workflow_id}", params={"includeTasks": "false"})
                s.raise_for_status()
                final_status = s.json().get("status", "UNKNOWN")
                if final_status in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
                    break
                time.sleep(0.25)
            phases["poll_to_complete"] = time.monotonic() - t

    except httpx.HTTPStatusError as e:
        return WaitForWebhookResponse(
            ok=False, workflow_name=wf_name, workflow_id=workflow_id, webhook_id=webhook_id,
            final_status="ERROR", delivery_status_code=delivery_status_code,
            duration_s=time.monotonic() - t_total, phases=phases,
            detail=f"{e.request.method} {e.request.url} -> {e.response.status_code}: {e.response.text[:200]}",
        )

    return WaitForWebhookResponse(
        ok=final_status == "COMPLETED",
        workflow_name=wf_name,
        workflow_id=workflow_id,
        webhook_id=webhook_id,
        final_status=final_status,
        delivery_status_code=delivery_status_code,
        duration_s=time.monotonic() - t_total,
        phases=phases,
    )
