"""FastAPI service that fires signed webhooks at any configured target.

Endpoints
---------

GET  /healthz                       liveness probe
GET  /verifiers                     list supported verifier names
POST /fire                          one-shot: caller provides target + verifier + secret + payload
POST /fire-named/{name}             use a preconfigured template, body is the payload
GET  /templates                     list configured templates (secrets redacted)

Templates are loaded from a JSON file pointed to by `--config`. Optional.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from signers import SIGNERS, SigningContext, sign

log = logging.getLogger("webhook-emitter")


class FireRequest(BaseModel):
    target_url: str = Field(..., description="Conductor base URL, e.g. http://localhost:7001")
    webhook_id: str = Field(..., description="Webhook id registered via POST /api/metadata/webhook")
    verifier: str = Field(..., description="One of HMAC_BASED, SIGNATURE_BASED, HEADER_BASED, SLACK_BASED, STRIPE, TWITTER, SENDGRID")
    secret: str = Field("", description="Verifier-specific secret. HMAC_BASED expects base64-encoded key bytes; others raw or PEM (see signers.py).")
    header_name: str = Field("X-Sig", description="Header to carry the signature (for HMAC_BASED / SIGNATURE_BASED / HEADER_BASED). Fixed for vendor schemes.")
    header_value: str = Field("", description="Literal value (HEADER_BASED only).")
    payload: Dict[str, Any] = Field(default_factory=dict, description="JSON object delivered as the event body.")
    timestamp: Optional[int] = Field(None, description="Unix epoch seconds; defaults to now. Used by SLACK_BASED/STRIPE/SENDGRID.")


class FireResponse(BaseModel):
    target_url: str
    status_code: int
    response_body: str
    sent_headers: Dict[str, str]


class Template(BaseModel):
    """A preconfigured webhook delivery. Payload may be provided at call time."""
    target_url: str
    webhook_id: str
    verifier: str
    secret: str = ""
    header_name: str = "X-Sig"
    header_value: str = ""
    default_payload: Dict[str, Any] = Field(default_factory=dict)


_TEMPLATES: Dict[str, Template] = {}


def _load_templates(path: Optional[Path]) -> None:
    if path is None:
        return
    if not path.exists():
        log.warning("config file %s does not exist; no templates loaded", path)
        return
    data = json.loads(path.read_text())
    for name, cfg in data.items():
        _TEMPLATES[name] = Template(**cfg)
    log.info("loaded %d templates: %s", len(_TEMPLATES), sorted(_TEMPLATES))


def _redact(t: Template) -> Dict[str, Any]:
    d = t.model_dump()
    if d.get("secret"):
        d["secret"] = "***"
    return d


def _fire(req: FireRequest) -> FireResponse:
    body_bytes = json.dumps(req.payload, separators=(",", ":"), sort_keys=False).encode("utf-8")
    try:
        sig_headers = sign(
            req.verifier,
            SigningContext(
                secret=req.secret,
                body_bytes=body_bytes,
                header_name=req.header_name,
                header_value=req.header_value,
                timestamp=req.timestamp,
            ),
        )
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"signing failed: {e}")

    headers = {"Content-Type": "application/json", **sig_headers}
    url = req.target_url.rstrip("/") + "/api/webhook/" + req.webhook_id

    log.info("firing webhook to %s verifier=%s headers=%s", url, req.verifier, list(sig_headers))
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(url, content=body_bytes, headers=headers)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"upstream request failed: {e}")

    return FireResponse(
        target_url=url,
        status_code=resp.status_code,
        response_body=resp.text,
        sent_headers=headers,
    )


app = FastAPI(title="webhook-emitter", version="0.1.0")


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/verifiers")
def verifiers() -> List[str]:
    return sorted(SIGNERS)


@app.get("/templates")
def list_templates() -> Dict[str, Dict[str, Any]]:
    return {name: _redact(t) for name, t in _TEMPLATES.items()}


@app.post("/fire", response_model=FireResponse)
def fire(req: FireRequest) -> FireResponse:
    return _fire(req)


@app.post("/fire-named/{name}", response_model=FireResponse)
def fire_named(name: str, payload: Dict[str, Any]) -> FireResponse:
    template = _TEMPLATES.get(name)
    if template is None:
        raise HTTPException(status_code=404, detail=f"no template '{name}' configured")
    body = {**template.default_payload, **payload}
    req = FireRequest(
        target_url=template.target_url,
        webhook_id=template.webhook_id,
        verifier=template.verifier,
        secret=template.secret,
        header_name=template.header_name,
        header_value=template.header_value,
        payload=body,
    )
    return _fire(req)


def main() -> None:
    parser = argparse.ArgumentParser(prog="webhook-emitter")
    parser.add_argument("--host", default=os.environ.get("EMITTER_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("EMITTER_PORT", "8765")))
    parser.add_argument("--config", type=Path, default=os.environ.get("EMITTER_CONFIG"))
    parser.add_argument("--log-level", default=os.environ.get("EMITTER_LOG_LEVEL", "info"))
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _load_templates(args.config)

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
