"""Webhook signers — one per verifier scheme.

Each signer returns the headers that need to be added to the outgoing request.
Body bytes are signed in canonical form (UTF-8 of the JSON serialization the
caller provides).

Real-world provider schemes are implemented faithfully; the OSS conductor
verifiers may not exercise every field. See README for OSS-side caveats.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Callable, Dict, Mapping


@dataclass(frozen=True)
class SigningContext:
    secret: str            # provider-shared secret. For HMAC schemes this is the
                           # raw or base64-encoded key per the verifier's expectation.
    body_bytes: bytes
    header_name: str = ""  # which header the signature lands in (for schemes
                           # that don't fix it, like HMAC_BASED / SIGNATURE_BASED /
                           # HEADER_BASED).
    header_value: str = "" # for HEADER_BASED only.
    timestamp: int | None = None  # caller may override; default = now.


def _ts(ctx: SigningContext) -> int:
    return ctx.timestamp if ctx.timestamp is not None else int(time.time())


def hmac_based(ctx: SigningContext) -> Dict[str, str]:
    """OSS `HMACVerifier`: HMAC-SHA256(body, base64-decoded-secret), base64-encoded.

    Header name is configurable (matches `WebhookConfig.headerKey`). Verifier
    strips an optional leading `HMAC ` prefix.
    """
    key = base64.b64decode(ctx.secret)
    mac = hmac.new(key, ctx.body_bytes, hashlib.sha256).digest()
    return {ctx.header_name: base64.b64encode(mac).decode("ascii")}


def signature_based(ctx: SigningContext) -> Dict[str, str]:
    """OSS `SignatureBasedVerifier`: header value = `sha256=<hex>` of HMAC-SHA256(body, secret).

    GitHub/GitLab-style. Secret is the raw key (NOT base64-encoded).
    """
    mac = hmac.new(ctx.secret.encode("utf-8"), ctx.body_bytes, hashlib.sha256).hexdigest()
    return {ctx.header_name: f"sha256={mac}"}


def header_based(ctx: SigningContext) -> Dict[str, str]:
    """OSS `HeaderBasedVerifier`: literal header value match. Lowest security.

    `ctx.header_value` is sent verbatim; secret/body are ignored.
    """
    return {ctx.header_name: ctx.header_value}


def slack_based(ctx: SigningContext) -> Dict[str, str]:
    """Real Slack signing: `v0:{ts}:{body}` → HMAC-SHA256 → `v0=<hex>`.

    Headers:
      - X-Slack-Request-Timestamp: <ts>
      - X-Slack-Signature: v0=<hex>

    Note: the OSS `SlackVerifier` only checks for a `challenge` field in the
    body (URL-handshake mode) — these signature headers are not validated.
    Sent anyway so the emitter works against real Slack-conforming verifiers.
    """
    ts = _ts(ctx)
    basestring = f"v0:{ts}:".encode("utf-8") + ctx.body_bytes
    digest = hmac.new(ctx.secret.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
    return {
        "X-Slack-Request-Timestamp": str(ts),
        "X-Slack-Signature": f"v0={digest}",
    }


def stripe(ctx: SigningContext) -> Dict[str, str]:
    """Real Stripe signing: `t=<ts>,v1=<hex>` where hex = HMAC-SHA256(`<ts>.<body>`, secret).

    Header: Stripe-Signature.
    """
    ts = _ts(ctx)
    basestring = f"{ts}.".encode("utf-8") + ctx.body_bytes
    digest = hmac.new(ctx.secret.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
    return {"Stripe-Signature": f"t={ts},v1={digest}"}


def twitter(ctx: SigningContext) -> Dict[str, str]:
    """Real Twitter Account Activity API signing: HMAC-SHA256(body, secret), base64.

    Header: x-twitter-webhooks-signature: sha256=<base64>.
    Twitter also requires a CRC handshake (separate GET endpoint) — emitter
    skips that since it only fires events, not handshakes.
    """
    mac = hmac.new(ctx.secret.encode("utf-8"), ctx.body_bytes, hashlib.sha256).digest()
    return {"x-twitter-webhooks-signature": "sha256=" + base64.b64encode(mac).decode("ascii")}


def sendgrid(ctx: SigningContext) -> Dict[str, str]:
    """SendGrid Event Webhooks: ECDSA signature over `<ts><body>` with the verification key.

    Headers:
      - X-Twilio-Email-Event-Webhook-Timestamp: <ts>
      - X-Twilio-Email-Event-Webhook-Signature: base64(ECDSA-SHA256)

    Requires `cryptography` extra (not in base deps); raises if attempted
    without it. Secret must be a PEM-encoded ECDSA private key.
    """
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
    except ImportError as e:
        raise RuntimeError(
            "sendgrid signing requires `cryptography`; install with `pip install cryptography`"
        ) from e

    ts = _ts(ctx)
    pkey = serialization.load_pem_private_key(ctx.secret.encode("utf-8"), password=None)
    payload = str(ts).encode("utf-8") + ctx.body_bytes
    signature = pkey.sign(payload, ec.ECDSA(hashes.SHA256()))
    return {
        "X-Twilio-Email-Event-Webhook-Timestamp": str(ts),
        "X-Twilio-Email-Event-Webhook-Signature": base64.b64encode(signature).decode("ascii"),
    }


SIGNERS: Mapping[str, Callable[[SigningContext], Dict[str, str]]] = {
    "HMAC_BASED": hmac_based,
    "SIGNATURE_BASED": signature_based,
    "HEADER_BASED": header_based,
    "SLACK_BASED": slack_based,
    "STRIPE": stripe,
    "TWITTER": twitter,
    "SENDGRID": sendgrid,
}


def sign(verifier: str, ctx: SigningContext) -> Dict[str, str]:
    """Dispatch to the named signer. Raises KeyError for unknown verifier."""
    try:
        return SIGNERS[verifier](ctx)
    except KeyError:
        raise KeyError(
            f"unknown verifier '{verifier}'; supported: {sorted(SIGNERS)}"
        ) from None
