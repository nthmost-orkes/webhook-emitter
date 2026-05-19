"""Signer correctness tests.

Each test pins the byte-exact header output for a fixed input so we know the
emitter's signature format won't drift. The HMAC_BASED test was cross-checked
against `openssl dgst -sha256 -hmac` during the conductor smoke run.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from signers import SigningContext, sign


BODY = b'{"event":"smoke-fired"}'


def test_hmac_based_matches_openssl_reference():
    # secret = base64("sekret-key")
    ctx = SigningContext(
        secret=base64.b64encode(b"sekret-key").decode(),
        body_bytes=BODY,
        header_name="X-Sig",
    )
    headers = sign("HMAC_BASED", ctx)
    expected = base64.b64encode(
        hmac.new(b"sekret-key", BODY, hashlib.sha256).digest()
    ).decode()
    assert headers == {"X-Sig": expected}


def test_signature_based_hex_with_sha256_prefix():
    ctx = SigningContext(
        secret="raw-secret",
        body_bytes=BODY,
        header_name="X-Hub-Signature-256",
    )
    headers = sign("SIGNATURE_BASED", ctx)
    expected_hex = hmac.new(b"raw-secret", BODY, hashlib.sha256).hexdigest()
    assert headers == {"X-Hub-Signature-256": f"sha256={expected_hex}"}


def test_header_based_passes_literal_value():
    ctx = SigningContext(
        secret="unused",
        body_bytes=BODY,
        header_name="X-Magic",
        header_value="open-sesame",
    )
    assert sign("HEADER_BASED", ctx) == {"X-Magic": "open-sesame"}


def test_slack_based_v0_format_with_timestamp():
    fixed_ts = 1700000000
    ctx = SigningContext(secret="slack-secret", body_bytes=BODY, timestamp=fixed_ts)
    headers = sign("SLACK_BASED", ctx)
    expected_basestring = f"v0:{fixed_ts}:".encode() + BODY
    expected_hex = hmac.new(b"slack-secret", expected_basestring, hashlib.sha256).hexdigest()
    assert headers == {
        "X-Slack-Request-Timestamp": str(fixed_ts),
        "X-Slack-Signature": f"v0={expected_hex}",
    }


def test_stripe_signature_format():
    fixed_ts = 1700000000
    ctx = SigningContext(secret="stripe-secret", body_bytes=BODY, timestamp=fixed_ts)
    headers = sign("STRIPE", ctx)
    expected_hex = hmac.new(
        b"stripe-secret", f"{fixed_ts}.".encode() + BODY, hashlib.sha256
    ).hexdigest()
    assert headers == {"Stripe-Signature": f"t={fixed_ts},v1={expected_hex}"}


def test_twitter_sha256_base64_in_named_header():
    ctx = SigningContext(secret="twitter-secret", body_bytes=BODY)
    headers = sign("TWITTER", ctx)
    expected_b64 = base64.b64encode(
        hmac.new(b"twitter-secret", BODY, hashlib.sha256).digest()
    ).decode()
    assert headers == {"x-twitter-webhooks-signature": f"sha256={expected_b64}"}


def test_sendgrid_signature_present_when_cryptography_installed():
    try:
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives import serialization
    except ImportError:
        pytest.skip("cryptography not installed")

    key = ec.generate_private_key(ec.SECP256R1())
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    fixed_ts = 1700000000
    ctx = SigningContext(secret=pem, body_bytes=BODY, timestamp=fixed_ts)
    headers = sign("SENDGRID", ctx)

    assert headers["X-Twilio-Email-Event-Webhook-Timestamp"] == str(fixed_ts)
    assert headers["X-Twilio-Email-Event-Webhook-Signature"]
    # ECDSA signatures are non-deterministic; presence + decodability is the contract.
    base64.b64decode(headers["X-Twilio-Email-Event-Webhook-Signature"])


def test_unknown_verifier_raises():
    with pytest.raises(KeyError):
        sign("NOPE", SigningContext(secret="", body_bytes=BODY))
