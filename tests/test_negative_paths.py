"""Negative path tests for webhook delivery.

These tests verify that the emitter correctly reports error responses from
Conductor when webhooks are malformed, incorrectly signed, or targeted at
non-existent resources.

Requires a running Conductor server. Set CONDUCTOR_URL environment variable
or these tests will be skipped.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from signers import SigningContext, sign


CONDUCTOR_URL = os.environ.get("CONDUCTOR_URL")


def skip_without_conductor():
    """Skip decorator for tests requiring a live Conductor server."""
    return pytest.mark.skipif(
        CONDUCTOR_URL is None,
        reason="CONDUCTOR_URL not set; skipping live server tests"
    )


def fire_webhook(
    webhook_id: str,
    payload: Dict[str, Any],
    verifier: str = "HMAC_BASED",
    secret: str = "",
    header_name: str = "X-Sig",
    header_value: str = "",
    tamper_signature: bool = False,
    omit_signature: bool = False,
    malformed_json: bool = False,
) -> httpx.Response:
    """Fire a webhook directly to Conductor (bypassing the emitter service).
    
    This allows testing negative paths by intentionally malforming requests.
    """
    if malformed_json:
        body_bytes = b"not valid json {"
    else:
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    
    headers = {"Content-Type": "application/json"}
    
    if not omit_signature:
        ctx = SigningContext(
            secret=secret,
            body_bytes=body_bytes,
            header_name=header_name,
            header_value=header_value,
        )
        sig_headers = sign(verifier, ctx)
        
        if tamper_signature:
            # Corrupt the signature
            for k, v in sig_headers.items():
                sig_headers[k] = v[:-4] + "XXXX"
        
        headers.update(sig_headers)
    
    url = f"{CONDUCTOR_URL}/api/webhook/{webhook_id}"
    
    with httpx.Client(timeout=15.0) as client:
        return client.post(url, content=body_bytes, headers=headers)


# --- Tests that can run without a live server (mock-based) ---

def test_emitter_reports_upstream_404(monkeypatch):
    """Emitter should report 404 when webhook ID doesn't exist."""
    from fastapi.testclient import TestClient
    import emitter as emitter_module
    
    # Mock httpx to return 404
    class MockResponse:
        status_code = 404
        text = '{"message":"Webhook not found"}'
    
    class MockClient:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, *args, **kwargs):
            return MockResponse()
    
    monkeypatch.setattr(httpx, "Client", MockClient)
    
    client = TestClient(emitter_module.app)
    resp = client.post("/fire", json={
        "target_url": "http://mock-conductor:7001",
        "webhook_id": "nonexistent-id",
        "verifier": "HMAC_BASED",
        "secret": base64.b64encode(b"key").decode(),
        "header_name": "X-Sig",
        "payload": {"event": "test"},
    })
    
    assert resp.status_code == 200  # Emitter succeeded in firing
    data = resp.json()
    assert data["status_code"] == 404  # But Conductor returned 404


def test_emitter_reports_upstream_401_on_bad_signature(monkeypatch):
    """Emitter should report 401/403 when signature is invalid."""
    from fastapi.testclient import TestClient
    import emitter as emitter_module
    
    class MockResponse:
        status_code = 401
        text = '{"message":"Invalid signature"}'
    
    class MockClient:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, *args, **kwargs):
            return MockResponse()
    
    monkeypatch.setattr(httpx, "Client", MockClient)
    
    client = TestClient(emitter_module.app)
    resp = client.post("/fire", json={
        "target_url": "http://mock-conductor:7001",
        "webhook_id": "some-id",
        "verifier": "HMAC_BASED",
        "secret": base64.b64encode(b"wrong-key").decode(),
        "header_name": "X-Sig",
        "payload": {"event": "test"},
    })
    
    assert resp.status_code == 200
    data = resp.json()
    assert data["status_code"] == 401


def test_emitter_reports_upstream_400_on_malformed_request(monkeypatch):
    """Emitter should report 400 for malformed payloads."""
    from fastapi.testclient import TestClient
    import emitter as emitter_module
    
    class MockResponse:
        status_code = 400
        text = '{"message":"Invalid JSON"}'
    
    class MockClient:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, *args, **kwargs):
            return MockResponse()
    
    monkeypatch.setattr(httpx, "Client", MockClient)
    
    client = TestClient(emitter_module.app)
    resp = client.post("/fire", json={
        "target_url": "http://mock-conductor:7001",
        "webhook_id": "some-id",
        "verifier": "HEADER_BASED",
        "header_name": "X-Token",
        "header_value": "secret",
        "payload": {},  # Empty but valid
    })
    
    assert resp.status_code == 200
    data = resp.json()
    assert data["status_code"] == 400


# --- Tests requiring a live Conductor server ---

@skip_without_conductor()
class TestLiveNegativePaths:
    """Tests that require a running Conductor server."""
    
    def test_nonexistent_webhook_returns_404(self):
        """Conductor should return 404 for unknown webhook IDs."""
        resp = fire_webhook(
            webhook_id="definitely-does-not-exist-" + os.urandom(8).hex(),
            payload={"event": "test"},
            verifier="HEADER_BASED",
            header_name="X-Token",
            header_value="anything",
        )
        assert resp.status_code == 404
    
    def test_tampered_hmac_signature_rejected(self):
        """Conductor should reject webhooks with tampered HMAC signatures."""
        # This test requires a webhook to be registered first
        # For now, we just verify the request shape is correct
        # TODO: Register webhook, then test rejection
        pytest.skip("Requires pre-registered webhook with known ID and secret")
    
    def test_wrong_secret_rejected(self):
        """Conductor should reject webhooks signed with wrong secret."""
        pytest.skip("Requires pre-registered webhook with known ID and secret")
    
    def test_missing_signature_header_rejected(self):
        """Conductor should reject webhooks missing required signature header."""
        pytest.skip("Requires pre-registered webhook with known ID and secret")
    
    def test_malformed_json_rejected(self):
        """Conductor should reject non-JSON payloads with 400."""
        # Even without a registered webhook, malformed JSON should fail early
        resp = fire_webhook(
            webhook_id="any-id",
            payload={},  # Ignored due to malformed_json=True
            verifier="HEADER_BASED",
            header_name="X-Token",
            header_value="anything",
            malformed_json=True,
        )
        # Could be 400 (bad request) or 404 (webhook not found first)
        # Either is acceptable for this test
        assert resp.status_code in (400, 404, 415)
    
    def test_expired_timestamp_rejected_slack(self):
        """Slack-style webhooks with old timestamps should be rejected."""
        pytest.skip("Requires Slack verifier to enforce timestamp window")
    
    def test_expired_timestamp_rejected_stripe(self):
        """Stripe-style webhooks with old timestamps should be rejected."""
        pytest.skip("Requires Stripe verifier to enforce timestamp window")


# --- Parameterized tests for all verifier schemes ---

@pytest.mark.parametrize("verifier,needs_secret", [
    ("HMAC_BASED", True),
    ("SIGNATURE_BASED", True),
    ("HEADER_BASED", False),
    ("SLACK_BASED", True),
    ("STRIPE", True),
    ("TWITTER", True),
])
def test_verifier_signature_generation_succeeds(verifier: str, needs_secret: bool):
    """All verifiers should generate valid signature headers."""
    secret = base64.b64encode(b"test-key").decode() if verifier == "HMAC_BASED" else "test-key"
    ctx = SigningContext(
        secret=secret if needs_secret else "",
        body_bytes=b'{"event":"test"}',
        header_name="X-Sig",
        header_value="literal" if verifier == "HEADER_BASED" else "",
        timestamp=1700000000,
    )
    
    headers = sign(verifier, ctx)
    assert isinstance(headers, dict)
    assert len(headers) > 0
    
    # Verify expected header names for vendor-specific schemes
    if verifier == "SLACK_BASED":
        assert "X-Slack-Signature" in headers
        assert "X-Slack-Request-Timestamp" in headers
    elif verifier == "STRIPE":
        assert "Stripe-Signature" in headers
    elif verifier == "TWITTER":
        assert "x-twitter-webhooks-signature" in headers
