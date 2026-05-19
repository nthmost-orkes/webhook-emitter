"""Bearer-token auth tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _client_with_token(monkeypatch, token: str | None) -> TestClient:
    if token is None:
        monkeypatch.delenv("EMITTER_TOKEN", raising=False)
    else:
        monkeypatch.setenv("EMITTER_TOKEN", token)
    import importlib

    import emitter as emitter_module

    importlib.reload(emitter_module)
    return TestClient(emitter_module.app)


def test_no_token_set_endpoint_open(monkeypatch):
    c = _client_with_token(monkeypatch, None)
    # /fire with garbage will fail upstream but auth must let it through.
    r = c.post("/fire", json={
        "target_url": "http://127.0.0.1:9", "webhook_id": "x",
        "verifier": "HMAC_BASED", "secret": "AA==", "header_name": "X",
        "payload": {}
    })
    assert r.status_code != 401


def test_token_set_missing_header_401(monkeypatch):
    c = _client_with_token(monkeypatch, "s3cret")
    r = c.post("/fire", json={
        "target_url": "http://127.0.0.1:9", "webhook_id": "x",
        "verifier": "HMAC_BASED", "secret": "AA==", "header_name": "X",
        "payload": {}
    })
    assert r.status_code == 401


def test_token_set_wrong_token_401(monkeypatch):
    c = _client_with_token(monkeypatch, "s3cret")
    r = c.post("/fire", headers={"Authorization": "Bearer wrong"}, json={
        "target_url": "http://127.0.0.1:9", "webhook_id": "x",
        "verifier": "HMAC_BASED", "secret": "AA==", "header_name": "X",
        "payload": {}
    })
    assert r.status_code == 401


def test_token_set_correct_token_passes(monkeypatch):
    c = _client_with_token(monkeypatch, "s3cret")
    r = c.post("/fire", headers={"Authorization": "Bearer s3cret"}, json={
        "target_url": "http://127.0.0.1:9", "webhook_id": "x",
        "verifier": "HMAC_BASED", "secret": "AA==", "header_name": "X",
        "payload": {}
    })
    # Will 502 (upstream refused) but NOT 401 — auth passed.
    assert r.status_code != 401


def test_healthz_always_open(monkeypatch):
    c = _client_with_token(monkeypatch, "s3cret")
    assert c.get("/healthz").status_code == 200


def test_verifiers_always_open(monkeypatch):
    c = _client_with_token(monkeypatch, "s3cret")
    assert c.get("/verifiers").status_code == 200
