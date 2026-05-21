"""Concurrency tests for webhook delivery.

These tests verify that the emitter can fire multiple webhooks in parallel
and that Conductor handles concurrent requests correctly.

Requires a running Conductor server. Set CONDUCTOR_URL environment variable
or these tests will be skipped.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

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


def fire_webhook_sync(
    webhook_id: str,
    payload: Dict[str, Any],
    verifier: str = "HEADER_BASED",
    secret: str = "",
    header_name: str = "X-Token",
    header_value: str = "test-token",
    target_url: str = None,
) -> Tuple[int, float]:
    """Fire a webhook and return (status_code, latency_ms)."""
    target = target_url or CONDUCTOR_URL
    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    
    ctx = SigningContext(
        secret=secret,
        body_bytes=body_bytes,
        header_name=header_name,
        header_value=header_value,
    )
    sig_headers = sign(verifier, ctx)
    headers = {"Content-Type": "application/json", **sig_headers}
    
    url = f"{target}/api/webhook/{webhook_id}"
    
    start = time.perf_counter()
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, content=body_bytes, headers=headers)
    elapsed_ms = (time.perf_counter() - start) * 1000
    
    return resp.status_code, elapsed_ms


# --- Mock-based concurrency tests (no live server needed) ---

def test_parallel_fires_via_emitter_api():
    """Verify the emitter can handle parallel /fire requests."""
    from fastapi.testclient import TestClient
    import emitter as emitter_module
    
    # Track how many times the mock was called
    call_count = 0
    call_times: List[float] = []
    
    class MockResponse:
        status_code = 200
        text = '{"status":"ok"}'
    
    class MockClient:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            call_times.append(time.perf_counter())
            time.sleep(0.01)  # Simulate network latency
            return MockResponse()
    
    # Patch httpx.Client
    original_client = httpx.Client
    httpx.Client = MockClient
    
    try:
        client = TestClient(emitter_module.app)
        
        def fire_one(i: int) -> int:
            resp = client.post("/fire", json={
                "target_url": "http://mock-conductor:7001",
                "webhook_id": f"webhook-{i}",
                "verifier": "HEADER_BASED",
                "header_name": "X-Token",
                "header_value": "test",
                "payload": {"seq": i},
            })
            return resp.status_code
        
        # Fire 20 requests in parallel using threads
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(fire_one, i) for i in range(20)]
            results = [f.result() for f in as_completed(futures)]
        
        # All should succeed
        assert all(r == 200 for r in results)
        assert call_count == 20
        
        # Verify some parallelism occurred (calls overlapped in time)
        if len(call_times) >= 2:
            # Sort times and check gaps - if fully sequential, gaps would be >= 10ms each
            call_times.sort()
            gaps = [call_times[i+1] - call_times[i] for i in range(len(call_times)-1)]
            avg_gap = sum(gaps) / len(gaps)
            # With parallelism, average gap should be much less than the sleep time
            assert avg_gap < 0.01, f"Requests appear sequential, avg gap: {avg_gap*1000:.1f}ms"
    
    finally:
        httpx.Client = original_client


def test_parallel_fires_different_verifiers():
    """Verify parallel requests with different verifier schemes."""
    from fastapi.testclient import TestClient
    import emitter as emitter_module
    
    verifiers_used: List[str] = []
    
    class MockResponse:
        status_code = 200
        text = '{"status":"ok"}'
    
    class MockClient:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, url: str, *args, **kwargs):
            # Extract verifier from URL or headers
            headers = kwargs.get("headers", {})
            if "X-Slack-Signature" in headers:
                verifiers_used.append("SLACK_BASED")
            elif "Stripe-Signature" in headers:
                verifiers_used.append("STRIPE")
            elif "x-twitter-webhooks-signature" in headers:
                verifiers_used.append("TWITTER")
            else:
                verifiers_used.append("OTHER")
            return MockResponse()
    
    original_client = httpx.Client
    httpx.Client = MockClient
    
    try:
        client = TestClient(emitter_module.app)
        
        requests = [
            {"verifier": "SLACK_BASED", "secret": "slack-secret"},
            {"verifier": "STRIPE", "secret": "stripe-secret"},
            {"verifier": "TWITTER", "secret": "twitter-secret"},
            {"verifier": "SLACK_BASED", "secret": "slack-secret-2"},
            {"verifier": "STRIPE", "secret": "stripe-secret-2"},
        ]
        
        def fire_one(req: dict) -> int:
            resp = client.post("/fire", json={
                "target_url": "http://mock-conductor:7001",
                "webhook_id": "test-webhook",
                "verifier": req["verifier"],
                "secret": req["secret"],
                "payload": {"verifier": req["verifier"]},
            })
            return resp.status_code
        
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(fire_one, req) for req in requests]
            results = [f.result() for f in as_completed(futures)]
        
        assert all(r == 200 for r in results)
        assert len(verifiers_used) == 5
        assert verifiers_used.count("SLACK_BASED") == 2
        assert verifiers_used.count("STRIPE") == 2
        assert verifiers_used.count("TWITTER") == 1
    
    finally:
        httpx.Client = original_client


# --- Live server concurrency tests ---

@skip_without_conductor()
class TestLiveConcurrency:
    """Concurrency tests requiring a running Conductor server."""
    
    def test_burst_to_nonexistent_webhooks(self):
        """Fire a burst of requests to non-existent webhooks.
        
        All should return 404, and the server should handle the load gracefully.
        """
        n_requests = 50
        webhook_ids = [f"nonexistent-{i}-{os.urandom(4).hex()}" for i in range(n_requests)]
        
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = [
                pool.submit(
                    fire_webhook_sync,
                    webhook_id=wid,
                    payload={"seq": i},
                )
                for i, wid in enumerate(webhook_ids)
            ]
            results = [f.result() for f in as_completed(futures)]
        
        status_codes = [r[0] for r in results]
        latencies = [r[1] for r in results]
        
        # All should be 404
        assert all(code == 404 for code in status_codes), f"Unexpected codes: {set(status_codes)}"
        
        # Latencies should be reasonable (< 5s each)
        assert all(lat < 5000 for lat in latencies), f"Slow requests: max={max(latencies):.0f}ms"
        
        # Log stats
        avg_lat = sum(latencies) / len(latencies)
        print(f"\nBurst test: {n_requests} requests, avg latency: {avg_lat:.1f}ms, max: {max(latencies):.1f}ms")
    
    def test_sustained_load(self):
        """Fire requests at a sustained rate for several seconds."""
        duration_sec = 3
        target_rps = 10  # requests per second
        
        results: List[Tuple[int, float]] = []
        start_time = time.perf_counter()
        
        def fire_at_rate():
            interval = 1.0 / target_rps
            fired = 0
            while time.perf_counter() - start_time < duration_sec:
                result = fire_webhook_sync(
                    webhook_id=f"load-test-{fired}",
                    payload={"seq": fired},
                )
                results.append(result)
                fired += 1
                
                # Sleep to maintain target rate
                elapsed = time.perf_counter() - start_time
                expected_elapsed = fired * interval
                if expected_elapsed > elapsed:
                    time.sleep(expected_elapsed - elapsed)
        
        fire_at_rate()
        
        # Analyze results
        status_codes = [r[0] for r in results]
        latencies = [r[1] for r in results]
        
        # All should be 404 (non-existent webhooks) - that's fine, we're testing load handling
        error_rate = sum(1 for c in status_codes if c >= 500) / len(status_codes)
        assert error_rate < 0.05, f"Error rate too high: {error_rate*100:.1f}%"
        
        avg_lat = sum(latencies) / len(latencies)
        p99_lat = sorted(latencies)[int(len(latencies) * 0.99)]
        
        print(f"\nSustained load: {len(results)} requests over {duration_sec}s")
        print(f"  Actual RPS: {len(results)/duration_sec:.1f}")
        print(f"  Avg latency: {avg_lat:.1f}ms, P99: {p99_lat:.1f}ms")
        print(f"  Error rate: {error_rate*100:.1f}%")


# --- Async concurrency test ---

@pytest.mark.asyncio
async def test_async_parallel_signing():
    """Verify signature generation can happen in parallel (CPU-bound but independent)."""
    import asyncio
    
    async def sign_one(i: int) -> Dict[str, str]:
        # Run CPU-bound signing in thread pool
        loop = asyncio.get_event_loop()
        ctx = SigningContext(
            secret="test-secret",
            body_bytes=json.dumps({"seq": i}).encode(),
            header_name="X-Sig",
            timestamp=1700000000 + i,
        )
        return await loop.run_in_executor(None, sign, "SLACK_BASED", ctx)
    
    # Sign 100 payloads in parallel
    tasks = [sign_one(i) for i in range(100)]
    results = await asyncio.gather(*tasks)
    
    # All should succeed with unique signatures (different timestamps)
    assert len(results) == 100
    signatures = [r["X-Slack-Signature"] for r in results]
    assert len(set(signatures)) == 100, "Expected unique signatures for different timestamps"
