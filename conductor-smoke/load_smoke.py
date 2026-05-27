#!/usr/bin/env python3
"""Concurrent load driver for the WAIT_FOR_WEBHOOK pathway.

This is the Phase D deliverable of the WAIT_FOR_WEBHOOK stress plan. Where
``composite_smoke.py`` proves the *functional* surface (FORK_JOIN, INLINE,
SWITCH, DO_WHILE, SUB_WORKFLOW, FORK_JOIN_DYNAMIC, JSON_JQ_TRANSFORM, …) one
workflow at a time, this harness runs N concurrent workflows that each block
on a webhook, fires the matching events back at conductor, and measures:

- throughput (completed workflows / wall-clock seconds, post-warmup)
- end-to-end latency (event_fired → completed): p50 / p95 / p99
- wait-task settle latency (start → event_fired): p95
- lost-event rate (workflows that timed out without reaching COMPLETED)
- terminal-status histogram

Workflow shape is intentionally minimal — ``seed (SET_VARIABLE) → wait
(WAIT_FOR_WEBHOOK) → final (INLINE)`` — so latency signal isolates the wait
pathway. Verifier is fixed at HMAC_BASED (the per-verifier matrix is already
covered by ``smoke.py``).

Exit 0 iff ``failed == 0 AND lost == 0`` (strict, per STRESS_PLAN.md §9).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import httpx

from composite_smoke import (
    VerifierCase,
    hmac_case,
    webhook_config_payload,
)


# ---------------------------------------------------------------------------
# Workflow definition (minimal: seed → wait → final).
# ---------------------------------------------------------------------------


def load_parent_def(name: str) -> Dict[str, Any]:
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "seed",
                "taskReferenceName": "seedRef",
                "type": "SET_VARIABLE",
                "inputParameters": {
                    "idx": "${workflow.input.idx}",
                    "runId": "${workflow.input.runId}",
                },
            },
            {
                "name": "wait",
                "taskReferenceName": "waitRef",
                "type": "WAIT_FOR_WEBHOOK",
                "inputParameters": {
                    "matches": {"$.idx": "${workflow.variables.idx}"},
                },
            },
            {
                "name": "final",
                "taskReferenceName": "finalRef",
                "type": "INLINE",
                "inputParameters": {
                    "evaluatorType": "javascript",
                    "expression": "(function(){return {ok: true};})();",
                    "event": "${waitRef.output}",
                },
            },
        ],
        "inputParameters": ["idx", "runId"],
        "outputParameters": {
            "idx": "${workflow.variables.idx}",
            "event": "${waitRef.output}",
        },
        "schemaVersion": 2,
        "restartable": True,
        "ownerEmail": "load@conductor.local",
        "timeoutPolicy": "ALERT_ONLY",
        "timeoutSeconds": 180,
    }


# ---------------------------------------------------------------------------
# Per-workflow timing record.
# ---------------------------------------------------------------------------


@dataclass
class WorkflowRun:
    idx: int
    workflow_id: Optional[str] = None
    t_start: float = 0.0          # monotonic, set right after start_workflow
    t_event_fired: float = 0.0    # monotonic, set right after /fire returns
    t_completed: float = 0.0      # monotonic, set when poll sees terminal
    status: str = "PENDING"
    error: Optional[str] = None

    @property
    def is_warmup(self) -> bool:
        return self._is_warmup

    _is_warmup: bool = False

    def latency_end_to_end_ms(self) -> Optional[float]:
        if self.status != "COMPLETED" or self.t_completed == 0.0:
            return None
        return (self.t_completed - self.t_event_fired) * 1000.0

    def settle_ms(self) -> Optional[float]:
        if self.t_event_fired == 0.0:
            return None
        return (self.t_event_fired - self.t_start) * 1000.0


# ---------------------------------------------------------------------------
# Async REST helpers (use AsyncClient under shared session for connection reuse).
# ---------------------------------------------------------------------------


async def a_start_workflow(
    client: httpx.AsyncClient, conductor_url: str, name: str, body: Dict[str, Any]
) -> str:
    r = await client.post(
        conductor_url.rstrip("/") + f"/api/workflow/{name}",
        json=body,
        timeout=20.0,
    )
    r.raise_for_status()
    return r.text.strip().strip('"')


async def a_get_workflow(
    client: httpx.AsyncClient, conductor_url: str, workflow_id: str
) -> Dict[str, Any]:
    r = await client.get(
        conductor_url.rstrip("/") + f"/api/workflow/{workflow_id}",
        params={"includeTasks": "false"},
        timeout=15.0,
    )
    r.raise_for_status()
    return r.json()


async def a_fire(
    client: httpx.AsyncClient,
    emitter_url: str,
    bearer: Optional[str],
    conductor_url: str,
    webhook_id: str,
    case: VerifierCase,
    body: Dict[str, Any],
) -> int:
    headers = {}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    r = await client.post(
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
    return r.json()["status_code"]


async def a_poll_until_terminal(
    client: httpx.AsyncClient,
    conductor_url: str,
    workflow_id: str,
    timeout_s: float,
    interval_s: float = 0.25,
) -> Tuple[str, bool]:
    """Return (terminal_status, timed_out_bool)."""
    deadline = time.monotonic() + timeout_s
    last_status = "UNKNOWN"
    while time.monotonic() < deadline:
        wf = await a_get_workflow(client, conductor_url, workflow_id)
        last_status = wf.get("status", "UNKNOWN")
        if last_status in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
            return last_status, False
        await asyncio.sleep(interval_s)
    return last_status, True


# ---------------------------------------------------------------------------
# Worker.
# ---------------------------------------------------------------------------


async def run_one_workflow(
    client: httpx.AsyncClient,
    args: argparse.Namespace,
    case: VerifierCase,
    workflow_name: str,
    webhook_id: str,
    run: WorkflowRun,
) -> WorkflowRun:
    try:
        run.workflow_id = await a_start_workflow(
            client,
            args.conductor_url,
            workflow_name,
            {"idx": run.idx, "runId": args.run_id},
        )
        # Set t_start AFTER start_workflow returns so settle latency reflects
        # ready-to-fire → fired, not start-call-latency + ready-to-fire → fired.
        # The previous placement (before the await) inflated settle_p95 on stacks
        # with slow workflow-start paths (e.g. redis stack that indexes through ES).
        run.t_start = time.monotonic()
        # Settle: let conductor dispatch FORK_JOIN/wait registration before we fire.
        # Random jitter avoids lockstep across workers.
        settle = (50 + random.uniform(0, args.start_jitter_ms)) / 1000.0
        await asyncio.sleep(settle)
        await a_fire(
            client,
            args.emitter_url,
            args.bearer,
            args.conductor_url,
            webhook_id,
            case,
            {"idx": run.idx, "runId": args.run_id},
        )
        run.t_event_fired = time.monotonic()
        status, timed_out = await a_poll_until_terminal(
            client, args.conductor_url, run.workflow_id, timeout_s=90.0
        )
        run.status = status
        if timed_out:
            run.error = "poll timeout"
        else:
            run.t_completed = time.monotonic()
    except Exception as exc:
        run.status = "FAILED"
        run.error = f"{type(exc).__name__}: {exc}"
    return run


# ---------------------------------------------------------------------------
# Metrics math.
# ---------------------------------------------------------------------------


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    values_sorted = sorted(values)
    k = (len(values_sorted) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(values_sorted) - 1)
    if lo == hi:
        return values_sorted[lo]
    return values_sorted[lo] + (values_sorted[hi] - values_sorted[lo]) * (k - lo)


@dataclass
class Report:
    backend_label: str
    concurrency: int
    total: int
    warmup: int
    wall_clock_s: float
    ok_count: int
    failed_count: int
    lost_count: int
    throughput: float
    e2e_p50_ms: float
    e2e_p95_ms: float
    e2e_p99_ms: float
    settle_p95_ms: float
    status_histogram: Dict[str, int] = field(default_factory=dict)


def build_report(
    runs: List[WorkflowRun],
    wall_clock_s: float,
    backend_label: str,
    concurrency: int,
    total: int,
    warmup: int,
) -> Report:
    measured = [r for r in runs if not r._is_warmup]
    e2e_latencies = [r.latency_end_to_end_ms() for r in measured if r.latency_end_to_end_ms() is not None]
    settle_latencies = [r.settle_ms() for r in runs if r.settle_ms() is not None]

    histogram: Dict[str, int] = {}
    for r in runs:
        histogram[r.status] = histogram.get(r.status, 0) + 1

    ok_count = sum(1 for r in runs if r.status == "COMPLETED")
    lost_count = sum(1 for r in runs if r.error == "poll timeout")
    failed_count = sum(1 for r in runs if r.status not in ("COMPLETED",) and r.error != "poll timeout")

    return Report(
        backend_label=backend_label,
        concurrency=concurrency,
        total=total,
        warmup=warmup,
        wall_clock_s=wall_clock_s,
        ok_count=ok_count,
        failed_count=failed_count,
        lost_count=lost_count,
        throughput=(ok_count / wall_clock_s) if wall_clock_s > 0 else 0.0,
        e2e_p50_ms=_percentile(e2e_latencies, 50),
        e2e_p95_ms=_percentile(e2e_latencies, 95),
        e2e_p99_ms=_percentile(e2e_latencies, 99),
        settle_p95_ms=_percentile(settle_latencies, 95),
        status_histogram=histogram,
    )


def print_report(rep: Report) -> None:
    print()
    print("=" * 72)
    print(f" LOAD backend={rep.backend_label} concurrency={rep.concurrency} total={rep.total}")
    print(f"      ok={rep.ok_count} failed={rep.failed_count} lost={rep.lost_count}")
    print(
        f"      throughput={rep.throughput:.2f}/s  "
        f"p50={rep.e2e_p50_ms:.0f}ms  "
        f"p95={rep.e2e_p95_ms:.0f}ms  "
        f"p99={rep.e2e_p99_ms:.0f}ms  "
        f"settle_p95={rep.settle_p95_ms:.0f}ms"
    )
    print()
    print(" Status histogram:")
    for st, n in sorted(rep.status_histogram.items()):
        print(f"   {st}: {n}")
    print()
    # CSV-friendly one-liner (for piping to a report).
    csv = (
        f"CSV,{rep.backend_label},{rep.concurrency},{rep.total},{rep.warmup},"
        f"{rep.ok_count},{rep.failed_count},{rep.lost_count},"
        f"{rep.throughput:.3f},{rep.e2e_p50_ms:.1f},{rep.e2e_p95_ms:.1f},"
        f"{rep.e2e_p99_ms:.1f},{rep.settle_p95_ms:.1f},{rep.wall_clock_s:.2f}"
    )
    print(csv)
    print("=" * 72)


# ---------------------------------------------------------------------------
# Main driver.
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    p.add_argument("--conductor-url", default=os.environ.get("CONDUCTOR_URL", "http://localhost:8000"))
    p.add_argument("--emitter-url", default=os.environ.get("EMITTER_URL", "http://localhost:8765"))
    p.add_argument("--bearer", default=os.environ.get("EMITTER_TOKEN"))
    p.add_argument("--backend-label", default="unknown", help="Used in the report only.")
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--total", type=int, default=100)
    p.add_argument("--warmup", type=int, default=10, help="Workflows to exclude from latency stats.")
    p.add_argument("--start-jitter-ms", type=float, default=50.0,
                   help="Extra random jitter (0..N ms) added to per-workflow settle.")
    p.add_argument("--keep-defs", action="store_true",
                   help="Don't deregister workflow/webhook on exit (for poking via UI).")
    return p.parse_args()


def _deregister_workflow_sync(conductor_url: str, name: str) -> None:
    try:
        httpx.delete(
            conductor_url.rstrip("/") + f"/api/metadata/workflow/{name}/1",
            timeout=10.0,
        )
    except Exception:
        pass


def _deregister_webhook_sync(conductor_url: str, webhook_id: str) -> None:
    try:
        httpx.delete(
            conductor_url.rstrip("/") + f"/api/metadata/webhook/{webhook_id}",
            timeout=10.0,
        )
    except Exception:
        pass


async def amain() -> int:
    args = parse_args()
    args.run_id = uuid.uuid4().hex[:8]

    parent_name = f"load-parent-{args.run_id}"

    # 1) Sanity-ping.
    try:
        sync = httpx.Client(timeout=5.0)
        sync.get(args.conductor_url.rstrip("/") + "/health").raise_for_status()
        sync.get(args.emitter_url.rstrip("/") + "/healthz").raise_for_status()
    except Exception as exc:
        print(f"sanity check failed: {exc}", file=sys.stderr)
        return 2
    finally:
        sync.close()

    case = hmac_case()

    # 2) Register parent workflow def once.
    print(f"load-smoke run_id={args.run_id} backend={args.backend_label} "
          f"concurrency={args.concurrency} total={args.total} warmup={args.warmup}")
    print(f"  conductor={args.conductor_url} emitter={args.emitter_url}")

    parent_def = load_parent_def(parent_name)
    r = httpx.put(
        args.conductor_url.rstrip("/") + "/api/metadata/workflow",
        json=[parent_def],
        timeout=15.0,
    )
    r.raise_for_status()

    # 3) Register webhook config once.
    webhook_payload = webhook_config_payload(
        f"load-webhook-{args.run_id}", parent_name, case
    )
    r = httpx.post(
        args.conductor_url.rstrip("/") + "/api/metadata/webhook",
        json=webhook_payload,
        timeout=10.0,
    )
    r.raise_for_status()
    webhook_id = r.json()["id"]

    runs: List[WorkflowRun] = [WorkflowRun(idx=i) for i in range(args.total)]
    for i in range(min(args.warmup, args.total)):
        runs[i]._is_warmup = True

    sem = asyncio.Semaphore(args.concurrency)
    completed_count = 0
    total = args.total

    async def _worker(run: WorkflowRun, client: httpx.AsyncClient) -> None:
        nonlocal completed_count
        async with sem:
            await run_one_workflow(client, args, case, parent_name, webhook_id, run)
            completed_count += 1
            if completed_count % max(1, total // 10) == 0:
                print(f"  progress: {completed_count}/{total}")

    limits = httpx.Limits(max_connections=args.concurrency * 4, max_keepalive_connections=args.concurrency * 2)
    t_wall_start = time.monotonic()
    try:
        async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
            await asyncio.gather(*(_worker(r, client) for r in runs))
    finally:
        wall_clock = time.monotonic() - t_wall_start
        if not args.keep_defs:
            _deregister_webhook_sync(args.conductor_url, webhook_id)
            _deregister_workflow_sync(args.conductor_url, parent_name)

    report = build_report(
        runs, wall_clock, args.backend_label, args.concurrency, args.total, args.warmup
    )
    print_report(report)

    return 0 if (report.failed_count == 0 and report.lost_count == 0) else 1


def main() -> int:
    try:
        return asyncio.run(amain())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
