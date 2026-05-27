#!/usr/bin/env python3
"""Composite-workflow stress harness for WAIT_FOR_WEBHOOK.

This harness goes beyond the single-task verifier matrix in ``smoke.py`` by
embedding a WAIT_FOR_WEBHOOK task inside a larger workflow that exercises the
broader system-task vocabulary: FORK_JOIN, JOIN, SWITCH, DO_WHILE, INLINE,
SET_VARIABLE, JSON_JQ_TRANSFORM, HTTP, SUB_WORKFLOW, FORK_JOIN_DYNAMIC and
TERMINATE.

What it proves
--------------
1. WAIT_FOR_WEBHOOK behaves correctly when sitting *inside* a workflow that
   also has every other major system task running in parallel branches and
   nested sub-workflows.
2. The webhook event delivered via webhook-emitter only completes the wait
   branch — the rest of the workflow proceeds independently and the top-level
   workflow only reaches COMPLETED when all branches have joined.
3. A second, tiny TERMINATE-only workflow ends in the expected terminal state
   (TERMINATED), confirming the runtime handles the TERMINATE path the
   composite intentionally avoids.

Two omissions vs. the original plan (see ``STRESS_PLAN.md`` §9):

* **EVENT task dropped.** The internal ``conductor:`` event sink is not
  reliably configured per-backend, so this harness omits ``emitEvt`` from the
  composite graph and from ``finalRef``'s aggregation. EVENT coverage stays
  out of scope until per-backend event-publisher behavior is settled.
* **TERMINATE inside composite stays in a never-taken default branch.** The
  composite still parses (registration-time validation) but never executes a
  TERMINATE. A dedicated ``terminate-smoke-{run_id}`` workflow run by this
  same script covers actual TERMINATE execution.

Run against a local conductor-server-lite + webhook-emitter::

    ./gradlew :conductor-server-lite:bootRun   # in another terminal
    webhook-emitter --port 8765                # in another terminal
    python conductor-smoke/composite_smoke.py
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
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx


# ---------------------------------------------------------------------------
# Verifier case (HMAC-only here; see smoke.py for the full verifier matrix).
# ---------------------------------------------------------------------------


@dataclass
class VerifierCase:
    name: str
    secret_value: str
    header_key: str = "X-Sig"
    header_value: str = ""
    config_headers: Dict[str, str] = field(default_factory=dict)
    url_verified: bool = False


def hmac_case() -> VerifierCase:
    # Mirrors smoke.py::hmac_case(). HMACVerifier base64-decodes the
    # configured secret; emitter expects the same base64-encoded value.
    raw = secrets.token_bytes(32)
    return VerifierCase("HMAC_BASED", base64.b64encode(raw).decode("ascii"))


# ---------------------------------------------------------------------------
# Workflow definitions.
# ---------------------------------------------------------------------------


def composite_child_def(name: str) -> Dict[str, Any]:
    """A small sub-workflow used by the parent's SUB_WORKFLOW branch."""
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "childSet",
                "taskReferenceName": "childSetRef",
                "type": "SET_VARIABLE",
                "inputParameters": {
                    "runId": "${workflow.input.runId}",
                    "tag": "child",
                },
            },
            {
                "name": "childTransform",
                "taskReferenceName": "childTransformRef",
                "type": "JSON_JQ_TRANSFORM",
                "inputParameters": {
                    # SET_VARIABLE writes to workflow.variables, not task.outputData.
                    "runId": "${workflow.variables.runId}",
                    "queryExpression": ".runId + \"-child\"",
                },
            },
            {
                "name": "childInline",
                "taskReferenceName": "childInlineRef",
                "type": "INLINE",
                "inputParameters": {
                    "evaluatorType": "javascript",
                    "expression": "(function(){return {childDone: true};})();",
                },
            },
        ],
        "inputParameters": ["runId"],
        "outputParameters": {
            "childDone": "${childInlineRef.output.result.childDone}",
            "transformed": "${childTransformRef.output.result}",
        },
        "schemaVersion": 2,
        "restartable": True,
        "ownerEmail": "smoke@conductor.local",
        "timeoutPolicy": "ALERT_ONLY",
        "timeoutSeconds": 180,
    }


def composite_parent_def(name: str, child_name: str) -> Dict[str, Any]:
    """The composite-stress parent workflow.

    Graph (EVENT task omitted per STRESS_PLAN §9):

        seed (SET_VARIABLE)
        transform (JSON_JQ_TRANSFORM)
        switchRef (SWITCH on nonce → INLINE branch by default)
        forkRef (FORK_JOIN over four branches)
          A: WAIT_FOR_WEBHOOK
          B: DO_WHILE wrapping an INLINE, then a SET_VARIABLE
          C: SUB_WORKFLOW
          D: FORK_JOIN_DYNAMIC of three INLINE tasks, then JOIN
        joinRef (JOIN waitRef, captureRef, childRef, dynJoinRef)
        finalRef (INLINE aggregating outputs)
    """
    # nonce is fixed to 1 so the SWITCH always takes the INLINE branch.
    # The HTTP/TERMINATE branches still parse and validate at registration.
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "seed",
                "taskReferenceName": "seedRef",
                "type": "SET_VARIABLE",
                "inputParameters": {
                    "runId": "${workflow.input.runId}",
                    "nonce": 1,
                },
            },
            {
                "name": "transform",
                "taskReferenceName": "transformRef",
                "type": "JSON_JQ_TRANSFORM",
                "inputParameters": {
                    # SET_VARIABLE writes to workflow.variables, not task.outputData.
                    "runId": "${workflow.variables.runId}",
                    "queryExpression": ".runId + \"-evt\"",
                },
            },
            {
                "name": "branchSwitch",
                "taskReferenceName": "switchRef",
                "type": "SWITCH",
                "evaluatorType": "value-param",
                "expression": "branchKey",
                "inputParameters": {
                    "branchKey": "${workflow.variables.nonce}",
                },
                "decisionCases": {
                    "0": [
                        {
                            "name": "httpPing",
                            "taskReferenceName": "httpRef",
                            "type": "HTTP",
                            "inputParameters": {
                                "http_request": {
                                    "uri": "${workflow.input.emitterUrl}/healthz",
                                    "method": "GET",
                                    "connectionTimeOut": 2000,
                                    "readTimeOut": 2000,
                                }
                            },
                        }
                    ],
                    "1": [
                        {
                            "name": "inlineOk",
                            "taskReferenceName": "inlineOkRef",
                            "type": "INLINE",
                            "inputParameters": {
                                "evaluatorType": "javascript",
                                "expression": "(function(){return {pingOk: true};})();",
                            },
                        }
                    ],
                },
                "defaultCase": [
                    {
                        "name": "terminateNoop",
                        "taskReferenceName": "terminateNoopRef",
                        "type": "TERMINATE",
                        "inputParameters": {
                            "terminationStatus": "FAILED",
                            "terminationReason": "unreachable default branch",
                        },
                    }
                ],
            },
            {
                "name": "fork",
                "taskReferenceName": "forkRef",
                "type": "FORK_JOIN",
                "forkTasks": [
                    # Branch A — WAIT_FOR_WEBHOOK (the unit under test).
                    [
                        {
                            "name": "wait",
                            "taskReferenceName": "waitRef",
                            "type": "WAIT_FOR_WEBHOOK",
                            "inputParameters": {
                                "matches": {"$.event": "smoke"}
                            },
                        }
                    ],
                    # Branch B — DO_WHILE that iterates 3× then a SET_VARIABLE
                    # captures the loop output for assertions.
                    [
                        {
                            "name": "counterLoop",
                            "taskReferenceName": "loopRef",
                            "type": "DO_WHILE",
                            "inputParameters": {},
                            "loopCondition": "if ($.loopRef['iteration'] < 3) { true; } else { false; }",
                            "loopOver": [
                                {
                                    "name": "bumpCounter",
                                    "taskReferenceName": "bumpRef",
                                    "type": "INLINE",
                                    "inputParameters": {
                                        "evaluatorType": "javascript",
                                        "expression": "(function(){return {i: 1};})();",
                                    },
                                }
                            ],
                        },
                        {
                            "name": "captureLoop",
                            "taskReferenceName": "captureRef",
                            "type": "SET_VARIABLE",
                            "inputParameters": {
                                "loopIterations": "${loopRef.output.iteration}",
                            },
                        },
                    ],
                    # Branch C — SUB_WORKFLOW.
                    [
                        {
                            "name": "childCall",
                            "taskReferenceName": "childRef",
                            "type": "SUB_WORKFLOW",
                            "subWorkflowParam": {
                                "name": child_name,
                                "version": 1,
                            },
                            "inputParameters": {
                                "runId": "${workflow.variables.runId}",
                            },
                        }
                    ],
                    # Branch D — FORK_JOIN_DYNAMIC over three INLINE tasks.
                    [
                        {
                            "name": "dynFork",
                            "taskReferenceName": "dynForkRef",
                            "type": "FORK_JOIN_DYNAMIC",
                            "dynamicForkTasksParam": "forkTasks",
                            "dynamicForkTasksInputParamName": "forkInputs",
                            "inputParameters": {
                                "forkTasks": [
                                    {
                                        "name": "dyn1",
                                        "taskReferenceName": "dyn1Ref",
                                        "type": "INLINE",
                                        "inputParameters": {
                                            "evaluatorType": "javascript",
                                            "expression": "(function(){return {n: 1};})();",
                                        },
                                    },
                                    {
                                        "name": "dyn2",
                                        "taskReferenceName": "dyn2Ref",
                                        "type": "INLINE",
                                        "inputParameters": {
                                            "evaluatorType": "javascript",
                                            "expression": "(function(){return {n: 2};})();",
                                        },
                                    },
                                    {
                                        "name": "dyn3",
                                        "taskReferenceName": "dyn3Ref",
                                        "type": "INLINE",
                                        "inputParameters": {
                                            "evaluatorType": "javascript",
                                            "expression": "(function(){return {n: 3};})();",
                                        },
                                    },
                                ],
                                "forkInputs": {
                                    "dyn1Ref": {},
                                    "dyn2Ref": {},
                                    "dyn3Ref": {},
                                },
                            },
                        },
                        {
                            "name": "dynJoin",
                            "taskReferenceName": "dynJoinRef",
                            "type": "JOIN",
                        },
                    ],
                ],
                "joinOn": [],
            },
            {
                "name": "join",
                "taskReferenceName": "joinRef",
                "type": "JOIN",
                "joinOn": ["waitRef", "captureRef", "childRef", "dynJoinRef"],
            },
            {
                "name": "final",
                "taskReferenceName": "finalRef",
                "type": "INLINE",
                "inputParameters": {
                    "evaluatorType": "javascript",
                    "expression": "(function(){return {ok: true};})();",
                    "wait": "${waitRef.output}",
                    "loop": "${captureRef.output}",
                    "child": "${childRef.output}",
                    "dyn": "${dynJoinRef.output}",
                },
            },
        ],
        "inputParameters": ["runId", "emitterUrl"],
        "outputParameters": {},
        "schemaVersion": 2,
        "restartable": True,
        "ownerEmail": "smoke@conductor.local",
        "timeoutPolicy": "ALERT_ONLY",
        "timeoutSeconds": 180,
    }


def terminate_workflow_def(name: str) -> Dict[str, Any]:
    """A minimal workflow that actually executes a TERMINATE task.

    Single INLINE → TERMINATE with status=TERMINATED. Verifies the runtime
    handles the TERMINATE path the composite intentionally avoids.
    """
    return {
        "name": name,
        "version": 1,
        "tasks": [
            {
                "name": "preTerminate",
                "taskReferenceName": "preRef",
                "type": "INLINE",
                "inputParameters": {
                    "evaluatorType": "javascript",
                    "expression": "(function(){return {about_to_terminate: true};})();",
                },
            },
            {
                "name": "terminateNow",
                "taskReferenceName": "terminateNowRef",
                "type": "TERMINATE",
                "inputParameters": {
                    "terminationStatus": "TERMINATED",
                    "terminationReason": "smoke-terminate-test",
                    "workflowOutput": {"terminatedBy": "smoke"},
                },
            },
        ],
        "inputParameters": [],
        "outputParameters": {},
        "schemaVersion": 2,
        "restartable": True,
        "ownerEmail": "smoke@conductor.local",
        "timeoutPolicy": "ALERT_ONLY",
        "timeoutSeconds": 60,
    }


# ---------------------------------------------------------------------------
# Webhook config helpers — mirror smoke.py.
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Conductor REST helpers — mirror smoke.py.
# ---------------------------------------------------------------------------


def register_workflow_defs(
    conductor_url: str, wfs: List[Dict[str, Any]]
) -> None:
    r = httpx.put(
        conductor_url.rstrip("/") + "/api/metadata/workflow",
        json=wfs,
        timeout=15.0,
    )
    r.raise_for_status()


def register_webhook(conductor_url: str, payload: Dict[str, Any]) -> str:
    r = httpx.post(
        conductor_url.rstrip("/") + "/api/metadata/webhook",
        json=payload,
        timeout=10.0,
    )
    r.raise_for_status()
    return r.json()["id"]


def start_workflow(
    conductor_url: str, name: str, input_data: Dict[str, Any]
) -> str:
    r = httpx.post(
        conductor_url.rstrip("/") + f"/api/workflow/{name}",
        json=input_data,
        timeout=10.0,
    )
    r.raise_for_status()
    return r.text.strip().strip('"')


def get_workflow(
    conductor_url: str, workflow_id: str, include_tasks: bool = True
) -> Dict[str, Any]:
    r = httpx.get(
        conductor_url.rstrip("/") + f"/api/workflow/{workflow_id}",
        params={"includeTasks": "true" if include_tasks else "false"},
        timeout=10.0,
    )
    r.raise_for_status()
    return r.json()


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


def poll_until_terminal(
    conductor_url: str,
    workflow_id: str,
    timeout_s: float,
    interval_s: float = 0.25,
) -> Dict[str, Any]:
    """Return the final workflow JSON when it reaches a terminal state."""
    deadline = time.monotonic() + timeout_s
    last_status = "UNKNOWN"
    last_wf: Dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_wf = get_workflow(conductor_url, workflow_id, include_tasks=True)
        last_status = last_wf.get("status", "UNKNOWN")
        if last_status in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
            return last_wf
        time.sleep(interval_s)
    last_wf["__timeout__"] = True
    last_wf["status"] = last_wf.get("status", last_status)
    return last_wf


def deregister_workflow(conductor_url: str, name: str, version: int = 1) -> None:
    try:
        httpx.delete(
            conductor_url.rstrip("/") + f"/api/metadata/workflow/{name}/{version}",
            timeout=10.0,
        )
    except Exception:
        pass


def deregister_webhook(conductor_url: str, webhook_id: str) -> None:
    try:
        httpx.delete(
            conductor_url.rstrip("/") + f"/api/metadata/webhook/{webhook_id}",
            timeout=10.0,
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Assertions.
# ---------------------------------------------------------------------------


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def _tasks_by_ref(wf: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for t in wf.get("tasks", []):
        ref = t.get("referenceTaskName")
        if ref:
            out[ref] = t
    return out


def _check(name: str, ok: bool, detail: str) -> Check:
    return Check(name=name, ok=ok, detail=detail)


def assert_composite(wf: Dict[str, Any]) -> List[Check]:
    """Run per-task assertions on a terminal composite workflow result."""
    checks: List[Check] = []
    by_ref = _tasks_by_ref(wf)

    # 1. seed task wrote runId into workflow.variables.
    seed = by_ref.get("seedRef")
    expected = (wf.get("input") or {}).get("runId")
    if not seed:
        checks.append(_check("seedRef.runId", False, "seedRef task missing"))
    else:
        # SET_VARIABLE writes to workflow.variables, not task.outputData; the
        # task's inputData echoes the resolved input parameters.
        var_run_id = (wf.get("variables") or {}).get("runId")
        in_run_id = (seed.get("inputData") or {}).get("runId")
        ok = var_run_id == expected == in_run_id and bool(expected)
        checks.append(
            _check(
                "seedRef.runId",
                ok,
                f"variables.runId={var_run_id!r} inputData.runId={in_run_id!r} expected={expected!r}",
            )
        )

    # 2. transformRef result ends with -evt.
    tr = by_ref.get("transformRef")
    if not tr:
        checks.append(_check("transformRef.result", False, "transformRef missing"))
    else:
        result = (tr.get("outputData") or {}).get("result")
        ok = isinstance(result, str) and result.endswith("-evt")
        checks.append(
            _check(
                "transformRef.result.endswith(-evt)",
                ok,
                f"result={result!r}",
            )
        )

    # 3. SWITCH: exactly one of httpRef / inlineOkRef ran; terminateNoop never.
    http_ran = "httpRef" in by_ref
    inline_ok_ran = "inlineOkRef" in by_ref
    terminate_ran = "terminateNoopRef" in by_ref
    one_of_branch = (http_ran ^ inline_ok_ran) and not terminate_ran
    checks.append(
        _check(
            "switch.exactly-one-branch",
            one_of_branch,
            f"http={http_ran} inlineOk={inline_ok_ran} terminate={terminate_ran}",
        )
    )

    # 4. waitRef COMPLETED and event payload present.
    wait = by_ref.get("waitRef")
    if not wait:
        checks.append(_check("waitRef.status", False, "waitRef missing"))
    else:
        status = wait.get("status")
        out = wait.get("outputData") or {}
        # The wait task's output mirrors the event body (or includes an
        # "event" key). Accept either the raw payload or a nested wrapper.
        contains_event = (
            "event" in out
            or any(isinstance(v, dict) and v.get("event") == "smoke" for v in out.values())
        )
        checks.append(
            _check(
                "waitRef.status==COMPLETED",
                status == "COMPLETED",
                f"status={status} outputKeys={list(out.keys())}",
            )
        )
        checks.append(
            _check(
                "waitRef.outputData has event payload",
                contains_event,
                f"output={out}",
            )
        )

    # 5. DO_WHILE iterated >= 3 times.
    loop = by_ref.get("loopRef")
    if not loop:
        checks.append(_check("loopRef.iteration>=3", False, "loopRef missing"))
    else:
        iters = (loop.get("outputData") or {}).get("iteration")
        checks.append(
            _check(
                "loopRef.iteration>=3",
                isinstance(iters, int) and iters >= 3,
                f"iteration={iters}",
            )
        )

    # 6. SUB_WORKFLOW completed with childDone == true.
    child = by_ref.get("childRef")
    if not child:
        checks.append(_check("childRef.childDone", False, "childRef missing"))
    else:
        status = child.get("status")
        out = child.get("outputData") or {}
        # SUB_WORKFLOW outputData is the sub-workflow's outputParameters.
        child_done = out.get("childDone")
        checks.append(
            _check(
                "childRef.status==COMPLETED",
                status == "COMPLETED",
                f"status={status}",
            )
        )
        checks.append(
            _check(
                "childRef.outputData.childDone==True",
                child_done is True,
                f"childDone={child_done!r} out={out}",
            )
        )

    # 7. FORK_JOIN_DYNAMIC: dynJoinRef COMPLETED with 3 joined branches.
    dyn_join = by_ref.get("dynJoinRef")
    if not dyn_join:
        checks.append(_check("dynJoinRef.3-branches", False, "dynJoinRef missing"))
    else:
        status = dyn_join.get("status")
        out = dyn_join.get("outputData") or {}
        # JOIN emits one key per joined ref.
        joined = {k for k in out.keys() if k.startswith("dyn") and k.endswith("Ref")}
        checks.append(
            _check(
                "dynJoinRef.status==COMPLETED",
                status == "COMPLETED",
                f"status={status}",
            )
        )
        checks.append(
            _check(
                "dynJoinRef has 3 joined branches",
                len(joined) >= 3,
                f"joined={sorted(joined)} keys={sorted(out.keys())}",
            )
        )

    # 8. Top-level COMPLETED.
    status = wf.get("status")
    checks.append(
        _check(
            "workflow.status==COMPLETED",
            status == "COMPLETED",
            f"status={status}",
        )
    )

    return checks


def assert_terminate(wf: Dict[str, Any]) -> List[Check]:
    """Assertions for the small TERMINATE-only workflow."""
    checks: List[Check] = []
    by_ref = _tasks_by_ref(wf)

    pre = by_ref.get("preRef")
    checks.append(
        _check(
            "terminate.preRef==COMPLETED",
            bool(pre) and pre.get("status") == "COMPLETED",
            f"status={(pre or {}).get('status')}",
        )
    )
    term = by_ref.get("terminateNowRef")
    checks.append(
        _check(
            "terminate.terminateNowRef==COMPLETED",
            bool(term) and term.get("status") == "COMPLETED",
            f"status={(term or {}).get('status')}",
        )
    )
    status = wf.get("status")
    checks.append(
        _check(
            "terminate.workflow.status==TERMINATED",
            status == "TERMINATED",
            f"status={status}",
        )
    )
    return checks


# ---------------------------------------------------------------------------
# Run loops.
# ---------------------------------------------------------------------------


def run_composite(
    conductor_url: str,
    emitter_url: str,
    bearer: Optional[str],
    run_id: str,
    keep_defs: bool,
) -> Tuple[List[Check], Dict[str, Any]]:
    parent_name = f"composite-stress-{run_id}"
    child_name = f"composite-child-{run_id}"
    webhook_name = f"composite-cfg-{run_id}"
    case = hmac_case()

    # Register child first; parent's SUB_WORKFLOW references it by name+version.
    register_workflow_defs(conductor_url, [composite_child_def(child_name)])
    register_workflow_defs(
        conductor_url, [composite_parent_def(parent_name, child_name)]
    )

    webhook_id = register_webhook(
        conductor_url, webhook_config_payload(webhook_name, parent_name, case)
    )

    workflow_id = ""
    try:
        workflow_id = start_workflow(
            conductor_url,
            parent_name,
            {"runId": run_id, "emitterUrl": emitter_url},
        )
        print(f"  composite workflow_id={workflow_id}")

        # Settle so FORK_JOIN dispatches and waitRef is registered with
        # InMemoryWebhookTaskService before the event arrives.
        time.sleep(1.0)

        sc, resp = fire_via_emitter(
            emitter_url,
            bearer,
            conductor_url,
            webhook_id,
            case,
            {"event": "smoke", "runId": run_id},
        )
        if sc >= 300:
            print(f"  emitter→conductor non-2xx: {sc}: {resp[:200]}")

        wf = poll_until_terminal(conductor_url, workflow_id, timeout_s=60.0)
        checks = assert_composite(wf)
        return checks, wf
    finally:
        if not keep_defs:
            deregister_webhook(conductor_url, webhook_id)
            deregister_workflow(conductor_url, parent_name)
            deregister_workflow(conductor_url, child_name)


def run_terminate(
    conductor_url: str, run_id: str, keep_defs: bool
) -> Tuple[List[Check], Dict[str, Any]]:
    name = f"terminate-smoke-{run_id}"
    register_workflow_defs(conductor_url, [terminate_workflow_def(name)])
    workflow_id = ""
    try:
        workflow_id = start_workflow(conductor_url, name, {})
        print(f"  terminate workflow_id={workflow_id}")
        wf = poll_until_terminal(conductor_url, workflow_id, timeout_s=30.0)
        checks = assert_terminate(wf)
        return checks, wf
    finally:
        if not keep_defs:
            deregister_workflow(conductor_url, name)


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


def _print_checks(label: str, checks: List[Check]) -> int:
    passed = sum(1 for c in checks if c.ok)
    print(f"\n{label}: {passed}/{len(checks)} checks passed")
    width = max((len(c.name) for c in checks), default=10)
    for c in checks:
        marker = "PASS" if c.ok else "FAIL"
        print(f"  [{marker}] {c.name:<{width}}  {c.detail}")
    return passed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--conductor-url",
        default=os.environ.get("CONDUCTOR_URL", "http://localhost:7001"),
    )
    ap.add_argument(
        "--emitter-url",
        default=os.environ.get("EMITTER_URL", "http://localhost:8765"),
    )
    ap.add_argument(
        "--bearer",
        default=os.environ.get("EMITTER_TOKEN"),
        help="Bearer token for webhook-emitter (if it requires auth).",
    )
    ap.add_argument(
        "--run-id",
        default=None,
        help="Override the random run id suffix.",
    )
    ap.add_argument(
        "--keep-defs",
        action="store_true",
        help="Don't deregister workflows/webhooks on exit.",
    )
    args = ap.parse_args()

    # Sanity-ping both services first.
    try:
        httpx.get(
            args.conductor_url.rstrip("/") + "/api/admin/config", timeout=3.0
        ).raise_for_status()
    except Exception as e:
        print(f"conductor not reachable at {args.conductor_url}: {e}", file=sys.stderr)
        return 3
    try:
        httpx.get(
            args.emitter_url.rstrip("/") + "/healthz", timeout=3.0
        ).raise_for_status()
    except Exception as e:
        print(
            f"webhook-emitter not reachable at {args.emitter_url}: {e}",
            file=sys.stderr,
        )
        return 3

    run_id = args.run_id or uuid.uuid4().hex[:8]
    print(
        f"composite-smoke run_id={run_id} conductor={args.conductor_url} "
        f"emitter={args.emitter_url}"
    )

    # 1. Composite workflow.
    print("\n→ composite workflow")
    composite_checks, composite_wf = run_composite(
        args.conductor_url, args.emitter_url, args.bearer, run_id, args.keep_defs
    )
    composite_passed = _print_checks("composite", composite_checks)

    # 2. Standalone TERMINATE workflow.
    print("\n→ terminate workflow")
    term_checks, term_wf = run_terminate(
        args.conductor_url, run_id, args.keep_defs
    )
    term_passed = _print_checks("terminate", term_checks)

    total_checks = composite_checks + term_checks
    total_passed = composite_passed + term_passed
    print("\n" + "=" * 60)
    print(f"summary: {total_passed}/{len(total_checks)} total checks passed")

    if total_passed != len(total_checks):
        # Dump the workflows so failures can be diagnosed without re-running.
        print("\ncomposite workflow status:", composite_wf.get("status"))
        print("terminate workflow status:", term_wf.get("status"))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
