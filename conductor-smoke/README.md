# conductor-smoke

End-to-end smoke harness that proves `webhook-emitter` → `conductor` → `WAIT_FOR_WEBHOOK` completion works across every verifier scheme conductor supports.

## What it proves

For each verifier (`HMAC_BASED`, `SIGNATURE_BASED`, `HEADER_BASED`, `SLACK_BASED`, `STRIPE`, `TWITTER`):

1. Register a workflow def whose single task is `WAIT_FOR_WEBHOOK` with `matches: {"$.event": "smoke"}`.
2. Register a webhook config bound to that workflow with a freshly-generated secret.
3. Start the workflow → conductor's worker enqueues the wait task; it idles.
4. Fire a signed event via `webhook-emitter` carrying `{"event": "smoke"}`.
5. Poll the workflow status until it reaches `COMPLETED` (or fail).

A pass means: conductor's verifier accepted the signature, the worker dequeued the event, the matcher hash agreed with the registration hash, the wait task completed, and the workflow finished.

## Prerequisites

Two services running locally:

```shell
# Terminal 1 — conductor server (this PR's branch)
cd ~/projects/git/conductor-oss/conductor
./gradlew :conductor-server-lite:bootRun

# Terminal 2 — webhook-emitter
webhook-emitter --port 8765
```

## Run

```shell
python conductor-smoke/smoke.py
```

Options:

```shell
--conductor-url   default http://localhost:7001 or $CONDUCTOR_URL
--emitter-url     default http://localhost:8765 or $EMITTER_URL
--bearer          required if EMITTER_TOKEN is set on the emitter
--verifiers       comma-separated subset, e.g. HMAC_BASED,STRIPE
```

Exit code is 0 if every selected verifier reached `COMPLETED`, 1 if any failed.

## Output

```
smoke run_id=a1b2c3d4 conductor=http://localhost:7001 emitter=http://localhost:8765
verifiers: ['HMAC_BASED', 'SIGNATURE_BASED', 'HEADER_BASED', 'SLACK_BASED', 'STRIPE', 'TWITTER']

  → HMAC_BASED ... PASS (1.12s)
  → SIGNATURE_BASED ... PASS (1.04s)
  → HEADER_BASED ... PASS (1.01s)
  → SLACK_BASED ... PASS (1.06s)
  → STRIPE ... PASS (1.08s)
  → TWITTER ... PASS (1.03s)

============================================================
summary: 6/6 verifiers passed
  [PASS] HMAC_BASED       1.12s  workflow=<id> final=COMPLETED
  ...
```

## Notes

- **SENDGRID** is skipped by default — requires generating an ECDSA keypair. Easy to add when needed.
- **SLACK_BASED** uses `urlVerified: true` at registration to bypass the URL-handshake challenge step. The event body still includes a `challenge` field so it's a valid Slack-shaped payload; conductor's `SlackVerifier` short-circuits on `urlVerified` and the event flows to the worker.
- All runs use a fresh `run_id` (random 8-char suffix) for workflow + webhook names so repeated runs against the same server don't collide.
- Workflow def is registered with `overwrite=true` — safe to re-run.

## Why not in conductor-oss CI?

Live smoke against a running server is too flaky and slow for the main test suite (would need a testcontainer + bootstrapped server per run). Per-verifier example-based and property tests live in `conductor-oss/conductor`'s `webhooks-oss/src/test/`. This harness is the manual / on-demand counterpart.

## Composite workflow stress (`composite_smoke.py`)

`composite_smoke.py` extends the single-task `smoke.py` by wrapping a `WAIT_FOR_WEBHOOK` task inside a larger workflow that exercises the broader system-task vocabulary in parallel: `FORK_JOIN`, `JOIN`, `SWITCH`, `DO_WHILE`, `INLINE`, `SET_VARIABLE`, `JSON_JQ_TRANSFORM`, `HTTP`, `SUB_WORKFLOW`, and `FORK_JOIN_DYNAMIC`. The webhook is fired the same way; the harness then verifies every branch finished and the top-level workflow reached `COMPLETED`.

It also runs a second, tiny `TERMINATE`-only workflow to confirm the runtime handles the `TERMINATE` path the composite intentionally avoids (the composite parks `TERMINATE` in a never-taken `SWITCH` default).

Two deliberate omissions vs. the original plan (see `STRESS_PLAN.md` §9):

- **No `EVENT` task.** The internal `conductor:` event sink is not reliably configured per-backend; `composite_smoke.py` drops `emitEvt` from the graph and from `finalRef`'s aggregation.
- **`TERMINATE` inside the composite stays in a never-taken `SWITCH` default.** Actual `TERMINATE` execution is covered by the small standalone workflow.

```shell
python conductor-smoke/composite_smoke.py
```

Options:

```shell
--conductor-url   default http://localhost:7001 or $CONDUCTOR_URL
--emitter-url     default http://localhost:8765 or $EMITTER_URL
--bearer          required if EMITTER_TOKEN is set on the emitter
--run-id          override the random 8-char run id suffix
--keep-defs       don't deregister workflows / webhooks on exit
                  (handy for poking via the UI afterwards)
```

Verifier is fixed to `HMAC_BASED` — the per-verifier matrix is already covered by `smoke.py`. Composite stress fixes the verifier to reduce signal-to-noise.

Exit code is 0 if all per-task assertions pass (11 composite + 3 terminate = 14 total).

Sample passing output:

```
composite-smoke run_id=be7ae5ce conductor=http://localhost:7001 emitter=http://localhost:8765

→ composite workflow
  composite workflow_id=<wf-id>

composite: 11/11 checks passed
  [PASS] seedRef.runId                         ...
  [PASS] transformRef.result.endswith(-evt)    ...
  [PASS] switch.exactly-one-branch             http=False inlineOk=True terminate=False
  [PASS] waitRef.status==COMPLETED             ...
  [PASS] waitRef.outputData has event payload  ...
  [PASS] loopRef.iteration>=3                  iteration=3
  [PASS] childRef.status==COMPLETED            ...
  [PASS] childRef.outputData.childDone==True   ...
  [PASS] dynJoinRef.status==COMPLETED          ...
  [PASS] dynJoinRef has 3 joined branches      ...
  [PASS] workflow.status==COMPLETED            status=COMPLETED

→ terminate workflow
  terminate workflow_id=<wf-id>

terminate: 3/3 checks passed
  [PASS] terminate.preRef==COMPLETED            status=COMPLETED
  [PASS] terminate.terminateNowRef==COMPLETED   status=COMPLETED
  [PASS] terminate.workflow.status==TERMINATED  status=TERMINATED

============================================================
summary: 14/14 total checks passed
```

## Per-backend matrix (`matrix.sh`)

`matrix.sh` wraps `smoke.py` to validate the full webhook flow against each persistence backing in turn. It brings up the corresponding docker-compose stack from a local `conductor` checkout, waits for `/health`, fires the 6-verifier smoke at the exposed port, then tears down.

```shell
# Single backing
matrix.sh --backing postgres

# All supported backings (postgres, mysql, redis, cassandra)
matrix.sh --backing all
```

Configuration via env vars: `CONDUCTOR_REPO` (default `~/projects/git/conductor-oss/conductor`), `EMITTER_URL`, `CONDUCTOR_PORT`, `HEALTH_TIMEOUT`. Pass `--build` to force `docker compose build` (needed after source changes); `--keep-up` to leave the stack running for poking afterwards.

Requires `conductor:server` image built from the branch under test — usually `cd $CONDUCTOR_REPO/docker && docker compose -f docker-compose-postgres.yaml build` once, then `matrix.sh` for the runs.

SQLite isn't in the matrix — it's already covered by in-process testcontainer-free tests in the conductor repo (`conductor-sqlite-persistence:test`).
