# WAIT_FOR_WEBHOOK Stress Plan

Status: **draft** — to be executed by subagents.

## 0. Why this exists

The OSS PR on branch `feat/webhooks-from-orkes-split` in `conductor-oss/conductor` adds
the `WAIT_FOR_WEBHOOK` task type plus per-persistence-backing DAOs + cleanup jobs
(Postgres, MySQL, SQLite, Redis, Cassandra). The single-task `smoke.py` harness already
proves the verifier-chain works end-to-end per backend. What it does **not** prove:

1. `WAIT_FOR_WEBHOOK` behaves correctly when sitting **inside** workflows that exercise
   the broader system-task vocabulary (FORK_JOIN, JOIN, SWITCH, DO_WHILE, INLINE,
   SET_VARIABLE, JSON_JQ_TRANSFORM, HTTP, EVENT, SUB_WORKFLOW, DYNAMIC_FORK).
2. The wait-task pathway tolerates **concurrent load** — many workflows blocked on
   webhooks at the same time, with events arriving interleaved.
3. The full-feature pathway works on **hybrid persistence stacks** (e.g. Postgres for
   metadata + Redis for queues), not just single-backend stacks.

This document is the canonical plan for closing those three gaps. Phases are intended
to be farmed out to focused agents, one phase per agent.

## 1. Glossary

- **Composite workflow**: the multi-task workflow defined in §3 that surrounds a
  `WAIT_FOR_WEBHOOK` task with the full system-task vocabulary.
- **Backend**: a persistence configuration. Single-backend = one storage system handles
  both metadata and queues. Hybrid = different systems for metadata vs queues.
- **Functional pass**: one composite workflow run reaches `COMPLETED` and all subtask
  outputs match expected shape.
- **Load pass**: N concurrent composite workflows complete within tolerance bounds
  (p95 latency, lost-event rate).
- **PR branch**: `feat/webhooks-from-orkes-split` in `conductor-oss/conductor`.

## 2. Scope

In scope:

- One composite workflow def + one sub-workflow def covering all system tasks listed.
- Functional pass against each backend below.
- Load pass against each backend that survives functional.

Out of scope (named explicitly so agents don't expand):

- Chaos / fault injection (kill-broker mid-flight, network partitions, etc.).
- HTTPS/TLS variation — emitter and conductor both run plain HTTP locally.
- `SENDGRID` verifier (still skipped per existing `smoke.py` rationale).
- Verifier coverage beyond `HMAC_BASED` for the composite — the per-verifier matrix is
  already proven by `smoke.py`. Composite stress fixes verifier to `HMAC_BASED` to
  reduce signal-to-noise.

## 3. Composite workflow shape

Workflow name: `composite-stress-{run_id}`. Schema version 2. Owner email
`smoke@conductor.local`. Timeout 180s, `ALERT_ONLY`.

Sub-workflow name: `composite-child-{run_id}`. Same owner/timeout policy.

### 3.1 Task graph

```
  ┌────────────────────────────────────────────────────────────────┐
  │ composite-stress-{run_id}                                      │
  │                                                                │
  │   seed                SET_VARIABLE                             │
  │     └─ seedTs, runId, nonce                                    │
  │                                                                │
  │   transform           JSON_JQ_TRANSFORM                        │
  │     └─ derives matchEvent string from seed                     │
  │                                                                │
  │   branchSwitch        SWITCH (on nonce%2)                      │
  │     case "0":  http_ping  HTTP → emitter /healthz              │
  │     case "1":  inline_ok  INLINE → {pingOk:true}               │
  │     default:    terminate_noop TERMINATE (never taken)         │
  │                                                                │
  │   fork                FORK_JOIN [A, B, C, D]                   │
  │     A:                                                         │
  │       wait            WAIT_FOR_WEBHOOK                         │
  │         matches: {"$.event": "smoke"}                          │
  │     B:                                                         │
  │       counterLoop     DO_WHILE                                 │
  │         loop tasks: [bumpCounter INLINE]                       │
  │         loopCondition: $.counterLoop['iteration'] < 3          │
  │       captureLoop     SET_VARIABLE                             │
  │     C:                                                         │
  │       childCall       SUB_WORKFLOW (composite-child-{run_id})  │
  │     D:                                                         │
  │       dynFork         DYNAMIC_FORK                             │
  │         forkTasks computed from inline: 3× INLINE              │
  │       dynJoin         JOIN                                     │
  │                                                                │
  │   join                JOIN [waitRef, captureLoopRef,           │
  │                              childCallRef, dynJoinRef]         │
  │                                                                │
  │   emitEvt             EVENT                                    │
  │     sink: conductor:composite.done                             │
  │                                                                │
  │   final               INLINE                                   │
  │     aggregates: wait.output, captureLoop.output,               │
  │                  childCall.output, dynJoin.output              │
  └────────────────────────────────────────────────────────────────┘

  ┌────────────────────────────────────────────────────────────────┐
  │ composite-child-{run_id}                                       │
  │                                                                │
  │   childSet           SET_VARIABLE  → echoes parent runId       │
  │   childTransform     JSON_JQ_TRANSFORM                         │
  │   childInline        INLINE  → {childDone:true}                │
  └────────────────────────────────────────────────────────────────┘
```

### 3.2 Coverage matrix (which task category from the user's selection lives where)

| Category | Tasks used |
|---|---|
| Core control flow | `FORK_JOIN`, `JOIN`, `SWITCH`, `DO_WHILE`, `TERMINATE` (parsed in default case, never executed) |
| Data tasks | `INLINE` (×5), `SET_VARIABLE` (×3 incl. child), `JSON_JQ_TRANSFORM` (×2 incl. child) |
| Outbound IO | `HTTP` (one branch of SWITCH), `EVENT` (terminal) |
| Sub-workflow + dynamic | `SUB_WORKFLOW`, `DYNAMIC_FORK` + `JOIN` |
| Wait | `WAIT_FOR_WEBHOOK` (the unit under test) |

Known gap: `TERMINATE` is present in the workflow def but lives in a SWITCH default case
that the runtime never takes. This verifies registration/parsing only; firing TERMINATE
inside a multi-branch workflow would short-circuit the wait branch and defeat the test.
Document this in any output report and call it a follow-up if reviewer asks.

### 3.3 Concrete task definitions (JSON snippets agents can paste)

These are not meant to be copy-and-runnable as a complete workflow def, but they nail
down the shape of each task so agents writing the harness don't have to invent it.

```json
{ "name":"seed", "taskReferenceName":"seedRef", "type":"SET_VARIABLE",
  "inputParameters": { "runId":"${workflow.input.runId}", "nonce":1, "seedTs":"${CPEWF_TIMESTAMP}" } }

{ "name":"transform", "taskReferenceName":"transformRef", "type":"JSON_JQ_TRANSFORM",
  "inputParameters": {
    "queryExpression": ".runId + \"-evt\"",
    "runId":"${seedRef.output.runId}"
  } }

{ "name":"branchSwitch", "taskReferenceName":"switchRef", "type":"SWITCH",
  "evaluatorType":"value-param",
  "expression":"branchKey",
  "inputParameters": { "branchKey":"${seedRef.output.nonce}" },
  "decisionCases": {
    "0": [ { "name":"httpPing", "taskReferenceName":"httpRef", "type":"HTTP",
            "inputParameters": { "http_request": {
               "uri":"${workflow.input.emitterUrl}/healthz",
               "method":"GET", "connectionTimeOut":2000, "readTimeOut":2000 } } } ],
    "1": [ { "name":"inlineOk", "taskReferenceName":"inlineOkRef", "type":"INLINE",
            "inputParameters": { "evaluatorType":"javascript",
               "expression":"function e(){return {pingOk:true};} e();" } } ]
  },
  "defaultCase": [ { "name":"terminateNoop", "taskReferenceName":"terminateRef",
                     "type":"TERMINATE",
                     "inputParameters": { "terminationStatus":"FAILED",
                                          "terminationReason":"unreachable" } } ] }

{ "name":"fork", "taskReferenceName":"forkRef", "type":"FORK_JOIN",
  "forkTasks": [
    [ /* branch A */ { "name":"wait", "taskReferenceName":"waitRef",
                        "type":"WAIT_FOR_WEBHOOK",
                        "inputParameters": { "matches":{"$.event":"smoke"} } } ],
    [ /* branch B */
      { "name":"counterLoop", "taskReferenceName":"loopRef", "type":"DO_WHILE",
        "loopCondition":"$.loopRef['iteration'] < 3",
        "loopOver": [
          { "name":"bumpCounter", "taskReferenceName":"bumpRef", "type":"INLINE",
            "inputParameters": { "evaluatorType":"javascript",
               "expression":"function e(){return {i: $.loopRef.iteration};} e();" } }
        ] },
      { "name":"captureLoop", "taskReferenceName":"captureRef", "type":"SET_VARIABLE",
        "inputParameters": { "loopOutput":"${loopRef.output}" } }
    ],
    [ /* branch C */
      { "name":"childCall", "taskReferenceName":"childRef", "type":"SUB_WORKFLOW",
        "subWorkflowParam": { "name":"composite-child-{run_id}", "version":1 },
        "inputParameters": { "runId":"${seedRef.output.runId}" } }
    ],
    [ /* branch D */
      { "name":"dynFork", "taskReferenceName":"dynForkRef", "type":"DYNAMIC_FORK",
        "dynamicForkTasksParam":"forkTasks",
        "dynamicForkTasksInputParamName":"forkInputs",
        "inputParameters": {
          "forkTasks": [
            { "name":"dyn1", "taskReferenceName":"dyn1Ref", "type":"INLINE",
              "inputParameters":{"evaluatorType":"javascript",
                 "expression":"function e(){return {n:1};} e();"} },
            { "name":"dyn2", "taskReferenceName":"dyn2Ref", "type":"INLINE",
              "inputParameters":{"evaluatorType":"javascript",
                 "expression":"function e(){return {n:2};} e();"} },
            { "name":"dyn3", "taskReferenceName":"dyn3Ref", "type":"INLINE",
              "inputParameters":{"evaluatorType":"javascript",
                 "expression":"function e(){return {n:3};} e();"} }
          ],
          "forkInputs": { "dyn1Ref":{}, "dyn2Ref":{}, "dyn3Ref":{} }
        } },
      { "name":"dynJoin", "taskReferenceName":"dynJoinRef", "type":"JOIN" }
    ]
  ],
  "joinOn":[] }

{ "name":"join", "taskReferenceName":"joinRef", "type":"JOIN",
  "joinOn":["waitRef","captureRef","childRef","dynJoinRef"] }

{ "name":"emitEvt", "taskReferenceName":"emitRef", "type":"EVENT",
  "sink":"conductor:composite.done",
  "inputParameters": { "runId":"${seedRef.output.runId}" } }

{ "name":"final", "taskReferenceName":"finalRef", "type":"INLINE",
  "inputParameters": {
    "evaluatorType":"javascript",
    "expression":"function e(){return {ok:true};} e();",
    "wait":"${waitRef.output}",
    "loop":"${captureRef.output}",
    "child":"${childRef.output}",
    "dyn":"${dynJoinRef.output}"
  } }
```

Agent note: these snippets are **shape-accurate but not guaranteed-runnable**. Conductor's
JSON_JQ_TRANSFORM expects `queryExpression` (verify); INLINE evaluator names may be
`javascript` or `graaljs` depending on server config. The agent writing `composite_smoke.py`
**must** verify each against the running server during iteration (§5.1).

### 3.4 Phase A field-name corrections (authoritative)

Phase A ran the iteration in §3.3 against a live sqlite conductor and recorded these
corrections. Later phases should treat these as the canonical field names, not the
§3.3 snippets:

- **`SET_VARIABLE` outputs go to `workflow.variables`, NOT `task.outputData`.** The
  §3.3 references `${seedRef.output.runId}` and `${seedRef.output.nonce}` resolve to
  null. Correct form: `${workflow.variables.runId}` and `${workflow.variables.nonce}`.
  Source-of-truth: `core/src/main/java/com/netflix/conductor/core/execution/tasks/SetVariable.java`.
- **`DYNAMIC_FORK` is not a valid `type` value.** The actual TaskType is
  **`FORK_JOIN_DYNAMIC`**. Source: `ForkJoinDynamicTaskMapper.java`.
- `JSON_JQ_TRANSFORM.queryExpression` — confirmed correct as documented in §3.3.
- `INLINE evaluatorType: "javascript"` — confirmed correct. INLINE output lands in
  `outputData.result` (the script's return value), not as raw top-level fields.
- `DO_WHILE.loopCondition` is a GraalVM JS expression with input bound as `$`. Both
  `$.loopRef['iteration'] < 3` and `if ($.loopRef['iteration'] < 3) { true; } else { false; }`
  work since the last expression value is the result.
- `TERMINATE` with `terminationStatus: "TERMINATED"` produces workflow `status: TERMINATED`.
  The standalone TERMINATE workflow lives in `composite_smoke.py` alongside the composite.

JDK note: conductor-server-lite gradle build requires **JDK 21**. The wrapper picks
up the system default (often JDK 17 on this Mac), which fails protogen. Set
`JAVA_HOME` to the JDK 21 install (`/opt/homebrew/opt/openjdk@21` on Apple Silicon
homebrew) before running bootRun.

## 4. Harness contracts

Two new Python scripts live in `conductor-smoke/`, mirroring style of existing
`smoke.py`. No new dependencies beyond `httpx` (already in pyproject).

### 4.1 `composite_smoke.py` (functional)

CLI:

```
composite_smoke.py
  --conductor-url   (env CONDUCTOR_URL, default http://localhost:7001)
  --emitter-url     (env EMITTER_URL,   default http://localhost:8765)
  --bearer          (env EMITTER_TOKEN; for emitter auth)
  --run-id          optional override; default = uuid4 hex[:8]
  --keep-defs       don't bother deregistering on exit (useful for poking via UI)
```

Behavior:

1. Sanity-ping `/api/admin/config` on conductor and `/healthz` on emitter.
2. Register `composite-child-{run_id}` first (parent SUB_WORKFLOW references it).
3. Register `composite-stress-{run_id}`.
4. Register webhook config bound to parent workflow, verifier `HMAC_BASED`, fresh
   base64-encoded 32-byte secret. Reuse `hmac_case()` from `smoke.py`.
5. Start parent workflow with input
   `{ "runId": "{run_id}", "emitterUrl": "{emitter_url}" }`.
6. Sleep 1.0s to let FORK_JOIN dispatch and the wait task register with
   `InMemoryWebhookTaskService` (or per-backend equivalent).
7. POST `/fire` to emitter with body `{"event":"smoke","runId":"{run_id}"}`.
8. Poll `/api/workflow/{wf_id}?includeTasks=true` every 250ms until terminal state or
   60s.
9. **Per-task assertions** (this is where composite earns its keep over single-task
   smoke):
   - `seedRef.outputData.runId == "{run_id}"`.
   - `transformRef.outputData.result` ends with `-evt`.
   - Either `httpRef.status == COMPLETED` or `inlineOkRef.status == COMPLETED`, never
     both, never `terminateRef`.
   - `waitRef.status == COMPLETED` and `waitRef.outputData` contains the event payload.
   - `loopRef.outputData.iteration >= 3`.
   - `childRef.status == COMPLETED` and `childRef.outputData.childDone == true`.
   - `dynJoinRef.status == COMPLETED` with 3 joined branches present.
   - Top-level workflow `status == COMPLETED`.
10. Print per-task PASS/FAIL table. Exit 0 if all pass.

### 4.2 `load_smoke.py` (concurrent)

CLI:

```
load_smoke.py
  --conductor-url   ...
  --emitter-url     ...
  --bearer          ...
  --backend-label   string, used only in the report (e.g. "postgres+redis")
  --concurrency     workers firing in parallel (default 10)
  --total           total workflows to run (default 100)
  --warmup          workflows to discard from latency stats (default 10)
  --start-jitter-ms random jitter per start to avoid lockstep (default 50)
```

Behavior:

1. Register one parent workflow def + one child def **once** per run (named
   `composite-load-{run_id}`, `composite-load-child-{run_id}`). Don't re-register
   per iteration — registration is not part of what we're measuring.
2. Register one webhook config bound to the parent workflow.
3. Worker pool of `--concurrency` asyncio tasks. Each:
   - Pulls next workflow index from a shared counter.
   - Starts workflow with input including the index.
   - Sleeps for the per-workflow settle (50–100ms).
   - Fires emitter event with payload `{"event":"smoke","idx":N}`.
   - Polls until terminal (max 90s per workflow).
   - Records `t_start`, `t_event_fired`, `t_completed`, terminal status.
4. After all complete (or terminal), compute and print:
   - Throughput: completed / wall-clock seconds.
   - Latency p50, p95, p99 (event-fired → completed). Excludes warmup.
   - Wait latency p95 (start → event-fired): how long the wait task was idle.
   - Lost-event rate: workflows that timed out without entering COMPLETED.
   - Per-status histogram (COMPLETED / FAILED / TIMED_OUT / etc.).
5. Output a one-line CSV-friendly summary plus a human-readable block, e.g.:

```
LOAD backend=postgres+redis concurrency=10 total=100 ok=100 failed=0
     throughput=8.42/s p50=950ms p95=1.4s p99=2.1s wait_p95=120ms
```

6. Exit 0 if `failed == 0` and `lost == 0`.

### 4.3 `matrix.sh` extensions

Add two new backings: `sqlite`, `postgres+redis`, `mysql+redis`. The `--backing all`
target now iterates: `sqlite, postgres, mysql, redis, cassandra, postgres+redis,
mysql+redis`.

For `sqlite`: no docker. Spawn conductor in-process via
`./gradlew :conductor-server-lite:bootRun` from `$CONDUCTOR_REPO`. Wait for
`http://localhost:7001/health`. Run smoke. Tear down.

Hybrid composes referenced below are authored in §6.

Add a `--harness` flag: `smoke` (existing single-task) or `composite` (new). Defaults to
`smoke` for back-compat. `--harness load --total N --concurrency C` runs `load_smoke.py`
instead and forwards args.

## 5. Phases

Each phase is one farm-out unit. Phases are sequential; later phases assume earlier ones
landed.

### Phase A — Composite workflow harness

> **Model/effort: Opus or Sonnet 4.5 HIGH.** The §3.3 task snippets are shape-accurate
> but not field-name-guaranteed. The iteration loop in step 3 below requires opening
> conductor sources (a large Java codebase) and resisting the urge to retry with
> guesses. Sonnet 4.5 medium reliably falls into a guess-and-retry loop on these:
> `JSON_JQ_TRANSFORM.queryExpression` vs `expression`, INLINE `evaluatorType` of
> `javascript` vs `graaljs` vs `nashorn`, `DYNAMIC_FORK` param wiring, EVENT sink
> validity. Use Opus, or Sonnet at high effort with an explicit "read source before
> retrying" instruction in the prompt.

Deliverable: `conductor-smoke/composite_smoke.py` matching §4.1.

Agent procedure:

1. Bring up sqlite conductor (`./gradlew :conductor-server-lite:bootRun`) and
   webhook-emitter locally.
2. Write the harness. Iterate against the running sqlite server until all per-task
   assertions pass.
3. Common iteration failure modes to expect, with first-line debug suggestions:
   - `JSON_JQ_TRANSFORM` rejects query → check `queryExpression` field name and that
     output is referenced as `${transformRef.output.result}`.
   - `INLINE` rejects expression → server may default to `graaljs`; switch
     `evaluatorType`.
   - `DYNAMIC_FORK` doesn't fan out → `dynamicForkTasksParam` must point at an
     `inputParameters` key whose value is the array.
   - `EVENT` task FAILED with `no sink found` → use `conductor:composite.done` (the
     internal sink) not `sqs://...`.
   - WAIT_FOR_WEBHOOK never fires → confirm event payload has `event:"smoke"` at top
     level (matcher is JSONPath on the parsed body).
4. Commit harness with message `feat(conductor-smoke): composite workflow stress harness`.
5. Update `conductor-smoke/README.md` to reference the new script.

Acceptance: `python composite_smoke.py` against local sqlite-backed conductor exits 0
with all 10 per-task assertions green.

### Phase B — Functional matrix (single backends)

> **Model/effort: Sonnet 4.5 medium for execution; escalate on failure.** Mechanical
> bring-up + harness execution is fine. If any backend fails (step 4), the bug-filing
> step needs more horsepower — a good issue title and a credible root-cause hypothesis
> require reading the new DAO sources for the failing backend. Either escalate that
> sub-task to Opus, or have Sonnet capture the failure dossier per §7 and stop, then
> hand off to a higher-effort agent for the issue write-up.

Deliverable: passing run of `composite_smoke.py` against each of postgres, mysql, redis,
cassandra.

Agent procedure:

1. Extend `matrix.sh` per §4.3 (add `--harness` flag; wire `composite` mode).
2. Run `matrix.sh --backing postgres --harness composite --build`. Capture full output.
3. Repeat for mysql, redis, cassandra. Stop on first failure for triage; do not skip
   ahead.
4. If a backend fails with the composite that passed with single-task smoke, the
   failure is almost certainly in the new wait-task DAO path. Capture: workflow JSON,
   final workflow state from `/api/workflow/{id}?includeTasks=true`, last 200 lines of
   server logs. File as separate issue against `conductor-oss/conductor`, labelled
   `bug, area: server, fix: code, critical` (if it blocks the PR from merging).
5. Commit matrix.sh changes with `feat(conductor-smoke): composite harness in matrix.sh`.

Acceptance: all four single backends green on composite. Per-backend log files saved
in `conductor-smoke/runs/{date}/{backend}-composite.log` (gitignored — add pattern to
`.gitignore` if not already covered).

### Phase C — Hybrid stacks

> **Model/effort: Opus, or Sonnet 4.5 HIGH with explicit research instructions.**
> Authoring the hybrid composes requires looking up the real Spring property names
> conductor uses to select db vs. queue backings — these are NOT guessable, and the
> existing single-backend compose files don't all share the same convention. The plan
> says "do not guess" but Sonnet medium will produce plausible-looking YAML with
> wrong prop names that fail at startup with an unhelpful Spring error.
>
> Specifically the agent must grep `conductor/server-config/src/main/resources/` and
> the existing `docker/server/config/*.properties` files for `conductor.db.type` /
> `conductor.queue.type` (or equivalent) BEFORE writing YAML. Frame the prompt so
> source-reading is step 1, not a fallback.

Deliverable: two new compose files in `conductor/docker/`, plus passing functional runs.

Agent procedure:

1. Author `conductor/docker/docker-compose-postgres-redis.yaml`. Model it on
   `docker-compose-postgres.yaml` but add a redis service and override conductor
   server env to use redis for queues:
   - `CONFIG_PROP=config-postgres-redis.properties` (new properties file you'll
     create alongside the existing ones in `conductor/docker/server/config/`).
   - In the properties file: `conductor.db.type=postgres`,
     `conductor.queue.type=redis_standalone` (verify exact prop names in
     `conductor/server-config` first — do not guess).
   - Redis service: `redis:7-alpine` with `redis-server --appendonly yes`.
   - Healthcheck on redis: `redis-cli ping`.
   - Expose conductor on the same port as the single-backend composes for matrix.sh
     compatibility (`CONDUCTOR_PORT` default 8000).
2. Repeat for `docker-compose-mysql-redis.yaml`.
3. Wire both into `matrix.sh` `COMPOSE_FOR` map.
4. Run `matrix.sh --backing postgres+redis --harness composite --build`. Triage and
   iterate.
5. Repeat for mysql+redis.

Acceptance: both hybrid stacks reach `composite_smoke.py` exit 0. Compose files added
to PR or filed as separate PR against `conductor-oss/conductor` (preferred — they're
generally useful, not test-only).

### Phase D — Load driver

> **Model/effort: Sonnet 4.5 medium.** This is the safest phase for Sonnet — clear
> spec, asyncio worker pool is a standard pattern, metrics math is mechanical. No
> conductor-source-reading required. Default agent fits cleanly.

Deliverable: `conductor-smoke/load_smoke.py` matching §4.2.

Agent procedure:

1. Bring up sqlite conductor + emitter. Write the script. Verify it runs at
   `--concurrency 5 --total 20` without flakes against sqlite.
2. Establish baseline numbers from sqlite — these are the **floor**, not the target.
   Sqlite serializes writes, so throughput will be low.
3. Commit as `feat(conductor-smoke): concurrent load driver for composite workflow`.

Acceptance: clean run at `--concurrency 5 --total 20` against sqlite with `failed=0,
lost=0`. Reported metrics look sane (p50 < p95 < p99, throughput > 0).

### Phase E — Load matrix

> **Model/effort: Sonnet 4.5 medium for execution; Opus for the anomalies write-up.**
> Steps 1 (run each backend, save log) and 2's table are mechanical — Sonnet medium
> is fine. The "anomalies paragraphs" in step 2 are interpretive: if cassandra's p95
> is 4× the median, *why* — cold cache, JVM warmup, queue contention, something
> WAIT_FOR_WEBHOOK-specific? Sonnet medium will write a generic "this backend was
> slower" sentence. Opus is worth the cost for that paragraph since it's what a human
> reviewer will actually read.
>
> Practical split: have Sonnet produce the table + raw-numbers section and stop. A
> second pass (Opus, or you reviewing manually) adds the anomalies analysis.

Deliverable: load_smoke.py runs at `--concurrency 10 --total 100` against every
backend that survived Phase B/C. Per-backend report saved.

Agent procedure:

1. For each backend in [sqlite, postgres, mysql, redis, cassandra, postgres+redis,
   mysql+redis]:
   - Bring up the stack (sqlite via bootRun; others via matrix.sh's bring-up).
   - Run `load_smoke.py --backend-label {backend} --concurrency 10 --total 100`.
   - Save output to `conductor-smoke/runs/{date}/{backend}-load.log`.
   - Tear down.
2. Compose final report `conductor-smoke/runs/{date}/REPORT.md`:
   - Table: backend × (throughput, p50, p95, p99, failed, lost).
   - Anomalies section: any backend where p95 > 3× the median across backends, or
     `lost > 0`, gets a paragraph.
3. Attach the report to the WAIT_FOR_WEBHOOK PR as a comment.

Acceptance: all backends complete the load pass without `lost > 0`. Anomalies
documented and either resolved or filed as follow-up issues.

## 6. Backend matrix reference

| Backend | Bring-up | Compose file | Port | Notes |
|---|---|---|---|---|
| sqlite | `./gradlew :conductor-server-lite:bootRun` from `$CONDUCTOR_REPO` | n/a | 7001 | Serializes writes. Throughput floor. |
| postgres | matrix.sh | `docker-compose-postgres.yaml` | 8000 | Existing, no changes. |
| mysql | matrix.sh | `docker-compose-mysql.yaml` | 8000 | Existing. |
| redis | matrix.sh | `docker-compose.yaml` | 8000 | The plain compose file is redis-backed. |
| cassandra | matrix.sh | `docker-compose-cassandra-es7.yaml` | 8000 | Slow startup; raise `HEALTH_TIMEOUT` to 600 if needed. |
| postgres+redis | matrix.sh | `docker-compose-postgres-redis.yaml` (NEW, Phase C) | 8000 | Postgres = metadata, Redis = queue. |
| mysql+redis | matrix.sh | `docker-compose-mysql-redis.yaml` (NEW, Phase C) | 8000 | Same shape with mysql. |

## 7. Failure-capture template

When a phase fails, the agent owning that phase produces a `FAILURE_{phase}_{backend}.md`
in `conductor-smoke/runs/{date}/` with:

```markdown
## Phase: X, Backend: Y

**Symptom**: one sentence

**Reproduction**:
- Command run
- Run ID (use the one printed by the harness)

**Observed**:
- Workflow final status
- Per-task statuses (from `/api/workflow/{id}?includeTasks=true`, paste outputData)
- Last 100 log lines from conductor-server

**Hypothesis**: ...

**Filed as**: link to issue (or "not yet filed")
```

When in doubt about issue placement, see workspace CLAUDE.md "Filing Issues in
conductor-oss/getting-started" — composite harness regressions in a specific verifier
chain go to `conductor-oss/conductor` (not the getting-started repo) because they're
server-side bugs, not onboarding paper cuts.

## 8. Farm-out summary (one agent per phase)

Default model for this work is **Sonnet 4.5 at medium effort**. Deviations flagged
below and inline at the top of each phase. Use this table to dispatch.

| Phase | Model / effort | Why deviation | Agent prompt outline |
|---|---|---|---|
| A | **Opus** or Sonnet 4.5 HIGH | Conductor-source reading required to nail field names that the JSON snippets don't fully pin down; Sonnet medium falls into guess-and-retry on `JSON_JQ_TRANSFORM`, INLINE evaluator, DYNAMIC_FORK shape, EVENT sink. | "Write composite_smoke.py per §4.1 and §3. Iterate against local sqlite conductor until all 10 per-task assertions pass. When a task type errors on registration or runtime, open the conductor source for that task type FIRST before retrying with a guess." |
| B | Sonnet 4.5 medium (execution) + **Opus** on first failure (triage write-up) | Execution is mechanical. Bug write-up against the PR needs to read new DAO sources to be credible. | "Run composite_smoke.py against each single backend via matrix.sh. On any failure, capture §7 dossier and STOP — do not file the issue yourself." |
| C | **Opus** or Sonnet 4.5 HIGH | Hybrid compose authoring requires looking up real Spring prop names; guessable-looking YAML will fail at startup. | "Step 1 is grepping `conductor/server-config` and `docker/server/config/*.properties` for the db / queue selection props. Only after that is settled, author the YAML." |
| D | Sonnet 4.5 medium | Clear spec, standard asyncio pattern, no source reading. | "Write load_smoke.py per §4.2. Verify baseline against sqlite at concurrency=5 total=20." |
| E (execution) | Sonnet 4.5 medium | Run, save logs, build table. | "Run load_smoke.py against each backend at concurrency=10 total=100. Save per-backend logs. Build the metrics table. STOP before writing anomalies prose." |
| E (anomalies prose) | **Opus**, or human reviewer | Interpretive — Sonnet medium produces generic "this backend was slower" sentences. | "Given the REPORT.md table draft, write the anomalies section: for each backend with p95 > 3× median or any non-zero lost rate, propose a credible mechanism." |

### Quick rule of thumb

- **Read-the-Java-source phases**: A, C, and Phase-B-triage → Opus.
- **Pure-Python harness + execution phases**: D, B-execution, E-execution → Sonnet medium.
- **Interpretive write-up**: E-anomalies → Opus or human.

Each agent should read §0–§4 of this document plus its assigned phase section. Phase
boundaries are deliberately clean so the next agent only needs to know the contract,
not the iteration history.

## 9. Resolved decisions (formerly open questions)

Resolved by the user 2026-05-26 before farm-out:

1. **EVENT sink**: **Drop the EVENT task from the composite.** Document the gap in the
   final report and PR comment. Do not attempt to enable the internal event publisher
   per-backend. Phase A harness should omit `emitEvt` from the task graph and from
   `final`'s aggregation; §3.1 and §3.3 should be read with that subtraction in mind.
2. **TERMINATE coverage**: **Add a second small dedicated workflow** that actually
   fires TERMINATE, run it alongside the composite. Phase A deliverable expands to
   include a `terminate_smoke.py` (or integrate into `composite_smoke.py` as a
   secondary workflow run) that registers a minimal workflow whose path reaches a
   TERMINATE task and verifies the workflow ends in `TERMINATED`/`FAILED` per the
   `terminationStatus` set. Keep it tiny — a single SWITCH-or-INLINE prelude into a
   TERMINATE is enough.
3. **Load tolerances**: **Strict — `lost == 0` is failure.** No tolerance band. If
   cassandra flakes under cold caches, that's a real signal worth surfacing, not a
   threshold to mask.
