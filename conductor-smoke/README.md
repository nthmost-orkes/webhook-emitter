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
