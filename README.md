# webhook-emitter

A tiny FastAPI service that fires HMAC-signed webhooks at any HTTP target on demand. Built specifically to drive integration testing of [conductor-oss/conductor](https://github.com/conductor-oss/conductor)'s `webhooks-oss` receive path without needing real Stripe / Slack / GitHub accounts.

Supports all seven verifier schemes that conductor recognizes:

| Verifier | Signature scheme |
|---|---|
| `HMAC_BASED` | HMAC-SHA256 of body, base64. Secret must be base64-encoded key bytes. |
| `SIGNATURE_BASED` | `sha256=<hex>` of HMAC-SHA256. GitHub-style. |
| `HEADER_BASED` | Literal header value (no signing). |
| `SLACK_BASED` | Slack `v0:ts:body` HMAC-SHA256, sent in `X-Slack-Signature` + `X-Slack-Request-Timestamp`. |
| `STRIPE` | `t=<ts>,v1=<hex>` HMAC-SHA256(`<ts>.<body>`), in `Stripe-Signature`. |
| `TWITTER` | HMAC-SHA256(body), base64, in `x-twitter-webhooks-signature: sha256=...`. |
| `SENDGRID` | ECDSA-SHA256 over `<ts><body>` with PEM private key. Requires `cryptography`. |

## Run locally

```shell
pip install -e .
webhook-emitter --port 8765
```

## Auth

If `EMITTER_TOKEN` is set, `POST /fire`, `POST /fire-named/{name}`, and `GET /templates` require `Authorization: Bearer <token>`. `/healthz` and `/verifiers` are always open. If `EMITTER_TOKEN` is unset, all endpoints are open (local-dev mode).

```shell
export EMITTER_TOKEN="long-random-string"
webhook-emitter --port 8765

# Calls without the header now 401
curl -X POST http://localhost:8765/fire ...  # 401
curl -X POST http://localhost:8765/fire -H "Authorization: Bearer long-random-string" ...  # OK
```

**Public deployments MUST set `EMITTER_TOKEN`.** The systemd unit sources it from `/etc/webhook-emitter/env`.

Or with templates pre-loaded:

```shell
webhook-emitter --port 8765 --config ./config.example.json
```

## Quick fire

```shell
# Register a webhook on conductor first (returns its id)
WID=$(curl -sS -X POST http://localhost:7001/api/metadata/webhook \
  -H "Content-Type: application/json" \
  -d '{"name":"test","verifier":"HMAC_BASED","headerKey":"X-Sig","secretKey":"X-Sig","secretValue":"c2VrcmV0LWtleQ==","sourcePlatform":"smoke","workflowsToStart":{"wf-smoke":1}}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')

# Fire it
curl -X POST http://localhost:8765/fire \
  -H "Content-Type: application/json" \
  -d "{
    \"target_url\": \"http://localhost:7001\",
    \"webhook_id\": \"$WID\",
    \"verifier\": \"HMAC_BASED\",
    \"secret\": \"c2VrcmV0LWtleQ==\",
    \"header_name\": \"X-Sig\",
    \"payload\": {\"event\": \"fired-via-emitter\"}
  }"
```

## Named templates

Edit `config.example.json`, fill in registered webhook ids, then:

```shell
curl -X POST http://localhost:8765/fire-named/smoke-hook-hmac \
  -H "Content-Type: application/json" \
  -d '{"event": "fired-via-template"}'
```

## OSS conductor verifier quirks (worth knowing)

The conductor verifiers in `webhooks-oss` don't always implement the full real-world scheme. The emitter still sends the full real signature so it stays useful against real providers (and any future tightening of the OSS verifiers).

- **`SlackVerifier`**: only checks for a `challenge` field in the body during URL verification. Once `webhookConfig.urlVerified == true`, it returns empty (no signature check). The emitter still sends Slack-format headers.
- **`HMACVerifier`**: base64-decodes `secretValue` to get the key bytes. So when you POST a config, `secretValue` must already be base64. The emitter's `secret` parameter takes the same base64-encoded form.
- **`SignatureBasedVerifier`** + **`HeaderBasedVerifier`**: abstract bases — there isn't a concrete verifier registered for every variation. Check `WebhookConfig.Verifier` enum values that are actually wired.

## Deploy on loki.local

1. `scp -r ~/projects/git/webhook-emitter loki.local:/opt/webhook-emitter`
2. On loki: `cd /opt/webhook-emitter && python3 -m venv .venv && .venv/bin/pip install -e .`
3. Copy `config.example.json` to `/etc/webhook-emitter/config.json` and fill in real webhook ids.
4. `sudo cp systemd/webhook-emitter.service /etc/systemd/system/`
5. `sudo systemctl daemon-reload && sudo systemctl enable --now webhook-emitter`
6. `curl http://loki.local:8765/healthz` to verify.

## Tests

```shell
pip install -e '.[test]'
pytest
```

Eight signer tests pin the byte-exact output for each scheme. HMAC was cross-checked against `openssl dgst -sha256 -hmac` during the conductor smoke run.
