# Webhook Testing Strategy

This document describes the current state of webhook integration testing for [conductor-oss/conductor](https://github.com/conductor-oss/conductor) and the roadmap for comprehensive coverage.

## Current State: webhook-emitter

The `webhook-emitter` service is a FastAPI application that fires correctly-signed webhooks at a Conductor server. It supports all seven verifier schemes that Conductor recognizes.

### What It Tests

| Capability | Coverage |
|------------|----------|
| Signature generation for all 7 verifiers | Yes |
| HTTP delivery to Conductor's `/api/webhook/{id}` endpoint | Yes |
| Bearer token authentication | Yes |
| Template-based repeatable configs | Yes |

### What It Does NOT Test

| Gap | Impact |
|-----|--------|
| Workflow completion after webhook delivery | Can't verify the webhook actually unblocked a waiting task |
| Task output population | Can't verify payload was injected into task outputData |
| Invalid signature rejection | Only happy-path tested |
| Missing webhook ID handling | No 404 verification |
| Hash-based task routing | No collision or miss testing |
| Concurrency | Single-threaded, sequential delivery |
| Idempotency / replay protection | No duplicate delivery testing |
| Matcher expression evaluation | Deferred to PR C |

### Architecture

```
┌─────────────────┐         POST /fire          ┌─────────────────┐
│ webhook-emitter │ ──────────────────────────> │    Conductor    │
│  (loki.local)   │    signed webhook payload   │  (target_url)   │
└─────────────────┘                             └─────────────────┘
        │                                               │
        │ returns: status_code, response_body           │
        │          sent_headers                         │
        ▼                                               ▼
   Manual inspection                             ??? (not verified)
```

The emitter reports what HTTP response Conductor returned, but cannot verify what happened inside Conductor (task state changes, workflow progression, event audit records).

## Test Categories Needed

### 1. Happy Path (Current: Partial)

- [x] Valid signature accepted
- [x] HTTP 200 returned
- [ ] Task transitions to COMPLETED
- [ ] Workflow proceeds to next step
- [ ] Payload appears in task outputData
- [ ] IncomingWebhookEvent audit record created

### 2. Negative Path (Current: None)

- [ ] Invalid signature → 401/403
- [ ] Wrong secret → 401/403
- [ ] Malformed signature header → 400
- [ ] Webhook ID doesn't exist → 404
- [ ] Webhook exists but no task waiting → 200 but no completion
- [ ] Malformed payload (not JSON) → 400
- [ ] Timestamp too old (Slack/Stripe schemes) → 401
- [ ] Replay attack (same event ID twice) → idempotent handling

### 3. Hash Routing (Current: None)

- [ ] Two workflows with identical `matches` both receive webhook
- [ ] Same workflow, different versions → only correct version completes
- [ ] Task ref name with iteration suffix (`task__1`) → stripped correctly
- [ ] Empty matches map → valid hash computed
- [ ] Nested Map in matches → documented instability verified

### 4. Concurrency (Current: None)

- [ ] 100 simultaneous webhooks to same endpoint
- [ ] Parallel webhooks to different workflow instances
- [ ] Race condition: webhook arrives before task is registered
- [ ] Race condition: task completes while webhook is in flight

### 5. Verifier-Specific (Current: Signer unit tests only)

- [ ] HMAC_BASED: base64-encoded secret handling
- [ ] SLACK_BASED: URL verification handshake (challenge field)
- [ ] STRIPE: timestamp tolerance window
- [ ] TWITTER: CRC handshake (GET endpoint)
- [ ] SENDGRID: ECDSA signature verification

### 6. Operational (Current: None)

- [ ] Metrics incremented on receive
- [ ] Debug logs emitted with correlation IDs
- [ ] Event retention / cleanup

## Roadmap

### Phase 1: Negative Path Testing

Add tests to `webhook-emitter` that verify Conductor correctly rejects:
- Invalid signatures
- Unknown webhook IDs
- Malformed payloads

These can be tested with the current architecture since rejection is observable via HTTP status codes.

### Phase 2: End-to-End Workflow Verification

Options:

**Option A: Extend webhook-emitter**

Add a verification step that queries Conductor's API after firing:

```python
def fire_and_verify(req: FireRequest, expected_task_status: str) -> VerifyResponse:
    fire_resp = _fire(req)
    # Poll Conductor API for task state
    task = poll_task_status(req.target_url, workflow_id, task_ref)
    assert task.status == expected_task_status
    return VerifyResponse(fire=fire_resp, task=task)
```

Requires: workflow_id and task_ref passed in request, or discovered via search.

**Option B: Java integration tests in conductor test-harness**

```java
@Test
void webhookCompletesWaitingTask() {
    // 1. Register webhook config
    WebhookConfig config = createWebhookConfig("HMAC_BASED", secret);
    webhookDao.createWebhook(config.getId(), config);
    
    // 2. Start workflow with WAIT_FOR_WEBHOOK task
    String workflowId = workflowExecutor.startWorkflow(workflowDef, input);
    
    // 3. Wait for task to be scheduled
    await().until(() -> getTask(workflowId, "wait_ref").getStatus() == IN_PROGRESS);
    
    // 4. Fire webhook via HTTP
    HttpResponse resp = fireWebhook(config.getId(), payload, secret);
    assertEquals(200, resp.statusCode());
    
    // 5. Verify task completed
    await().until(() -> getTask(workflowId, "wait_ref").getStatus() == COMPLETED);
    
    // 6. Verify payload in output
    Task task = getTask(workflowId, "wait_ref");
    assertEquals(payload, task.getOutputData().get("webhookPayload"));
}
```

Requires: Full Conductor server running (Testcontainers or embedded).

**Recommendation:** Both. The emitter stays useful for manual testing and debugging; Java tests provide CI-integrated verification.

### Phase 3: Concurrency Testing

Add to Java test-harness:

```java
@Test
void parallelWebhooksAllComplete() {
    // Start 100 workflow instances
    List<String> workflowIds = IntStream.range(0, 100)
        .mapToObj(i -> startWorkflow())
        .collect(toList());
    
    // Fire 100 webhooks in parallel
    ExecutorService pool = Executors.newFixedThreadPool(20);
    List<Future<HttpResponse>> futures = workflowIds.stream()
        .map(id -> pool.submit(() -> fireWebhook(id)))
        .collect(toList());
    
    // All should succeed
    for (Future<HttpResponse> f : futures) {
        assertEquals(200, f.get().statusCode());
    }
    
    // All tasks should complete
    for (String id : workflowIds) {
        await().until(() -> getTask(id, "wait_ref").getStatus() == COMPLETED);
    }
}
```

### Phase 4: Hash Collision Testing

```java
@Test
void sameHashMultipleTasksBothComplete() {
    // Two workflows, same matches
    String wf1 = startWorkflow(matches);
    String wf2 = startWorkflow(matches);
    
    // Single webhook
    fireWebhook(webhookId, payload);
    
    // Both should complete (same hash bucket)
    await().until(() -> getTask(wf1, "wait_ref").getStatus() == COMPLETED);
    await().until(() -> getTask(wf2, "wait_ref").getStatus() == COMPLETED);
}

@Test
void differentVersionsDifferentHashes() {
    // Workflow v1 and v2, same matches
    String wf1 = startWorkflow(defV1, matches);
    String wf2 = startWorkflow(defV2, matches);
    
    // Webhook configured for v1 only
    fireWebhook(webhookId, payload);  // receiverWorkflowNamesToVersions = {wf: 1}
    
    // Only v1 completes
    await().until(() -> getTask(wf1, "wait_ref").getStatus() == COMPLETED);
    assertEquals(IN_PROGRESS, getTask(wf2, "wait_ref").getStatus());
}
```

## Running Tests

### Current (webhook-emitter unit tests)

```shell
cd webhook-emitter
pip install -e '.[test]'
pytest
```

### Future (Java integration tests)

```shell
cd conductor
./gradlew :conductor-test-harness:test --tests '*Webhook*'
```

## Contributing

When adding new webhook functionality:

1. Add signer test if new signature scheme
2. Add negative test for new error condition
3. Add e2e test for new workflow behavior
4. Update this document with coverage status
