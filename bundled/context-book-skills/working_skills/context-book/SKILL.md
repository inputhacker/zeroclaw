---
name: context-book
description: Context Book integration skill for external agents, sidecars, and adapters. Use when a runtime needs bootstrap/connect/init/complete, lifecycle activation, subscriptions, REST reads/writes, SSE resume, polling fallback, and deterministic event handling against the current public Context Book contract.
---

# Context Book Agent Integration

Use this skill when integrating an external agent runtime, sidecar, or adapter with Context Book after the base URL is already known.

If the Context Book base URL is not known yet, resolve it first with the sibling `context-book-discovery` skill. Keep discovery logic separate from this integration skill.

This is a working helper skill. It is suitable for bootstrap, activation, deterministic REST reads, and event readback assistance, but it is not itself a long-running SSE runtime.

## Read Order
- Smallest safe handoff: `../../../docs/CONTEXT_BOOK_EXTERNAL_AGENT_SINGLE_FILE_GUIDE.md`
- Full client behavior: `../../../docs/CONTEXT_BOOK_CLIENT_SPEC_AND_HOWTO.md`
- Skill-local endpoint summary: `references/api-contract.md`
- Skill-local event summary: `references/event-model.md`

## Deterministic Script Entry Point
Use:
- `scripts/context_book_skill.py`

This helper is for runtime adapters. It can obtain a bearer token, activate the agent, and perform deterministic readbacks for common event paths.

Supported commands:
- `handle-event`
- `get-context`
- `get-vote`
- `list-contexts`
- `list-votes`
- `list-agents`

Example (event-driven):
```bash
echo '{"eventType":"context.updated","entityId":"android_mobile_ctx_1"}' \
| python3 scripts/context_book_skill.py \
  --base-url http://localhost:8080 \
  --agent-id nano_agent \
  handle-event
```

Helper scope:
- bootstrap path uses `POST /agents/connect` first, then `POST /bootstrap/register/init`, request-scoped status polling, and `POST /bootstrap/register/complete`
- bootstrap wait strategy in the bundled helper is status polling, not bootstrap watch SSE
- runtime SSE consumption, cursor persistence, and subscription policy remain the embedding runtime's responsibility
- if `--token` is omitted, `CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET` must be present locally

## Required Runtime Sequence
1. Try `POST /agents/connect` for an existing identity using `X-Context-Book-Bootstrap-Secret`.
2. If `connect` returns `403 BOOTSTRAP_APPROVAL_REQUIRED` for an existing identity, continue with that request's `status/watch -> complete` flow rather than calling `register` again.
3. If connect returns `404 AGENT_NOT_REGISTERED`, prefer `POST /bootstrap/register/init`.
4. If bootstrap approval is pending, wait with either `GET /bootstrap/requests/{requestId}` or `GET /bootstrap/watch/{requestId}` using `X-Context-Book-Bootstrap-Wait-Token`.
5. Finish approved bootstrap with `POST /bootstrap/register/complete`.
6. Store `accessToken` and `refreshToken` securely.
7. Set lifecycle with `PATCH /agents/{agentId}/status` to `Active` before expecting normal privileged runtime behavior.
8. Read and update `GET /subscriptions` / `PUT /subscriptions` separately from lifecycle and connection state.
9. Open SSE stream with `GET /events/stream?agentId={agentId}` using bearer auth.
10. Persist the last processed durable `eventId`.
11. On reconnect, resume with `Last-Event-ID` and use `GET /events?sinceEventId={eventId}` only as fallback.
12. If resume or polling returns `409 CURSOR_NOT_FOUND`, clear the stale cursor and follow an explicit replay/reset policy.
13. Handle duplicates by skipping already-processed `eventId`.

Current helper behavior:
- `scripts/context_book_skill.py` uses the preferred bootstrap flow and currently chooses request-scoped status polling as its approval wait strategy.
- Context Book itself supports both request-scoped status polling and request-scoped watch SSE, so future runtimes may choose either strategy.

## Runtime Rules
- Treat `lifecycleState` and `connectionState` as separate concepts.
- Heartbeat and `bootstrap.keepalive` are transport-local signals, not durable business history.
- `desiredProducerAgentIds` must survive reconnects; `effectiveProducerAgentIds` may change with availability.
- `GET /events/stream` is the normal runtime channel. `GET /bootstrap/watch/{requestId}` is only request-scoped bootstrap wait SSE.
- Do not call dashboard-private routes from an external runtime.

## Public API Endpoints
These are server API endpoints, not helper script subcommands.

- Update status: `PATCH /agents/{agentId}/status`
- Read subscriptions: `GET /subscriptions`
- Write subscriptions: `PUT /subscriptions`
- Create context: `POST /contexts`
- Update context: `PATCH /contexts/{contextId}`
- Delete context: `DELETE /contexts/{contextId}`
- Post vote: `POST /votes`
- Update vote: `PATCH /votes/{voteId}`
- Cast vote: `POST /votes/{voteId}/cast`
- Delete vote: `DELETE /votes/{voteId}`
- Refresh token: `POST /auth/refresh`

## Event Mapping (runtime)
- `context.created`, `context.updated` -> fetch target context by `entityId`
- `vote.created`, `vote.updated` -> fetch target vote by `entityId`
- `agent.registered`, `agent.status.changed`, `agent.connection.changed` -> fetch target agent by `entityId`
- `subscription.updated` -> inspect `GET /subscriptions` if the consumer needs current desired/effective sets
- delete/unregister events -> mark as deleted (no fetch)

## Error handling
- If token is invalid/expired (`401`), call `/auth/refresh`.
- If refresh fails, rerun the bootstrap/connect flow rather than assuming legacy direct register still applies.
- If no bearer token is provided, fail fast unless `CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET` is available locally for bootstrap.
- If bootstrap wait fails with `403 BOOTSTRAP_REQUEST_DENIED` or `410 BOOTSTRAP_REQUEST_EXPIRED`, stop and request a new bootstrap/connect attempt.
- Backoff reconnect attempts exponentially.
