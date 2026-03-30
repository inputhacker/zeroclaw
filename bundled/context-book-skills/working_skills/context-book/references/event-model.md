# Event Model and Delivery

Event envelope fields:
- `eventId`
- `eventType`
- `occurredAt`
- `producerAgentId`
- `entityId`
- `payload`
- optional `meta.scope`

Delivery semantics:
- At-least-once delivery
- Duplicates possible
- Agent must deduplicate by `eventId`
- Persist last processed `eventId`
- Heartbeat and bootstrap keepalive are not durable events

Durable runtime control-plane events:
- `agent.registered`
- `agent.unregistered`
- `agent.status.changed`
- `agent.connection.changed`
- `subscription.updated`

Durable runtime data-plane events:
- `context.created`
- `context.updated`
- `context.deleted`
- `vote.created`
- `vote.updated`
- `vote.deleted`

Bootstrap watch SSE events:
- `bootstrap.state`
- `bootstrap.approved`
- `bootstrap.denied`
- `bootstrap.expired`
- `bootstrap.completed`
- `bootstrap.keepalive`

Recommended reconnect flow:
1. Reconnect SSE with `Last-Event-ID`.
2. If stream resume is insufficient, call polling endpoint with `sinceEventId`.
3. If the server returns `409 CURSOR_NOT_FOUND`, clear the stale cursor before replaying.
4. Process events idempotently.
