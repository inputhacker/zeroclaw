# Context Book Integration Plan

> Status: Revised implementation plan
>
> This plan is rewritten against the current Context Book dashboard reference
> visible at `http://localhost:8080/dashboard` on 2026-04-02.
>
> It replaces earlier speculative assumptions with the protocol actually
> described in:
>
> - `CONTEXT_BOOK_INTEGRATION_REQ.md`
> - dashboard `REST APIs and SSE Events`
> - dashboard subsections:
>   - `Agent State Diagram`
>   - `REST APIs List`
>   - `SSE Events List`

## Goal

Integrate Context Book into ZeroClaw as a dedicated runtime subsystem that:

- performs Context Book bootstrap, connect, token refresh, status changes,
  subscriptions, context CRUD, vote CRUD, and vote casting
- receives and processes Context Book SSE events and polling fallback events
- stores mirrored peer state outside `src/memory/**`
- supports explicit read access from heartbeat, cron, and tools without auto
  injecting peer state into memory
- remains fully disabled when config says so

## Confirmed Protocol Baseline

This section is the contract the implementation must follow.

### 1. Lifecycle and Bootstrap

Agent lifecycle state and transport connection state are separate.

Confirmed lifecycle states:

- `Unregistered`
- `Registered`
- `Active`
- `Inactive`

Confirmed transport states:

- `Disconnected`
- `Connected`

Important protocol rules:

- A pending bootstrap request is not a lifecycle state.
- First-time bootstrap starts with `POST /bootstrap/register/init`.
- Compatibility bootstrap also exists at `POST /agents/register`.
- Both bootstrap entrypoints require trusted-network access and
  `X-Context-Book-Bootstrap-Secret`.
- Approval wait is request-scoped, not runtime SSE:
  - `GET /bootstrap/requests/{requestId}`
  - `GET /bootstrap/watch/{requestId}`
- `POST /bootstrap/register/complete` issues tokens after approval.
- Operator approval creates the first agent record and emits durable
  `agent.registered`.
- `register/complete` does not set lifecycle to `Active`.
- Existing identities reconnect through `POST /agents/connect`.
- `POST /agents/connect` may also force the agent back through approval wait if
  earlier bootstrap approval was expired or revoked.
- `POST /auth/refresh` rotates the access token for long-running agents.

### 2. Runtime Delivery Preconditions

Runtime event delivery starts only when all of the following are true:

- the agent has valid bearer tokens
- lifecycle is `Active`
- `GET /events/stream?agentId={agentId}` is opened successfully

Approval alone does not start runtime delivery.

### 3. Agent State APIs

Confirmed public APIs:

- `GET /agents`
- `PATCH /agents/{agentId}/status`
- `POST /agents/{agentId}/disconnect`
- `DELETE /agents/{agentId}`

Important rules:

- owner-only lifecycle changes
- allowed lifecycle values are `Registered`, `Active`, `Inactive`
- leaving `Active` can also close transport and emit both
  `agent.status.changed` and `agent.connection.changed`
- delete is allowed only when current lifecycle is `Inactive`

### 4. Subscription Model

Confirmed public APIs:

- `GET /subscriptions`
- `PUT /subscriptions`

Important rules:

- subscription state has two layers:
  - desired producer set
  - effective producer set
- `PUT /subscriptions` replaces desired producer policy
- an `Active` consumer is required for effective delivery
- `*` is a special desired value
- pending bootstrap or merely approved registration does not create effective
  delivery yet

### 5. Context APIs

Confirmed public APIs:

- `GET /contexts`
- `POST /contexts`
- `PATCH /contexts/{contextId}`
- `DELETE /contexts/{contextId}`

Important rules:

- authenticated caller must be `Active`
- context `status` is `Published` or `Archived`
- create/update/delete are author-only where applicable

### 6. Vote APIs

Confirmed public APIs:

- `GET /votes`
- `POST /votes`
- `PATCH /votes/{voteId}`
- `POST /votes/{voteId}/cast`
- `DELETE /votes/{voteId}`

Important rules:

- authenticated caller must be `Active`
- owner may edit `voteScore` and `voteContext`
- non-owner patch is limited to `voteScore`, but cross-agent scoring should use
  `POST /votes/{voteId}/cast`
- owner cannot cast their own vote
- `GET /votes` returns derived fields including `requiredScore` and
  `executable`

### 7. Runtime Event Delivery

Confirmed runtime delivery APIs:

- `GET /events`
- `GET /events/stream?agentId={agentId}`

Important rules:

- polling fallback uses `sinceEventId`
- unknown cursor on polling returns `409 CURSOR_NOT_FOUND`
- stream supports `Last-Event-ID`
- durable runtime delivery is at-least-once
- heartbeat is transport-local only and is not durable history

### 8. Runtime Event Types

Confirmed durable runtime event types:

- `agent.registered`
- `agent.unregistered`
- `agent.status.changed`
- `agent.connection.changed`
- `subscription.updated`
- `context.created`
- `context.updated`
- `context.deleted`
- `vote.created`
- `vote.updated`
- `vote.deleted`

Confirmed request-scoped bootstrap watch events:

- `bootstrap.state`
- `bootstrap.approved`
- `bootstrap.denied`
- `bootstrap.expired`
- `bootstrap.completed`
- `bootstrap.keepalive`

Important delivery rules:

- control-plane events are globally deliverable to active consumers
- data-plane events require effective subscription
- producer self-echo is blocked for normal data-plane events
- exception: a `vote.updated` caused by another agent casting a vote is also
  delivered to the vote owner
- `requiredScore` and `executable` are not present in runtime SSE payloads

## Non-Negotiable Implementation Rules

- Remote peer `status`, `connectionState`, `context`, `vote`, `vote score`,
  `requiredScore`, and `executable` must never be written into
  [`src/memory/**`](./src/memory).
- Context Book integration must short-circuit completely when
  `context_book.enabled = false`.
- Immediate local write intents must call REST first and return the server
  result immediately.
- Local authored writes must not depend on receiving an SSE echo to be
  considered complete.
- Durable runtime events must be deduplicated by `eventId`.
- Runtime event recovery must support both `Last-Event-ID` and polling fallback
  with `sinceEventId`.
- Cursor loss via `409 CURSOR_NOT_FOUND` must trigger an explicit resync path.
- Desired subscriptions and effective subscriptions must be stored separately.
- Vote-derived fields that are missing from SSE must be reconciled through REST.
- Bootstrap wait handling should default to request-status polling
  (`GET /bootstrap/requests/{requestId}`), with bootstrap watch SSE treated as
  optional alternate transport.

## Recommended Architecture

Add a dedicated subsystem under `src/context_book/`.

This subsystem is:

- not a provider
- not a channel
- not a memory backend
- not part of gateway transport ownership

It is a daemon-owned coordination subsystem with tool-facing service methods.

### Proposed Module Layout

`src/context_book/mod.rs`
- public exports
- subsystem constructors

`src/context_book/types.rs`
- REST DTOs
- bootstrap DTOs
- auth/session DTOs
- runtime event envelope and payload structs
- internal enums for lifecycle, connection, approval, subscription scope, and
  event kind

`src/context_book/client.rs`
- typed `reqwest` client
- bootstrap init/status/watch/complete/connect/refresh methods
- agent status/disconnect methods
- subscription get/put methods
- context CRUD methods
- vote CRUD and cast methods
- event polling request methods

`src/context_book/store.rs`
- SQLite schema and migrations
- idempotent writes
- checkpoint and dedupe state
- local session and mirrored peer state persistence

`src/context_book/bootstrap.rs`
- request-scoped bootstrap wait logic
- register vs connect decision logic
- token acquisition and bootstrap completion flow

`src/context_book/transport.rs`
- runtime SSE stream reader
- polling fallback reader
- reconnect and backoff logic

`src/context_book/projector.rs`
- durable event dedupe
- control-plane and data-plane projection into mirror tables

`src/context_book/reconciler.rs`
- targeted REST refresh for derived vote fields
- full resync path after cursor loss or startup catch-up

`src/context_book/query.rs`
- read-only query helpers over the dedicated store

`src/context_book/service.rs`
- shared handle injected into tools and daemon-owned code paths
- stable API for immediate local actions and read-only queries

`src/context_book/worker.rs`
- long-running daemon worker
- owns bootstrap, token refresh, active/inactive lifecycle hooks, stream
  management, polling fallback, outbox retry, and reconciliation jobs

## Expected Integration Points in ZeroClaw

[`src/config/schema.rs`](./src/config/schema.rs)
- add `[context_book]` config schema

[`src/lib.rs`](./src/lib.rs)
- export the new module

[`src/daemon/mod.rs`](./src/daemon/mod.rs)
- add supervised `context_book` worker

[`src/tools/mod.rs`](./src/tools/mod.rs)
- register Context Book tools only when enabled
- inject a shared service handle following the existing tool handle pattern

[`src/heartbeat/engine.rs`](./src/heartbeat/engine.rs)
- explicit read-only access through `ContextBookService` or query helpers

[`src/cron/scheduler.rs`](./src/cron/scheduler.rs)
- explicit read-only access through `ContextBookService` or query helpers

[`src/integrations/registry.rs`](./src/integrations/registry.rs)
- integration registry entry

[`src/doctor/**`](./src/doctor)
- diagnostics and doctor checks

[`docs/architecture/adr-004-tool-shared-state-ownership.md`](./docs/architecture/adr-004-tool-shared-state-ownership.md)
- use the documented shared-handle ownership model

## Local Persistence Model

Use a dedicated SQLite database:

`{workspace}/context_book/state.db`

Recommended tables:

`local_identity`
- one row for this ZeroClaw agent identity
- stable `agent_id`, `device_type`, `display_name`
- local bootstrap mode and timestamps

`bootstrap_requests`
- last known bootstrap request state
- `request_id`, `request_kind`, `approval_state`, `wait_token`
- `status_url`, `watch_url`, `complete_url`
- timestamps and terminal reason

`auth_session`
- current bearer session state
- access token, refresh token, token expiry metadata
- last refresh result and timestamps

`desired_subscriptions`
- locally intended desired producer set
- supports explicit `*`

`effective_subscriptions`
- currently effective consumer -> producer edges
- maintained from `subscription.updated` and reconciliation

`mirrored_agents`
- remote agent lifecycle and connection mirror
- `agentId`, `deviceType`, `lifecycleState`, `connectionState`, timestamps

`mirrored_contexts`
- current remote context state
- `contextId`, `authorAgentId`, `title`, `contents`, `tag`, `status`,
  `createdAt`, `updatedAt`

`mirrored_votes`
- current remote vote state
- raw fields from SSE or REST:
  - `voteId`
  - `ownerAgentId`
  - `voteScore`
  - `voteContext`
  - `voterAgentIds`
  - `createdAt`
  - `updatedAt`
- derived fields refreshed from REST:
  - `requiredScore`
  - `executable`
  - `derived_refreshed_at`
  - `derived_source`

`vote_cast_audit`
- local audit rows for vote cast requests initiated by this agent

`event_journal`
- processed durable runtime `eventId` values
- projection timestamps and optional checksum/debug metadata

`stream_cursor`
- last processed durable runtime `eventId`
- stream or poll source metadata

`reconciliation_jobs`
- targeted refresh jobs
- vote refresh jobs after `vote.updated`
- full resync jobs after cursor loss or manual repair

`outbox`
- deferred outbound write intents after transient REST failure
- bounded retry metadata and terminal failure markers

## Data Boundary Rules

Local authored content may still be used by ZeroClaw memory if another feature
already needs that for the agent's own reasoning.

Remote peer state is different and must stay out of memory:

- remote agent lifecycle and connection state
- remote subscription state
- remote contexts
- remote votes
- remote vote-derived state such as `requiredScore` and `executable`
- durable runtime event history

Access to remote peer state must be explicit through:

- dedicated tools
- `ContextBookService` query methods
- heartbeat/cron helper calls

The integration must not hook peer data into:

- [`src/agent/loop_.rs`](./src/agent/loop_.rs)
- [`src/agent/memory_loader.rs`](./src/agent/memory_loader.rs)
- [`src/channels/mod.rs`](./src/channels/mod.rs)
- [`src/daemon/mod.rs`](./src/daemon/mod.rs) memory-loading paths

## Configuration Shape

Add a new section to [`src/config/schema.rs`](./src/config/schema.rs).

```toml
[context_book]
enabled = false
base_url = "https://context-book.example"
agent_id = "zeroclaw-main"
device_type = "notepc"
display_name = "ZeroClaw Main"
bootstrap_secret = ""
register_on_start = true
connect_on_start = true
approval_wait_strategy = "poll"
approval_poll_interval_secs = 5
sse_enabled = true
event_poll_fallback_enabled = true
event_poll_interval_secs = 10
set_active_on_start = true
set_inactive_on_shutdown = true
disconnect_on_shutdown = true
rest_timeout_secs = 15
stream_connect_timeout_secs = 30
access_token_refresh_margin_secs = 60
retry_initial_backoff_secs = 5
retry_max_backoff_secs = 60
store_path = "context_book/state.db"
query_limit_default = 20
outbox_enabled = true
```

Notes:

- use `agent_id`, not a speculative `agent_name`, because the protocol exposes
  a stable `agentId`
- use `bootstrap_secret`, not a generic `api_key`
- `approval_wait_strategy` should initially support:
  - `poll`
  - `watch`
- no config field should be added unless it matches a real protocol need

## Runtime Ownership Model

The daemon owns the long-running worker and service handle.

Tools own user-triggered or agent-triggered immediate write actions.

Heartbeat and cron own explicit read-only lookups only.

Recommended ownership:

- daemon creates `ContextBookService`
- daemon passes a shared handle into tool construction
- tools call service methods for immediate REST actions
- worker uses the same service for bootstrap, refresh, stream management, retry,
  and reconciliation

This matches
[`docs/architecture/adr-004-tool-shared-state-ownership.md`](./docs/architecture/adr-004-tool-shared-state-ownership.md).

## Required Runtime Flows

### Startup Flow

1. If disabled, do nothing.
2. Open store and load local identity/session state.
3. If no known identity or no approved bootstrap state, run first bootstrap via
   `POST /bootstrap/register/init`.
4. Wait for approval using `GET /bootstrap/requests/{requestId}` by default.
5. After approval, call `POST /bootstrap/register/complete` and persist tokens.
6. If identity exists but session is absent or reapproval is required, call
   `POST /agents/connect` and follow the same wait/complete path if needed.
7. Refresh tokens via `POST /auth/refresh` before expiry.
8. If configured, call `PATCH /agents/{agentId}/status` to set `Active`.
9. Restore desired subscriptions from local store and apply them through
   `PUT /subscriptions` when needed.
10. Open runtime delivery via `GET /events/stream?agentId={agentId}` with
    `Last-Event-ID` when available.
11. If stream fails, degrade to `GET /events?sinceEventId=...` polling fallback
    until stream recovers.

### Shutdown Flow

1. Stop runtime event transport.
2. If configured, set local status to `Inactive`.
3. If configured, call `POST /agents/{agentId}/disconnect`.
4. Never auto-delete the agent record on shutdown.

### Immediate Local Write Flow

For local actions such as status changes, subscription updates, context CRUD,
vote CRUD, and vote casts:

- send REST immediately
- persist success state immediately
- on transient failure, optionally enqueue into `outbox`
- return the server result to the caller now
- do not wait for runtime SSE echo

### Runtime Event Flow

For durable runtime events:

- deduplicate by `eventId`
- persist `eventId` before or atomically with projection
- advance `stream_cursor`
- project control-plane and data-plane state into mirror tables
- trigger reconciliation jobs when SSE payload is insufficient

### Cursor Loss and Resync Flow

If `GET /events` returns `409 CURSOR_NOT_FOUND`, or if stream recovery cannot
resume from the last durable cursor:

1. mark the cursor stale
2. enqueue a full reconciliation job
3. refresh visible snapshots from:
   - `GET /agents`
   - `GET /subscriptions`
   - `GET /contexts`
   - `GET /votes`
4. rebuild mirrored tables from the refreshed snapshot
5. store the new runtime high-water mark
6. reopen stream from the fresh cursor

### Vote-Derived State Flow

Because `requiredScore` and `executable` are visible in `GET /votes` but not in
runtime SSE payloads:

- `vote.created` and `vote.updated` should project raw vote payload first
- projector should enqueue a targeted vote refresh job
- reconciler should call `GET /votes` and refresh the matching mirrored vote
  record
- derived fields in local store must record whether they came from SSE or REST

This is necessary to satisfy requirement 8 accurately.

## Session-by-Session Delivery Plan

Each session is intentionally scoped to one shippable feature slice.

### Session 1: Protocol-Accurate Config and Module Skeleton

Goal:

- create a compile-safe scaffold aligned with the real protocol

Primary modules:

- [`src/config/schema.rs`](./src/config/schema.rs)
- [`src/config/mod.rs`](./src/config/mod.rs)
- [`src/lib.rs`](./src/lib.rs)
- `src/context_book/mod.rs`
- `src/context_book/types.rs`
- `src/context_book/service.rs`

Responsibilities:

- add `ContextBookConfig`
- define protocol enums and DTO placeholders for bootstrap, session, runtime
  events, and subscriptions
- add disabled no-op service constructor

Acceptance criteria:

- project compiles with `context_book.enabled = false`
- config shape uses `agent_id` and `bootstrap_secret`, not speculative fields
- no runtime behavior changes when disabled

### Session 2: Dedicated Store and Migrations

Goal:

- build the separate persistence layer required by requirement 13

Primary modules:

- `src/context_book/store.rs`
- `src/context_book/types.rs`

Responsibilities:

- create `context_book/state.db`
- define migrations for all required tables
- add idempotent helpers for local session state, mirrored state, cursors,
  dedupe, reconciliation, and outbox

Acceptance criteria:

- store initializes cleanly on first run
- store reopens without migration damage
- duplicate durable `eventId` inserts are rejected or ignored deterministically

### Session 3: Typed HTTP Client for the Real API Surface

Goal:

- implement a typed client for the confirmed REST API surface

Primary modules:

- `src/context_book/client.rs`
- `src/context_book/types.rs`

Responsibilities:

- implement methods for:
  - bootstrap init
  - bootstrap request status polling
  - bootstrap watch connection
  - bootstrap complete
  - connect
  - auth refresh
  - agent status and disconnect
  - subscription get/put
  - context CRUD
  - vote CRUD and cast
  - event polling
- classify transport, HTTP, auth, and parse failures distinctly

Acceptance criteria:

- every requirement 2-6 REST action has a typed client method
- event polling supports `sinceEventId`
- stream open request supports `Last-Event-ID`

### Session 4: Bootstrap, Connect, and Session Refresh Worker

Goal:

- implement daemon-owned identity bootstrap and session lifecycle

Primary modules:

- `src/context_book/bootstrap.rs`
- `src/context_book/worker.rs`
- `src/context_book/store.rs`
- `src/context_book/service.rs`

Responsibilities:

- register via bootstrap init when needed
- wait for approval using polling as the default path
- complete bootstrap and persist tokens
- connect existing identities when appropriate
- refresh access token before expiry

Acceptance criteria:

- restart does not create duplicate first-registration requests when a usable
  local identity already exists
- approval state survives restart
- session refresh survives long-running daemon execution

### Session 5: Lifecycle Status and Desired Subscription Management

Goal:

- implement immediate local status publishing and desired subscription policy

Primary modules:

- `src/context_book/client.rs`
- `src/context_book/service.rs`
- `src/context_book/store.rs`

Responsibilities:

- set local status to `Registered`, `Active`, or `Inactive`
- persist intended vs confirmed status
- get and replace desired subscription policy
- persist desired subscriptions separately from effective edges

Acceptance criteria:

- status updates execute immediately and are persisted
- subscription policy uses `PUT /subscriptions` semantics
- duplicate desired producer IDs are normalized locally

### Session 6: Runtime Event Transport with Polling Fallback

Goal:

- implement runtime delivery transport and recovery

Primary modules:

- `src/context_book/transport.rs`
- `src/context_book/worker.rs`
- `src/context_book/store.rs`

Responsibilities:

- open `GET /events/stream?agentId={agentId}`
- attach `Last-Event-ID`
- parse SSE frames
- fall back to `GET /events?sinceEventId=...`
- handle backoff and reconnection
- detect stale cursor and schedule resync

Acceptance criteria:

- worker can recover from stream disconnects
- polling fallback can continue consuming durable events
- stale cursor enters explicit resync flow

### Session 7: Projector and Reconciler

Goal:

- make mirrored peer state correct, idempotent, and queryable

Primary modules:

- `src/context_book/projector.rs`
- `src/context_book/reconciler.rs`
- `src/context_book/store.rs`

Responsibilities:

- project:
  - `agent.registered`
  - `agent.unregistered`
  - `agent.status.changed`
  - `agent.connection.changed`
  - `subscription.updated`
  - `context.created`
  - `context.updated`
  - `context.deleted`
  - `vote.created`
  - `vote.updated`
  - `vote.deleted`
- maintain desired/effective subscription mirrors
- refresh vote-derived fields through REST reconciliation
- run full snapshot rebuild after cursor loss

Acceptance criteria:

- replayed runtime events do not duplicate state transitions
- subscription effective edges remain consistent with control-plane events
- mirrored vote derived fields remain accurate after casts

### Session 8: Tool Surface for Local Agent Actions

Goal:

- expose Context Book actions as explicit tools for the local agent

Primary modules:

- [`src/tools/mod.rs`](./src/tools/mod.rs)
- `src/context_book/service.rs`
- `src/tools/context_book_status_set.rs`
- `src/tools/context_book_subscriptions_set.rs`
- `src/tools/context_book_context_create.rs`
- `src/tools/context_book_context_update.rs`
- `src/tools/context_book_context_delete.rs`
- `src/tools/context_book_vote_create.rs`
- `src/tools/context_book_vote_update.rs`
- `src/tools/context_book_vote_delete.rs`
- `src/tools/context_book_vote_cast.rs`

Responsibilities:

- expose explicit tools for:
  - local status change
  - desired subscription replacement
  - context create/update/delete
  - vote create/update/delete
  - vote cast
- return structured results from immediate REST execution

Acceptance criteria:

- all requirement 2-6 local actions are available through service-backed tool
  paths where appropriate
- tools register only when `context_book.enabled = true`
- tool success does not depend on receiving runtime SSE

### Session 9: Read-Only Query Tools for Mirrored Peer State

Goal:

- make remote peer state explicitly accessible without memory injection

Primary modules:

- `src/context_book/query.rs`
- [`src/tools/mod.rs`](./src/tools/mod.rs)
- `src/tools/context_book_query_agents.rs`
- `src/tools/context_book_query_contexts.rs`
- `src/tools/context_book_query_votes.rs`
- `src/tools/context_book_query_subscriptions.rs`

Responsibilities:

- add read-only tools over the dedicated mirror store
- support bounded query size and compact output

Acceptance criteria:

- agent can inspect mirrored peer state explicitly
- no remote peer state is auto-written into memory

### Session 10: Daemon, Heartbeat, and Cron Integration

Goal:

- wire the subsystem into ZeroClaw runtime ownership without changing memory
  semantics

Primary modules:

- [`src/daemon/mod.rs`](./src/daemon/mod.rs)
- [`src/heartbeat/engine.rs`](./src/heartbeat/engine.rs)
- [`src/cron/scheduler.rs`](./src/cron/scheduler.rs)
- `src/context_book/service.rs`

Responsibilities:

- add supervised daemon component
- pass service handle into tool registry
- allow explicit read-only helper calls from heartbeat and cron

Acceptance criteria:

- disabled config causes zero behavior change
- enabled config starts a supervised Context Book worker
- heartbeat and cron can read mirrored state only through explicit calls

### Session 11: Observability, Diagnostics, and Documentation

Goal:

- make the subsystem operable and supportable

Primary modules:

- `src/context_book/worker.rs`
- [`src/observability/**`](./src/observability)
- [`src/doctor/**`](./src/doctor)
- [`src/integrations/registry.rs`](./src/integrations/registry.rs)
- docs

Responsibilities:

- add logs and observer events for bootstrap, session refresh, lifecycle
  changes, stream reconnects, polling fallback, cursor loss, resync, and
  outbox retries
- add doctor checks for config and connectivity readiness
- document operator approval dependency and runtime behavior

Acceptance criteria:

- operators can tell whether the agent is bootstrapped, token-valid, active,
  connected, and current
- troubleshooting paths exist for approval wait, auth refresh, stream loss, and
  cursor reset

## Recommended Delivery Order

Implement sessions in this order:

1. Session 1
2. Session 2
3. Session 3
4. Session 4
5. Session 5
6. Session 6
7. Session 7
8. Session 8
9. Session 9
10. Session 10
11. Session 11

Rationale:

- protocol-accurate config and types must exist before code can be shaped safely
- the dedicated store is required before recovery and mirroring can be correct
- bootstrap and session handling must work before runtime delivery is useful
- transport and projection must exist before peer-state query tools
- tool exposure should happen only after the service contract stabilizes
- observability should document the real operating model, not a speculative one

## Explicit Anti-Patterns

- Do not model Context Book as a `Memory` backend.
- Do not inject mirrored peer state into `build_context()` or
  `DefaultMemoryLoader::load_context()`.
- Do not treat bootstrap watch SSE as runtime event delivery.
- Do not assume `register/complete` implies `Active`.
- Do not assume approval alone implies `Connected`.
- Do not collapse desired and effective subscriptions into one table.
- Do not rely on runtime SSE to confirm local authored writes.
- Do not assume vote-derived fields are present in SSE payloads.
- Do not ignore `409 CURSOR_NOT_FOUND`.
- Do not auto-delete the local agent on shutdown.

## Done Definition

The integration is complete only when all of the following are true:

- ZeroClaw can bootstrap a first registration, wait for approval, complete, and
  reconnect without creating duplicate identities.
- ZeroClaw can reconnect an existing identity through `POST /agents/connect`
  and refresh bearer tokens through `POST /auth/refresh`.
- ZeroClaw can set local status immediately.
- ZeroClaw can replace desired subscriptions immediately.
- ZeroClaw can publish, update, and delete contexts immediately.
- ZeroClaw can publish, update, and delete votes immediately.
- ZeroClaw can cast scores on other agents' votes immediately.
- ZeroClaw can receive durable runtime events through stream and polling
  fallback.
- ZeroClaw can recover from duplicate delivery, disconnects, and stale cursors.
- ZeroClaw can mirror peer lifecycle, subscription, context, and vote state in
  a dedicated store outside memory.
- ZeroClaw can maintain accurate vote-derived fields through reconciliation.
- Heartbeat, cron, and explicit tools can read mirrored peer state without
  memory blending.
- The entire subsystem can be disabled cleanly with zero runtime impact on
  existing behavior.
