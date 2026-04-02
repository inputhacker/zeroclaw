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
- automatically maintains the required subscribe-all policy for other agents by
  converging desired subscriptions to `["*"]`
- receives and processes Context Book SSE events and polling fallback events
- stores mirrored peer state outside `src/memory/**`
- supports explicit read access from heartbeat, cron, and tools without auto
  injecting peer state into memory
- remains fully disabled when config says so

Dashboard-private operator routes shown in the dashboard reference are protocol
context only. ZeroClaw runtime integration should use the public agent-facing
REST and SSE surface, not dashboard-private approval APIs.

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
- `POST /bootstrap/register/init`, `POST /agents/register`, and
  `POST /agents/connect` all require trusted-network access and
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
- When a `Connect` request is pushed back into approval wait, the runtime must
  support both completion styles described by the dashboard reference:
  `POST /bootstrap/register/complete` and a retried legacy
  `POST /agents/connect`.
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

- `GET /subscriptions` is consumer-scoped and returns only the authenticated
  agent's `consumerAgentId`, `desiredProducerAgentIds[]`, and
  `effectiveProducerAgentIds[]`
- subscription state has two layers:
  - desired producer set
  - effective producer set
- `PUT /subscriptions` replaces desired producer policy via
  `producerAgentIds[]`
- an `Active` consumer is required for effective delivery
- self-subscription, duplicates, blank values, and unknown producer IDs are
  ignored by the server
- `*` is a special desired value
- pending bootstrap or merely approved registration does not create effective
  delivery yet
- requirement 6 maps to a desired policy of `["*"]` for the local consumer in
  this integration plan

### 5. Context APIs

Confirmed public APIs:

- `GET /contexts`
- `POST /contexts`
- `PATCH /contexts/{contextId}`
- `DELETE /contexts/{contextId}`

Important rules:

- authenticated caller must be `Active`
- context `status` is `Published` or `Archived`
- create may use an optional owner-scoped `contextId`
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
- create requires `voteContext`; `voteId` and `voteScore` are optional
- owner may edit `voteScore` and `voteContext`
- non-owner patch is limited to `voteScore`, but cross-agent scoring should use
  `POST /votes/{voteId}/cast`
- owner cannot cast their own vote
- cast `voteScore` defaults to `1` and must be a positive finite number when
  provided
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
- stream query `agentId` must match the authenticated token owner
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

- durable runtime SSE envelopes include `eventId`, `eventType`, `occurredAt`,
  `producerAgentId`, `entityId`, `payload`, and implementation field
  `meta.scope`
- control-plane events are globally deliverable to active consumers
- data-plane events require effective subscription
- producer self-echo is blocked for normal data-plane events
- exception: a non-owner `vote.updated` whose `ownerAgentId` matches the
  consumer is also delivered to the vote owner, including casts and non-owner
  score-only updates
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
- v1 runtime bootstrap should standardize on `POST /bootstrap/register/init`;
  legacy `POST /agents/register` is compatibility context, not a required
  primary execution path.
- v1 runtime should converge local desired subscriptions to `["*"]` after the
  agent becomes `Active`.

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
- runtime event envelope and payload structs, including `meta.scope`
- internal enums for lifecycle, connection, approval, subscription scope, and
  event kind

`src/context_book/client.rs`
- typed `reqwest` client
- bootstrap init/status/watch/complete/connect/refresh methods
- agent list/status/disconnect methods
- subscription get/put methods
- context list/CRUD methods
- vote list/CRUD and cast methods
- event polling and stream-open request methods

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
  management, polling fallback, and reconciliation jobs

## Expected Integration Points in ZeroClaw

[`src/config/schema.rs`](./src/config/schema.rs)
- add `[context_book]` config schema

[`src/onboard/wizard.rs`](./src/onboard/wizard.rs)
- ask for required Context Book settings during interactive onboarding when the
  feature is enabled

[`src/main.rs`](./src/main.rs)
- ensure `zeroclaw onboard` and related setup entrypoints can populate Context
  Book config without requiring later manual edits

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

[`src/security/secrets.rs`](./src/security/secrets.rs)
- reuse the existing `SecretStore` for session token persistence

[`install.sh`](./install.sh)
- keep Context Book support present in normal installs and pass required values
  into onboarding when available

[`docs/architecture/adr-004-tool-shared-state-ownership.md`](./docs/architecture/adr-004-tool-shared-state-ownership.md)
- use the documented shared-handle ownership model

## Local Persistence Model

Use a dedicated SQLite database:

`{workspace}/state/context_book/state.db`

Recommended tables:

`local_identity`
- one row for this ZeroClaw agent identity
- stable `agent_id`, `device_type`, `display_name`
- local bootstrap history/state markers and timestamps

`bootstrap_requests`
- last known bootstrap request state
- `request_id`, `request_kind`, `approval_state`, `next_action`, `wait_token`
- `status_url`, `watch_url`, `complete_url`
- timestamps and terminal reason

`auth_session`
- current bearer session state
- encrypted secret-store references for access token and refresh token, plus
  token expiry metadata
- last refresh result and timestamps

`desired_subscriptions`
- locally intended desired producer set
- v1 should seed and maintain `["*"]` to satisfy requirement 6
- schema may still store a normalized set for future compatibility

`effective_subscriptions`
- current effective producer set for the local authenticated consumer
- rebuilt from `GET /subscriptions` plus `subscription.updated`

`mirrored_agents`
- remote agent lifecycle and connection mirror
- `agentId`, `deviceType`, `lifecycleState`, `connectionState`, `createdAt`,
  `updatedAt`, optional `lastSeenAt`

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
- projection timestamps plus `eventType`, `occurredAt`, and optional debug
  metadata

`stream_cursor`
- last processed durable runtime `eventId`
- stream or poll source metadata

`reconciliation_jobs`
- targeted refresh jobs
- vote refresh jobs after `vote.updated`
- initial snapshot sync jobs
- bounded resync jobs after cursor loss or manual repair

## Data Boundary Rules

Local authored content may still be used by ZeroClaw memory if another feature
already needs that for the agent's own reasoning.

Remote peer state is different and must stay out of memory:

- remote agent lifecycle and connection state
- remote contexts
- remote votes
- remote vote-derived state such as `requiredScore` and `executable`
- durable runtime event history

Local desired/effective subscription state must also stay outside memory.

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
approval_poll_interval_secs = 5
event_poll_interval_secs = 10
set_active_on_start = true
set_inactive_on_shutdown = true
disconnect_on_shutdown = true
rest_timeout_secs = 15
stream_connect_timeout_secs = 30
access_token_refresh_margin_secs = 60
retry_initial_backoff_secs = 5
retry_max_backoff_secs = 60
store_path = "state/context_book/state.db"
```

Notes:

- use `agent_id`, not a speculative `agent_name`, because the protocol exposes
  a stable `agentId`; bootstrap init should serialize this configured value into
  the required request field `agentName`, then persist the server-returned
  canonical `agentId`
- use `bootstrap_secret`, not a generic `api_key`
- request-status polling is the default approval wait path; bootstrap watch SSE
  is an internal alternate transport and does not need a separate user-facing
  config key in v1
- onboarding should ask only for fields that are operationally required to make
  the feature usable:
  - `enabled`
  - `base_url`
  - `agent_id`
  - `device_type`
  - `display_name`
  - `bootstrap_secret`
- stream delivery plus polling fallback are required runtime behaviors when the
  integration is enabled, so they should not be split into extra feature toggles
- no config field should be added unless it matches a real protocol need

## Runtime Ownership Model

The daemon owns the long-running worker and service handle.

Tools own user-triggered or agent-triggered immediate write actions.

Heartbeat and cron own explicit read-only lookups only.

Recommended ownership:

- daemon creates `ContextBookService`
- daemon passes a shared handle into tool construction
- tools call service methods for immediate REST actions
- worker uses the same service for bootstrap, refresh, stream management,
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
   `POST /agents/connect` and, if approval wait is required, complete via either
   `POST /bootstrap/register/complete` or a retried legacy
   `POST /agents/connect` after approval.
7. Refresh tokens via `POST /auth/refresh` before expiry.
8. If configured, call `PATCH /agents/{agentId}/status` to set `Active`.
9. Restore the required subscribe-all desired policy from local store; if no
   prior value exists, seed `["*"]`, then apply it through `PUT /subscriptions`
   once the agent is `Active`.
10. If no durable cursor exists yet, run an initial snapshot sync from:
    - `GET /agents`
    - `GET /contexts`
    - `GET /votes`
    - `GET /subscriptions` for the local consumer only
11. Open runtime delivery via `GET /events/stream?agentId={agentId}` with
    `Last-Event-ID` when available.
12. If stream fails, degrade to `GET /events?sinceEventId=...` polling fallback
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
- return the server result on success, or an explicit failure on transient or
  terminal error
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
3. refresh snapshot-capable state from:
   - `GET /agents`
   - `GET /contexts`
   - `GET /votes`
   - `GET /subscriptions` for the local consumer only
4. rebuild agent/context/vote mirror tables from those snapshots
5. clear and rebuild only the local consumer subscription snapshot tables from
   `GET /subscriptions`
6. clear the stale durable cursor
7. reopen stream without `Last-Event-ID`; the next delivered runtime event
   establishes the new cursor

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

Status:

- Completed on 2026-04-02: config schema, `src/context_book/` skeleton, DTO
  placeholders, and disabled no-op service scaffold landed.

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
  events, and subscriptions, including the exact runtime SSE envelope fields
- add disabled no-op service constructor

Acceptance criteria:

- project compiles with `context_book.enabled = false`
- config shape uses `agent_id` and `bootstrap_secret`, not speculative fields
- no runtime behavior changes when disabled

### Session 2: Dedicated Store and Migrations

Goal:

- build the separate persistence layer required by requirement 13

Status:

- Completed on 2026-04-02: dedicated SQLite store, schema migration v1, and
  idempotent helpers for local identity/session state, subscriptions, mirrored
  state, stream cursor, event dedupe, and reconciliation queue landed with
  focused store tests.

Primary modules:

- `src/context_book/store.rs`
- `src/context_book/types.rs`

Responsibilities:

- create the store at the configured `store_path`
- define migrations for all required tables
- add idempotent helpers for local session state, mirrored state, cursors,
  dedupe, and reconciliation

Acceptance criteria:

- store initializes cleanly on first run
- store reopens without migration damage
- duplicate durable `eventId` inserts are rejected or ignored deterministically

### Session 3: Typed HTTP Client for the Real API Surface

Goal:

- implement a typed client for the confirmed REST API surface

Status:

- Completed on 2026-04-02: typed `reqwest` client for bootstrap, connect,
  refresh, agent/status, subscriptions, context CRUD, vote CRUD/cast, event
  polling, and SSE stream-open requests landed with focused client tests.

Primary modules:

- `src/context_book/client.rs`
- `src/context_book/types.rs`

Responsibilities:

- implement methods for:
  - bootstrap init
  - bootstrap request status polling
  - bootstrap complete
  - connect
  - auth refresh
  - agent list, status, and disconnect
  - subscription get/put
  - context list/CRUD
  - vote list/CRUD and cast
  - event polling
  - event stream open with `Last-Event-ID`
- bootstrap watch SSE support is optional after the polling path works
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
- a reapproval-required connect flow can finish through either of the two
  completion styles documented by the dashboard reference
- session refresh survives long-running daemon execution

### Session 5: Lifecycle Status and Desired Subscription Management

Goal:

- implement immediate local status publishing and the required subscribe-all
  desired subscription policy

Primary modules:

- `src/context_book/client.rs`
- `src/context_book/service.rs`
- `src/context_book/store.rs`

Responsibilities:

- set local status to `Registered`, `Active`, or `Inactive`
- persist intended vs confirmed status
- get and replace desired subscription policy for the local consumer
- ensure the runtime-converged desired policy is `["*"]`
- persist desired subscriptions separately from effective edges

Acceptance criteria:

- status updates execute immediately and are persisted
- subscription policy uses `PUT /subscriptions` semantics
- the worker converges desired subscriptions to `["*"]` after activation

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
- maintain local desired/effective subscription mirrors
- refresh vote-derived fields through REST reconciliation
- run initial snapshot sync and bounded snapshot rebuild after cursor loss

Acceptance criteria:

- replayed runtime events do not duplicate state transitions
- local effective subscriptions remain consistent with `GET /subscriptions`
  and control-plane events
- mirrored vote derived fields remain accurate after casts

### Session 8: Tool Surface for Local Agent Actions

Goal:

- expose Context Book actions as explicit tools for the local agent

Primary modules:

- [`src/tools/mod.rs`](./src/tools/mod.rs)
- `src/context_book/service.rs`
- `src/tools/context_book_status_set.rs`
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
  - context create/update/delete
  - vote create/update/delete
  - vote cast
- return structured results from immediate REST execution

Acceptance criteria:

- all requirement 2-5 local actions are available through service-backed tool
  paths where appropriate
- requirement 6 is enforced automatically by the worker through `["*"]`
  subscription convergence rather than a generic manual subscription-edit tool
- tools register only when `context_book.enabled = true`
- tool success does not depend on receiving runtime SSE

### Session 9: Read-Only Query Tools for Mirrored Peer State

Goal:

- make remote peer state explicitly accessible without memory injection

Status:

- Completed sub-item on 2026-04-03: added a dedicated read-only query layer in
  `src/context_book/query.rs` with bounded mirrored-agent/context/vote queries
  and mirrored subscription snapshots backed by `src/context_book/store.rs`

Primary modules:

- `src/context_book/query.rs`
- [`src/tools/mod.rs`](./src/tools/mod.rs)
- `src/tools/context_book_query_agents.rs`
- `src/tools/context_book_query_contexts.rs`
- `src/tools/context_book_query_votes.rs`
- `src/tools/context_book_query_subscriptions.rs`

Responsibilities:

- add read-only tools over the dedicated mirror store
- expose local desired/effective subscriptions separately
- support bounded query size and compact output

Acceptance criteria:

- agent can inspect mirrored peer state explicitly
- no remote peer state is auto-written into memory

### Session 10: Onboarding and Installer Integration

Goal:

- satisfy requirement 14 by wiring required configuration into onboarding and
  standard install flows

Status:

- Completed on 2026-04-03
- Completed sub-item on 2026-04-03: interactive onboarding now prompts for the
  required `context_book` fields when the user enables the feature
- Completed sub-item on 2026-04-03: non-interactive `zeroclaw onboard` and
  installer-driven setup can now seed the required `context_book` keys and
  surface those inputs when provided
- Completed sub-item on 2026-04-03: docs now distinguish first-bootstrap
  required Context Book values from optional runtime tuning in the operator
  config reference and one-click bootstrap guide

Primary modules:

- [`src/onboard/wizard.rs`](./src/onboard/wizard.rs)
- [`src/main.rs`](./src/main.rs)
- [`install.sh`](./install.sh)
- docs

Responsibilities:

- add onboarding prompts for required Context Book fields when the user enables
  the feature
- make non-interactive and installer-driven setup able to seed the same config
  keys without inventing extra protocol fields
- document which values are mandatory for first bootstrap vs optional tuning
- ensure Context Book support ships as part of the normal ZeroClaw install
  footprint rather than as a separate plugin or post-install add-on

Acceptance criteria:

- `zeroclaw onboard` can produce a valid `[context_book]` section without manual
  follow-up edits when the user opts in
- install/onboarding flows do not require dashboard-private routes or browser
  participation
- requirement 14 is met without adding speculative install-time feature flags

### Session 11: Daemon, Heartbeat, and Cron Integration

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

### Session 12: Observability, Diagnostics, and Documentation

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
  changes, stream reconnects, polling fallback, cursor loss, and resync
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
12. Session 12

Rationale:

- protocol-accurate config and types must exist before code can be shaped safely
- the dedicated store is required before recovery and mirroring can be correct
- bootstrap and session handling must work before runtime delivery is useful
- transport and projection must exist before peer-state query tools
- tool exposure should happen only after the service contract stabilizes
- onboarding/install integration should happen after config and service
  contracts stabilize, but before final operational hardening
- observability should document the real operating model, not a speculative one

## Explicit Anti-Patterns

- Do not model Context Book as a `Memory` backend.
- Do not inject mirrored peer state into `build_context()` or
  `DefaultMemoryLoader::load_context()`.
- Do not treat bootstrap watch SSE as runtime event delivery.
- Do not assume `register/complete` implies `Active`.
- Do not assume approval alone implies `Connected`.
- Do not collapse desired and effective subscriptions into one table.
- Do not treat requirement 6 as an optional per-producer preference in v1; the
  runtime must converge to subscribe-all via `["*"]`.
- Do not rely on runtime SSE to confirm local authored writes.
- Do not assume vote-derived fields are present in SSE payloads.
- Do not ignore `409 CURSOR_NOT_FOUND`.
- Do not auto-delete the local agent on shutdown.
- Do not make Context Book support depend on a dashboard-only onboarding or
  install path.

## Done Definition

The integration is complete only when all of the following are true:

- ZeroClaw can bootstrap a first registration, wait for approval, complete, and
  reconnect without creating duplicate identities.
- ZeroClaw can reconnect an existing identity through `POST /agents/connect`
  and refresh bearer tokens through `POST /auth/refresh`.
- ZeroClaw can set local status immediately.
- ZeroClaw converges desired subscriptions to `["*"]` for all other registered
  agents and keeps local desired/effective subscription state queryable.
- ZeroClaw can publish, update, and delete contexts immediately.
- ZeroClaw can publish, update, and delete votes immediately.
- ZeroClaw can cast scores on other agents' votes immediately.
- ZeroClaw can receive durable runtime events through stream and polling
  fallback.
- ZeroClaw can recover from duplicate delivery, disconnects, and stale cursors.
- ZeroClaw can mirror peer lifecycle, context, and vote state, plus local
  desired/effective subscriptions, in a dedicated store outside memory.
- ZeroClaw can maintain accurate vote-derived fields through reconciliation.
- Heartbeat, cron, and explicit tools can read mirrored peer state without
  memory blending.
- ZeroClaw install and `zeroclaw onboard` can enable the feature and capture the
  required config without dashboard-private steps or manual config surgery.
- The entire subsystem can be disabled cleanly with zero runtime impact on
  existing behavior.
