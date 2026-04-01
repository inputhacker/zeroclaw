# Context Book Integration Plan

> ⚠️ **Status: Proposal / Roadmap**
>
> This document describes a staged implementation plan for integrating a
> Context Book server into ZeroClaw. It is intentionally implementation-oriented
> and organized so that a future work session can complete exactly one feature
> slice at a time.

## Goal

Integrate an external Context Book service that provides REST APIs and SSE
events, while preserving ZeroClaw's existing architecture boundaries:

- long-running lifecycle work belongs in the daemon
- agent-triggered external actions belong in tools or dedicated runtime
  services
- peer-agent state must not be stored in ZeroClaw memory
- externally received state must be stored in a separate file or storage layer
- SSE processing must be idempotent and replay-safe

## Non-Negotiable Constraints

- Peer agent `status`, `context`, `vote`, `vote score`, and `executable`
  updates must never be written into [`src/memory/**`](./src/memory).
- Context Book integration must be fully disabled when config says so.
- REST-triggered writes from the local agent must be sent immediately.
- SSE events must be deduplicated across reconnects and restarts.
- The implementation should fit ZeroClaw's existing subsystem layout instead of
  bending `provider`, `channel`, or `memory` abstractions.

## Recommended Architecture

Add a new runtime subsystem under `src/context_book/`.

This subsystem is not a provider, not a channel, and not a memory backend. It
is an external coordination integration with:

- outbound command-style operations over REST
- inbound event-style operations over SSE
- separate local persistence for mirrored remote state
- daemon-owned lifecycle and reconnection behavior

### Proposed Module Layout

`src/context_book/mod.rs`
- Public exports and top-level constructors.

`src/context_book/types.rs`
- All REST request/response DTOs.
- All SSE event payload structs.
- Stable internal enums for event kinds and object kinds.

`src/context_book/client.rs`
- Async reqwest client wrapper.
- Methods for `register`, `approval status`, `status update`, `context CRUD`,
  `vote CRUD`, `vote cast`, `subscribe`.
- Authentication headers and timeout handling.

`src/context_book/store.rs`
- Local SQLite store under `workspace/context_book/state.db`.
- Tables for local registration state, subscriptions, mirrored agents,
  contexts, votes, vote casts, outbox, event journal, and stream checkpoint.
- Idempotent upsert/delete operations.

`src/context_book/sse.rs`
- SSE transport reader and parser.
- Reconnect loop.
- `Last-Event-ID` or checkpoint-based resume support when the upstream API
  supports it.

`src/context_book/projector.rs`
- Applies parsed SSE events into local store.
- Centralizes deduplication and state transition logic.

`src/context_book/worker.rs`
- Long-running daemon worker.
- Owns registration bootstrap, approval polling, status bootstrap, SSE loop,
  and retry processing for queued outbound actions.

`src/context_book/query.rs`
- Read-only query helpers used by tools.
- Fetches mirrored peer state without touching ZeroClaw memory.

`src/context_book/service.rs`
- Shared runtime handle for tools and daemon components.
- Exposes client + store-backed operations with a stable API.

### Expected Existing Integration Points

[`src/config/schema.rs`](./src/config/schema.rs)
- Add `[context_book]` config schema.

[`src/lib.rs`](./src/lib.rs)
- Export the new module.

[`src/daemon/mod.rs`](./src/daemon/mod.rs)
- Add a supervised `context_book` worker.

[`src/tools/mod.rs`](./src/tools/mod.rs)
- Register new Context Book tools when enabled.

[`src/heartbeat/**`](./src/heartbeat)
- Read-only access only, via query tools or service helpers.

[`src/cron/**`](./src/cron)
- Read-only access only, via query tools or service helpers.

## Required Local Persistence

Use a dedicated SQLite database:

`{workspace}/context_book/state.db`

Recommended tables:

`local_agent_registration`
- One row for this ZeroClaw instance.
- Stores remote registration id, approval status, current published status,
  last approval check time, last successful heartbeat with server, and last
  known server metadata.

`subscriptions`
- Tracks which remote agents are subscribed.
- Stores local intent and last server confirmation.

`agents`
- Mirrored state for remote agents known through subscription or SSE.

`contexts`
- Mirrored current state of remote contexts.
- Keyed by remote `context_id`.

`votes`
- Mirrored current state of remote votes.
- Keyed by remote `vote_id`.

`vote_casts`
- Local record of score-cast requests initiated by this agent.
- Useful for auditability and duplicate prevention.

`event_journal`
- Stores processed SSE event ids or stable dedupe keys.
- Prevents duplicate application.

`stream_checkpoint`
- Stores last SSE checkpoint such as `event_id`, `sequence`, or server cursor.

`outbox`
- Stores deferred outbound write actions when immediate REST calls fail.
- Enables retry without losing intent.

## Data Boundary Rules

The local agent's own authored content may continue to use existing ZeroClaw
memory when appropriate for the agent's internal reasoning.

Remote peer content must follow different rules:

- remote `status` is not memory
- remote `context` is not memory
- remote `vote` is not memory
- remote `vote score` and `executable` changes are not memory
- SSE event history is not memory

ZeroClaw memory injection currently happens in:

- [`src/agent/loop_.rs`](./src/agent/loop_.rs)
- [`src/agent/memory_loader.rs`](./src/agent/memory_loader.rs)
- [`src/channels/mod.rs`](./src/channels/mod.rs)
- [`src/daemon/mod.rs`](./src/daemon/mod.rs)

Context Book integration must not hook into those memory-loading paths for peer
data. Access must be explicit through dedicated tools or service queries.

## External API Assumptions That Must Be Confirmed

The full implementation should not proceed beyond scaffolding until these are
verified against the real Context Book API.

- Does registration return a stable `agent_id`?
- Is approval a separate polling endpoint or part of registration fetch?
- Is `status` a dedicated REST endpoint or part of agent update?
- Are `context` and `vote` object ids client-generated or server-generated?
- Does SSE deliver a stable `event_id`, `sequence`, or cursor?
- Does SSE support resume via `Last-Event-ID` or equivalent?
- Is there a REST backfill API for missed events after disconnect?
- Are `delete` events tombstones or hard removals?
- Is `vote cast` idempotent per `(vote_id, caster_agent_id)`?
- Are subscriptions create-only, create/delete, or create/update?

If any of the resume/idempotency assumptions are false, the store and worker
design still stands, but additional reconciliation APIs will be required.

## Configuration Shape

Add a new section to [`src/config/schema.rs`](./src/config/schema.rs).

```toml
[context_book]
enabled = false
base_url = "https://context-book.example"
api_key = ""
agent_name = "zeroclaw-main"
agent_description = ""
register_on_start = true
set_active_on_start = true
set_inactive_on_shutdown = true
approval_poll_interval_secs = 30
sse_enabled = true
sse_path = "/events"
rest_timeout_secs = 15
connect_timeout_secs = 10
retry_backoff_secs = 5
max_retry_backoff_secs = 60
replay_on_reconnect = true
checkpoint_enabled = true
outbox_enabled = true
query_limit_default = 20
store_path = "context_book/state.db"
```

Fields can be adjusted after the real API is confirmed, but `enabled` must be
present from the first implementation slice.

## Runtime Ownership Model

The daemon should own the long-lived worker.

Tools should own the direct user- or agent-triggered REST actions.

The shared service handle should hide the difference.

Recommended ownership:

- daemon creates `ContextBookService`
- daemon passes shared handle into tool registry
- tools call service methods for immediate REST actions
- worker uses the same service for registration, retries, and SSE projection

This matches the existing shared-state guidance in
[`docs/architecture/adr-004-tool-shared-state-ownership.md`](./docs/architecture/adr-004-tool-shared-state-ownership.md).

## Session-by-Session Plan

Each session below is intentionally scoped to one shippable feature. The goal
is to make forward progress without mixing unrelated concerns.

---

## Session 1: Config and Module Skeleton

### Session 1: Config and Module Skeleton Goal

Create a no-op scaffold that can be compiled and configured, but does not yet
perform any network I/O.

### Session 1: Config and Module Skeleton Primary Modules

- [`src/config/schema.rs`](./src/config/schema.rs)
- [`src/config/mod.rs`](./src/config/mod.rs)
- [`src/lib.rs`](./src/lib.rs)
- `src/context_book/mod.rs`
- `src/context_book/types.rs`
- `src/context_book/service.rs`

### Session 1: Config and Module Skeleton Responsibilities

- Add `ContextBookConfig` to config schema.
- Export the new module from `lib.rs`.
- Add minimal public types and a disabled no-op service constructor.

### Session 1: Config and Module Skeleton In Scope

- config struct and defaults
- serde/json schema support
- compile-safe module wiring
- basic unit tests for default config

### Session 1: Config and Module Skeleton Out of Scope

- SQLite
- REST
- SSE
- daemon wiring
- tool registration

### Session 1: Config and Module Skeleton Acceptance Criteria

- Project compiles with `context_book.enabled = false` default.
- No behavior changes when the feature is not configured.
- New module can be imported from elsewhere.

### Session 1: Config and Module Skeleton Tests

- config default constructibility
- serde roundtrip for `ContextBookConfig`

---

## Session 2: Dedicated Local Store Foundation

### Session 2: Dedicated Local Store Foundation Goal

Create the separate persistence layer required by requirement 13.

### Session 2: Dedicated Local Store Foundation Primary Modules

- `src/context_book/store.rs`
- `src/context_book/types.rs`
- [`src/lib.rs`](./src/lib.rs)

### Session 2: Dedicated Local Store Foundation Responsibilities

- Create `workspace/context_book/state.db`.
- Define schema and migration bootstrap.
- Provide methods for:
  - registration state read/write
  - subscription upsert/list
  - agent mirror upsert
  - context mirror upsert/delete
  - vote mirror upsert/delete
  - event journal dedupe insert/check
  - checkpoint read/write
  - outbox enqueue/dequeue/mark-complete

### Session 2: Dedicated Local Store Foundation In Scope

- SQLite schema
- migration-safe initialization
- idempotent upsert helpers

### Session 2: Dedicated Local Store Foundation Out of Scope

- REST calls
- SSE parsing
- daemon worker

### Session 2: Dedicated Local Store Foundation Acceptance Criteria

- Store initializes on a clean workspace.
- Store reopens cleanly on restart.
- Duplicate event keys are rejected or ignored deterministically.

### Session 2: Dedicated Local Store Foundation Tests

- creates database and tables
- upsert paths preserve last-write-wins semantics
- event dedupe works across repeated inserts

---

## Session 3: REST DTOs and HTTP Client Foundation

### Session 3: REST DTOs and HTTP Client Foundation Goal

Implement a typed HTTP client without connecting it to the daemon or tools yet.

### Session 3: REST DTOs and HTTP Client Foundation Primary Modules

- `src/context_book/types.rs`
- `src/context_book/client.rs`
- [`src/config/schema.rs`](./src/config/schema.rs)

### Session 3: REST DTOs and HTTP Client Foundation Responsibilities

- Define REST DTOs for registration, status update, context CRUD, vote CRUD,
  vote cast, and subscribe.
- Add a reusable reqwest client wrapper with:
  - auth header injection
  - timeout config
  - JSON request/response handling
  - stable error mapping

### Session 3: REST DTOs and HTTP Client Foundation In Scope

- typed request builders
- typed response parsing
- endpoint path joining

### Session 3: REST DTOs and HTTP Client Foundation Out of Scope

- retries
- outbox processing
- daemon startup use
- tool exposure

### Session 3: REST DTOs and HTTP Client Foundation Acceptance Criteria

- Client methods exist for every required REST operation in requirements 2-6.
- Errors clearly distinguish transport failure, HTTP status failure, and parse
  failure.

### Session 3: REST DTOs and HTTP Client Foundation Tests

- URL joining tests
- auth header construction tests
- DTO serde tests

---

## Session 4: Agent Registration and Approval Polling

### Session 4: Agent Registration and Approval Polling Goal

Implement the local agent bootstrap flow against Context Book.

### Session 4: Agent Registration and Approval Polling Primary Modules

- `src/context_book/client.rs`
- `src/context_book/store.rs`
- `src/context_book/service.rs`
- `src/context_book/worker.rs`

### Session 4: Agent Registration and Approval Polling Responsibilities

- Register the local agent over REST.
- Persist remote registration id and approval state.
- Poll approval status until approved.
- Expose registration state via service API.

### Session 4: Agent Registration and Approval Polling In Scope

- registration REST call
- approval polling loop logic
- local persistence of approval state

### Session 4: Agent Registration and Approval Polling Out of Scope

- daemon supervisor
- active/inactive status push
- SSE connection
- tools

### Session 4: Agent Registration and Approval Polling Acceptance Criteria

- Registration is performed once and stored locally.
- Restart does not create duplicate registrations when a valid registration is
  already known.
- Approval state survives restart.

### Session 4: Agent Registration and Approval Polling Tests

- registration state persistence
- approval poll state transitions
- reusing stored registration id on restart

---

## Session 5: Status Lifecycle Publishing

### Session 5: Status Lifecycle Publishing Goal

Implement local agent status updates such as `active` and `inactive`.

### Session 5: Status Lifecycle Publishing Primary Modules

- `src/context_book/client.rs`
- `src/context_book/service.rs`
- `src/context_book/store.rs`
- `src/context_book/types.rs`

### Session 5: Status Lifecycle Publishing Responsibilities

- Add service method to push status immediately.
- Persist last locally intended status and last server-confirmed status.
- Define status enum mapping.

### Session 5: Status Lifecycle Publishing In Scope

- `set_status(active|inactive|...)`
- local persistence
- idempotent update shortcut when no real change is needed

### Session 5: Status Lifecycle Publishing Out of Scope

- daemon startup/shutdown hooks
- user-facing tool
- retry queue

### Session 5: Status Lifecycle Publishing Acceptance Criteria

- Status push works as an immediate REST action.
- Last known status is visible from local store.

### Session 5: Status Lifecycle Publishing Tests

- status enum serialization
- no-op when setting the same status twice
- persistence of intended vs confirmed status

---

## Session 6: Subscribe and Subscription Mirror

### Session 6: Subscribe and Subscription Mirror Goal

Implement local subscription management for other registered agents.

### Session 6: Subscribe and Subscription Mirror Primary Modules

- `src/context_book/client.rs`
- `src/context_book/store.rs`
- `src/context_book/service.rs`

### Session 6: Subscribe and Subscription Mirror Responsibilities

- Add `subscribe` operation over REST.
- Persist subscribed agent ids locally.
- Add basic query helpers for known subscriptions.

### Session 6: Subscribe and Subscription Mirror In Scope

- subscribe REST call
- local subscription table write
- list/get helpers

### Session 6: Subscribe and Subscription Mirror Out of Scope

- unsubscribe unless the real API supports it
- SSE processing
- remote agent state mirror

### Session 6: Subscribe and Subscription Mirror Acceptance Criteria

- A successful subscription is stored locally.
- Duplicate subscribe attempts are deduplicated locally.

### Session 6: Subscribe and Subscription Mirror Tests

- subscription dedupe
- subscription listing

---

## Session 7: Context CRUD Tools

### Session 7: Context CRUD Tools Goal

Expose local-agent context create, update, and delete as agent-callable tools.

### Session 7: Context CRUD Tools Primary Modules

- `src/context_book/client.rs`
- `src/context_book/service.rs`
- `src/tools/mod.rs`
- `src/tools/context_book_context_create.rs`
- `src/tools/context_book_context_update.rs`
- `src/tools/context_book_context_delete.rs`

### Session 7: Context CRUD Tools Responsibilities

- Add tools for local agent context publishing.
- Send REST immediately.
- Return structured tool output suitable for the LLM.

### Session 7: Context CRUD Tools In Scope

- tool schemas
- direct REST execution
- successful response formatting

### Session 7: Context CRUD Tools Out of Scope

- outbox fallback
- remote mirror queries
- SSE ingestion

### Session 7: Context CRUD Tools Acceptance Criteria

- Agent can publish, update, and delete Context Book contexts immediately.
- Tools are only registered when `context_book.enabled = true`.

### Session 7: Context CRUD Tools Tests

- tool parameter validation
- success/error output contract
- conditional tool registration

---

## Session 8: Vote CRUD Tools

### Session 8: Vote CRUD Tools Goal

Expose local-agent vote create, update, and delete as agent-callable tools.

### Session 8: Vote CRUD Tools Primary Modules

- `src/context_book/client.rs`
- `src/context_book/service.rs`
- `src/tools/mod.rs`
- `src/tools/context_book_vote_create.rs`
- `src/tools/context_book_vote_update.rs`
- `src/tools/context_book_vote_delete.rs`

### Session 8: Vote CRUD Tools Responsibilities

- Add vote CRUD tools.
- Keep tool behavior parallel to context CRUD tools.

### Session 8: Vote CRUD Tools In Scope

- tool schemas
- direct REST execution

### Session 8: Vote CRUD Tools Out of Scope

- vote cast
- outbox fallback
- SSE projection

### Session 8: Vote CRUD Tools Acceptance Criteria

- Vote CRUD is available through tools and executes immediately.

### Session 8: Vote CRUD Tools Tests

- parameter validation
- success/error handling

---

## Session 9: Vote Cast Tool

### Session 9: Vote Cast Tool Goal

Expose score-casting against another agent's vote.

### Session 9: Vote Cast Tool Primary Modules

- `src/context_book/client.rs`
- `src/context_book/service.rs`
- `src/tools/mod.rs`
- `src/tools/context_book_vote_cast.rs`

### Session 9: Vote Cast Tool Responsibilities

- Add tool to cast a score on a remote vote.
- Persist a local audit row in `vote_casts`.

### Session 9: Vote Cast Tool In Scope

- score-cast REST call
- local cast audit persistence

### Session 9: Vote Cast Tool Out of Scope

- mirrored score update via SSE
- duplicate prevention beyond local best effort

### Session 9: Vote Cast Tool Acceptance Criteria

- Tool can cast a score immediately.
- Local store records the cast request.

### Session 9: Vote Cast Tool Tests

- tool schema validation
- audit row insertion

---

## Session 10: Daemon Worker Bootstrap

### Session 10: Daemon Worker Bootstrap Goal

Wire Context Book into the supervised daemon lifecycle without SSE yet.

### Session 10: Daemon Worker Bootstrap Primary Modules

- [`src/daemon/mod.rs`](./src/daemon/mod.rs)
- `src/context_book/worker.rs`
- `src/context_book/service.rs`

### Session 10: Daemon Worker Bootstrap Responsibilities

- Add a new supervised `context_book` component.
- On startup:
  - initialize store
  - register if configured
  - poll approval if required
  - optionally set local agent status to `active`
- On shutdown:
  - optionally set status to `inactive`

### Session 10: Daemon Worker Bootstrap In Scope

- daemon supervisor wiring
- worker bootstrap lifecycle

### Session 10: Daemon Worker Bootstrap Out of Scope

- SSE connection
- outbox replay
- query tools

### Session 10: Daemon Worker Bootstrap Acceptance Criteria

- Daemon starts and supervises a `context_book` component when enabled.
- Disabled config results in no behavior change.

### Session 10: Daemon Worker Bootstrap Tests

- daemon component starts only when enabled
- bootstrap logic skips safely when disabled

---

## Session 11: SSE Transport and Reconnect Loop

### Session 11: SSE Transport and Reconnect Loop Goal

Implement the raw SSE reader and checkpoint-aware reconnect logic.

### Session 11: SSE Transport and Reconnect Loop Primary Modules

- `src/context_book/sse.rs`
- `src/context_book/types.rs`
- `src/context_book/store.rs`
- `src/context_book/worker.rs`

### Session 11: SSE Transport and Reconnect Loop Responsibilities

- Open SSE stream.
- Parse events and deserialize event payloads.
- Persist stream checkpoint metadata.
- Reconnect with backoff.
- Reuse `Last-Event-ID` or equivalent checkpoint if supported.

### Session 11: SSE Transport and Reconnect Loop In Scope

- transport-only SSE logic
- backoff
- checkpoint reads/writes

### Session 11: SSE Transport and Reconnect Loop Out of Scope

- applying events into mirrored state
- exposing event data to tools

### Session 11: SSE Transport and Reconnect Loop Acceptance Criteria

- Worker can stay connected and recover from disconnects.
- Last known checkpoint survives restart.

### Session 11: SSE Transport and Reconnect Loop Tests

- SSE frame parsing
- checkpoint persistence
- reconnect/backoff math

---

## Session 12: SSE Projection and Idempotent Mirror Updates

### Session 12: SSE Projection and Idempotent Mirror Updates Goal

Consume parsed SSE events and update local mirrored state without duplication.

### Session 12: SSE Projection and Idempotent Mirror Updates Primary Modules

- `src/context_book/projector.rs`
- `src/context_book/store.rs`
- `src/context_book/types.rs`

### Session 12: SSE Projection and Idempotent Mirror Updates Responsibilities

- Accept parsed events for:
  - agent status changes
  - context create/update/delete
  - vote create/update/delete
  - vote score/executable changes
- Generate stable dedupe keys.
- Record event journal entries.
- Apply state transitions to mirrored tables.

### Session 12: SSE Projection and Idempotent Mirror Updates In Scope

- event dedupe
- event application
- local mirror maintenance

### Session 12: SSE Projection and Idempotent Mirror Updates Out of Scope

- read/query tools
- prompt injection

### Session 12: SSE Projection and Idempotent Mirror Updates Acceptance Criteria

- Replayed SSE events do not create duplicate local state changes.
- Delete events remove or tombstone mirrored state consistently.

### Session 12: SSE Projection and Idempotent Mirror Updates Tests

- duplicate event replay
- out-of-order update handling
- create/update/delete projections

---

## Session 13: Outbox and Retry for Failed Immediate REST Writes

### Session 13: Outbox and Retry for Failed Immediate REST Writes Goal

Protect immediate write operations from transient network failure.

### Session 13: Outbox and Retry for Failed Immediate REST Writes Primary Modules

- `src/context_book/store.rs`
- `src/context_book/service.rs`
- `src/context_book/worker.rs`
- `src/context_book/client.rs`

### Session 13: Outbox and Retry for Failed Immediate REST Writes Responsibilities

- Queue failed immediate write requests into `outbox`.
- Add worker retry loop with backoff.
- Mark queued items completed only after confirmed success.

### Session 13: Outbox and Retry for Failed Immediate REST Writes In Scope

- outbox persistence
- retry execution
- bounded retry metadata

### Session 13: Outbox and Retry for Failed Immediate REST Writes Out of Scope

- advanced dead-letter flows
- operator CLI

### Session 13: Outbox and Retry for Failed Immediate REST Writes Acceptance Criteria

- Tool-initiated writes still return immediate failure information, but intent
  is retained for retry when configured.
- Retried writes are not sent repeatedly after success.

### Session 13: Outbox and Retry for Failed Immediate REST Writes Tests

- outbox enqueue on failure
- successful retry completion
- duplicate retry suppression

---

## Session 14: Read-Only Query Tools for Peer State

### Session 14: Read-Only Query Tools for Peer State Goal

Make mirrored peer-agent state accessible to the agent without using memory.

### Session 14: Read-Only Query Tools for Peer State Primary Modules

- `src/context_book/query.rs`
- `src/context_book/service.rs`
- `src/tools/mod.rs`
- `src/tools/context_book_query_agents.rs`
- `src/tools/context_book_query_contexts.rs`
- `src/tools/context_book_query_votes.rs`

### Session 14: Read-Only Query Tools for Peer State Responsibilities

- Add read-only tools for:
  - current subscribed agents and statuses
  - mirrored contexts
  - mirrored votes and vote scores
- Enforce query limits and compact output formatting.

### Session 14: Read-Only Query Tools for Peer State In Scope

- read-only store queries
- tool exposure for explicit access

### Session 14: Read-Only Query Tools for Peer State Out of Scope

- automatic prompt injection
- automatic memory hydration

### Session 14: Read-Only Query Tools for Peer State Acceptance Criteria

- Agent can explicitly inspect peer state when needed.
- No mirrored peer data is auto-written into memory.

### Session 14: Read-Only Query Tools for Peer State Tests

- query pagination/limit behavior
- tool output formatting
- no-memory side effect assertions where practical

---

## Session 15: Heartbeat and Cron Reference Integration

### Session 15: Heartbeat and Cron Reference Integration Goal

Allow heartbeat and scheduled work to reference Context Book state explicitly.

### Session 15: Heartbeat and Cron Reference Integration Primary Modules

- [`src/daemon/mod.rs`](./src/daemon/mod.rs)
- [`src/cron/scheduler.rs`](./src/cron/scheduler.rs)
- `src/context_book/service.rs`
- `src/context_book/query.rs`

### Session 15: Heartbeat and Cron Reference Integration Responsibilities

- Add helper APIs so heartbeat or cron-triggered workflows can consult mirrored
  peer state.
- Keep the access explicit and bounded.

### Session 15: Heartbeat and Cron Reference Integration In Scope

- helper calls for daemon/cron code paths
- optional prompt snippets built from explicit Context Book queries

### Session 15: Heartbeat and Cron Reference Integration Out of Scope

- automatic global prompt injection into every agent run
- general-purpose memory blending

### Session 15: Heartbeat and Cron Reference Integration Acceptance Criteria

- Heartbeat and cron flows can read Context Book state when explicitly asked to.
- Peer state remains outside the normal memory recall path.

### Session 15: Heartbeat and Cron Reference Integration Tests

- helper-level unit tests
- regression checks that existing memory context behavior remains unchanged

---

## Session 16: Observability, Diagnostics, and Docs

### Session 16: Observability, Diagnostics, and Docs Goal

Make the subsystem operable in production.

### Session 16: Observability, Diagnostics, and Docs Primary Modules

- `src/context_book/worker.rs`
- [`src/observability/**`](./src/observability)
- [`src/doctor/**`](./src/doctor)
- [`src/integrations/registry.rs`](./src/integrations/registry.rs)
- docs

### Session 16: Observability, Diagnostics, and Docs Responsibilities

- Add logs and observer events for:
  - registration state
  - approval transitions
  - SSE connect/disconnect
  - retry success/failure
  - checkpoint advancement
- Add doctor checks and integration registry entry.
- Document config and operational model.

### Session 16: Observability, Diagnostics, and Docs In Scope

- observability
- diagnostics
- docs

### Session 16: Observability, Diagnostics, and Docs Out of Scope

- protocol redesign
- schema redesign

### Session 16: Observability, Diagnostics, and Docs Acceptance Criteria

- Operators can tell whether Context Book is enabled, connected, approved, and
  current.
- Troubleshooting paths exist for the most common failures.

### Session 16: Observability, Diagnostics, and Docs Tests

- doctor checks
- integration registry status

## Recommended Delivery Order

Implement sessions in this order:

1. Session 1
2. Session 2
3. Session 3
4. Session 4
5. Session 5
6. Session 6
7. Session 10
8. Session 11
9. Session 12
10. Session 7
11. Session 8
12. Session 9
13. Session 13
14. Session 14
15. Session 15
16. Session 16

Rationale:

- the store and client are prerequisites for everything
- registration and daemon ownership should exist before SSE
- SSE mirroring should exist before query tools
- write tools should exist after the service contract stabilizes
- observability and docs should be last, after the operating model is real

## Session Discipline Rules

To preserve the one-session-one-feature rule:

- do not implement REST writes and SSE ingest in the same session
- do not add query tools in the same session as mirrored-state projection
- do not combine daemon wiring with tool registration unless the session goal is
  specifically lifecycle wiring
- do not touch ZeroClaw memory behavior in any Context Book session unless the
  sole task is asserting non-integration and regression coverage
- each session must end with a compile-clean state and focused tests

## Explicit Anti-Patterns

- Do not model Context Book as a `Memory` backend.
- Do not inject peer-agent data into `build_context()` or
  `DefaultMemoryLoader::load_context()`.
- Do not hide Context Book reads inside generic memory recall tools.
- Do not make SSE processing depend on channel or gateway runtime paths.
- Do not require the gateway to be running for Context Book daemon behavior.
- Do not create duplicate agent registrations on every restart.
- Do not assume SSE delivery is exactly-once.

## Done Definition for the Full Integration

The integration is complete only when all of the following are true:

- ZeroClaw can register with Context Book and survive restart without duplicate
  registration.
- ZeroClaw can publish status, contexts, votes, and vote casts immediately.
- ZeroClaw can subscribe to other agents.
- ZeroClaw can receive and project SSE updates for subscribed agents.
- Remote peer state is persisted in a dedicated store, not memory.
- Replayed or duplicated SSE events do not cause duplicate local processing.
- Heartbeat, cron, and user-triggered workflows can explicitly query mirrored
  peer state.
- The subsystem can be disabled cleanly through config with zero runtime impact
  on existing behavior.
