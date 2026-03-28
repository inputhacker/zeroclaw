# CONTEXT_BOOK_INTEGRATION_ARCH_DIAGRAM

Last updated: 2026-03-29
Based on: `CONTEXT_BOOK_INTEGRATION_PLAN.md`

## 1. Text-Based Architecture Diagram

```text
+-----------------------------------------------------------------------------------+
|                                   ZeroClaw Process                                |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  App Bootstrap / Wiring                                                           |
|  - creates process-scoped singleton ContextBookHandle                             |
|  - injects same handle into daemon, services, tools                               |
|  - no lazy secondary worker/store/client creation inside tools                    |
|                                                                                   |
|  +---------------------------+          +--------------------------------------+   |
|  | Daemon Supervisor         |          | Non-Daemon Paths                     |   |
|  | src/daemon/mod.rs         |          | agent::run / one-shot CLI / tools    |   |
|  |                           |          |                                      |   |
|  | owns the only long-lived  |          | service-only mode                    |   |
|  | Context Book worker       |          | no auto-start SSE subscription loop  |   |
|  +------------+--------------+          +------------------+-------------------+   |
|               |                                            |                       |
|               v                                            v                       |
|  +--------------------------------------------------------------------------------+
|  |                         src/context_book/*                                      |
|  |                                                                                |
|  |  +--------------------+    +--------------------+    +----------------------+  |
|  |  | handle.rs          |<-->| service.rs         |<-->| tools/context_book_* |  |
|  |  | Arc<RwLock<_>>     |    | high-level API     |    | query / CRUD / cast  |  |
|  |  | runtime state      |    | for tools/sched    |    | cache-first reads    |  |
|  |  +---------+----------+    +---------+----------+    +----------------------+  |
|  |            ^                         ^                                          |
|  |            |                         |                                          |
|  |  +---------+----------+    +---------+----------+                               |
|  |  | worker.rs          |--->| policy.rs          |                               |
|  |  | SSE / reconnect /  |    | vote / cast rules  |                               |
|  |  | poll fallback      |    +--------------------+                               |
|  |  +---------+----------+                                                        |
|  |            |                                                                   |
|  |            v                                                                   |
|  |  +---------+----------+    +--------------------+    +----------------------+  |
|  |  | events.rs          |--->| store.rs           |<-->| config.rs            |  |
|  |  | parse / classify   |    | SQLite cache       |    | runtime resolution   |  |
|  |  | control/data/hb    |    | dedup + cursor     |    | endpoint/identity    |  |
|  |  +---------+----------+    +---------+----------+    +----------------------+  |
|  |            |                         ^                                          |
|  |            v                         |                                          |
|  |  +---------+----------+              |                                          |
|  |  | client.rs          |--------------+                                          |
|  |  | REST + SSE client  |                                                         |
|  |  | auth / refresh /   |                                                         |
|  |  | retry / proxy      |                                                         |
|  |  +--------------------+                                                         |
|  +--------------------------------------------------------------------------------+
|                                                                                   |
|  Shared Platform Integrations                                                     |
|  - src/config/schema.rs: ContextBookConfig source of truth                        |
|  - auth / security::SecretStore: token/bootstrap secret storage                   |
|  - doctor / health: freshness, last_sync, connection_state exposure               |
|  - runtime proxy + outbound host validation: enforced before connect / I/O        |
|                                                                                   |
+-----------------------------------------------------------------------------------+
                 |                                       |
                 | REST / SSE                            | credentials only
                 v                                       v
+-----------------------------------+        +--------------------------------------+
| External Context Book Runtime     |        | Existing ZeroClaw Auth / Secrets     |
| - POST /agents/connect            |        | - token lookup / storage             |
| - GET /events/stream              |        | - refresh ownership decided once     |
| - GET /events?sinceEventId=...    |        | - never stored in context_book DB    |
| - /contexts / /votes / /cast      |        +--------------------------------------+
| - desired/effective subscriptions |
+-----------------------------------+
                 |
                 v
+-----------------------------------------------------------------------------------+
| Local Context Book Cache DB: workspace/context_book/cache.db                      |
|-----------------------------------------------------------------------------------|
| cb_runtime_state                                                                  |
| cb_desired_subscriptions                                                          |
| cb_effective_subscriptions                                                        |
| cb_events_seen                                                                    |
| cb_context_snapshots                                                              |
| cb_vote_snapshots                                                                 |
| cb_agent_snapshots                                                                |
|                                                                                   |
| Rules                                                                             |
| - separate from memory DB                                                         |
| - no token / refresh_token / bootstrap secret                                     |
| - dedup + snapshot upsert + cursor commit in one transaction                      |
| - heartbeat / keepalive not stored                                                |
+-----------------------------------------------------------------------------------+

Primary behavioral boundaries
1. Context Book data does not auto-flow into `memory`; it is only referenced by tool/cron/heartbeat helpers.
2. `lifecycleState` and `connectionState` stay distinct in cache, runtime state, and diagnostics.
3. `desiredProducerAgentIds` and `effectiveProducerAgentIds` stay separately persisted.
4. The daemon owns the only long-lived subscription worker; non-daemon paths remain on-demand.
5. SSE is primary, polling is fallback-only, and resume always prefers `Last-Event-ID`.
```

## 2. Mermaid Architecture Diagram

```mermaid
flowchart TB
    subgraph ZP[ZeroClaw Process]
        AB[App Bootstrap / Wiring]
        CFG[src/config/schema.rs\nContextBookConfig source of truth]
        AUTH[auth / security::SecretStore\ncredential storage]
        DOC[doctor / health\nstatus, freshness, last sync]
        PROXY[runtime proxy + outbound\nhost validation]

        subgraph DAEMON[Daemon Path]
            D[daemon supervisor]
            W[context_book::worker\nSSE owner]
        end

        subgraph NONDAEMON[Non-Daemon Path]
            ND[agent::run / one-shot CLI / tool execution]
        end

        subgraph CB[src/context_book]
            H[handle.rs\nContextBookHandle]
            S[service.rs]
            T[context_book_* tools]
            P[policy.rs]
            C[client.rs]
            E[events.rs]
            ST[store.rs\nSQLite cache]
            RTCFG[config.rs\nruntime resolver]
        end
    end

    subgraph EXT[External Context Book Runtime]
        CONNECT[connect/bootstrap]
        STREAM[SSE stream]
        POLL[poll fallback]
        API[contexts / votes / cast / subscriptions]
    end

    subgraph CACHE[workspace/context_book/cache.db]
        RS[cb_runtime_state]
        DS[cb_desired_subscriptions]
        ES[cb_effective_subscriptions]
        EV[cb_events_seen]
        CS[cb_context_snapshots]
        VS[cb_vote_snapshots]
        AG[cb_agent_snapshots]
    end

    AB --> H
    AB --> D
    AB --> S
    AB --> T
    CFG --> RTCFG
    RTCFG --> C
    AUTH --> C
    PROXY --> C

    D --> W
    W --> H
    W --> C
    W --> E
    E --> ST
    C --> ST
    ST --> H
    S --> H
    S --> ST
    T --> S
    S --> P
    ND --> S

    C --> CONNECT
    C --> STREAM
    C -. fallback only .-> POLL
    S --> API

    ST --> RS
    ST --> DS
    ST --> ES
    ST --> EV
    ST --> CS
    ST --> VS
    ST --> AG

    H --> DOC
    ST --> DOC

    classDef guard fill:#fff4db,stroke:#9a6700,color:#3b2f00;
    classDef state fill:#eaf4ff,stroke:#1f6feb,color:#0b306a;
    classDef runtime fill:#eefbea,stroke:#1a7f37,color:#0e4429;

    class CFG,AUTH,DOC,PROXY guard;
    class H,S,T,P,C,E,ST,RTCFG runtime;
    class RS,DS,ES,EV,CS,VS,AG state;
```

## 3. Runtime Flow Summary

```text
1. Bootstrap creates a single ContextBookHandle and shared store/service wiring.
2. Daemon supervisor starts the only long-lived Context Book worker.
3. Worker resolves endpoint/config, validates outbound policy, then connects.
4. Bootstrap/connect uses existing auth/secrets flow; cache DB stores state only.
5. Runtime consumes SSE with Last-Event-ID resume and eventId dedup.
6. On SSE failure, bounded polling fallback runs only until SSE recovers.
7. Event handling updates snapshots and cursor atomically in the cache DB.
8. Tools read cache-first, optionally use remote read-through, and sync writes after remote success.
9. Doctor/health reads handle/store state to report freshness, sync status, and degraded conditions.
10. General chat flow does not auto-inject Context Book data into memory or prompts.
```
