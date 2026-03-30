---
name: context-book-discovery
description: Discover a Context Book endpoint for an external agent or adapter using mDNS/DNS-SD and a deterministic helper script. Use when a runtime needs the base URL before bootstrap or SSE integration, and keep this additive instead of changing in-repo rust_agent, nano_agent, or other built-in discovery flows.
---

# Context Book Endpoint Discovery

Use this skill only to resolve a Context Book endpoint for external agents.

This is a working helper skill. It resolves the upstream base URL but does not implement runtime SSE handling.

This skill is additive. Do not change the built-in discovery implementations under:
- `agents/rust_agent`
- `agents/nano_agent`
- `context_book_sse_client/factory`

## Deterministic Script Entry Point
Use:
- `scripts/context_book_discovery.py`

The script:
- browses `_contextbook._tcp.local.` via `avahi-browse`
- filters candidates using the public discovery contract
- prefers env/priority/weight-compatible instances
- optionally preflights reachability before returning the endpoint
- uses `CONTEXT_BOOK_URL` as an explicit manual override when configured, before discovery

Runtime prerequisites:
- the helper currently depends on `avahi-browse` for mDNS/DNS-SD discovery
- if `avahi-browse` is unavailable, use `--manual-url` or `CONTEXT_BOOK_URL` as the explicit endpoint handoff

Read `../../../docs/CONTEXT_BOOK_DISCOVERY_SPEC.md` only when changing discovery policy or TXT interpretation.

## Commands
- `discover`: return the selected endpoint
- `report`: print the candidate set, filtering reasons, and selected endpoint

Examples:
```bash
python3 scripts/context_book_discovery.py discover
python3 scripts/context_book_discovery.py --preferred-env dev report
python3 scripts/context_book_discovery.py --require-feature events_resume discover
CONTEXT_BOOK_URL=http://localhost:8080 python3 scripts/context_book_discovery.py discover
```

Precedence:
- explicit manual endpoint override via `--manual-url` or `CONTEXT_BOOK_URL`
- discovered endpoint
- no helper-selected fallback beyond that; any additional fallback is the embedding runtime's responsibility

## Selection Rules
- require `service=context-book`
- require `ver=1`
- require `api` to include both `rest` and `sse`
- prefer matching `env`
- prefer lower `priority`
- prefer higher `weight`
- use `instance_id` or service fullname as deterministic tie-breaker
- enforce TXT `features` only when the caller passes `--require-feature`
- `--reject-local-trust` rejects candidates advertising `bootstrap=trusted-network+shared-secret`

Current implementation note:
- the running server may advertise only the base TXT keys plus a smaller `features` set
- treat `features` as preference unless the caller explicitly requires them

## After Discovery
- use the sibling `context-book` skill for bootstrap, activation, subscriptions, REST, and event handling
- validate runtime behavior before assuming the full corrected contract: split `lifecycleState`/`connectionState`, desired/effective subscriptions, explicit `agent.connection.changed`, explicit `vote.deleted`, and `409 CURSOR_NOT_FOUND` for stale cursors
