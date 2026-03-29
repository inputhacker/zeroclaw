# CONTEXT_BOOK_INTEGRATION_PLAN

Last updated: 2026-03-29
Repository: `zeroclaw`

## 1. Goal

`zeroclaw`에 Context Book 연동을 추가하되, 기존 아키텍처(모듈 분리, daemon supervisor, tool registry, memory 설계)를 최대한 유지한다.

핵심 목표:
- Context Book의 다른 agent 상태/context/vote를 구독/수집
- 수집 데이터는 `memory`에 즉시 주입하지 않고 별도 참조 저장소에 보존
- 로컬 context/vote CRUD 및 vote cast 지원
- heartbeat/cron/사용자 요청 시점에만 참조 데이터를 활용

## 2. Source Of Truth

아래 문서를 우선 순위대로 준수한다.

1. `ctxbk/docs/CONTEXT_BOOK_EXTERNAL_AGENT_SINGLE_FILE_GUIDE.md`
2. `ctxbk/docs/SPEC.md`
3. `ctxbk/docs/CONTEXT_BOOK_CLIENT_SPEC_AND_HOWTO.md`
4. `ctxbk/docs/CONTEXT_BOOK_SSE_PLUGIN_SPEC.md` (direct REST+SSE 기준)
5. `ctxbk/docs/CONTEXT_BOOK_DISCOVERY_SPEC.md` (discovery 사용 시)
6. `ctxbk/docs/EXTERNAL_AGENT_SSE_ADAPTER_TASKS.md` (task skeleton)

전제:
- Phase 2 이상 구현에 들어가기 전에 위 `ctxbk/docs/*` 문서가 현재 작업 트리 또는 팀이 합의한 참조 경로에서 실제로 접근 가능해야 한다.
- spec 원문 접근 경로가 없는 상태에서는 네트워크/protocol 세부 구현을 진행하지 않는다.

## 3. Non-Negotiable Contract Requirements

- `lifecycleState`와 `connectionState`를 분리 처리
- SSE는 `at-least-once` 전제로 `eventId` dedup 필수
- 재연결 시 `Last-Event-ID` 사용
- polling fallback은 SSE 불능 시에만 사용, 복구 즉시 중단
- heartbeat/keepalive는 transport-only로 취급(비즈니스 이벤트로 저장/전달 금지)
- `409 CURSOR_NOT_FOUND` 명시 처리(리플레이/커서 리셋 정책)
- `POST /agents/connect`의 `403 BOOTSTRAP_APPROVAL_REQUIRED`는 기존 identity의 connect-side reapproval으로 처리하고, 같은 request의 wait/complete 흐름으로 이어간다
- `desiredProducerAgentIds`와 `effectiveProducerAgentIds` 분리 유지
- transient disconnect 시 desired subscription 의도 보존
- `agent.connection.changed`, `vote.deleted` 명시 처리
- cast 유발 `vote.updated` owner-delivery 예외 반영

## 4. ZeroClaw Design Principle For Integration

기존 구조를 최대한 보존하기 위해 다음 원칙을 적용한다.

1. 신규 기능은 독립 모듈 `src/context_book/*`로 캡슐화한다.
2. daemon에는 독립 컴포넌트(`context_book`)를 supervisor로 추가한다.
3. 기존 `memory` 주입 경로(`agent/loop_.rs`)는 기본 동작을 유지한다.
4. Context Book 데이터는 별도 로컬 저장소(SQLite)에 보관한다.
5. 인증 정보(access/refresh token, bootstrap secret)는 cache DB에 직접 저장하지 않고 기존 `auth` / `security::SecretStore` 경로를 재사용한다.
6. worker와 tool이 공유하는 장수명 상태는 `Arc<RwLock<_>>` handle 패턴으로 주입한다.
7. LLM wake-up은 기본 비활성(sync-only)로 유지한다.
8. 현재 daemon shutdown 경로는 supervisor task를 `abort()`하는 구조이므로, graceful disconnect가 필요하면 `context_book`만의 우회 구현이 아니라 daemon 공통 종료 신호 계약(`CancellationToken` 또는 동등한 cooperative stop signal)을 먼저 정의한다.
9. 현재 health registry는 일반 component 상태(`status`, `last_ok`, `last_error`, `restart_count`)만 제공하므로, Context Book의 상세 freshness/last_sync/connection_state는 health schema 확장 또는 `doctor`의 store/runtime snapshot 직접 조회 중 하나를 명시적으로 선택해 노출한다.
10. persisted config schema는 기존 관례대로 `src/config/schema.rs`가 source of truth이며, `src/context_book/config.rs`는 별도 직렬화 루트가 아니라 runtime resolution/validation helper로 한정한다.
11. auth 재사용은 "토큰 저장소 재사용"과 "refresh 책임 재사용"을 구분해서 설계한다. 일반 bearer token 조회는 기존 `AuthService`를 우선 활용하되, refresh가 필요한 경우 Context Book 전용 auth profile kind를 `AuthService`에 추가할지, `context_book::client`가 refresh protocol을 소유할지 명시한다.
   또한 refresh protocol 자체는 spec 기준으로 Authorization Server의 OAuth refresh grant(`POST /oauth2/token`)를 기본 계약으로 두고, 레거시 `POST /auth/refresh`는 명시적 호환 모드로만 취급할지 여부를 Phase 2 전에 고정한다.
12. `ContextBookHandle`과 로컬 store/service 상태는 process-scoped singleton으로 생성하고, daemon/agent/tool 경로에 주입한다. tool이 자체적으로 별도 worker/store/client를 lazy-init하는 패턴은 금지한다.
13. 장수명 subscription worker의 소유권은 daemon에 둔다. daemon이 없는 one-shot CLI/isolated agent 경로는 기본적으로 service-only 모드로 동작하며, on-demand read/write는 허용하되 SSE subscription loop를 자동 기동하지 않는다.
14. 일반 사용자 요청에 대한 Context Book 활용은 Phase 1~5 기본 정책으로 "명시적 tool 호출"에 한정한다. 일반 chat turn의 system prompt / reference block 자동 주입은 별도 평가 전까지 도입하지 않는다.
15. `context_book::client`는 기존 ZeroClaw의 runtime proxy/egress 정책을 따라야 하며, 별도 우회 네트워크 경로를 만들지 않는다. outbound endpoint 검증은 connect/discovery 직후, 실제 I/O 전에 수행한다.

## 5. Requirement Mapping (User Req 1~6)

### Req 1: 디자인 훼손 최소화
- `src/context_book` 신규 모듈 + 기존 `daemon`, `config`, `tools` 최소 확장
- 기존 trait/factory/tool registry 패턴 유지
- 기존 auth/secrets/health/doctor 경로를 재사용하고 중복 저장소를 만들지 않음

### Req 2: 다른 agent 구독 및 상태/context/vote 수집
- `PUT /subscriptions`, `GET /subscriptions`
- `GET /events/stream?agentId=...` 소비
- control-plane/data-plane 라우팅 분리

### Req 3: memory 즉시 추가 금지, 필요 시 참조 보존
- `context_book_cache.db`에 context/vote/event snapshot 저장
- `memory_store` 자동 연동 금지
- heartbeat/cron/tool 호출 시 조회하여 참조

### Req 4: 로컬 context posting/update/delete
- `POST /contexts`, `PATCH /contexts/{contextId}`, `DELETE /contexts/{contextId}` 지원
- author/Active 제약 및 에러 코드 처리

### Req 5: context 기반 action 검토 후 vote posting/update/delete
- 로컬+원격 context 조회 후 정책 엔진(실행가능성/동의 판단)에서 vote 결정
- `POST /votes`, `PATCH /votes/{voteId}`, `DELETE /votes/{voteId}` 지원

### Req 6: 다른 agent vote에 대한 casting
- `POST /votes/{voteId}/cast` 지원
- owner cast 금지 및 중복 cast 방지 전처리

## 6. Target Architecture

### 6.1 New Module Layout

`src/context_book/`
- `mod.rs`: 공개 API 및 wiring
- `config.rs`: persisted schema 정의가 아니라, `src/config/schema.rs`의 `ContextBookConfig`를 바탕으로 한 runtime resolution/validation helper
- `handle.rs`: worker/tool/service 공유 상태 handle (`Arc<RwLock<_>>`)
- `client.rs`: REST+SSE 클라이언트, 인증/리프레시/재시도
- `events.rs`: 이벤트 파싱/분류(control/data/heartbeat)
- `store.rs`: 로컬 SQLite 캐시, cursor, dedup 인덱스, desired subscriptions
- `worker.rs`: daemon background loop (SSE consume, reconnect, polling fallback)
- `service.rs`: 툴/스케줄러에서 사용하는 상위 서비스 API
- `policy.rs`: vote/cast 실행 가능성 판단 규칙

추가 원칙:
- `Tool` 구현체는 직접 전역 상태를 만들지 않고 `ContextBookHandle`을 생성자 인자로 받는다.
- worker가 수집한 최신 runtime 상태(연결 상태, 마지막 sync 시각, 캐시 freshness)는 handle과 store를 통해 tool/doctor 경로에 노출한다.
- 필요 이상으로 새로운 trait를 만들지 말고, 실제 재사용 지점이 생길 때만 추상화한다.
- `ContextBookHandle` 생성 책임은 app bootstrap/wiring 레이어에 두고, `all_tools_with_runtime(...)`, daemon worker, 관련 service가 같은 handle/store를 공유한다.
- handle이 없다고 해서 tool 내부에서 새로운 worker/store를 만들지 않는다. handle 미주입 경로는 명시적으로 unavailable/read-only degraded 모드로 처리한다.

### 6.2 Daemon Integration

`src/daemon/mod.rs`
- `cron`, `heartbeat`와 동일한 supervisor 패턴으로 `context_book` worker 추가
- health component name: `context_book`
- daemon 시작 메시지와 state snapshot의 component 목록에도 `context_book` 반영
- shutdown 시 worker abort만 하지 말고 가능하면 best-effort graceful disconnect / lifecycle update 수행
- `doctor` 진단에서 `context_book`의 freshness / last sync / connection error surface를 확인 가능하게 함
- `context_book` SSE/poll worker는 daemon supervisor가 소유하는 유일한 장수명 subscription owner로 정의한다
- 이를 위해 Phase 2 전에 daemon 공통 종료 시그널 경로(`CancellationToken`, oneshot stop channel, 또는 동등한 cooperative shutdown 메커니즘)를 추가할지 여부를 먼저 확정한다
- 또한 진단 노출 방식은 아래 둘 중 하나를 선택한다:
  - health registry를 확장해 `context_book` 전용 상세 필드를 포함
  - health는 일반 liveness만 유지하고, `doctor`는 `context_book` store/runtime snapshot을 직접 읽어 세부 진단
- 첫 구현에서는 두 방식을 동시에 도입하지 않고 하나만 선택한다

### 6.3 Config Integration

`src/config/schema.rs`
- `Config`에 `context_book: ContextBookConfig` 추가
- 기본값은 `enabled=false`
- 추천 필드:
  - `enabled`
  - `manual_url` (manual override)
  - `discovery_enabled`
  - `service_type` (default `_contextbook._tcp.local.`)
  - `subscription_mode` (`manual|auto`)
  - `subscription_seed` (`[]` 또는 `["*"]`)
  - `forward_to_host` (default `false`)
  - `polling_fallback_enabled`
  - `reconnect_backoff_ms`, `max_reconnect_backoff_ms`
  - `cursor_not_found_policy` (`reset|fail`)
  - `bootstrap_secret_env_key` (default `CONTEXT_BOOK_BOOTSTRAP_SECRET`)
  - `auth_profile` 또는 동등한 credential selector
  - `allowed_hosts` (default `[]`; 빈 값은 명시적으로 비활성 상태로 간주하거나 manual/discovery 성공 전 validation 단계에서 실패 처리)
  - `allow_private_hosts` (default `false`)
  - `agent_identity_override.agent_id`
  - `agent_identity_override.device_type`
  - `agent_identity_override.display_name`

설정 계약:
- persisted serde/JsonSchema source of truth는 `src/config/schema.rs`의 `ContextBookConfig`이다
- `src/context_book/config.rs`가 필요하다면 이는 endpoint resolution, identity resolution, auth selector resolution 같은 runtime helper만 담당한다
- `agent_id`, `device_type`, `display_name`는 최상위에 흩뿌리지 말고 `agent_identity_override` 같은 명시적 하위 블록으로 묶는다.
- identity 우선순위는 `manual override > workspace/profile 기반 값 > 안정적인 기본값`으로 고정한다.
- 기존 ZeroClaw `identity` / workspace 개념과 충돌하지 않도록 "Context Book에 보고하는 런타임 식별자"와 "에이전트 페르소나/프롬프트 identity"를 분리한다.
- config 키 추가 시 `Default`, serde round-trip, 최소/기본 config 호환성, 문서화 범위를 계획에 포함한다.
- `allowed_hosts` / `allow_private_hosts`는 tool 계층 밖에서 동작하는 장수명 client에도 동일한 outbound 검증을 적용하기 위한 최소 계약으로 사용한다.
- proxy는 새 config 키를 만들지 않고 기존 runtime proxy 설정을 재사용한다. 필요하면 proxy service selector에 `context_book` 전용 key를 추가해 기존 `build_runtime_proxy_client*` 경로로 통합한다.

### 6.4 Tool Integration

`src/tools/mod.rs` registry에 Context Book tools 추가:
- `context_book_status`
- `context_book_subscriptions_get`
- `context_book_subscriptions_set`
- `context_book_context_create`
- `context_book_context_update`
- `context_book_context_delete`
- `context_book_vote_create`
- `context_book_vote_update`
- `context_book_vote_delete`
- `context_book_vote_cast`
- `context_book_contexts_query` (로컬 캐시/원격 조회 정책 포함)
- `context_book_votes_query` (로컬 캐시/원격 조회 정책 포함)

기본 정책:
- 읽기 툴은 로컬 캐시 우선, 필요 시 원격 read-through
- 쓰기 툴은 원격 성공 후 로컬 캐시 동기화
- tool registration 시 `ContextBookHandle` + `ContextBookService`를 주입하고, 실행 시에는 가능한 한 blocking validation을 피한다
- config 변경 또는 auth rotation 시 cached validation 상태를 무효화할 수 있어야 한다
- tool 이름 수는 많으므로 Phase 1~2에서는 `status`, `subscriptions_get`, `contexts_query`, `votes_query` 중심으로 먼저 열고, 쓰기 계열은 연결/권한 계약이 안정화된 뒤 추가한다
- 일반 user turn에서는 Context Book 데이터를 자동으로 프롬프트에 삽입하지 않고, `context_book_*` tool 또는 cron/heartbeat helper를 통해서만 참조한다.
- daemon 외 실행 경로에서 tool은 공유 handle/service를 사용하되, background subscription worker를 암묵적으로 시작하지 않는다.

### 6.5 Data Persistence Strategy

별도 DB 예시: `workspace/context_book/cache.db`

테이블(초안):
- `cb_runtime_state` (`agent_id`, `last_event_id`, `last_sync_at`, `last_connect_at`, `cursor_generation`, `updated_at`)
- `cb_desired_subscriptions` (`producer_agent_id`, `updated_at`)
- `cb_effective_subscriptions` (`producer_agent_id`, `updated_at`)
- `cb_events_seen` (`event_id`, `event_type`, `occurred_at`, `producer_agent_id`)
- `cb_context_snapshots` (`context_id`, `author_agent_id`, `title`, `contents`, `tag`, `status`, `raw_json`, `updated_at`)
- `cb_vote_snapshots` (`vote_id`, `owner_agent_id`, `vote_score`, `vote_context`, `voter_agent_ids_json`, `required_score`, `executable`, `raw_json`, `updated_at`)
- `cb_agent_snapshots` (`agent_id`, `lifecycle_state`, `connection_state`, `raw_json`, `updated_at`)

주의:
- heartbeat comment / bootstrap keepalive는 저장하지 않는다.
- 메모리 테이블과 분리한다.
- 기존 `memory::SqliteMemory::new_named()`를 재사용하지 않는다. 해당 구현은 `workspace/memory/*.db`와 memory schema를 전제로 하므로, Context Book은 별도 경로와 별도 schema를 갖는 독립 store를 구현한다.
- access/refresh token은 여기에 저장하지 않는다. 자격 증명은 기존 `auth` / `SecretStore`에 저장하고, cache DB에는 식별자/커서/상태만 둔다.
- `event dedup + snapshot upsert + cursor commit`은 하나의 transaction 경계 안에서 처리한다.
- SQLite는 WAL 모드와 명시적 schema init을 사용하고, worker/tool 동시 접근을 전제로 lock contention을 줄인다.
- 대용량 원문 payload는 필요한 경우에만 `raw_json`으로 저장하고, size cap / pruning 정책을 둔다.

## 7. Runtime Flow (Direct REST+SSE)

0. Outbound Policy Check
- manual override 또는 discovery 결과 endpoint는 실제 connect 전에 host validation을 통과해야 한다
- reqwest client는 기존 runtime proxy 경로를 사용한다
- private/local endpoint 허용 여부는 `allow_private_hosts` 계약을 따른다

1. Endpoint 결정
- 우선순위: manual override > discovery > fallback(정책 허용 시)

2. Bootstrap / Connect
- `POST /agents/connect` 우선
- `403 BOOTSTRAP_APPROVAL_REQUIRED` + request-scoped wait metadata가 오면, 기존 identity에 대한 connect-side reapproval로 간주하고 `register/init`로 되돌아가지 않은 채 동일 request의 wait/complete 흐름을 이어간다
- `404 AGENT_NOT_REGISTERED` 시 `POST /bootstrap/register/init`
- 필요 시 request-scoped wait(`status` 또는 `watch`) 후 `complete`
- 토큰 획득 후 `PATCH /agents/{agentId}/status` -> `Active`
- 토큰 저장은 기존 auth/secrets 계층을 재사용한다
- bearer token 조회는 기존 `AuthService` 경로를 우선 사용한다
- refresh 책임은 구현 전에 명시적으로 둘 중 하나를 선택한다:
  - `AuthService`에 Context Book용 profile kind/refresh 로직 추가
  - `context_book::client`가 refresh protocol을 소유하되, 저장은 기존 auth/secrets 계층에 위임
- refresh endpoint/protocol 계약은 Phase 2 전에 문서로 고정한다:
  - 기본값: Authorization Server의 OAuth refresh grant (`POST /oauth2/token`)
  - 선택적 호환 모드: legacy `POST /auth/refresh`를 deployment capability가 명시된 경우에만 허용
- 어떤 방식을 택하든 cache DB에는 token/refresh_token/bootstrap secret을 저장하지 않는다

2.5 Post-Bootstrap Contract Validation
- discovery 또는 manual endpoint로 연결한 뒤 full behavior를 켜기 전에 runtime API contract를 검증한다
- 최소 확인 항목:
  - `lifecycleState` / `connectionState` 분리 노출
  - `desiredProducerAgentIds` / `effectiveProducerAgentIds` 동시 노출
  - `agent.status.changed` 와 `agent.connection.changed` 구분
  - `vote.deleted` 지원
  - unknown resume cursor에 대한 `409 CURSOR_NOT_FOUND`
- discovery metadata와 runtime behavior가 불일치하면 runtime behavior를 기준으로 판단하고, full feature enable 대신 degraded/read-only 또는 disconnect를 선택할 수 있어야 한다

3. Runtime SSE
- `GET /events/stream?agentId=...`
- resume 시 `Last-Event-ID` 사용
- 이벤트 dedup 후 처리 경계에서 cursor 커밋

4. Reconnect / Poll Fallback
- SSE 실패 시 bounded backoff
- SSE unavailable 기간에만 `GET /events?sinceEventId=...`
- SSE 복구 시 polling 중단

5. Event Routing
- control-plane: agent/subscription 상태 캐시 업데이트
- data-plane: context/vote snapshot 캐시 업데이트
- 기본은 sync-only (live host forwarding off)

6. Shutdown / Restart
- daemon shutdown 시 best-effort로 local agent 상태를 `Inactive` 또는 spec이 요구하는 종료 상태로 전이
- abrupt abort에도 desired subscription과 cursor는 복구 가능해야 함
- restart 후에는 persisted desired subscription / cursor / auth profile을 사용해 idempotent resume

## 8. Implementation Phases

### Phase 1 (Foundation)
- [x] `context_book` config + module skeleton + store + status tool
- [x] daemon worker skeleton + health integration + doctor surface
- [x] no-op SSE loop/diagnostics 먼저 통과
- [x] shared handle/wiring 규약 확정
- [x] bootstrap/ownership 계약 확정: daemon, `agent::run`, `AgentBuilder`, 기타 tool registry 생성 경로가 어떤 방식으로 동일 handle을 주입받는지 먼저 고정
- [x] non-daemon 경로는 service-only 모드이며 SSE worker auto-start 금지 규칙을 문서와 코드에 함께 반영
- [x] graceful shutdown, doctor 상세 노출, auth refresh 책임 중 무엇을 어디서 소유하는지 계약을 먼저 문서에 고정
- [x] outbound proxy/host validation 계약을 함께 고정
- [x] config default / serde round-trip 테스트 추가

Phase 1 current status (2026-03-29):
- 완료된 3개 구현 묶음: config/runtime helper + module/store/status tool foundation
- 완료된 3개 구현 묶음: daemon no-op worker + health/doctor/state file integration
- 완료된 3개 구현 묶음: shared handle/service-only wiring contract
- 완료된 3개 구현 묶음: shared bootstrap API + handle refresh contract + tool/daemon ownership wiring 고정
- 완료된 3개 구현 묶음: daemon cooperative shutdown token + context_book graceful stop persistence + state writer stop 경로 추가
- 완료된 3개 구현 묶음: doctor 상세 진단 확장 + auth refresh ownership/protocol contract 고정
- 다음 턴 시작 지점: `Phase 2`의 첫 작업인 `bootstrap/connect/refresh + SSE consume + dedup + cursor persistence`부터 진행한다.

Validation completed on 2026-03-29:
- `cargo fmt --all`
- `cargo check --lib --tests`
- `cargo test context_book_config_round_trips --lib`
- `cargo test status_tool_reports_runtime_snapshot --lib`
- `cargo test worker_persists_idle_runtime_state --lib`
- `cargo test state_file_path_uses_config_directory --lib`
- `cargo test bootstrap_refreshes_resolved_contract_for_existing_handle --lib`
- `cargo test worker_persists_stopped_state_on_shutdown_signal --lib`
- `cargo test daemon_state_reports_context_book_contract_details --lib`
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets -- -D warnings` → 현재 작업과 무관한 기존 lint (`src/security/firejail.rs:163`, `inefficient_to_string`)로 실패. 이번 변경 범위를 벗어나므로 미수정.

### Phase 2 (Connectivity)
- bootstrap/connect/refresh + SSE consume + dedup + cursor persistence
- `connect`의 `403 BOOTSTRAP_APPROVAL_REQUIRED` reapproval flow 반영
- polling fallback + `409 CURSOR_NOT_FOUND` 처리
- auth/secrets 재사용 경로 연결
- refresh endpoint/protocol 계약(`oauth2/token` 기본, legacy `/auth/refresh` 호환 여부) 구현 반영
- graceful shutdown / resume 계약 반영
- runtime proxy + outbound host validation 경로 연결
- post-bootstrap contract validation 및 degraded 모드 결정 경로 연결

### Phase 3 (Subscriptions + Read Path)
- desired/effective subscription 관리
- events 기반 캐시 동기화
- query tools (`contexts/votes/subscriptions/status`)
- read-through 정책과 cache freshness 표면화
- doctor/health에 stale or degraded 상태 반영

### Phase 4 (Write Path)
- context CRUD 툴/서비스
- vote CRUD + cast 툴/서비스
- 권한/제약(owner, Active, duplicate cast) 처리
- 원격 성공 후 로컬 캐시 sync의 실패 보정 규칙 정의

### Phase 5 (Policy + Scheduler/Heartbeat Hook)
- 로컬+원격 context 기반 vote/cast 의사결정 헬퍼
- heartbeat/cron에서 필요 시 참조하는 read helper 추가
- memory 비침투 유지 검증
- 기본 정책은 cron/heartbeat helper 또는 명시적 tool 조회로만 제한한다
- 일반 user chat turn에 대한 자동 reference block/system prompt 주입은 이번 범위에서 제외한다

### Phase 6 (Conformance Tests + Hardening)
- resume/dedup/disconnect/subscription persistence tests
- cast 예외 케이스 검증
- observability/log redaction/성능 점검
- SQLite contention / transaction 경계 / restart recovery 검증
- config reload 또는 credential rotation 이후 재검증 동작 확인

## 9. Testing & Verification Checklist

필수 테스트:
1. 빈 상태 bootstrap 성공
2. SSE 재연결 시 `Last-Event-ID` resume
3. replay duplicate가 side effect 중복을 만들지 않음
4. heartbeat 프레임이 비즈니스 저장/LLM 입력으로 가지 않음
5. transient disconnect 후 desired subscriptions 유지
6. `lifecycleState`/`connectionState` 분리 진단 확인
7. `409 CURSOR_NOT_FOUND` 정책 동작 검증
8. `vote.deleted` 이벤트 명시 처리
9. cast 유발 `vote.updated` owner 예외 처리
10. context/vote 정보가 memory에 자동 주입되지 않음
11. token/secret이 cache DB나 일반 로그에 평문으로 남지 않음
12. worker와 tool의 동시 접근에서도 DB lock/partial commit으로 cursor 유실이 발생하지 않음
13. daemon restart 이후 desired subscription / last_event_id / auth profile로 정상 복구됨
14. config TOML round-trip과 default config 호환성이 유지됨
15. shutdown 시 best-effort disconnect 또는 상태 전이가 실행됨

권장 명령:
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`

## 10. Risk & Mitigation

리스크 티어:
- **High risk**
- 이유: 장수명 네트워크 worker, daemon supervisor 변경, tool registry 확장, auth/secrets 연동, 외부 상태 동기화가 함께 포함됨

리스크:
- SSE 재연결/커서 경계 버그로 중복 처리 가능
- subscription 의도 손실
- token/secret 로그 노출
- memory 오염(요구사항 3 위반)
- tool/worker shared state 경계 불명확으로 stale state 또는 multi-client 누수 발생 가능
- tool registry가 여러 경로에서 생성되는 구조와 충돌해 중복 worker / 분리된 handle/store가 생길 수 있음
- SQLite write contention으로 partial sync / cursor commit skew 가능
- daemon abort 기반 종료로 graceful disconnect 불능
- health/doctor 진단 경계가 모호해 잘못된 정상 판정 또는 stale 판정 가능
- auth 재사용 범위가 불명확해 token refresh 책임 중복 또는 누락 가능
- daemon 밖 장수명/온디맨드 client가 기존 proxy/egress 정책을 우회할 수 있음

완화:
- dedup + idempotent upsert
- desired/effective 분리 저장
- 토큰/secret redaction 강제
- context_book cache와 memory 경로 물리 분리
- auth/secrets 재사용으로 credential 저장 중복 제거
- handle pattern + explicit transaction boundary + WAL 사용
- process-scoped singleton handle + daemon-only subscription ownership 고정
- cooperative shutdown contract 명시 후 구현
- health와 doctor의 역할 분리를 먼저 고정
- auth 저장/조회/refresh 책임을 한 계층에만 두고 중복 구현 금지
- 기존 runtime proxy + outbound host validation을 `context_book::client`에 강제

## 11. Scope Control

이번 연동에서 제외:
- dashboard private route 의존
- speculative federation/분산 합의 확장
- 기존 memory backend 구조 변경
- host live forwarding 기본 활성화
- tool execution context 전면 개편(`ClientId` 도입 자체)은 이번 작업 범위에서 제외
- 범용 daemon framework 대수술은 제외하되, `context_book` graceful shutdown에 필수적인 최소한의 cooperative stop signal 추가는 범위에 포함

## 12. Documentation & Contract Update

구현과 함께 반드시 반영:

1. `src/config/schema.rs`
- `ContextBookConfig` 추가
- `Default` 반영
- serde/round-trip 테스트 추가

2. `src/tools/mod.rs`
- Context Book tool registry wiring
- shared handle 생성/주입 구조 반영

3. 운영/참고 문서
- 필요 시 `docs/reference/api/config-reference.md`에 새 config 섹션 추가
- daemon/doctor 출력이 바뀌면 관련 reference 또는 ops 문서 갱신
- 보안/로그 redaction 영향이 있으면 PR notes에 rollback 및 risk 명시

## 13. Session Restart Protocol

다음 세션 시작 시 아래 순서로 진행:

1. 이 문서(`CONTEXT_BOOK_INTEGRATION_PLAN.md`) 확인
2. `Phase 1`부터 순차 구현
3. 구현 전에 `auth/secrets 재사용`, `shared handle`, `identity precedence`, `graceful shutdown 계약`, `doctor/health 경계` 다섯 항목이 코드 구조에 반영되는지 먼저 확인
4. 추가로 `daemon-only worker ownership`, `general user turn은 tool-only integration`, `outbound proxy/host validation`, `ctxbk spec 접근성` 네 항목을 먼저 확인
5. 각 Phase 종료 시 체크리스트와 테스트 결과 업데이트
6. 변경된 파일/동작/리스크를 문서에 즉시 반영
