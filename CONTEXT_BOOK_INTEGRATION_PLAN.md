# CONTEXT_BOOK_INTEGRATION_PLAN

Last updated: 2026-03-28
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

## 3. Non-Negotiable Contract Requirements

- `lifecycleState`와 `connectionState`를 분리 처리
- SSE는 `at-least-once` 전제로 `eventId` dedup 필수
- 재연결 시 `Last-Event-ID` 사용
- polling fallback은 SSE 불능 시에만 사용, 복구 즉시 중단
- heartbeat/keepalive는 transport-only로 취급(비즈니스 이벤트로 저장/전달 금지)
- `409 CURSOR_NOT_FOUND` 명시 처리(리플레이/커서 리셋 정책)
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
- `config.rs`: Context Book 전용 설정 모델/검증
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

### 6.2 Daemon Integration

`src/daemon/mod.rs`
- `cron`, `heartbeat`와 동일한 supervisor 패턴으로 `context_book` worker 추가
- health component name: `context_book`
- daemon 시작 메시지와 state snapshot의 component 목록에도 `context_book` 반영
- shutdown 시 worker abort만 하지 말고 가능하면 best-effort graceful disconnect / lifecycle update 수행
- `doctor` 진단에서 `context_book`의 freshness / last sync / connection error surface를 확인 가능하게 함

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
  - `agent_identity_override.agent_id`
  - `agent_identity_override.device_type`
  - `agent_identity_override.display_name`

설정 계약:
- `agent_id`, `device_type`, `display_name`는 최상위에 흩뿌리지 말고 `agent_identity_override` 같은 명시적 하위 블록으로 묶는다.
- identity 우선순위는 `manual override > workspace/profile 기반 값 > 안정적인 기본값`으로 고정한다.
- 기존 ZeroClaw `identity` / workspace 개념과 충돌하지 않도록 "Context Book에 보고하는 런타임 식별자"와 "에이전트 페르소나/프롬프트 identity"를 분리한다.
- config 키 추가 시 `Default`, serde round-trip, 최소/기본 config 호환성, 문서화 범위를 계획에 포함한다.

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
- access/refresh token은 여기에 저장하지 않는다. 자격 증명은 기존 `auth` / `SecretStore`에 저장하고, cache DB에는 식별자/커서/상태만 둔다.
- `event dedup + snapshot upsert + cursor commit`은 하나의 transaction 경계 안에서 처리한다.
- SQLite는 WAL 모드와 명시적 schema init을 사용하고, worker/tool 동시 접근을 전제로 lock contention을 줄인다.
- 대용량 원문 payload는 필요한 경우에만 `raw_json`으로 저장하고, size cap / pruning 정책을 둔다.

## 7. Runtime Flow (Direct REST+SSE)

1. Endpoint 결정
- 우선순위: manual override > discovery > fallback(정책 허용 시)

2. Bootstrap / Connect
- `POST /agents/connect` 우선
- `404 AGENT_NOT_REGISTERED` 시 `POST /bootstrap/register/init`
- 필요 시 request-scoped wait(`status` 또는 `watch`) 후 `complete`
- 토큰 획득 후 `PATCH /agents/{agentId}/status` -> `Active`
- 토큰 저장/갱신은 기존 auth/secrets 계층에서 수행하고, worker는 auth service를 통해 bearer token을 조회한다

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
- `context_book` config + module skeleton + store + status tool
- daemon worker skeleton + health integration + doctor surface
- no-op SSE loop/diagnostics 먼저 통과
- shared handle/wiring 규약 확정
- config default / serde round-trip 테스트 추가

### Phase 2 (Connectivity)
- bootstrap/connect/refresh + SSE consume + dedup + cursor persistence
- polling fallback + `409 CURSOR_NOT_FOUND` 처리
- auth/secrets 재사용 경로 연결
- graceful shutdown / resume 계약 반영

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
- cron/heartbeat가 기존 memory recall과 별개로 Context Book reference block을 주입할지, 아니면 tool 조회로만 제한할지 명시

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

리스크:
- SSE 재연결/커서 경계 버그로 중복 처리 가능
- subscription 의도 손실
- token/secret 로그 노출
- memory 오염(요구사항 3 위반)
- tool/worker shared state 경계 불명확으로 stale state 또는 multi-client 누수 발생 가능
- SQLite write contention으로 partial sync / cursor commit skew 가능

완화:
- dedup + idempotent upsert
- desired/effective 분리 저장
- 토큰/secret redaction 강제
- context_book cache와 memory 경로 물리 분리
- auth/secrets 재사용으로 credential 저장 중복 제거
- handle pattern + explicit transaction boundary + WAL 사용

## 11. Scope Control

이번 연동에서 제외:
- dashboard private route 의존
- speculative federation/분산 합의 확장
- 기존 memory backend 구조 변경
- host live forwarding 기본 활성화
- tool execution context 전면 개편(`ClientId` 도입 자체)은 이번 작업 범위에서 제외

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
3. 구현 전에 `auth/secrets 재사용`, `shared handle`, `identity precedence` 세 항목이 코드 구조에 반영되는지 먼저 확인
4. 각 Phase 종료 시 체크리스트와 테스트 결과 업데이트
5. 변경된 파일/동작/리스크를 문서에 즉시 반영
