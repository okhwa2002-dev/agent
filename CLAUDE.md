# Edge Agent — 실시간 메시지 수집 에이전트

단말기가 MQTT로 발행하는 JSON 메시지를 엣지에서 실시간 수신하여 **원본 보관 → 단말 식별 → 업무별 파생 저장**을 단일 프로세스로 처리하고, 각 단계 오류를 추적 가능한 형태로 기록하는 에이전트.

- 설계 문서: [docs/superpowers/specs/2026-06-09-realtime-message-pipeline-design.md](docs/superpowers/specs/2026-06-09-realtime-message-pipeline-design.md)
- 구현 계획: [docs/superpowers/plans/](docs/superpowers/plans/)
- MQTT 발행·테스트 사용법: [docs/mqtt-usage.md](docs/mqtt-usage.md)
- 부하 테스트 보고서: [docs/load-test-report.md](docs/load-test-report.md)
- **운영 가이드(명령어 모음): [docs/operations.md](docs/operations.md)** — 기동/종료, 헬스·지표, 단말 등록, 재처리, DLQ, 장애 대응
- 워커 수 K 튜닝 가이드: [docs/k-tuning.md](docs/k-tuning.md) — 측정 결과·재측정 방법·조정 기준

---

## 1. 아키텍처

```
단말(MQTT, QoS1) ─┐
                  ▼  에이전트 (단일 프로세스, 엣지)
   ┌────────────────────────────────────────────────────┐
   │ [수신] MqttSubscriber → Redis Streams XADD → MQTT ack │
   │        (폭주 흡수: 즉시 적재·즉시 ack, 브로커 백업 방지)  │
   │           │                                          │
   │           ▼  WorkerPool (K개 워커, consumer group)     │
   │ ① 파싱       claim → JSON 파싱                         │
   │ ② 원본 저장   messages_raw INSERT  ← 내구성 지점        │
   │              └ 성공 후에만 Redis XACK (실패→reclaim 재시도)│
   │ ③ 단말 조회   imei → devices → device_id              │
   │ ④ 위치 파생   lat/lon → domain_location (등록 단말)    │
   │ ⑤ 업무 파생   messageCode → 파서 → domain_<code>       │
   │ ✗ 각 단계 오류 → error_log (message_id로 추적)        │
   │ ☠ delivery 초과(poison) → DLQ(messages:dlq)           │
   └────────────────────────────────────────────────────┘
                          │
                       PostgreSQL (엣지 로컬/근접)
```

**기술 스택:** Node.js 20+, TypeScript(ESM/NodeNext), mqtt.js 5, ioredis(Redis Streams), pg(node-postgres), Vitest, @testcontainers/{postgresql,redis}. 패키지 매니저 npm.

> **로컬 버퍼(Redis Streams):** 순간 대량 발행(폭주)을 흡수하는 완충 단계. 수신 즉시 Redis에 XADD하고 곧바로 MQTT ack하여 브로커 송신 큐가 백업/유실되지 않게 한다. 실제 PG 적재는 K개 워커(`WORKER_CONCURRENCY`, 기본 4)가 consumer group으로 병렬 드레인한다. 설계: [docs/superpowers/specs/2026-06-12-redis-buffer-design.md](docs/superpowers/specs/2026-06-12-redis-buffer-design.md).

---

## 2. 처리 흐름과 규칙

**버퍼 경계:** MQTT 수신부와 처리부는 Redis Streams로 분리된다.
- **수신부**(`MqttSubscriber`): 메시지를 `messages:stream`에 XADD → 성공 시 **MQTT ack**. (폭주를 흡수하는 1차 완충. 단 최종 내구성 지점은 아니며, Redis는 AOF everysec로 영속화한다.)
- **처리부**(`WorkerPool`, K개): consumer group으로 엔트리를 claim → 아래 1건 처리 → 성공 시 **Redis XACK + XDEL**. 처리 실패 시 ack하지 않아 `XAUTOCLAIM`(reclaim)으로 재시도되고, delivery가 `maxRetry`를 넘으면 DLQ(`messages:dlq`)로 격리한다.

워커가 처리하는 1건의 순서 (`src/service/messageProcessor.ts`):

```
claim → JSON 파싱
  ├ 실패 → error_log(stage=ingest, raw_text 보존) → XACK (재처리 무의미)
  └ 성공
     → messageId 부여 + 공통 헤더 추출(imei/messageCode/process_dttm/lat/lon)
     → imei로 device_id 조회
     → messages_raw INSERT (device_id 포함, ON CONFLICT DO NOTHING)  [내구성 지점]
        ├ INSERT 실패(PG 다운) → XACK 안 함 → reclaim 재시도 (한도 초과 시 DLQ)
        ├ 중복(message_key) → 기존 message_id로 파생 멱등 재실행 후 XACK
        │                     (원본 저장 직후 크래시로 누락된 파생 복구)
        └ 신규 → ★ Redis XACK ★
     → 분기
        ├ device_id 없음(미등록) → error_yn=Y + error_detail + error_log(device_lookup) → 종료
        │                          (도메인·위치 저장 안 함, 원본만 보존)
        └ device_id 있음
             ├ 위치 projection: lat/lon 있으면 domain_location 저장 (실패 → error_log(location))
             └ messageCode projection:
                  ├ 전용 파서(Fault) → domain_<code> / 없으면(catch-all) → domain_generic(키별 행 EAV)
                  ├ 성공 → error_yn=N (그대로)
                  └ 실제 파싱/저장 예외 → error_yn=Y + error_detail + error_log(projection)
```

### 핵심 규칙

- **폭주 흡수(Redis 버퍼):** 수신 즉시 `messages:stream`에 XADD하고 곧바로 MQTT ack한다. 순간 대량 발행 시 브로커 송신 큐가 백업되어 메시지를 떨구는 것을 막는다(at-least-once 1차 완충). Redis는 AOF everysec로 영속화.
- **무손실(at-least-once):** `messages_raw` INSERT가 단일 내구성 지점이다. 워커는 INSERT 성공 후에만 **Redis XACK**한다. PG 장애 시 XACK하지 않아 `XAUTOCLAIM`으로 재처리되고, `maxRetry` 초과 시 DLQ로 격리(poison 무한 재시도 방지)한다.
- **무중복(멱등):** `messages_raw.message_key`(TEXT UNIQUE)가 멱등 키이며 `ON CONFLICT (message_key) DO NOTHING`으로 재전송·재처리 중복을 흡수한다(K개 워커 병렬·reclaim 재시도에도 안전). `message_key`는 결정적으로 유도된다(단말 고유 ID 우선, 없으면 `deviceId + 원본텍스트`의 sha256). `message_id`는 BIGSERIAL 숫자 PK로, 도메인/에러 테이블이 참조한다.
- **원본이 진실의 원천:** 어떤 단계가 실패해도 `messages_raw` 원본은 항상 보존된다. 파서 수정·단말 등록 후 원본을 재처리하면 복구된다(단말 재수집 불필요).
- **원본 저장 이후 단계는 비치명:** 원본 저장(=XACK) 다음의 위치/업무 파생 실패는 재처리를 유발하지 않고 `error_log` + `error_yn`로 격리한다(poison message 무한 재시도 방지).
- **단계별 추적:** 모든 오류는 `error_log`에 `message_id` + `stage`로 기록되어 원본·업무·에러를 한 키로 추적한다.

---

## 3. 데이터베이스 스키마

전체 DDL과 컬럼 코멘트: [src/db/schema.sql](src/db/schema.sql) (기동·테스트 시 `applySchema`가 `IF NOT EXISTS`로 멱등 적용).

| 테이블 | 역할 | 키 |
|---|---|---|
| `devices` | 단말 마스터. `device_id` ↔ `imei` 매핑 | PK `device_id`(BIGSERIAL), UNIQUE `imei` |
| `messages_raw` | 원본 적재(불변, bronze). 원본 JSON + 공통 헤더(imei/process_dttm/lat/lon) | PK `message_id`(BIGSERIAL), UNIQUE `message_key`(멱등), FK `device_id`, `imei`(추적) |
| `domain_fault` | 업무(고장) 파생. `message.{ftp,sp,pcode}` | PK `id`, UNIQUE FK `message_id`, `device_id` |
| `domain_location` | 공통 위치 파생(messageCode 무관, 등록 단말) | PK `id`, UNIQUE FK `message_id`, `device_id` |
| `domain_generic` | 범용 업무 파생(catch-all, EAV). 본문 키마다 한 행 | PK `id`, FK `message_id`, `device_id`, `key`/`value`, UNIQUE(message_id,key) |
| `error_log` | 단계별 오류 추적 | PK `id`, 참조 `message_id`/`message_key`, `stage` |

- 모든 테이블은 생성일시 `created_at TIMESTAMPTZ NOT NULL DEFAULT now()` 보유.
- 각 업무 도메인 테이블은 단말별 조회를 위해 `device_id NOT NULL`을 가진다.
- `error_log.stage`: `ingest | device_lookup | projection | location`.

추적 조회 예시:
```sql
SELECT d.imei, r.message_code, r.error_yn, r.error_detail,
       f.ftp, f.sp, f.pcode, l.latitude, l.longitude,
       e.stage AS error_stage, e.detail AS error_detail
FROM messages_raw r
LEFT JOIN devices d USING (device_id)
LEFT JOIN domain_fault f USING (message_id)
LEFT JOIN domain_location l USING (message_id)
LEFT JOIN error_log e USING (message_id)
WHERE r.message_id = $1;
```

---

## 4. 디렉터리 구조

```
src/
├── ingest/
│   ├── MqttSubscriber.ts   # MQTT 구독, manual ack(handleMessage 오버라이드)
│   └── messageId.ts        # 결정적 멱등 키 유도(순수)
├── buffer/
│   ├── redisPool.ts        # ioredis 클라이언트 팩토리
│   ├── RedisStreamQueue.ts # Redis Streams 큐(enqueue/claim/reclaim/ack/toDlq/listDlq/requeueDlq)
│   └── WorkerPool.ts       # K개 워커 드레인 풀(consumer group, DLQ)
├── header.ts               # rawPayload → 공통 헤더 추출(순수)
├── db/
│   ├── pool.ts             # pg Pool 팩토리
│   ├── schema.sql          # 전체 DDL + 코멘트
│   ├── applySchema.ts      # 스키마 적용 헬퍼
│   └── mapper.ts           # MyBatis식 XML 매퍼 로더 (#{name}→$1 + 값 바인딩)
├── repo/
│   ├── deviceRepo.ts       # imei → device_id 조회/등록
│   ├── rawRepo.ts          # messages_raw 멱등 INSERT / markError / 재처리 조회·갱신
│   ├── domainRepo.ts       # domain_fault INSERT
│   ├── locationRepo.ts     # domain_location INSERT
│   └── errorRepo.ts        # error_log 기록
├── parsers/
│   ├── types.ts            # DomainParser 인터페이스
│   ├── faultParser.ts      # Fault 파서(샘플)
│   └── registry.ts         # messageCode → 파서 매핑
├── service/
│   ├── projectionService.ts # raw → 도메인 파생 + 상태/에러
│   ├── locationProjector.ts # 공통 위치 파생
│   ├── messageProcessor.ts  # 수신 1건 오케스트레이션
│   └── reprocessService.ts  # error_yn='Y' 원본 재처리 배치(단말 등록·파서 수정 후 복구)
├── metrics/
│   ├── stats.ts             # AgentStats/HealthStatus + Prometheus 포매터(순수)
│   ├── counters.ts          # 처리량·지연 누적 카운터(순수, WorkerPool이 기록)
│   ├── statsCollector.ts    # Redis(XLEN/XPENDING/DLQ)+PG(에러 건수)+counters 수집, 헬스체크
│   └── metricsServer.ts     # /health(200/503) + /metrics HTTP 서버(node:http, 무의존)
├── config/config.ts        # 환경설정 로드
├── types.ts                # RawMessage, Clock
├── main.ts                 # 조립(DI: Redis 큐+워커풀+수신부) + graceful shutdown
├── reprocess.ts            # 재처리 배치 CLI 진입점 (npm run reprocess)
├── dlq.ts                  # DLQ 조회·재투입 CLI 진입점 (npm run dlq)
└── device.ts               # 단말 등록/목록 CLI 진입점 (npm run device)
```

설계 원칙: 각 파일은 단일 책임. 파서·헤더 추출 등 변환 로직은 I/O 없는 순수 함수로 격리하여 단위 테스트 가능. `MessageProcessor`가 유일한 처리 오케스트레이터.

**SQL 위치:** 모든 쿼리는 프로젝트 루트 `mappers/<repo>.xml`(MyBatis식 매퍼)에 분리. repo는 `getQuery('<ns>','<id>', params)`로 SQL을 받아 실행한다. `#{name}`은 로더가 `$1` 위치 파라미터로 변환하므로 **pg 파라미터라이즈드 쿼리**가 유지된다(PG 안전, 인젝션 방어). 새 쿼리는 해당 XML에 추가. (mappers는 루트라 dist 복사 불필요 — `../../mappers`가 src/dist 양쪽에서 해석됨)

---

## 5. 실행 방법

### 환경변수 (.env)

`.env.example`을 `.env`로 복사해 값을 채우면 `import 'dotenv/config'`로 자동 로드된다(`main.ts`). `.env`는 gitignore. 또는 셸에서 직접 `$env:DATABASE_URL=...`(PowerShell) 지정도 가능.

### 설치 / 빌드 / 테스트
```bash
npm install
npm run build      # tsc + dist/db/schema.sql 복사
npm test           # vitest run (통합 테스트는 Docker 필요)
```

> 통합 테스트는 testcontainers로 실제 컨테이너(PostgreSQL·Redis·Mosquitto)를 띄운다. **로컬에 Docker 필요.** E2E(`src/e2e.test.ts`)는 전체 파이프라인을 조립해 300건 폭주 무손실을 검증하며, push/PR 시 GitHub Actions CI(`.github/workflows/ci.yml`)에서도 동일하게 실행된다.

### 환경변수 (`src/config/config.ts`)
| 변수 | 필수 | 기본값 | 설명 |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | PostgreSQL 연결 문자열 |
| `MQTT_URL` | ✅ | — | MQTT 브로커 URL — 브로커 인증 계정을 URL에 포함 (예: `mqtt://agent:agentmqttpw@localhost:1883`) |
| `MQTT_TOPIC` | ✅ | — | 구독 토픽 (예: `device/+/msg`) |
| `MQTT_CLIENT_ID` | | `edge-agent` | clean:false durable 세션용 안정 ID |
| `REDIS_URL` | ✅ | — | Redis 버퍼 연결 URL — requirepass 비밀번호 포함 (예: `redis://:agentredispw@localhost:6379`) |
| `WORKER_CONCURRENCY` | | `4` | 처리 워커 수 K (pg 풀은 `K+4`로 생성) |
| `METRICS_PORT` | | `9100` | 관측성 HTTP 포트 (`/health`, `/metrics`). `0`이면 비활성 |
| `METRICS_HOST` | | `127.0.0.1` | 관측성 바인드 주소 (기본 로컬 전용, 컨테이너는 `0.0.0.0`) |

QoS는 1로 고정. 토픽은 `device/<deviceId>/...` 형식을 가정한다(두 번째 세그먼트를 보조 deviceId로 추출). 스트림/그룹/DLQ 이름은 `messages:stream` / `agent-workers` / `messages:dlq`로 고정.

> **브로커 큐 설정(무손실 전제):** Mosquitto 기본 `max_queued_messages=1000`이면 순간 대량 발행 시 1000건 초과분을 **브로커가** 떨군다(에이전트 도달 전 유실). [docker/mosquitto/config/mosquitto.conf](docker/mosquitto/config/mosquitto.conf)에서 `max_queued_messages 0`(무제한) + `max_inflight_messages 1000`으로 폭주분을 수용하고, 에이전트는 Redis 버퍼로 즉시 ack해 큐를 빠르게 비운다. 이 둘이 함께 있어야 폭주 무손실이 성립한다.

### 로컬 실행 (Docker Compose로 의존성 기동)
의존성(Mosquitto·PostgreSQL·Redis)은 `docker-compose.yml`로 한 번에 띄운다.
```bash
docker compose up -d         # mosquitto(1883/9001) + postgres(5435) + redis(6379)
cp .env.example .env         # 값 확인 후
npm run build && npm start
```
기동 시 스키마가 자동 적용된다. 메시지가 처리되려면 해당 `imei`가 `devices`에 등록되어 있어야 한다(미등록은 원본+에러만 기록). 등록: `npm run device -- register <imei>`.

> **인증·인가(하드닝):** Mosquitto는 익명 차단 + 계정 파일([passwd](docker/mosquitto/config/passwd): `agent`(에이전트/운영), `device`(단말 공용)) + **ACL**([acl](docker/mosquitto/config/acl): 단말은 자기 clientId 토픽 `device/%c/msg`에만 발행, 구독은 agent만) + `message_size_limit 64KB`. Redis는 `requirepass`. 클라이언트는 URL에 계정 포함(`.env.example` 참조). **포트 노출**: MQTT(1883/9001)만 외부 공개, PG(5435)·Redis(6379)·관측성(9100)은 `127.0.0.1` 바인딩(로컬 전용). compose 크리덴셜은 `${POSTGRES_PASSWORD:-...}` 식 env 주입 — **운영 배포 시 dev 기본 계정 반드시 교체**(mosquitto_passwd 재생성 + `.env`). catch-all 파생은 본문 키 200개 상한(초과 시 error_yn=Y 격리). TLS는 미적용(향후 과제).

### 에이전트 컨테이너 실행 (supervision)
```bash
docker compose --profile agent up -d --build   # Dockerfile 빌드 + restart: unless-stopped + /health 헬스체크
```
크래시 시 Docker가 자동 재기동하고, `HEALTHCHECK`가 `/health`(9100)를 15초 주기로 확인한다. 로컬 개발(`npm start`)과 **동시에 띄우지 말 것**(같은 `MQTT_CLIENT_ID`·9100 포트 충돌).

**부하 테스트:** `node scripts/load.mjs --count 2000 --imei load-001 --topic device/A/msg`로 N건을 순간 발행하고, `messages_raw` 건수 == N(중복·에러 0)인지 확인한다.

### 관측성 (/health, /metrics)

에이전트는 `METRICS_PORT`(기본 9100)에서 HTTP 두 엔드포인트를 노출한다(`src/metrics/`).
- **`GET /health`**: `{ok, mqtt, redis, pg}` JSON. MQTT 연결·Redis ping·PG SELECT 1 모두 정상이면 200, 하나라도 실패면 503 (liveness/readiness 프로브용).
- **`GET /metrics`**: Prometheus 텍스트.
  - 상태 gauge: `agent_stream_backlog`(스트림 잔량 XLEN), `agent_stream_pending`(미ack XPENDING), `agent_dlq_depth`(DLQ 적재), `agent_raw_error_rows`(error_yn='Y' 건수), `agent_error_log_total{stage=...}`(단계별 오류 건수).
  - 처리량·지연 counter(프로세스 시작 후 누적, `WorkerPool` 기록): `agent_processed_total`(처리 성공), `agent_process_failed_total`(실패·재시도), `agent_dlq_moved_total`(DLQ 이동), `agent_e2e_latency_ms_sum`(수신→처리완료 지연 합; 평균 = sum/processed, Prometheus에선 rate(sum)/rate(count)), `agent_e2e_latency_ms_max`(최대 지연).
- PG 장기 다운 시 `agent_stream_backlog` 증가로 적체를 감지할 수 있다(경보 기준으로 사용 권장).

### DLQ 운영 (poison 메시지 조회·재투입)

```bash
npm run dlq -- list [n]        # 적재분 조회 (오래된 순, 기본 20건, JSON 한 줄씩)
npm run dlq -- requeue [id]    # 원인 수정 후 messages:stream으로 재투입. id 생략 시 전부
```

`REDIS_URL`만 필요(에이전트와 별개 실행 가능). 재투입 건은 새 엔트리로 적재되어 delivery 카운트가 리셋되고 워커가 다시 처리한다. 재투입+DLQ 삭제는 MULTI로 원자 실행(중복 재투입 방지). 적체량은 `/metrics`의 `agent_dlq_depth`로 감시.

### 원본 재처리 배치 (단말 등록·파서 수정 후 복구)

```bash
npm run reprocess    # DATABASE_URL만 필요 (에이전트와 별개 실행 가능)
```

`error_yn='Y'`인 `messages_raw`를 순회하며 원본(raw_payload)에서 파생을 복구한다(`src/service/reprocessService.ts`).
- **미등록 단말 행**(device_id NULL): imei 재조회 → 등록됐으면 device_id 매핑 + 에러 해제 + 위치/업무 파생 실행. 여전히 미등록이면 건너뜀(유지).
- **projection 실패 행**(device_id 있음): 에러 해제 후 파생 재실행(파서 수정 반영). 실패하면 기존 경로로 다시 `error_yn='Y'` → 다음 실행에서 재시도.
- 도메인 INSERT는 전부 `ON CONFLICT DO NOTHING`이라 반복 실행해도 멱등. 완료 시 scanned/reprocessed/stillUnregistered 요약을 로그로 남긴다.

---

## 6. 개발 규칙

- **TDD:** 실패 테스트 → 최소 구현 → 통과 → 커밋. 단위(순수 함수) + 통합(testcontainers PG).
- **커밋:** 태스크 단위 원자적 커밋. `feat:` / `refactor:` / `chore:` / `docs:` 접두사. 기능 브랜치 작업 후 `--no-ff`로 main 병합.
- **무손실/멱등 불변식 유지:** 원본 INSERT 성공 후에만 ack, `message_key` UNIQUE로 멱등.
- **순수 함수 격리:** 파서/헤더 추출 등 변환 로직에 I/O를 두지 않는다(테스트 용이성).

### 새 업무(messageCode) 추가 절차 — 개방-폐쇄

전용 파서가 없으면 자동으로 `domain_generic`(EAV, 키별 행)에 저장된다(catch-all). 업무 본문은 `message` 중첩이 있으면 그 객체를, 없으면 공통 5키(imei/messageCode/process_dttm/latitude/longitude) 제외한 최상위 키를 사용한다. 전용 테이블이 필요한 코드만 아래 절차로 추가한다(기존 코드 미수정, 추가만):

1. **테이블:** `src/db/schema.sql`에 `domain_<code>` 추가 (`id BIGSERIAL PK`, `message_id BIGINT UNIQUE FK`, `device_id BIGINT NOT NULL`, 업무 컬럼, `created_at`, 코멘트).
2. **Repo:** 해당 테이블 INSERT 메서드 (`domainRepo`에 추가 또는 전용 repo).
3. **파서:** `src/parsers/<code>Parser.ts`에 `DomainParser` 구현 (`parse`는 순수, `insert(repo, messageId, deviceId, parsed)`).
4. **등록:** `src/parsers/registry.ts`의 `defaultRegistry()`에 파서 추가.
5. **테스트:** 파서 단위 테스트 + (필요 시) 통합 테스트.

> 전용 파서 없는 코드는 자동으로 `domain_generic`(EAV)에 저장되고 `error_yn='N'`이다. 실제 파싱/저장 예외 시에만 `error_yn='Y'` + `error_detail` + `error_log(projection)`로 격리된다.

---

## 7. 작업 이력 (요약)

1. **설계(brainstorming → spec):** device→agent→server 실시간 파이프라인 요구사항 정리. 초안은 엣지 에이전트(Redis 버퍼) + HTTP + 중앙 서버(PG) 2-프로세스.
2. **에이전트 v1 구현:** MQTT 수신 → Redis Streams(at-least-once) → HTTP 배치 전송. (이후 서버 흡수로 대체)
3. **아키텍처 전환:** 서버 제거 → **에이전트가 PG까지 직접 처리하는 단일 프로세스**로 통합. Redis/HTTP 제거, PG 직접 적재 + manual ack로 무손실 유지.
4. **데이터 모델 확정:** 단말 마스터(`devices`, device_id↔imei) + 원본(`messages_raw`) + 업무 파생(`domain_*`) + 전용 에러(`error_log`). 업무 파싱은 애플리케이션 파서(레지스트리), DB 프로시저 대신.
5. **업무 테이블 확장:** 각 도메인 테이블에 `device_id` 추가, 공통 위치(`domain_location`) 분리(messageCode 무관, 등록 단말 전용).
6. **스키마 정리:** 모든 테이블 `created_at`(생성일시) 통일 + `COMMENT ON` 코멘트를 각 테이블 아래에 정리.
7. **부하 테스트·손실 진단:** 2000건 순간 발행 시 1040건만 저장됨을 발견. 원인은 에이전트가 아니라 **Mosquitto 브로커 송신 큐**(`max_queued_messages` 기본 1000) 오버플로. 에이전트 자체는 받은 건 100% 무손실·무중복 처리.
8. **Redis 버퍼 재도입(폭주 흡수):** 수신부와 처리부를 **Redis Streams**로 분리. 수신 즉시 XADD→MQTT ack(브로커 큐 백업 방지), K개 워커(consumer group, 기본 4)가 PG로 병렬 드레인, 실패는 reclaim 재시도·poison은 DLQ. 더불어 브로커 `max_queued_messages 0`/`max_inflight_messages 1000` 상향. 재검증: 2000건 **전량 저장(손실 0·중복 0·에러 0)**.
9. **DLQ 판정 버그 수정 + 원본 재처리 배치:** `reclaim`이 delivery 횟수를 2로 하드코딩해 `maxRetry` 초과 판정이 영원히 거짓(poison 무한 재시도)이던 버그를 XPENDING 실측 조회로 수정. `error_yn='Y'` 원본에서 파생을 복구하는 재처리 배치(`npm run reprocess`, §5) 추가 — 뒤늦게 등록된 단말 매핑·파서 수정 반영, 멱등.
10. **관측성:** `/health`(MQTT·Redis·PG 상태, 200/503) + `/metrics`(Prometheus: 스트림 적체·미ack·DLQ·에러 건수) HTTP 서버 추가(`METRICS_PORT`, 기본 9100, 무의존 node:http). SIGINT/SIGTERM 중복 수신 가드도 추가.
11. **Redis 큐 하드닝:** consumer group 시작점 `'$'`→`'0'`(그룹 재생성 시 기존 잔량 유실 방지), `ack`(XACK+XDEL)·`toDlq`(DLQ XADD+XACK+XDEL)를 MULTI로 원자화(중간 크래시 시 acked 엔트리 잔류·DLQ 중복 적재 방지).
12. **중복 재수신 시 파생 복구:** 원본 INSERT 직후 크래시하면 재처리에서 중복 판정으로 파생이 조용히 누락되던 구멍을 수정 — 중복이어도 기존 `message_id`를 조회해 위치/업무 파생을 멱등 재실행(전 도메인 INSERT가 ON CONFLICT DO NOTHING이라 안전).
13. **DLQ 운영 도구:** `npm run dlq -- list|requeue`(§5) 추가 — poison 적재분 조회, 원인 수정 후 스트림 재투입(MULTI 원자, delivery 리셋).
14. **보안·운영 하드닝:** Mosquitto 익명 차단+계정 파일, Redis requirepass(클라이언트는 URL 인증), 에이전트 Dockerfile+compose `agent` 프로파일(restart 정책+`/health` HEALTHCHECK로 supervision), 단말 등록 CLI(`npm run device -- register|list`). 전 구간 실검증(인증 브로커 발행→domain_fault 저장, 컨테이너 healthy).
15. **보안 하드닝 2차:** PG·Redis·관측성 포트 `127.0.0.1` 바인딩(외부 공개는 MQTT만), MQTT ACL(단말은 자기 clientId 토픽만 발행 — 브로커 Denied 실검증) + `device` 계정 분리, `message_size_limit 64KB`(70KB 발행 Dropped 실검증), catch-all 키 200개 상한(TDD), compose 크리덴셜 env 주입화, 컨테이너 non-root(`USER node`) + `METRICS_HOST`(기본 127.0.0.1).
16. **Redis 재시작 내구성 검증 + 문서 정합:** AOF everysec 컨테이너에서 적재·claim 후 `SHUTDOWN NOSAVE`(강제 종료) → 재시작 시 스트림 잔량·미ack(pending)·consumer group이 AOF 복구로 전부 보존됨을 자동 테스트로 검증(`src/buffer/redisDurability.test.ts`). mqtt-usage.md를 인증·ACL·크기 제한 반영해 갱신.
17. **처리량·지연 지표:** `AgentCounters`(순수 누적 카운터)를 `WorkerPool`이 기록(성공/실패/DLQ 이동 + 수신 `receivedAt`→처리완료 지연 sum/max), `StatsCollector` 경유로 `/metrics`에 counter 노출. 라이브 검증(발행 3건 → processed_total 3, 평균 지연 27ms).
18. **K 튜닝 가이드:** 2000건 폭주 드레인을 K=1/2/4/8로 실측(15.9→7.9→5.3→3.4s, 손실·에러 0) — [docs/k-tuning.md](docs/k-tuning.md)에 결과·재측정 절차·조정 기준(늘릴 신호/소용없는 경우/연결 비용) 문서화.
18. **E2E 자동화 + 수신 병목 수정:** `src/e2e.test.ts`(Mosquitto+PG+Redis 컨테이너, main.ts와 동일 배선, 300건 폭주 → 전량·무중복·에러 0 검증) + GitHub Actions CI(`.github/workflows/ci.yml`). E2E가 **수신 enqueue와 워커의 블로킹 XREADGROUP이 단일 Redis 연결을 공유해 수신 처리량이 ~4건/s로 캡핑되던 병목**을 발견 — 워커별 전용 연결(`forWorker()`, duplicate)로 분리해 300건 저장 78초 → 1초로 개선.

상세 단계별 계획과 코드는 `docs/superpowers/plans/`의 각 계획 문서에 기록되어 있다(Redis 버퍼: [docs/superpowers/plans/2026-06-12-redis-buffer.md](docs/superpowers/plans/2026-06-12-redis-buffer.md)).

---

## 8. 향후 작업 (후보)

- 추가 업무 테이블(§6 절차로 확장).
- TLS: MQTT(8883)·Redis·PG 전송 암호화 (인증서 체계 등 인프라 결정 필요).
- payload imei ↔ 토픽 deviceId 대조 검증(imei 스푸핑 방지 — 단말 식별 스펙 확정 필요).
- 운영 배포 시 dev 기본 계정 교체 절차(compose는 env 주입 완료, mosquitto passwd 재생성은 수동 — secret 관리 체계).
