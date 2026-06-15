# Edge Agent — 실시간 메시지 수집 에이전트

단말기가 MQTT로 발행하는 JSON 메시지를 엣지에서 실시간 수신하여 **원본 보관 → 단말 식별 → 업무별 파생 저장**을 단일 프로세스로 처리하고, 각 단계 오류를 추적 가능한 형태로 기록하는 에이전트.

- 설계 문서: [docs/superpowers/specs/2026-06-09-realtime-message-pipeline-design.md](docs/superpowers/specs/2026-06-09-realtime-message-pipeline-design.md)
- 구현 계획: [docs/superpowers/plans/](docs/superpowers/plans/)
- MQTT 발행·테스트 사용법: [docs/mqtt-usage.md](docs/mqtt-usage.md)
- 부하 테스트 보고서: [docs/load-test-report.md](docs/load-test-report.md)

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
        ├ 중복(message_key) → XACK 후 종료
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
│   ├── RedisStreamQueue.ts # Redis Streams 큐(enqueue/claim/reclaim/ack/toDlq)
│   └── WorkerPool.ts       # K개 워커 드레인 풀(consumer group, DLQ)
├── header.ts               # rawPayload → 공통 헤더 추출(순수)
├── db/
│   ├── pool.ts             # pg Pool 팩토리
│   ├── schema.sql          # 전체 DDL + 코멘트
│   ├── applySchema.ts      # 스키마 적용 헬퍼
│   └── mapper.ts           # MyBatis식 XML 매퍼 로더 (#{name}→$1 + 값 바인딩)
├── repo/
│   ├── deviceRepo.ts       # imei → device_id 조회/등록
│   ├── rawRepo.ts          # messages_raw 멱등 INSERT / markError(error_yn,error_detail)
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
│   └── messageProcessor.ts  # 수신 1건 오케스트레이션
├── config/config.ts        # 환경설정 로드
├── types.ts                # RawMessage, Clock
└── main.ts                 # 조립(DI: Redis 큐+워커풀+수신부) + graceful shutdown
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

> 통합 테스트는 `@testcontainers/postgresql`로 실제 PostgreSQL 컨테이너를 띄운다. **로컬에 Docker 필요.**

### 환경변수 (`src/config/config.ts`)
| 변수 | 필수 | 기본값 | 설명 |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | PostgreSQL 연결 문자열 |
| `MQTT_URL` | ✅ | — | MQTT 브로커 URL (예: `mqtt://localhost:1883`) |
| `MQTT_TOPIC` | ✅ | — | 구독 토픽 (예: `device/+/msg`) |
| `MQTT_CLIENT_ID` | | `edge-agent` | clean:false durable 세션용 안정 ID |
| `REDIS_URL` | ✅ | — | Redis 버퍼 연결 URL (예: `redis://localhost:6379`) |
| `WORKER_CONCURRENCY` | | `4` | 처리 워커 수 K (pg 풀은 `K+4`로 생성) |

QoS는 1로 고정. 토픽은 `device/<deviceId>/...` 형식을 가정한다(두 번째 세그먼트를 보조 deviceId로 추출). 스트림/그룹/DLQ 이름은 `messages:stream` / `agent-workers` / `messages:dlq`로 고정.

> **브로커 큐 설정(무손실 전제):** Mosquitto 기본 `max_queued_messages=1000`이면 순간 대량 발행 시 1000건 초과분을 **브로커가** 떨군다(에이전트 도달 전 유실). [docker/mosquitto/config/mosquitto.conf](docker/mosquitto/config/mosquitto.conf)에서 `max_queued_messages 0`(무제한) + `max_inflight_messages 1000`으로 폭주분을 수용하고, 에이전트는 Redis 버퍼로 즉시 ack해 큐를 빠르게 비운다. 이 둘이 함께 있어야 폭주 무손실이 성립한다.

### 로컬 실행 (Docker Compose로 의존성 기동)
의존성(Mosquitto·PostgreSQL·Redis)은 `docker-compose.yml`로 한 번에 띄운다.
```bash
docker compose up -d         # mosquitto(1883/9001) + postgres(5435) + redis(6379)
cp .env.example .env         # 값 확인 후
npm run build && npm start
```
기동 시 스키마가 자동 적용된다. 메시지가 처리되려면 해당 `imei`가 `devices`에 등록되어 있어야 한다(미등록은 원본+에러만 기록).

**부하 테스트:** `node scripts/load.mjs --count 2000 --imei load-001 --topic device/A/msg`로 N건을 순간 발행하고, `messages_raw` 건수 == N(중복·에러 0)인지 확인한다.

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

상세 단계별 계획과 코드는 `docs/superpowers/plans/`의 각 계획 문서에 기록되어 있다(Redis 버퍼: [docs/superpowers/plans/2026-06-12-redis-buffer.md](docs/superpowers/plans/2026-06-12-redis-buffer.md)).

---

## 8. 향후 작업 (후보)

- 추가 업무 테이블(§6 절차로 확장).
- 미등록 단말 / `parse_error` 자동 재처리 배치(단말 등록·파서 수정 후 원본 재파싱).
- 관측성: error_yn='Y' 건수, error_log 단계별 건수, Redis stream/DLQ 적체, end-to-end latency 지표 + `/metrics`.
- DLQ 운영: `messages:dlq` 적재분 조회·재처리(원인 수정 후 stream으로 재투입) 도구.
- Redis 장애/재시작 내구성 검증(AOF 복구), 워커 수 K 튜닝 가이드.
- E2E: `docker compose`(Mosquitto+PG+Redis) + 에이전트로 단말→DB 전 구간·폭주 무손실 자동 검증(현재 `scripts/load.mjs` 수동 검증 → CI화).
