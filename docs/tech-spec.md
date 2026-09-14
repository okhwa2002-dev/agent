# 기술 스펙 — Edge Agent (실시간 메시지 수집 에이전트)

> 현재 구현 기준 **기술 스택·구조 요약**(개발자용). 처리 프로세스·데이터 모델·운영·보안·성능 검증의 상세는 **[docs/processing-spec.md](processing-spec.md)**(처리 프로세스 명세서)를 참조한다.
> 관련 문서: [CLAUDE.md](../CLAUDE.md) · [운영 가이드](operations.md) · [MQTT 사용법](mqtt-usage.md) · [K 튜닝](k-tuning.md) · [부하 테스트](load-test-report.md) · [설계 문서](superpowers/specs/)

기준일 2026-09-14 / 기준 커밋 `de02035`

## 1. 개요

단말기가 MQTT로 발행하는 JSON 메시지를 엣지에서 실시간 수신하여 **원본 보관 → 단말 식별 → 업무별 파생 저장**을 **단일 프로세스**로 처리하는 에이전트. 각 단계 오류를 추적 가능하게 기록한다.

## 2. 기술 스택

| 구분 | 내용 |
|---|---|
| 런타임 | Node.js 20+ (컨테이너 이미지 `node:20-alpine`) |
| 언어 | TypeScript 5.6 — ESM(`type: module`), `NodeNext`, `strict: true`, target ES2022 |
| MQTT | `mqtt` 5 (mqtt.js) — QoS 1, manual ack |
| 로컬 버퍼 | Redis 7 Streams + consumer group (`ioredis` 5) — AOF `everysec` |
| DB | PostgreSQL 16 + `pg`(node-postgres), 풀 크기 `K+4` |
| SQL 관리 | MyBatis식 XML 매퍼(`mappers/*.xml`, 6개) + 자체 로더(`#{name}`→`$1` pg 파라미터) |
| 설정 | `dotenv`(.env) |
| 관측성 | `node:http` 자체 구현 (`/health`, `/metrics`) — 외부 의존 없음 |
| 테스트 | `vitest` 2 + `testcontainers`(PostgreSQL·Redis·Mosquitto 실 컨테이너) |
| 인프라 | Docker Compose (Mosquitto + PostgreSQL + Redis + 선택적 agent 프로파일) |
| CI | GitHub Actions (`.github/workflows/ci.yml`) — push/PR 시 전체 테스트 |
| 코드 규모 | 소스 33 / 테스트 20 파일 (테스트 79건) |

런타임 의존성은 `mqtt`, `ioredis`, `pg`, `dotenv` 4개뿐이다.

## 3. 아키텍처 (단일 프로세스, 엣지)

```
단말(MQTT QoS1) → [Mosquitto] → 에이전트(단일 프로세스) → PostgreSQL
                                  │
                                  ├ 수신부  MqttSubscriber
                                  │   메시지를 Redis Streams에 XADD → 즉시 MQTT ack (폭주 흡수)
                                  │
                                  ├ 버퍼   messages:stream (+ DLQ messages:dlq)
                                  │
                                  ├ 처리부  WorkerPool (K개 워커, consumer group)
                                  │   ① JSON 파싱  ② 헤더 추출·멱등 키 생성
                                  │   ③ imei→device_id 조회
                                  │   ④ messages_raw INSERT   ← 내구성 지점
                                  │   ⑤ 공통 위치 파생  ⑥ 업무 파생
                                  │   → 성공 시 XACK+XDEL / 실패 시 reclaim 재시도 / 5회 초과 시 DLQ
                                  │
                                  └ 관측성  /health, /metrics (METRICS_PORT, 기본 9100)
```

**수신부와 처리부는 Redis Streams를 경계로 분리**되어 있다. 수신은 내용을 해석하지 않고 즉시 적재·ack하므로 순간 대량 발행 시에도 브로커 송신 큐가 백업되지 않는다. 실제 PG 적재는 K개 워커가 병렬 드레인한다.

> 연혁: 초기엔 "에이전트(Redis 버퍼) + HTTP + 중앙서버(PG)" 2-프로세스였으나 PG가 엣지 근접이라 서버를 흡수해 단일 프로세스로 통합했다(Redis/HTTP 제거). 이후 부하 테스트에서 브로커 큐 오버플로에 의한 손실이 확인되어 **폭주 흡수 목적으로 Redis Streams 버퍼를 재도입**하고 브로커 큐 한도를 상향했다.

## 4. 핵심 보장 (요약)

- **무손실(at-least-once, 2단):** 수신부는 버퍼 적재 성공 후에만 MQTT ack, 워커는 `messages_raw` INSERT 성공 후에만 XACK. 어느 지점에서 실패해도 소유권이 직전 단계에 남는다.
- **무중복(멱등):** `messages_raw.message_key` TEXT UNIQUE + `ON CONFLICT DO NOTHING`. 멱등 키는 결정적 유도(단말 고유 ID 우선, 없으면 `deviceId+원본텍스트` sha256). 모든 파생 INSERT도 `ON CONFLICT DO NOTHING`.
- **비치명 격리:** 원본 저장 이후의 파생 실패는 재시도하지 않고 `error_yn='Y'` + `error_log`로 격리한다(poison 무한 재시도 방지).
- **재시도 종료성:** delivery 5회 초과 시 DLQ(`messages:dlq`)로 격리. XACK+XDEL, DLQ 이동, DLQ 재투입은 모두 MULTI 원자 실행.
- **순서:** K개 워커 병렬 처리로 **전역 순서는 보장하지 않는다.** 정렬은 `process_dttm`/`received_at`/`created_at`으로 한다.
- **복구:** 원본이 진실의 원천. 파서 수정·단말 등록 후 `npm run reprocess`로 원본에서 파생 복구(멱등).

상세: [processing-spec.md §3~§5](processing-spec.md)

## 5. 데이터 모델 (PostgreSQL, 숫자 키)

| 테이블 | 키/특징 |
|---|---|
| `devices` | PK `device_id`(BIGSERIAL), UNIQUE `imei` |
| `messages_raw` | PK `message_id`(BIGSERIAL), UNIQUE `message_key`(멱등), FK `device_id`, `imei`, `error_yn`(Y/N)+`error_detail`, `raw_payload` JSONB, 공통헤더(message_code/process_dttm/lat/lon), `received_at` |
| `domain_fault` | PK `id`, UNIQUE FK `message_id`, `device_id`, ftp/sp/pcode (전용 파서) |
| `domain_location` | PK `id`, UNIQUE FK `message_id`, `device_id`, lat/lon (공통 위치) |
| `domain_generic` | PK `id`, FK `message_id`, `device_id`, key/value (EAV, catch-all), UNIQUE(message_id,key) |
| `error_log` | PK `id`, `message_id`/`message_key`, `stage`, detail, raw_text |

- 모든 테이블 `created_at TIMESTAMPTZ DEFAULT now()`. 업무 테이블은 `device_id NOT NULL`.
- **라우팅:** `messageCode="Fault"` → 전용 `domain_fault`, 그 외 전부 → `domain_generic`(키별 행, 본문 키 200개 상한). 업무 본문은 `message` 중첩 또는 공통 5키(imei/messageCode/process_dttm/latitude/longitude) 제외한 평면 키를 자동 인식.
- `error_log.stage`: `ingest | device_lookup | projection | location`.
- 전체 DDL·코멘트: [src/db/schema.sql](../src/db/schema.sql). 기동 시 `applySchema`가 `IF NOT EXISTS`로 멱등 적용.

## 6. 디렉터리 구조

```
src/
├─ ingest/        MqttSubscriber(manual ack), messageId(멱등키)
├─ buffer/        redisPool, RedisStreamQueue(스트림·DLQ), WorkerPool(K개 워커)
├─ header.ts      공통헤더·업무본문 추출(순수)
├─ db/            pool, schema.sql, applySchema, mapper(XML 로더)
├─ repo/          device·raw·domain·location·generic·error
├─ parsers/       DomainParser, faultParser, registry(개방-폐쇄)
├─ service/       projectionService, locationProjector, messageProcessor(오케스트레이터), reprocessService
├─ metrics/       stats(순수 포매터), counters, statsCollector, metricsServer
├─ config/        환경설정 로드
├─ logger.ts      파일+콘솔, 일일 로테이션
├─ main.ts        DI 조립 + graceful shutdown
├─ reprocess.ts   재처리 배치 CLI    (npm run reprocess)
├─ dlq.ts         DLQ 조회·재투입 CLI (npm run dlq)
└─ device.ts      단말 등록·목록 CLI  (npm run device)
mappers/          device/raw/domain/generic/location/error .xml (SQL 분리, 루트에 위치)
```

설계 원칙: 단일 책임, 변환 로직은 I/O 없는 순수 함수로 격리, `MessageProcessor`가 유일한 처리 오케스트레이터.

## 7. 인프라 (docker-compose)

| 서비스 | 포트 | 비고 |
|---|---|---|
| Mosquitto 2 | `1883`(TCP) + `9001`(WS) — **외부 공개** | 익명 차단 + 계정 파일 + ACL, `message_size_limit 64KB`, `max_queued_messages 0`, `max_inflight_messages 1000`, persistence |
| PostgreSQL 16 | `127.0.0.1:5435`→5432 | db `agent_db`, 계정은 `${POSTGRES_PASSWORD}` 주입 |
| Redis 7 | `127.0.0.1:6379` | AOF `everysec`, `requirepass` |
| agent (선택) | `127.0.0.1:9100` | `--profile agent`, non-root(`USER node`), `restart: unless-stopped` + `/health` HEALTHCHECK(15s) |

외부에 공개되는 포트는 MQTT뿐이다. TLS는 미적용(향후 과제).

## 8. 관측성 / 로깅

- **`/health`** — `{ok,mqtt,redis,pg}`, 하나라도 실패 시 503.
- **`/metrics`** — Prometheus 텍스트. 상태 gauge(`agent_stream_backlog`/`_pending`/`agent_dlq_depth`/`agent_raw_error_rows`/`agent_error_log_total{stage}`) + 누적 counter(`agent_processed_total`/`_process_failed_total`/`_dlq_moved_total`/`_e2e_latency_ms_sum`/`_max`).
- 로그: `LOG_DIR`(기본 `./logs`)의 `agent.log`, JSON 한 줄, 파일+콘솔 동시 출력. 일일 로테이션(`agent-YYYY-MM-DD.log`, 기동 시·60초 주기·기록 시 체크). 타임스탬프 로컬 시간 `YYYY-MM-DD HH:mm:ss.SSS`.

## 9. 환경변수 (.env / dotenv)

| 변수 | 필수 | 기본 |
|---|---|---|
| `DATABASE_URL` | ✅ | — |
| `MQTT_URL` | ✅ | — (브로커 계정 포함) |
| `MQTT_TOPIC` | ✅ | (예 `device/+/msg`) |
| `REDIS_URL` | ✅ | — (requirepass 포함) |
| `MQTT_CLIENT_ID` | | `edge-agent` |
| `WORKER_CONCURRENCY` | | `4` (pg 풀 `K+4`) |
| `METRICS_PORT` / `METRICS_HOST` | | `9100` / `127.0.0.1` |
| `LOG_DIR` | | `./logs` |

QoS는 1로 고정. 스트림/그룹/DLQ 이름은 `messages:stream` / `agent-workers` / `messages:dlq`로 고정.

## 10. 테스트 / 개발 규칙

- **TDD** — 단위(순수 함수: messageId/header/parser/mapper/logger/stats/counters) + 통합(testcontainers PG·Redis: repo/projection/messageProcessor/reprocess/큐/내구성) + **E2E**(`src/e2e.test.ts`: Mosquitto+PG+Redis 실 컨테이너로 전체 파이프라인 조립, 300건 폭주 무손실 검증).
- 전체 79건, `npm test`로 실행(Docker 필요). push/PR 시 CI에서 동일 실행.
- 커밋: `feat/refactor/chore/docs` 접두사, 기능 브랜치 `--no-ff` 병합. **commit·push는 수동**(사용자 처리).

## 11. 향후 확장 후보

- 새 업무 코드: 파서+테이블 추가(개방-폐쇄) 또는 자동 `domain_generic`.
- TLS: MQTT(8883)·Redis·PG 전송 암호화 (인증서 체계 등 인프라 결정 필요).
- payload `imei` ↔ 토픽 deviceId 대조 검증(스푸핑 방지 — 단말 식별 스펙 확정 필요).
- 운영 배포 시 dev 기본 계정 교체 절차(compose는 env 주입 완료, mosquitto passwd 재생성은 수동).
