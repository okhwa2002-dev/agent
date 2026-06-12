# 기술 스펙 — Edge Agent (실시간 메시지 수집 에이전트)

> 현재 구현 기준 기술 스펙 요약. 아키텍처·규칙 상세는 [CLAUDE.md](../CLAUDE.md), 설계 문서는 [docs/superpowers/specs/](superpowers/specs/), MQTT 사용법은 [docs/mqtt-usage.md](mqtt-usage.md) 참조.

## 1. 개요
단말기가 MQTT로 발행하는 JSON 메시지를 엣지에서 실시간 수신하여 **원본 보관 → 단말 식별 → 업무별 파생 저장**을 **단일 프로세스**로 처리하는 에이전트. 각 단계 오류를 추적 가능하게 기록한다.

## 2. 기술 스택

| 구분 | 내용 |
|---|---|
| 런타임 | Node.js (v22, 20+ 호환) |
| 언어 | TypeScript 5.6 — ESM(`type: module`), `NodeNext`, `strict: true`, target ES2022 |
| MQTT | `mqtt` 5 (mqtt.js) — QoS 1, manual ack |
| DB | PostgreSQL 16 + `pg`(node-postgres) |
| SQL 관리 | MyBatis식 XML 매퍼(`mappers/*.xml`) + 자체 로더(`#{name}`→`$1` pg 파라미터) |
| 설정 | `dotenv`(.env) |
| 테스트 | `vitest` + `@testcontainers/postgresql`(실 PG 컨테이너) |
| 인프라 | Docker Compose (Mosquitto + PostgreSQL) |
| 코드 규모 | 소스 22 / 테스트 11 파일 |

## 3. 아키텍처 (단일 프로세스, 엣지)

```
단말(MQTT QoS1) → [Mosquitto] → 에이전트(단일 프로세스) → PostgreSQL
                                  ① 수신·파싱 (manual ack)
                                  ② 원본 저장 messages_raw  ← 내구성 지점(성공 후 ack)
                                  ③ imei→device_id 조회
                                  ④ 공통 위치 + 업무 파생
                                  ✗ 단계 오류 → error_yn / error_log
```

> 초기엔 "에이전트(Redis 버퍼) + HTTP + 중앙서버(PG)" 2-프로세스였으나, PG가 엣지 로컬/근접이라 **서버를 흡수해 단일 프로세스로 통합**(Redis/HTTP 제거).

## 4. 처리 흐름 & 핵심 보장

- **무손실(at-least-once):** `messages_raw` INSERT 성공 후에만 MQTT puback. PG 장애 시 ack 안 함 → broker 재전송. (`MqttSubscriber`의 `handleMessage` 오버라이드로 ack 타이밍 제어)
- **무중복(멱등):** `messages_raw.message_key`(TEXT UNIQUE) + `ON CONFLICT (message_key) DO NOTHING`. `message_key`는 결정적 유도(단말 고유 ID 우선, 없으면 `deviceId+원본텍스트` sha256).
- **동시성:** mqtt.js 백프레셔로 메시지를 직렬 처리(레이스 없음). 50건 동시 발행 → 정확히 50건 저장 실측 검증.
- **원본이 진실의 원천:** 어느 단계가 실패해도 원본은 보존, 재처리로 복구.
- **단계별 추적:** 오류를 `error_yn`/`error_detail`(요약) + `error_log`(stage별 상세)로 기록.

## 5. 데이터 모델 (PostgreSQL, 숫자 키)

| 테이블 | 키/특징 |
|---|---|
| `devices` | PK `device_id`(BIGSERIAL), UNIQUE `imei` |
| `messages_raw` | PK `message_id`(BIGSERIAL), UNIQUE `message_key`(멱등), FK `device_id`, `imei`, `error_yn`(Y/N)+`error_detail`, `raw_payload` JSONB, 공통헤더(message_code/process_dttm/lat/lon) |
| `domain_fault` | PK `id`, UNIQUE FK `message_id`, `device_id`, ftp/sp/pcode (전용 파서) |
| `domain_location` | PK `id`, UNIQUE FK `message_id`, `device_id`, lat/lon (공통 위치) |
| `domain_generic` | PK `id`, FK `message_id`, `device_id`, key/value (EAV, catch-all), UNIQUE(message_id,key) |
| `error_log` | PK `id`, `message_id`/`message_key`, `stage`, detail, raw_text |

- 모든 테이블 `created_at TIMESTAMPTZ DEFAULT now()`. 업무 테이블은 `device_id NOT NULL`.
- **라우팅:** `messageCode="Fault"` → 전용 `domain_fault`, 그 외 전부 → `domain_generic`(키별 행). 업무 본문은 `message` 중첩 또는 공통 5키(imei/messageCode/process_dttm/latitude/longitude) 제외한 평면 키를 자동 인식.
- `error_log.stage`: `ingest | device_lookup | projection | location`.
- 전체 DDL·코멘트: [src/db/schema.sql](../src/db/schema.sql).

## 6. 디렉터리 구조

```
src/
├─ ingest/        MqttSubscriber(manual ack), messageId(멱등키)
├─ header.ts      공통헤더·업무본문 추출(순수)
├─ db/            pool, schema.sql, applySchema, mapper(XML 로더)
├─ repo/          device·raw·domain·location·generic·error
├─ parsers/       DomainParser, faultParser, registry(개방-폐쇄)
├─ service/       projectionService, locationProjector, messageProcessor(오케스트레이터)
├─ config/        환경설정 로드
├─ logger.ts      파일+콘솔, 일일 로테이션
└─ main.ts        DI 조립 + graceful shutdown
mappers/          device/raw/domain/generic/location/error .xml (SQL 분리)
```

설계 원칙: 단일 책임, 변환 로직은 I/O 없는 순수 함수로 격리, `MessageProcessor`가 유일한 처리 오케스트레이터.

## 7. 인프라 (docker-compose)

| 서비스 | 포트 | 비고 |
|---|---|---|
| Mosquitto | `1883`(TCP) + `9001`(WebSocket) | 익명 허용, persistence |
| PostgreSQL 16 | `5435`→5432 | db `agent_db`, user/pw `agent`/`agentpw` |

## 8. 로깅
- 파일 `D:\workspace\ok2020\log\agent.log` + 콘솔 동시 출력, JSON 한 줄.
- 일일 로테이션: 날짜가 바뀌면 `agent-YYYY-MM-DD.log`로 백업 후 새 파일(기동 시·60초 주기·기록 시 체크).
- 타임스탬프 로컬 시간 `YYYY-MM-DD HH:mm:ss.SSS`.

## 9. 환경변수 (.env / dotenv)

| 변수 | 필수 | 기본 |
|---|---|---|
| `DATABASE_URL` | ✅ | — |
| `MQTT_URL` | ✅ | — |
| `MQTT_TOPIC` | ✅ | (예 `device/+/msg`) |
| `MQTT_CLIENT_ID` | | `edge-agent` |
| `LOG_DIR` | | `D:\workspace\ok2020\log` |

## 10. 테스트 / 개발 규칙
- **TDD** — 단위(순수 함수: messageId/header/parser/mapper/logger) + 통합(testcontainers PG: repo/projection/messageProcessor). E2E는 실 인프라로 수동 검증(중복·동시성·로테이션 등).
- 커밋: `feat/refactor/chore/docs` 접두사, 기능 브랜치 `--no-ff` 병합. **commit·push는 수동**(사용자 처리).

## 11. 향후 확장 후보
- 새 업무 코드: 파서+테이블 추가(개방-폐쇄) 또는 자동 `domain_generic`.
- DB 정기점검(장시간 중지) 대비 로컬 영속 버퍼(SQLite/Redis) 도입 검토.
- 원본+파생 단일 트랜잭션(크래시 일관성), 미등록/parse 재처리 배치, 관측성(`/metrics`).
