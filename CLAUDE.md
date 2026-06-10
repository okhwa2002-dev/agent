# Edge Agent — 실시간 메시지 수집 에이전트

단말기가 MQTT로 발행하는 JSON 메시지를 엣지에서 실시간 수신하여 **원본 보관 → 단말 식별 → 업무별 파생 저장**을 단일 프로세스로 처리하고, 각 단계 오류를 추적 가능한 형태로 기록하는 에이전트.

- 설계 문서: [docs/superpowers/specs/2026-06-09-realtime-message-pipeline-design.md](docs/superpowers/specs/2026-06-09-realtime-message-pipeline-design.md)
- 구현 계획: [docs/superpowers/plans/](docs/superpowers/plans/)

---

## 1. 아키텍처

```
단말(MQTT, QoS1) ─┐
                  ▼  에이전트 (단일 프로세스, 엣지)
   ┌────────────────────────────────────────────────────┐
   │ ① 수신·파싱   MqttSubscriber (manual ack)            │
   │ ② 원본 저장   messages_raw INSERT  ← 내구성 지점       │
   │              └ 성공 후에만 MQTT ack                    │
   │ ③ 단말 조회   imei → devices → device_id              │
   │ ④ 위치 파생   lat/lon → domain_location (등록 단말)    │
   │ ⑤ 업무 파생   messageCode → 파서 → domain_<code>       │
   │ ✗ 각 단계 오류 → error_log (message_id로 추적)        │
   └────────────────────────────────────────────────────┘
                          │
                       PostgreSQL (엣지 로컬/근접)
```

**기술 스택:** Node.js 20+, TypeScript(ESM/NodeNext), mqtt.js 5, pg(node-postgres), Vitest, @testcontainers/postgresql. 패키지 매니저 npm.

---

## 2. 처리 흐름과 규칙

수신 1건의 처리 순서 (`src/service/messageProcessor.ts`):

```
수신 → JSON 파싱
  ├ 실패 → error_log(stage=ingest, raw_text 보존) → ack (재전송 무의미)
  └ 성공
     → messageId 부여 + 공통 헤더 추출(imei/messageCode/process_dttm/lat/lon)
     → imei로 device_id 조회
     → messages_raw INSERT (device_id 포함, ON CONFLICT DO NOTHING)  [내구성 지점]
        ├ INSERT 실패(PG 다운) → ack 안 함 → broker 재전송
        ├ 중복(message_id) → ack 후 종료
        └ 신규 → ★ MQTT ack ★
     → 분기
        ├ device_id 없음(미등록) → status=unregistered_device + error_log(device_lookup) → 종료
        │                          (도메인·위치 저장 안 함, 원본만 보존)
        └ device_id 있음
             ├ 위치 projection: lat/lon 있으면 domain_location 저장 (실패 → error_log(location))
             └ messageCode 파서: domain_<code> 저장
                 ├ 성공 → status=parsed
                 └ 미등록 코드/실패 → status=parse_error + error_log(projection)
```

### 핵심 규칙

- **무손실(at-least-once):** `messages_raw` INSERT가 단일 내구성 지점이다. INSERT 성공 후에만 MQTT puback(ack)을 보낸다. PG 장애 시 ack하지 않아 broker가 재전송한다.
- **무중복(멱등):** `messages_raw.message_key`(TEXT UNIQUE)가 멱등 키이며 `ON CONFLICT (message_key) DO NOTHING`으로 재전송 중복을 흡수한다. `message_key`는 결정적으로 유도된다(단말 고유 ID 우선, 없으면 `deviceId + 원본텍스트`의 sha256). `message_id`는 BIGSERIAL 숫자 PK로, 도메인/에러 테이블이 참조한다.
- **원본이 진실의 원천:** 어떤 단계가 실패해도 `messages_raw` 원본은 항상 보존된다. 파서 수정·단말 등록 후 원본을 재처리하면 복구된다(단말 재수집 불필요).
- **ack 이후 단계는 비치명:** 원본 저장(=ack) 다음의 위치/업무 파생 실패는 재전송을 유발하지 않고 `error_log` + `status`로 격리한다(poison message 무한 재전송 방지).
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
| `error_log` | 단계별 오류 추적 | PK `id`, 참조 `message_id`/`message_key`, `stage` |

- 모든 테이블은 생성일시 `created_at TIMESTAMPTZ NOT NULL DEFAULT now()` 보유.
- 각 업무 도메인 테이블은 단말별 조회를 위해 `device_id NOT NULL`을 가진다.
- `error_log.stage`: `ingest | device_lookup | projection | location`.

추적 조회 예시:
```sql
SELECT d.imei, r.message_code, r.status,
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
├── header.ts               # rawPayload → 공통 헤더 추출(순수)
├── db/
│   ├── pool.ts             # pg Pool 팩토리
│   ├── schema.sql          # 전체 DDL + 코멘트
│   └── applySchema.ts      # 스키마 적용 헬퍼
├── repo/
│   ├── deviceRepo.ts       # imei → device_id 조회/등록
│   ├── rawRepo.ts          # messages_raw 멱등 INSERT / status 전이
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
└── main.ts                 # 조립(DI) + graceful shutdown
```

설계 원칙: 각 파일은 단일 책임. 파서·헤더 추출 등 변환 로직은 I/O 없는 순수 함수로 격리하여 단위 테스트 가능. `MessageProcessor`가 유일한 처리 오케스트레이터.

---

## 5. 실행 방법

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

QoS는 1로 고정. 토픽은 `device/<deviceId>/...` 형식을 가정한다(두 번째 세그먼트를 보조 deviceId로 추출).

### 로컬 실행 (Docker로 의존성 기동)
```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=agent postgres:16-alpine
docker run -d -p 1883:1883 eclipse-mosquitto:2 \
  sh -c "printf 'listener 1883\nallow_anonymous true\n' > /mosquitto/config/mosquitto.conf && mosquitto -c /mosquitto/config/mosquitto.conf"

DATABASE_URL=postgres://postgres:pw@localhost:5432/agent \
  MQTT_URL=mqtt://localhost:1883 MQTT_TOPIC='device/+/msg' npm start
```
기동 시 스키마가 자동 적용된다. 메시지가 처리되려면 해당 `imei`가 `devices`에 등록되어 있어야 한다(미등록은 원본+에러만 기록).

---

## 6. 개발 규칙

- **TDD:** 실패 테스트 → 최소 구현 → 통과 → 커밋. 단위(순수 함수) + 통합(testcontainers PG).
- **커밋:** 태스크 단위 원자적 커밋. `feat:` / `refactor:` / `chore:` / `docs:` 접두사. 기능 브랜치 작업 후 `--no-ff`로 main 병합.
- **무손실/멱등 불변식 유지:** 원본 INSERT 성공 후에만 ack, `message_key` UNIQUE로 멱등.
- **순수 함수 격리:** 파서/헤더 추출 등 변환 로직에 I/O를 두지 않는다(테스트 용이성).

### 새 업무(messageCode) 추가 절차 — 개방-폐쇄

기존 코드를 수정하지 않고 **추가만** 한다:

1. **테이블:** `src/db/schema.sql`에 `domain_<code>` 추가 (`id BIGSERIAL PK`, `message_id BIGINT UNIQUE FK`, `device_id BIGINT NOT NULL`, 업무 컬럼, `created_at`, 코멘트).
2. **Repo:** 해당 테이블 INSERT 메서드 (`domainRepo`에 추가 또는 전용 repo).
3. **파서:** `src/parsers/<code>Parser.ts`에 `DomainParser` 구현 (`parse`는 순수, `insert(repo, messageId, deviceId, parsed)`).
4. **등록:** `src/parsers/registry.ts`의 `defaultRegistry()`에 파서 추가.
5. **테스트:** 파서 단위 테스트 + (필요 시) 통합 테스트.

> 미등록 messageCode는 자동으로 `status=parse_error` + `error_log(projection)`로 격리되므로, 파서 추가 후 해당 원본을 재처리하면 반영된다.

---

## 7. 작업 이력 (요약)

1. **설계(brainstorming → spec):** device→agent→server 실시간 파이프라인 요구사항 정리. 초안은 엣지 에이전트(Redis 버퍼) + HTTP + 중앙 서버(PG) 2-프로세스.
2. **에이전트 v1 구현:** MQTT 수신 → Redis Streams(at-least-once) → HTTP 배치 전송. (이후 서버 흡수로 대체)
3. **아키텍처 전환:** 서버 제거 → **에이전트가 PG까지 직접 처리하는 단일 프로세스**로 통합. Redis/HTTP 제거, PG 직접 적재 + manual ack로 무손실 유지.
4. **데이터 모델 확정:** 단말 마스터(`devices`, device_id↔imei) + 원본(`messages_raw`) + 업무 파생(`domain_*`) + 전용 에러(`error_log`). 업무 파싱은 애플리케이션 파서(레지스트리), DB 프로시저 대신.
5. **업무 테이블 확장:** 각 도메인 테이블에 `device_id` 추가, 공통 위치(`domain_location`) 분리(messageCode 무관, 등록 단말 전용).
6. **스키마 정리:** 모든 테이블 `created_at`(생성일시) 통일 + `COMMENT ON` 코멘트를 각 테이블 아래에 정리.

상세 단계별 계획과 코드는 `docs/superpowers/plans/`의 각 계획 문서에 기록되어 있다.

---

## 8. 향후 작업 (후보)

- 추가 업무 테이블(§6 절차로 확장).
- 미등록 단말 / `parse_error` 자동 재처리 배치(단말 등록·파서 수정 후 원본 재파싱).
- 관측성: status별 건수, error_log 단계별 건수, end-to-end latency 지표 + `/metrics`.
- E2E: Mosquitto + PG + 에이전트 docker-compose로 단말→DB 전 구간 검증.
