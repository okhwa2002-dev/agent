# 실시간 메시지 수집 파이프라인 설계 (개정: 단일 에이전트)

**작성일:** 2026-06-09 (개정)
**상태:** 설계 승인됨

> **개정 이력:** 초기 설계는 "엣지 에이전트(Redis 버퍼) + HTTP + 중앙 서버(PG)"의 2-프로세스 구조였다. 본 개정에서 **서버를 제거하고 에이전트가 DB까지 직접 처리하는 단일 프로세스**로 통합한다. PostgreSQL이 엣지 로컬/근접에 있어 수신 즉시 PG 적재가 가능하므로 Redis 버퍼도 제거한다.

## 1. 개요

단말기가 발행하는 JSON 메시지를 엣지 상주 에이전트가 실시간 수신하여, **원본 보관 → 단말 식별 → 업무별 파생 저장**을 한 프로세스에서 수행한다. 각 단계의 오류는 전용 에러 테이블에 message_id로 기록해 추적·재처리를 보장한다.

### 1.1 핵심 요구사항

| 항목 | 결정 |
|---|---|
| 에이전트 형태 | 엣지 상주 단일 프로세스 (Node.js/TypeScript) |
| 단말→에이전트 통신 | MQTT (QoS 1), 브로커는 Mosquitto |
| 메시지 포맷 | JSON |
| 규모 | 중규모 (단말 수백~수천, 초당 수백~수천 메시지) |
| 유실 허용도 | **무손실 필수 (at-least-once)** |
| 저장소 | PostgreSQL (엣지 로컬/근접) |
| 처리 범위 | 원본 적재 + imei→device_id 조회 + 업무별 파생 저장 (모두 에이전트가 수행) |
| 업무 타입 라우팅 | JSON 페이로드 내 `messageCode` 필드 |
| 단말 관리 | 단말 마스터(`devices`) PK=`device_id` ↔ `imei`(UNIQUE) |
| 매핑 키 | `message_id`(숫자 surrogate PK) = 원본·업무·에러 테이블을 잇는 키. `message_key`(TEXT UNIQUE) = 멱등 판정 키 |
| 오류 기록 | 전용 단일 에러 테이블(`error_log`), 단계(stage)별 기록 |

### 1.2 설계 원칙

- **수신 즉시 원본 저장 → 그 후 MQTT ack** — 무손실의 핵심. PG raw INSERT가 내구성 지점.
- **원본(raw)이 진실의 원천** — 원본을 먼저 불변 저장하고, 업무 테이블은 거기서 파생. 업무 스키마/파싱이 바뀌어도 단말 재수집 없이 raw를 재파싱한다.
- **단계별 오류 추적** — 어느 단계가 실패해도 원본은 보존되고 `error_log`에 message_id로 기록되어 추적·재처리 가능.
- 컴포넌트는 하나의 책임만 가지며, 명확한 인터페이스로 통신(독립 테스트 가능).
- Pipeline/파서 등 변환 로직은 I/O 없는 순수 함수로 격리.

### 1.3 무손실 보장 메커니즘

Redis 버퍼가 없으므로 **PG의 `messages_raw` INSERT가 단일 내구성 지점**이다.

- 수신 → 원본 INSERT 성공 → **그때 MQTT ack**. INSERT 실패(PG 다운 등) → ack 안 함 → Mosquitto가 QoS1으로 재전송.
- `message_key` UNIQUE + `ON CONFLICT (message_key) DO NOTHING` → 재전송으로 인한 중복은 자동 흡수.
- 원본 저장(=ack) **이후** 단계(단말 조회·업무 파생)의 실패는 재전송을 유발하지 않는다. 원본은 이미 보존되어 있고 `error_log` + `error_yn`로 격리되어 별도 재처리한다(poison message 무한 재전송 방지).

## 2. 아키텍처 & 데이터 흐름

```
단말(MQTT) ─┐
            ▼  에이전트 (단일 프로세스, 엣지)
   ┌──────────────────────────────────────────────────┐
   │ ① 수신·파싱   MqttSubscriber (QoS 1, manual ack)   │
   │ ② 원본 저장   messages_raw INSERT  ← 내구성 지점     │
   │              └ 성공 후에만 MQTT ack                  │
   │ ③ 단말 조회   imei → devices → device_id            │
   │ ④ 업무 파생   messageCode → 파서 → domain_*          │
   │ ✗ 각 단계 오류 → error_log (message_id로 추적)      │
   └──────────────────────────────────────────────────┘
                     │
                  PostgreSQL
        (devices, messages_raw, domain_*, error_log)
```

### 2.1 데이터 흐름

1. **수신·파싱**: Mosquitto 구독 → 메시지 수신 → JSON 파싱. 파싱 실패 시 `error_log`(stage=ingest, raw_text 보존) 기록 후 ack.
2. **원본 저장**: `messageId` 부여 + 공통 헤더(imei, messageCode, process_dttm, lat/lon) 추출 → `messages_raw` INSERT(`ON CONFLICT DO NOTHING`). **성공 시 MQTT ack.** 중복이면 이후 단계 생략.
3. **단말 조회**: `imei`로 `devices` 조회 → `device_id`. 미등록이면 `error_yn='Y'` + `error_detail` + `error_log`(stage=device_lookup).
4. **업무 파생**: `messageCode`로 분기 → 전용(Fault)/범용(EAV) 저장. 실제 예외 시 `error_yn='Y'` + `error_detail` + `error_log`(stage=projection).

### 2.2 무손실-무중복 완결

MQTT QoS 1(수신 보장) + 원본 INSERT 후 ack(내구성) + `message_key` UNIQUE(멱등) 조합으로 단말→DB 전 구간 at-least-once + 무중복이 성립한다.

## 3. 에이전트 컴포넌트 분해

```
src/
├── ingest/
│   ├── MqttSubscriber.ts      # MQTT 구독·재연결, manual ack. 수신만 담당
│   ├── messageId.ts           # 결정적 messageId 유도(순수)
│   └── RawMessage.ts          # 수신 원본 타입
├── header.ts                  # rawPayload → 공통 헤더 추출(순수)
├── db/
│   ├── pool.ts                # pg Pool 팩토리
│   ├── schema.sql             # devices + messages_raw + domain_* + error_log
│   └── applySchema.ts         # schema 적용 헬퍼
├── repo/
│   ├── deviceRepo.ts          # devices 조회 (imei → device_id)
│   ├── rawRepo.ts             # messages_raw UPSERT/상태변경
│   ├── domainRepo.ts          # 업무 도메인 테이블 INSERT
│   └── errorRepo.ts           # error_log 기록
├── parsers/
│   ├── types.ts               # DomainParser 인터페이스
│   ├── faultParser.ts         # Fault 파서 (message.* → domain_fault)
│   └── registry.ts            # messageCode → 파서 매핑
├── service/
│   ├── projectionService.ts   # raw → 도메인 파생 + 상태/에러 기록
│   └── messageProcessor.ts    # 수신 1건 오케스트레이션 (②③④ + 에러)
├── config.ts                  # 환경설정 로드
└── main.ts                    # MQTT 구독 + 처리 파이프라인 조립 + graceful shutdown
```

### 3.1 컴포넌트별 책임

| 컴포넌트 | 책임 |
|---|---|
| MqttSubscriber | MQTT 구독, manual ack 제어, 자동 재연결. 처리 콜백 위임 |
| messageId | 결정적 멱등 키(message_key) 유도(단말 고유 ID 우선, 없으면 deviceId+rawText 해시) |
| header | rawPayload에서 공통 헤더(imei/messageCode/process_dttm/lat/lon) 추출 |
| DeviceRepo | imei → device_id 조회, 단말 등록 |
| RawRepo | messages_raw 멱등 INSERT, error_yn/error_detail 표시(markError) |
| DomainRepo | 업무 도메인 테이블 INSERT(멱등, device_id 포함) |
| LocationRepo | domain_location INSERT(멱등, device_id 포함) |
| ErrorRepo | error_log 기록 (stage, message_id, detail, raw_text) |
| ParserRegistry / DomainParser | messageCode → 파서. `insert(repo, messageId, deviceId, parsed)`. 새 업무 = 파서+테이블 추가(개방-폐쇄) |
| DomainRepo/GenericRepo | 전용 도메인 테이블 / 범용(domain_generic EAV, 키별 행) INSERT |
| ProjectionService | raw → 분기 파생(전용 파서=Fault, 그 외 catch-all=domain_generic) + parse_error 격리 |
| LocationProjector | lat/lon 있으면 domain_location 저장(messageCode 무관, 등록 단말만) |
| MessageProcessor | 수신 1건의 전 단계 오케스트레이션 + 단계별 에러 기록 + ack 신호 |

### 3.2 핵심 설계 포인트

- **MessageProcessor가 유일한 처리 오케스트레이터** — 원본저장→조회→파생 순서와 ack 타이밍, 단계별 에러 기록을 한 곳에서 관리.
- **파서/헤더 추출은 I/O 없는 순수 함수** — 단위 테스트 용이.
- **manual ack** — 원본 INSERT 성공 후에만 MQTT puback 전송(무손실 경계).

## 4. 처리 흐름 (메시지 1건, 단계별 오류 추적)

```
수신 → JSON 파싱
  ├ 실패 → error_log(stage=ingest, raw_text 보존) → ack (재전송 무의미)
  └ 성공
     → messageId 부여 + extractHeader
     → imei로 device_id 조회
     → messages_raw INSERT (device_id 포함, ON CONFLICT (message_key) DO NOTHING)   [내구성 지점]
        ├ INSERT 실패(PG 다운) → ack 안 함 → broker 재전송
        ├ 중복 → ack 후 종료 (이미 처리됨)
        └ 신규 → ★ MQTT ack ★
     → 분기:
        ├ device_id 없음(미등록) → error_yn='Y' + error_detail + error_log(stage=device_lookup)
        │                          → 종료 (도메인·위치 저장 안 함, 원본만 보존)
        └ device_id 있음 →
             ├ 위치 projection: lat/lon 있으면 domain_location(device_id) 저장
             │   └ 실패 → error_log(stage=location)
             └ messageCode projection:
                  ├ 전용 파서 있음(Fault) → domain_<code>(device_id) 저장
                  ├ 없음(catch-all) → 본문 키마다 domain_generic에 한 행씩(EAV) 저장
                  ├ 성공 → error_yn 그대로 'N'
                  └ 실제 파싱/저장 예외 → error_yn='Y' + error_detail + error_log(stage=projection)
```

**불변식:** 원본 저장(=ack) 이후 단계가 모두 실패해도 원본과 에러 기록은 남는다. PG 자체 장애만이 재전송을 유발하며, 그 경우에도 무손실이 유지된다.

## 5. DB 스키마(초안)

샘플 메시지:
```json
{
  "imei": "356938035643809",               // devices 조회 입력 → raw.device_id
  "messageCode": "Fault",                  // 라우팅 구분자 + 공통
  "process_dttm": "2026-06-09 09:03:00",   // 공통
  "message": { "ftp": "100", "sp": "12", "pcode": "P0001" },  // 업무 본문 → 도메인
  "latitude": "19.23222",                  // 공통
  "longitude": "203.12121"                 // 공통
}
```

> 모든 테이블은 생성일시 컬럼 `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`을 가진다. 실제 `schema.sql`에는 각 테이블 정의 아래에 `COMMENT ON TABLE/COLUMN` 코멘트가 정리되어 있다(아래는 구조 요약).

```sql
-- 단말 마스터 — device_id(숫자 PK) ↔ imei(자연키)
CREATE TABLE devices (
  device_id   BIGSERIAL PRIMARY KEY,
  imei        TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 원본 적재 (bronze, 불변) — 숫자 PK(message_id) + 멱등 키(message_key)
CREATE TABLE messages_raw (
  message_id    BIGSERIAL PRIMARY KEY,      -- 숫자 surrogate PK = 전 계층 매핑 키
  message_key   TEXT NOT NULL UNIQUE,       -- 멱등 키(에이전트 결정적 생성) = 재전송 중복 흡수
  device_id     BIGINT REFERENCES devices(device_id),  -- imei 조회 결과. 미등록 시 NULL
  imei          TEXT,                        -- payload 추출 (미등록 단말 추적용)
  message_code  TEXT NOT NULL,
  process_dttm  TIMESTAMPTZ,
  latitude      NUMERIC,
  longitude     NUMERIC,
  raw_payload   JSONB NOT NULL,             -- 단말 원본 JSON 무변형
  error_yn      CHAR(1) NOT NULL DEFAULT 'N',  -- 에러 여부 Y/N
  error_detail  TEXT,                          -- 에러 내용 (error_yn=Y일 때)
  received_at   TIMESTAMPTZ NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 업무 도메인 테이블 (silver, 파생). 각 업무단은 자체 시퀀스 id + message_id 참조 + device_id 보유.
CREATE TABLE domain_fault (
  id           BIGSERIAL PRIMARY KEY,                              -- 업무단 자체 시퀀스
  message_id   BIGINT NOT NULL UNIQUE REFERENCES messages_raw(message_id),  -- 원본 참조(UNIQUE, 멱등)
  device_id    BIGINT NOT NULL REFERENCES devices(device_id),
  ftp TEXT, sp TEXT, pcode TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE domain_location (
  id           BIGSERIAL PRIMARY KEY,
  message_id   BIGINT NOT NULL UNIQUE REFERENCES messages_raw(message_id),
  device_id    BIGINT NOT NULL REFERENCES devices(device_id),
  latitude NUMERIC, longitude NUMERIC,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 범용 업무 테이블 (catch-all, EAV). 전용 파서 없는 코드의 본문을 키마다 한 행씩 저장.
CREATE TABLE domain_generic (
  id           BIGSERIAL PRIMARY KEY,
  message_id   BIGINT NOT NULL REFERENCES messages_raw(message_id),
  device_id    BIGINT NOT NULL REFERENCES devices(device_id),
  message_code TEXT NOT NULL,
  key          TEXT NOT NULL,             -- 업무 본문 키
  value        TEXT,                       -- 값(객체면 JSON 문자열)
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (message_id, key)                 -- 재처리 멱등
);

-- 업무 본문 추출: rawPayload.message(중첩)가 있으면 그것, 없으면 공통 5키 제외한 최상위(평면).
-- domain_<다른업무코드> ... 전용 테이블 패턴 (자체 id PK + message_id UNIQUE FK + device_id NOT NULL).

-- 전용 에러 테이블 — 단계별 오류 추적
CREATE TABLE error_log (
  id           BIGSERIAL PRIMARY KEY,
  message_id   BIGINT,                      -- 원본 참조(원본 저장 전 오류면 NULL)
  message_key  TEXT,                        -- 멱등 키(있을 때)
  stage        TEXT NOT NULL,               -- ingest | device_lookup | projection | location
  message_code TEXT,
  imei         TEXT,
  detail       TEXT NOT NULL,
  raw_text     TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

> 전체 DDL과 컬럼 코멘트(`COMMENT ON ...`), 인덱스는 [src/db/schema.sql](../../../src/db/schema.sql) 참조. 모든 테이블에 `created_at`(생성일시) 보유.

**추적 조회 예시 (원본 + 업무 + 단말 + 에러):**
```sql
SELECT d.imei, r.device_id, r.message_code, r.error_yn, r.error_detail, r.process_dttm,
       f.ftp, f.sp, f.pcode,
       l.latitude, l.longitude,
       e.stage AS error_stage, e.detail AS error_detail
FROM messages_raw r
LEFT JOIN devices         d USING (device_id)
LEFT JOIN domain_fault    f USING (message_id)
LEFT JOIN domain_location l USING (message_id)
LEFT JOIN error_log       e USING (message_id)
WHERE r.message_id = $1;
```

`message_id` 하나로 원본·업무·단말·에러를 모두 추적할 수 있다.

## 6. 오류 처리 · 관측성 · 테스트

### 6.1 오류 처리 매트릭스

| 실패 지점 | 동작 | 무손실 |
|---|---|---|
| Mosquitto 연결 끊김 | 자동 재연결(백오프), QoS1이라 broker가 미전달 보관 | ✅ |
| JSON 파싱 실패 | error_log(stage=ingest, raw_text) 기록 후 ack (재전송 무의미) | ✅ (원본 보존) |
| messages_raw INSERT 실패(PG 다운) | ack 안 함 → broker 재전송 | ✅ |
| 중복(message_key) | ON CONFLICT (message_key)로 흡수, ack | ✅ (무중복) |
| 미등록 imei | 원본 저장됨. error_yn='Y' + error_detail + error_log(device_lookup). **도메인·위치 저장 안 함**. 단말 등록 후 재처리 | ✅ (원본 보존) |
| 전용 파서 없는 코드 | catch-all로 domain_generic(키별 행, EAV) 저장, error_yn='N' (오류 아님) | ✅ |
| 파싱/저장 실제 예외 | 원본 저장됨. error_yn='Y' + error_detail + error_log(projection). 수정 후 재처리 | ✅ (원본 보존) |
| 위치 저장 실패 | 원본 저장됨. error_log(location). 비치명(파서 projection은 계속) | ✅ (원본 보존) |

### 6.2 재처리

원본이 항상 보존되므로, `error_log` 또는 `messages_raw.error_yn='Y'`(error_detail 포함)를 기준으로 오류 행을 재파싱·재조회한다(단말 등록 또는 파서 수정 후). 단말 재수집 불필요.

### 6.3 관측성

- 핵심 지표: 수신율, error_yn='Y' 건수, error_log 단계별 건수, end-to-end latency.
- 구조화 로그(JSON) + 선택적 `/metrics`(Prometheus).

### 6.4 테스트 전략

- **단위**: messageId(멱등), header(추출), 파서(parse 순수 변환), locationProjector(lat/lon 있음→저장, 없음→스킵).
- **통합**(testcontainers PostgreSQL): repo(device/raw/domain/location/error), projectionService(device_id 전파 + parse_error 격리 + 에러 기록), messageProcessor(원본 저장 + 조회 + 위치/도메인 파생 + 단계별 에러 + ack 신호).
- **E2E**: Mosquitto + PG + 에이전트를 docker-compose로 기동, 단말 시뮬레이터 발행 → (a) messages_raw 원본, (b) domain_* 파생(device_id 포함), (c) domain_location 저장, (d) 미등록 imei→unregistered_device + error_log + 도메인/위치 미저장, (e) 미등록 코드→parse_error + error_log, (f) 중복→1건만 검증.

## 7. 향후 확장 경로

- 새 업무 코드: 파서 1개 + 도메인 테이블 1개 추가(개방-폐쇄).
- 대규모 전환 시: 수신과 파생 분리(원본 저장 후 비동기 워커가 파생), 또는 다시 Redis/큐 버퍼 도입.
- 미등록 단말/parse_error 자동 재처리 배치.
