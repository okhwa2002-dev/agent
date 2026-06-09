# 실시간 메시지 수집 파이프라인 설계

**작성일:** 2026-06-09
**상태:** 설계 승인 대기

## 1. 개요

단말기에서 발행하는 메시지를 엣지 상주 에이전트가 실시간으로 수신·가공하여 중앙 서버에 무손실로 저장하는 파이프라인을 설계한다.

### 1.1 핵심 요구사항

| 항목 | 결정 |
|---|---|
| 에이전트 형태 | 엣지/게이트웨이 상주 프로세스 (Node.js/TypeScript) |
| 단말→에이전트 통신 | MQTT (QoS 1), 브로커는 Mosquitto |
| 메시지 포맷 | JSON |
| 규모 | 중규모 (단말 수백~수천, 초당 수백~수천 메시지) |
| 유실 허용도 | **무손실 필수 (at-least-once)** |
| 에이전트 가공 단계 | 중계 + envelope 정규화 + 증강(enrichment). **업무 파싱은 하지 않음**(원본 payload 무변형 보존) |
| 에이전트→서버 통신 | HTTP 배치 POST |
| 큐 | Redis Streams + Consumer Group (엣지 로컬 병치, AOF everysec) |
| 서버 | 신규 설계 |
| 서버 저장소 | PostgreSQL |
| 저장 정책 | **원본 적재(raw) + 업무 타입별 파싱 저장(파생)** — 서버에서 수행 |
| 업무 타입 라우팅 | JSON 페이로드 내 `messageCode` 필드 |
| 단말 관리 | 단말 마스터(`devices`) PK=`device_id` ↔ `imei`(UNIQUE). payload의 imei로 device_id 조회. `device_id`가 단말 관리 키 |
| 매핑 키 | `message_id` = 원본·업무 테이블을 잇는 메시지 단위 키(PK/FK) |

### 1.2 설계 원칙

- **수신 즉시 디스크, 서버 ack 후 삭제** — 무손실의 핵심.
- **원본(raw)이 진실의 원천** — 서버는 원본을 먼저 불변 저장하고, 업무 테이블은 거기서 파생. 업무 스키마/파싱이 바뀌어도 단말 재수집 없이 raw를 재파싱한다.
- **에이전트는 원본 payload를 변형하지 않는다** — 메타데이터 envelope만 부착, 업무 파싱은 서버가 담당.
- 컴포넌트는 하나의 책임만 가지며, 명확한 인터페이스로 통신(독립 테스트 가능).
- 큐는 인터페이스로 추상화 — 향후 대규모 전환 시 구현체 교체 경로 확보.
- Pipeline 가공 로직은 I/O 없는 순수 변환으로 격리.

### 1.3 내구성 명시

AOF `everysec`은 Redis 프로세스 비정상 종료 시 **최대 약 1초 구간**의 손실 가능성이 이론상 존재한다(완전한 zero-loss는 `appendfsync always`만 보장). everysec은 성능·내구성의 표준 균형점이며, Redis가 엣지 로컬 병치이므로 네트워크 단절과는 무관하다. 절대 무손실이 필요해지면 `always`로 전환한다(설정 변경 한 줄).

## 2. 전체 아키텍처 & 데이터 흐름

```
┌──────────┐  MQTT   ┌─ 엣지 박스 ──────────────────────────────────┐  HTTP   ┌──────────┐
│  단말기   │ ──────▶ │ [Mosquitto] ──▶ [Node 에이전트] ──▶ [Redis    │ 배치POST │  서버     │
│ 수백~수천 │  QoS1   │  브로커         (구독·가공·전송)    Streams]   │ ──────▶ │  +  PG    │
└──────────┘         └──────────────────────────────────────────────┘         └──────────┘
```

### 2.1 데이터 흐름

1. **Ingest**: 에이전트가 Mosquitto 구독 → MQTT 메시지 수신 → 최소 검증(파싱 가능 여부) → **즉시 Redis Stream에 `XADD`** → 그 후 MQTT broker에 QoS 1 ack. 이 순서가 무손실의 첫 경계.
2. **Worker**: `XREADGROUP`으로 미처리 메시지를 배치로 claim(PEL 진입) → 변환/정규화/증강 → HTTP 배치 POST → 서버가 200 응답하면 그때 `XACK`(PEL에서 제거). 이것이 무손실의 두 번째 경계.
3. **장애 복구**: 서버 다운 → 전송 실패 → ack 안 함 → 메시지는 PEL 잔류 → 지수 백오프 재시도. 에이전트/워커 크래시 → Redis Stream·PEL에 보존 → 재기동 후 `XAUTOCLAIM`으로 회수.

### 2.2 End-to-End 보장

MQTT QoS 1 + Redis Streams ack 모델 + 서버 멱등 저장이 합쳐져 단말→서버 끝까지 at-least-once를 보장한다. 그 대가로 중복이 발생할 수 있어 **서버 측 멱등 저장이 필수**다(섹션 5).

## 3. 에이전트 내부 컴포넌트

```
src/
├── ingest/
│   ├── MqttSubscriber.ts      # MQTT 구독·재연결. 수신만 담당
│   └── RawMessage.ts          # 수신 원본 타입 (topic, payload, deviceId, receivedAt)
├── queue/
│   ├── MessageQueue.ts        # 인터페이스: enqueue / claimBatch / ack / reclaimStale
│   └── RedisStreamQueue.ts    # 구현체 (Redis Streams + Consumer Group)
├── pipeline/
│   ├── Transformer.ts         # 변환/정규화 (포맷·단위·스키마 검증)
│   ├── Enricher.ts            # 증강 (메타데이터 결합, 이벤트 트리거)
│   └── Pipeline.ts            # Transformer→Enricher 조합, 단일 진입점 (I/O 없는 순수 변환)
├── dispatch/
│   ├── ServerClient.ts        # HTTP 배치 POST + 재시도/백오프 + 멱등키 헤더
│   └── Dispatcher.ts          # XREADGROUP→가공→전송→XACK 루프
├── config/config.ts           # MQTT URL, 서버 URL, 배치크기, 간격 등
└── main.ts                    # 컴포넌트 조립(DI) + graceful shutdown
```

### 3.1 컴포넌트별 책임

| 컴포넌트 | 책임 | 의존 |
|---|---|---|
| MqttSubscriber | MQTT 구독, 수신 콜백, 자동 재연결. QoS 1 ack은 **enqueue 성공 후** 수행 | mqtt.js |
| MessageQueue (인터페이스) | `enqueue` / `claimBatch` / `ack` / `reclaimStale` 추상화 | 없음 |
| RedisStreamQueue | 위 인터페이스를 Redis Streams로 구현 | ioredis |
| Pipeline | RawMessage → ServerRecord. **원본 payload는 무변형 보존**하고 envelope(messageId, agentId, deviceId, receivedAt, rawPayload)만 구성. 업무 파싱 없음 | — |
| ServerClient | 배치 POST, 2xx/4xx/5xx 분기, 지수 백오프, 멱등 키 헤더 | undici/axios |
| Dispatcher | claim→가공→전송→ack 루프 + 실패 처리. 동시성·배치 타이밍 제어 | 위 전부 |

### 3.2 핵심 설계 포인트

- **MessageQueue 인터페이스 분리** → 현재 Redis Streams, 향후 대규모 시 NATS/Kafka 등으로 교체.
- **Pipeline은 I/O 없는 변환 로직** → 단위 테스트에서 입력→출력만 검증.
- **Dispatcher가 유일한 상태 변경 오케스트레이터** → 무손실 보장 로직을 한 곳에 모음.
- Redis는 `nack` 대신 **ack 안 하면 PEL에 자동 잔류**하므로 `reclaimStale`(XAUTOCLAIM 기반)로 재처리.

## 4. Redis Streams 큐 설계 & 상태 전이

### 4.1 키 구조

```
stream key:  messages:stream          # 모든 수신 메시지 (XADD)
group:       agent-workers            # Consumer Group (XGROUP CREATE)
consumers:   worker-1, worker-2 ...   # 각 워커 인스턴스
dlq:         messages:dlq             # Dead Letter Queue 스트림
```

### 4.2 메시지 한 건의 상태 전이

```
[MQTT 수신]
   │  XADD messages:stream * deviceId .. payload ..   ← 디스크(AOF) 기록
   ▼
(Stream 적재)  ──────────────────────────  여기까지 성공해야 MQTT QoS1 ack
   │  XREADGROUP GROUP agent-workers worker-1 COUNT 100
   ▼
(PEL 진입 = "처리중")
   │  Pipeline 가공 → ServerClient 배치 POST
   ├── 서버 200 ──▶ XACK messages:stream agent-workers <ids> ──▶ [완료, PEL 제거]
   └── 실패/타임아웃 ──▶ ack 안 함 ──▶ PEL 잔류 ──▶ XAUTOCLAIM 재처리
```

### 4.3 핵심 메커니즘

- **무손실의 두 경계**: ①`XADD` 성공 후에만 MQTT ack ②서버 200 후에만 `XACK`.
- **크래시 복구**: 처리 중 죽은 메시지는 PEL에 idle 상태로 잔류 → `XAUTOCLAIM`(idle > N초)으로 회수.
- **메모리 관리**: `XACK`만으로는 Stream에서 엔트리가 삭제되지 않음 → 주기적 `XTRIM`/`XDEL`로 정리(트리밍 정책 필요).
- **백프레셔**: 서버 장기 다운 시 Stream 누적 → `MAXLEN` 상한 + 디스크 사용량 모니터링으로 보호.

## 5. 서버 측 — 멱등 수신 & 원본 적재 + 업무별 파생 저장

at-least-once이므로 중복 메시지가 반드시 도착할 수 있고, 서버가 멱등 저장으로 흡수하는 것이 무손실-무중복의 마지막 퍼즐이다. 또한 서버는 **원본을 먼저 불변 저장한 뒤(bronze) 업무 타입별 테이블로 파생(silver)** 한다.

### 5.1 2단계 저장 흐름 (raw landing → projection)

```
배치 수신 → 검증
   │
   ① 원본 적재 (동기): raw_payload에서 imei·공통헤더 추출
   │     → devices 에서 imei로 device_id 조회 (미등록이면 device_id=NULL)
   │     → messages_raw 에 UPSERT (message_id UNIQUE)
   │        INSERT ... ON CONFLICT DO NOTHING  →  중복 흡수
   │        status = 'received' (미등록 단말이면 'unregistered_device')
   │   ── 여기까지 성공하면 200 응답 (에이전트 핸드셰이크 완결) ──
   │
   ② 파생 (비동기 또는 동기): messages_raw 의 미처리 행을 읽어
       message_code 로 업무 분기 → 도메인 테이블에 INSERT
         ├── 성공 → status = 'parsed'
         ├── 파싱 실패(스키마/타입 불명) → status = 'parse_error' + 사유 기록 (원본은 보존)
         └── 멱등: 도메인 테이블도 message_id UNIQUE → 재처리해도 1건
```

**핵심:** ②가 실패하거나 지연돼도 ①의 원본은 항상 남는다. 파싱 로직을 고친 뒤 `parse_error`/미처리 행만 재처리하면 단말 재수집 없이 복구된다.

> **동기 vs 비동기 파생:** 기본은 ①과 ② 모두 같은 트랜잭션 흐름에서 동기로 처리(중규모엔 충분, 단순). 파싱이 무거워지거나 처리량이 늘면 ②를 백그라운드 워커(`status='received'` 폴링)로 분리해 수신 지연과 분리한다. 스키마/상태 컬럼은 두 방식 모두를 지원하도록 설계한다.

### 5.2 수신 API (배치)

```
POST /api/v1/messages/batch
Headers: Idempotency-Key: <batch-uuid>     # 배치 단위 재시도 식별
Body: {
  agentId: "edge-01",
  messages: [
    { messageId, deviceId, receivedAt, rawPayload: { ...원본 JSON 그대로... } },
    ...
  ]
}
Response 200: { accepted: [messageId...], duplicated: [messageId...] }
```

`rawPayload`는 단말이 보낸 JSON을 **무변형**으로 담는다(업무 타입 필드 포함).

### 5.3 멱등성의 두 층위

1. **messageId (메시지 단위)** — 에이전트가 MQTT 수신 시점에 안정적 ID 부여. **기본 정책**: 단말이 고유 메시지 ID를 보내면 그것을 그대로 사용하고, 보내지 않으면 `deviceId + 단말타임스탬프 + 시퀀스`의 결정적 해시로 대체한다(재전송 시에도 동일 ID가 재현되어야 멱등이 성립). raw·도메인 테이블 모두 이 ID로 중복 판정. → 단말 페이로드에 고유 ID가 포함되는지는 구현 계획 단계에서 단말 스펙으로 확정.
2. **Idempotency-Key (배치 단위)** — 네트워크 타임아웃으로 동일 배치를 재전송할 때 식별. 이미 처리한 배치면 동일 응답 재반환.

### 5.4 서버 컴포넌트

```
server/
├── api/messagesController.ts   # POST /batch 핸들러, 검증
├── service/ingestService.ts    # ① imei→device_id 조회 + 원본 멱등 UPSERT 오케스트레이션
├── service/projectionService.ts# ② raw → 도메인 테이블 파싱·라우팅
├── parsers/                     # 업무 타입별 파서 (message_code → 파서 매핑 레지스트리)
│   ├── ParserRegistry.ts
│   └── <code>Parser.ts ...
├── repo/deviceRepo.ts          # devices 조회 (imei → device_id)
├── repo/rawRepo.ts             # messages_raw 접근 (UNIQUE message_id)
├── repo/domainRepo.ts          # 도메인 테이블 접근
└── db/schema.sql               # devices + raw + 도메인 테이블 + 인덱스
```

**파서 레지스트리:** `message_code` 값으로 파서를 조회하는 매핑 테이블(예: `"Fault"` → `FaultParser`). 새 업무 코드는 파서 1개 추가 + 도메인 테이블 1개 추가로 확장(개방-폐쇄). 미등록 코드는 `parse_error`로 격리.

### 5.5 스키마(초안)

샘플 메시지 기준으로 구체화한다. 단말 식별자(`imei`)는 **단말 마스터(`devices`) 한 곳에서 관리**하며, payload의 imei로 `devices`를 조회해 얻은 `device_id`를 raw에 저장한다. 공통 헤더 필드(`messageCode`, `process_dttm`, `latitude`, `longitude`)는 raw 테이블에 컬럼으로 파싱하고, 업무별 본문(`message.*`)만 도메인 테이블로 파생한다. 공통 필드는 복제하지 않고 `message_id`로 join 한다(단일 출처).

```
샘플 원본:
{
  "imei": "356938035643809",               → devices 조회 입력 → raw.device_id 로 매핑
  "messageCode": "Fault",                  → 라우팅 구분자 + 공통 (raw.message_code)
  "process_dttm": "2026-06-09 09:03:00",   → 공통 (raw.process_dttm)
  "message": { "ftp": "100",               → 업무 본문 → 도메인 테이블
               "sp": "12",
               "pcode": "P0001" },
  "latitude": "19.23222",                  → 공통 (raw.latitude)
  "longitude": "203.12121"                 → 공통 (raw.longitude)
}
```

> 서버 저장 시: ① `raw_payload`에서 `imei` 추출 → `devices`에서 `device_id` 조회 → raw에 저장. ② 나머지 공통 헤더는 raw 컬럼으로 파싱. `message_id`가 원본·업무 테이블을 잇는 매핑 키, `device_id`가 단말 관리 키다.
>
> **미등록 imei 처리:** `devices`에 없는 imei면 원본은 그대로 저장하되 `device_id`는 NULL, `status='unregistered_device'`로 격리한다(원본 무손실). 단말 등록 후 재처리하면 device_id가 채워진다.

```sql
-- ⓪ 단말 마스터 — device_id ↔ imei. 단말기 관리의 단일 출처
CREATE TABLE devices (
  device_id     TEXT PRIMARY KEY,           -- 단말 관리 키 (전 계층에서 사용)
  imei          TEXT NOT NULL UNIQUE,       -- 하드웨어 식별자 (payload에서 조회 입력)
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
  -- + 단말 메타데이터(모델, 설치 위치 등) 필요 시 확장
);

-- ① 원본 적재 (bronze, 불변) — 원본 JSON + 공통 헤더 파싱
CREATE TABLE messages_raw (
  message_id    TEXT PRIMARY KEY,           -- 멱등 판정 키 (에이전트 부여) = 전 계층 매핑 키
  agent_id      TEXT NOT NULL,
  device_id     TEXT REFERENCES devices(device_id),  -- imei 조회로 매핑. 미등록 시 NULL
  message_code  TEXT NOT NULL,              -- messageCode: 라우팅 구분자 + 공통
  process_dttm  TIMESTAMPTZ,                -- 단말 처리 시각 (공통)
  latitude      NUMERIC,                    -- 공통 위치
  longitude     NUMERIC,                    -- 공통 위치
  raw_payload   JSONB NOT NULL,             -- 단말 원본 JSON 전체 무변형 (imei 포함)
  status        TEXT NOT NULL DEFAULT 'received',  -- received | parsed | parse_error | unregistered_device
  parse_error   TEXT,                        -- 파생/조회 실패 사유
  received_at   TIMESTAMPTZ NOT NULL,        -- 에이전트 수신 시각
  stored_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_raw_status ON messages_raw (status) WHERE status <> 'parsed';
CREATE INDEX idx_raw_code   ON messages_raw (message_code, received_at);
CREATE INDEX idx_raw_device ON messages_raw (device_id, received_at);  -- 단말별 조회

-- ② 업무 타입별 도메인 테이블 (silver, 파생) — message_code 마다 하나
-- 예: messageCode = "Fault" → message.{ftp, sp, pcode}
CREATE TABLE domain_fault (
  message_id   TEXT PRIMARY KEY REFERENCES messages_raw(message_id),  -- 멱등 + lineage
  ftp          TEXT,
  sp           TEXT,
  pcode        TEXT
);
-- domain_<다른업무코드> ... 동일 패턴. 공통 필드(시각/위치)는 messages_raw에서 join.
```

**조회 예시 (도메인 + 공통 + 단말 결합):**
```sql
SELECT d.imei, r.device_id, r.message_code, r.process_dttm, r.latitude, r.longitude,
       f.ftp, f.sp, f.pcode
FROM domain_fault f
JOIN messages_raw r USING (message_id)
LEFT JOIN devices  d USING (device_id);
```

도메인 테이블의 `message_id`를 raw로 FK 연결하면 원본 추적(lineage)이 보장되고, 공통 필드 중복 저장 없이 단일 출처를 유지한다.

## 6. 오류 처리 · 백프레셔 · 관측성 · 테스트

### 6.1 오류 처리 매트릭스

| 실패 지점 | 동작 | 무손실 |
|---|---|---|
| Mosquitto 연결 끊김 | 에이전트 자동 재연결(백오프), QoS1이라 broker가 미전달 보관 | ✅ |
| XADD 실패(Redis 다운) | MQTT ack 안 함 → broker 재전송 | ✅ |
| Pipeline 가공 예외 | XACK 안 함 → PEL 잔류 → 재시도. N회 초과 시 DLQ로 이동 | ✅ (격리) |
| 서버 5xx/타임아웃 | XACK 안 함 → 지수 백오프 재시도. Stream 누적 | ✅ |
| 서버 4xx(잘못된 데이터) | 재시도 무의미 → DLQ 이동 + 알림 | ✅ (격리) |
| 워커 크래시 | PEL의 idle 메시지를 XAUTOCLAIM으로 회수 | ✅ |
| 서버 파생 파싱 실패(②) | **원본(messages_raw)은 이미 저장됨**. 해당 행 `status='parse_error'` + 사유 기록 → 파서 수정 후 재처리 | ✅ (원본 보존) |
| 미등록 업무 타입 | `parse_error`로 격리 + 알림. 원본 보존되므로 파서 추가 후 재파싱 | ✅ (원본 보존) |

### 6.2 백프레셔

- Stream `MAXLEN ~` 상한 + 디스크 사용량 모니터링.
- 상한 근접 시 경고 알림, 임계 초과 시 오래된 것부터 DLQ 백업 또는 운영자 개입.

### 6.3 관측성

- 핵심 지표: Stream 길이(미전송 적체), PEL 크기(처리 지연), DLQ 길이, 서버 전송 성공/실패율, end-to-end latency.
- 구조화 로그(JSON) + `/metrics` 엔드포인트(Prometheus 호환).

### 6.4 테스트 전략

- **단위**: Pipeline(순수 변환·원본 무변형 보존 검증). 업무 타입별 파서(각 `<type>Parser`) — raw JSON → 도메인 레코드 매핑, 잘못된 페이로드 → parse_error.
- **통합**: RedisStreamQueue — testcontainers로 실제 Redis 기동, enqueue/claim/ack/reclaim 검증. 크래시 시나리오(ack 전 죽음 → PEL 잔류 → 재처리) 포함. 서버 ingest/projection — 실제 PG로 raw 적재 + 도메인 파생 검증.
- **계약**: ServerClient ↔ 서버 API — 배치 포맷(rawPayload 무변형), 멱등 응답, 중복 처리.
- **E2E**: Mosquitto+Redis+에이전트+서버+PG를 docker-compose로 기동, 단말 시뮬레이터로 발행 → **(a) messages_raw 원본 저장, (b) 업무 타입별 도메인 테이블 파생** 모두 확인. **중복 주입 테스트**(같은 messageId 2회) → raw·도메인 각 1건만. **미등록 타입 주입** → raw 저장 + parse_error 격리. **재파싱 테스트** → parse_error 행을 파서 수정 후 재처리하면 도메인 테이블에 반영.

## 7. 향후 확장 경로

- 대규모 전환 시 MessageQueue 인터페이스를 Kafka/NATS JetStream 구현체로 교체.
- 다중 워커 수평 확장(Consumer Group이 이미 지원).
- 서버 수신단을 메시지 큐(Kafka) 기반 비동기 소비로 전환.
