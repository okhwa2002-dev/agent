# Redis Streams 로컬 버퍼 설계

**작성일:** 2026-06-12
**상태:** 설계 승인 대기

## 1. 배경 & 문제

부하 테스트(2000건 순간 발행)에서 **1040건만 저장되고 ~960건이 유실**됐다. 원인은 에이전트가 아니라 **브로커**다:

- 발행속도(~24,000/s) ≫ 에이전트 처리속도(~1,000/s, mqtt.js 백프레셔로 직렬 처리)
- Mosquitto가 느린 구독자를 위해 버퍼링하는 한계(`max_queued_messages` 기본 1000)를 초과 → 초과분 **드롭** (`Outgoing messages are being dropped for client edge-agent`)

에이전트가 받은 메시지는 전부 정확히 처리됐다(0 중복, 0 에러). 즉 **정합성은 문제없고, 순간 폭주를 흡수할 버퍼가 없는 게 문제**다.

## 2. 목표 / 비목표

**목표**
- 순간 폭주(수천 건 동시) 시 **브로커 드롭 0** — 수신 즉시 로컬 버퍼에 적재하여 브로커를 비운다.
- 버퍼에 쌓인 메시지를 워커가 PG로 드레인. 폭주는 흡수하고 천천히 빠진다(무손실).
- 처리량을 위해 **인프로세스 다중 워커**.

**비목표**
- DB 장시간 중지(정기점검) 대량 백로그 보존은 부수 효과로 일부 얻지만 1차 목적 아님.
- 단말→에이전트 프로토콜 변경(MQTT QoS1 유지).
- 업무 처리 로직(`messageProcessor`·repo·파서·스키마) 변경 — **그대로 재사용**.

## 3. 아키텍처

```
단말(MQTT QoS1) ─┐
                 ▼  에이전트 (단일 프로세스)
   ┌──────────────────────────────────────────────────────────┐
   │ [수신부] MqttSubscriber                                    │
   │   수신 → Redis XADD(messages:stream) → 성공 후 MQTT ack    │  ← 내구성 지점
   │                                                            │
   │ [워커 풀] K개 컨슈머 (worker-0 .. worker-K-1)               │
   │   XREADGROUP(BLOCK) → messageProcessor.handle → XACK        │
   │     실패(PG 등) → XACK 안 함 → 재시도 / XAUTOCLAIM 회수      │
   │     반복 실패(poison) → messages:dlq 이동                   │
   └──────────────────────────────────────────────────────────┘
              │                                  │
          Redis(AOF everysec)              PostgreSQL
```

**핵심 변화:** 내구성 지점이 `PG raw INSERT` → **`Redis XADD`**로 이동. 수신을 PG와 분리하여 폭주를 버퍼가 흡수한다.

## 4. 데이터 흐름

### 수신부 (MqttSubscriber)
1. MQTT 수신 → `XADD messages:stream * topic <t> payload <원본텍스트> receivedAt <iso>`
2. XADD 성공 → **MQTT puback(ack)**. XADD 실패(Redis 다운) → ack 안 함 → broker 재전송.

### 워커 풀 (K개 동시)
각 워커 루프:
1. `XREADGROUP GROUP agent-workers worker-i COUNT n BLOCK ms STREAMS messages:stream >`
2. 배치의 각 엔트리 → `messageProcessor.handle(topic, payloadBuffer)` (기존 로직 그대로)
3. 성공 → `XACK messages:stream agent-workers <id>` (+ 주기적 XTRIM/XDEL)
4. 처리 중 throw(인프라 오류) → XACK 안 함 → PEL 잔류 → 재시도
5. 기동 시/주기적으로 `XAUTOCLAIM`(idle > N초)으로 죽은 워커의 미ACK 회수
6. 같은 엔트리가 `delivery count > MAX_RETRY` → `messages:dlq`로 XADD 후 원본 XACK (poison 격리)

## 5. 컴포넌트

| 구분 | 파일/모듈 | 책임 |
|---|---|---|
| 신규 | `src/buffer/RedisStreamQueue.ts` | XADD/XREADGROUP/XACK/XAUTOCLAIM/XTRIM 래핑 |
| 신규 | `src/buffer/WorkerPool.ts` | K개 워커 루프 기동·정지, 재시도·DLQ |
| 변경 | `src/ingest/MqttSubscriber.ts` | handler 호출 → **enqueue(XADD)** 로 변경, ack=XADD 성공 |
| 변경 | `src/main.ts` | Redis 풀 + 큐 + 워커풀 조립 |
| 변경 | `src/config/config.ts` | `REDIS_URL`, `WORKER_CONCURRENCY` 추가 |
| 변경 | `src/db/pool.ts` | pg Pool `max ≥ K + 여유` |
| 유지 | `messageProcessor`·repo·mapper·parser·schema·logger | **무변경 재사용** |

> 의존성: `ioredis` 추가.

## 6. Redis Stream 설계

- 스트림: `messages:stream`
- 컨슈머 그룹: `agent-workers` (`XGROUP CREATE ... $ MKSTREAM`, BUSYGROUP 무시)
- 컨슈머: `worker-0 .. worker-{K-1}`
- 엔트리 필드: `topic`, `payload`(원본 텍스트), `receivedAt`
- DLQ 스트림: `messages:dlq` (poison 격리, 운영자 점검)
- 상태 전이: `XADD` → (XREADGROUP) PEL=처리중 → `XACK`=완료
- 메모리 관리: **XACK 직후 해당 엔트리를 `XDEL`로 삭제** → 스트림은 "미처리 + 처리중"만 보유(폭주 후 자연히 비워짐). `MAXLEN`은 정상 폭주로는 절대 닿지 않을 **매우 큰 안전 상한**으로만 둔다.
  > ⚠️ `XTRIM MAXLEN`으로 오래된 것을 자르는 방식은 **미처리(미XACK) 엔트리까지 잘라 유실**될 수 있으므로 일상 정리에는 쓰지 않는다. 정리는 "XACK된 것만 XDEL"로 정확히 한다.

## 7. 무손실 · 내구성 · 멱등

- **단말→Redis:** MQTT QoS1 + **XADD 성공 후 ack**. Redis 다운 시 ack 안 함 → 재전송.
- **Redis→PG:** PG 처리 성공 후에만 **XACK**. 워커 크래시/PG 다운 시 PEL 잔류 → 재처리.
- **Redis 영속:** **AOF `appendfsync everysec`** (성능·내구성 균형, 비정상 종료 시 ~1초 손실 창). 완전 무손실 필요 시 `always`.
- **멱등:** `messages_raw.message_key` UNIQUE + `ON CONFLICT DO NOTHING`. 재시도·재전송·XAUTOCLAIM 회수로 같은 메시지가 두 번 처리돼도 PG에서 1건만.

## 8. 오류 처리

| 상황 | 동작 |
|---|---|
| Redis XADD 실패(Redis 다운) | MQTT ack 안 함 → broker 재전송 |
| 워커 처리 throw(PG 다운 등 인프라) | XACK 안 함 → PEL 잔류 → 재시도(백오프). DB 복구 시 자동 진행 |
| 워커 크래시 | 다른 워커가 XAUTOCLAIM(idle>N초)으로 회수 |
| 논리 오류(미등록 imei/파싱 실패) | `messageProcessor`가 내부 흡수(error_yn/error_log) 후 정상 반환 → **XACK**(재시도 안 함) |
| poison(반복 throw, delivery>MAX_RETRY) | `messages:dlq`로 이동 + 로그 → 무한 재시도 방지 |
| 버퍼 폭증(Redis 미처리 누적) | 경고 로그 + 메트릭(스트림 길이/PEL). 안전 상한 도달은 **재앙적 백로그 신호 → 알림**이며, 미처리분을 자동으로 버리지 않는다(정리는 XACK된 것만 XDEL) |

> 핵심: `messageProcessor`는 논리 오류를 이미 내부에서 흡수하므로, **워커가 throw하는 건 인프라 오류뿐** → 대부분 재시도로 해결되고 DLQ는 진짜 poison 안전망.

## 9. 동시성 · 정합성

- consumer group이 **한 엔트리를 그룹 내 한 컨슈머에게만** 분배 → 두 워커가 같은 메시지를 동시에 처리하지 않음.
- 서로 다른 메시지 = 서로 다른 PG 행 → 동시 INSERT 무관, `message_key` UNIQUE로 충돌 흡수 → **레이스 없음**.
- **순서는 비보장**(워커별 병렬). 메시지가 서로 독립적이라 기능상 무방.
- **pg 풀 크기 ≥ K + 여유** (동시 쿼리 수용).

## 10. 설정 / 인프라

| 변수 | 필수 | 기본 | 설명 |
|---|---|---|---|
| `REDIS_URL` | ✅ | — | 예 `redis://localhost:6379` |
| `WORKER_CONCURRENCY` | | `4` | 인프로세스 워커 수 K |

- docker-compose에 **Redis 서비스**(`redis:7-alpine`, `--appendonly yes --appendfsync everysec`, 6379, 볼륨) 추가.
- `.env`/`.env.example`에 `REDIS_URL`, `WORKER_CONCURRENCY` 추가.

## 11. 테스트 전략

- **통합(testcontainers Redis):** `RedisStreamQueue` — enqueue/claim/ack/XAUTOCLAIM, 멱등.
- **통합:** `WorkerPool` — 정상 드레인, 실패 재시도, poison→DLQ, 크래시 회수.
- **기존 통합(testcontainers PG):** `messageProcessor`·repo 무변경 → 그대로 통과.
- **부하 재검증:** `scripts/load.mjs`로 2000건(이상) 폭주 → **전량 저장(드롭 0)** + 0 중복/0 에러 확인. 이전 1040 유실 → 무손실로 개선 입증.

## 12. 롤아웃 / 마이그레이션

- Redis 서비스 기동 → `.env`에 `REDIS_URL` 추가 → 빌드/배포.
- 스키마 변경 없음(기존 테이블 그대로). 무중단 전환 가능(수신부+워커 동시 기동).
- 기존 단일 프로세스 동작과 외부 인터페이스(MQTT 토픽, DB 스키마) 동일.

## 13. 트레이드오프 (수용)

- ➕ 폭주 흡수(브로커 드롭 0), DB 일시 장애에도 수신 지속, 다중 워커로 드레인 빠름.
- ➖ Redis 프로세스 1개 추가(운영·모니터링), 내구성 2계층, AOF everysec ~1초 창.
- 폭주 자체의 무손실은 버퍼가 보장하고, 워커 수는 드레인 지연(latency)에만 영향.
