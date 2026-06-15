# 부하 테스트 보고서 — Redis 버퍼 폭주 흡수 검증

- 일자: 2026-06-15
- 대상 커밋: `f40ce6d feat: Redis Streams buffer + worker pool for burst absorption`
- 관련 설계: [superpowers/specs/2026-06-12-redis-buffer-design.md](superpowers/specs/2026-06-12-redis-buffer-design.md)
- 목적: 순간 대량 발행(폭주) 시 **무손실·무중복**으로 PostgreSQL에 적재되는지 검증.

---

## 1. 배경 — 왜 했나

초기 부하 테스트에서 2,000건을 순간 발행했을 때 **1,040건만 저장**되는 손실이 발생했다.
원인 분석 결과 손실 지점은 에이전트가 아니라 **Mosquitto 브로커**였다.

- 발행자가 2,000건을 ~0.08s에 쏟아부으면, 브로커의 구독자(에이전트) 송신 큐가
  기본값 `max_queued_messages=1000`(+inflight ~40)에서 가득 차 **초과분 ~960건을 브로커가 버린다.**
- 에이전트 자체는 받은 건 100% 무손실·무중복 처리했다(에러 0).

→ 무손실에는 두 가지가 함께 필요하다:
1. **브로커 큐 용량** 상향 — 폭주분을 브로커가 버리지 않게.
2. **에이전트 즉시 ack** — 브로커 큐가 빨리 비워지게(Redis 버퍼).

## 2. 적용한 변경

| 영역 | 변경 |
|---|---|
| Mosquitto | `max_queued_messages 0`(무제한) + `max_inflight_messages 1000` |
| 에이전트 수신부 | 메시지를 `messages:stream`에 XADD 후 **즉시 MQTT ack**(폭주 흡수) |
| 에이전트 처리부 | K개 워커(consumer group, 기본 4)가 PG로 병렬 드레인, 성공 시 Redis XACK |
| 멱등/격리 | `messages_raw.message_key` UNIQUE(중복 흡수), 실패 reclaim 재시도, poison → DLQ |

## 3. 환경

- 호스트: Windows 11, Docker Compose
- 서비스: Mosquitto 2(1883), PostgreSQL 16(5435), Redis 7(6379, AOF everysec)
- 에이전트: `WORKER_CONCURRENCY=4`, pg pool max=K+4=8
- 발행 스크립트: [scripts/load.mjs](../scripts/load.mjs) (메시지마다 고유 `seq` → message_key 전부 상이)
- 메시지: `{ imei, messageCode:'Common', seq, v1, v2 }` — 전용 파서 없음 → `domain_generic`(본문 3키 EAV)

## 4. 시나리오 및 결과

### 4.0 회귀(수정 전 재현)
| 발행 | 저장 | 손실 | 비고 |
|---|---|---|---|
| 2,000 | 1,040 | 960 | 브로커 큐 오버플로(수정 전). 에러 0 — 에이전트는 정상 |

### 4.1 수정 후 — 단일 단말 폭주
| 시나리오 | 발행 속도 | 저장 | distinct_key | 에러 | 버퍼 피크 | 완전 처리 |
|---|---|---|---|---|---|---|
| 2,000건 | 20,202/s | **2,000** | 2,000 | 0 | ~76 | ~29s |
| 5,000건 | 29,940/s | **5,000** | 5,000 | 0 | ~2,142 | ~37s |
| 10,000건 | 42,553/s | **10,000** | 10,000 | 0 | ~4,716 | ~37s |

### 4.2 수정 후 — 다중 단말 동시 발행
5개 단말(load-001~005)이 **동시에** 각 2,000건 발행(총 10,000건 동시).
(load-001은 직전 단일 10,000 테스트 누적분 포함 → 12,000)

| 단말 | 저장 | distinct_key | 에러 |
|---|---|---|---|
| load-001 | 12,000 | 12,000 | 0 |
| load-002 | 2,000 | 2,000 | 0 |
| load-003 | 2,000 | 2,000 | 0 |
| load-004 | 2,000 | 2,000 | 0 |
| load-005 | 2,000 | 2,000 | 0 |

### 누적 최종 무결성 (총 20,000건)
| 지표 | 값 | 의미 |
|---|---|---|
| messages_raw | **20,000** | 손실 0 |
| distinct message_key | **20,000** | 중복 0 |
| error_yn=Y / error_log | **0 / 0** | 처리 에러 0 |
| domain_generic | **60,000** | 20,000 × 본문 3키 EAV 정상 |
| Redis DLQ / stream | **0 / 0** | poison 0, 완전 드레인 |

## 5. 관찰 — 버퍼가 실제로 폭주를 흡수

드레인 중 `XLEN messages:stream`(미처리 적체)이 발행 직후 치솟았다가 0으로 수렴했다.

- 10,000건 단일: 발행 0.23s(42,553/s) → 적체 **t=12s에 4,716건 피크** → t=37s에 0.
- 5단말 동시: 적체 **3,392건 피크** → t=34s에 0.

발행 속도가 워커 처리량을 순간적으로 추월해도, 초과분은 Redis가 버퍼링하고
MQTT는 이미 즉시 ack되어 **브로커가 한 건도 떨구지 않았다.** 이것이 설계 의도(폭주 흡수)의 실증이다.

## 6. 결론

- 수정 전 2,000건에서 960건을 잃던 손실이, 수정 후 **2,000 / 5,000 / 10,000 / 20,000(다중)** 모든 시나리오에서 **손실 0·중복 0·에러 0**으로 해소됨.
- consumer group이 각 엔트리를 정확히 한 워커에 분배 → 다중 단말 동시에도 단말별 카운트 정확, 교차 오염 없음.
- `message_key` UNIQUE 멱등성과 4-워커 병렬 처리가 안전하게 공존함을 실증.

## 7. 재현 방법

```bash
docker compose up -d
docker exec -i agent-postgres psql -U agent -d agent_db \
  -c "INSERT INTO devices (imei) VALUES ('load-001') ON CONFLICT DO NOTHING;"
npm run build && npm start            # 별도 셸에서 에이전트 기동

# 단일 폭주
node scripts/load.mjs --count 10000 --imei load-001 --topic device/A/msg

# 검증 (저장 == 발행, 중복·에러 0 확인)
docker exec -i agent-postgres psql -U agent -d agent_db \
  -c "SELECT count(*), count(DISTINCT message_key), count(*) FILTER (WHERE error_yn='Y')
      FROM messages_raw WHERE imei='load-001';"
```

> 적체 관찰: `docker exec agent-redis redis-cli XLEN messages:stream` (발행 직후 치솟았다 0으로 수렴).
> 격리 확인: `XLEN messages:dlq`(=0), `error_log` 건수(=0).
