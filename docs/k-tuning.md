# 워커 수 K(WORKER_CONCURRENCY) 튜닝 가이드

처리 워커 수 `WORKER_CONCURRENCY`(기본 4)를 언제, 어떻게 조정할지에 대한 기준.

## 1. K가 하는 일

- K개의 워커가 Redis Streams consumer group에서 병렬로 claim → PG 적재(원본+파생) → ack 한다.
- 워커마다 **전용 Redis 연결**을 쓰고(블로킹 XREADGROUP 격리), pg 풀은 `K+4`로 생성된다.
- K는 **드레인 속도**(버퍼→PG)만 좌우한다. 수신(MQTT→Redis)은 K와 무관하게 즉시 적재·ack된다.

## 2. 측정 결과 (참고치)

2000건 순간 발행(폭주) → 전량 PG 저장까지 벽시계 시간. 발행(~1.5s) 포함, 손실·중복·에러 0.

| K | 소요 | 처리율(근사) |
|---|---|---|
| 1 | 15.9s | ~130/s |
| 2 | 7.9s | ~260/s |
| 4 (기본) | 5.3s | ~380/s |
| 8 | 3.4s | ~590/s |

측정 환경: Windows 11 + Docker Desktop, 로컬 단일 호스트(브로커·Redis·PG·에이전트 동일 머신), 메시지 = Common 코드(원본 1행 + domain_generic 3행). 2026-07-08. **절대값은 하드웨어·메시지 형태에 따라 다르므로 반드시 대상 장비에서 재측정할 것.**

관찰: K=1→2는 거의 선형(2배), 이후 수확 체감(PG 단일 인스턴스 병목). K=8에서도 개선은 지속.

## 3. 재측정 방법 (대상 장비에서)

```powershell
npm run device -- register bench-k          # 측정용 단말 등록 (1회)

# K별 반복: 에이전트를 해당 K로 기동한 뒤
$env:WORKER_CONCURRENCY="8"; npm start      # (창1)

node scripts/load.mjs --count 2000 --imei bench-k --topic device/B/msg   # (창2) 발행 시작 = t0
# messages_raw 건수가 2000이 될 때까지의 시간 측정:
docker exec agent-postgres psql -U agent -d agent_db -c "SELECT count(*) FROM messages_raw WHERE imei='bench-k';"

# 정리 (다음 K 측정 전)
docker exec agent-postgres psql -U agent -d agent_db -c "DELETE FROM domain_generic WHERE message_id IN (SELECT message_id FROM messages_raw WHERE imei='bench-k'); DELETE FROM messages_raw WHERE imei='bench-k';"
```

또는 운영 중이라면 `/metrics`로 무중단 측정: 처리율 = `rate(agent_processed_total)`, 평균 지연 = `rate(agent_e2e_latency_ms_sum) / rate(agent_processed_total)`.

## 4. 조정 기준 (운영 판단 룰)

**늘려야 할 신호** (지표 기반):
- `agent_stream_backlog`가 유입 피크 때마다 쌓였다가 해소가 느리다 (드레인 < 유입).
- `agent_e2e_latency_ms` 평균이 지속 상승한다.
- 위 상태에서 PG CPU/IO에 여유가 있다.

**늘려도 소용없는 경우**:
- PG가 이미 포화(CPU/IO 100% 근처) → K를 올려도 경합만 증가. PG 쪽(하드웨어, 인덱스, 배치)을 먼저 본다.
- backlog 원인이 PG 다운/장애일 때 → K와 무관, 복구가 답.

**비용**: K를 올리면 PG 연결 `K+4`개 + Redis 연결 `K+1`개 + PG 동시 쓰기 부하가 함께 는다. PG `max_connections`(기본 100)와 다른 클라이언트(재처리 배치 등)를 고려해 여유를 남길 것.

**권장 시작점**: 기본 4로 시작 → 예상 피크 유입률의 **2~3배 드레인 처리율**이 확보되는 최소 K를 재측정으로 찾는다(폭주 후 빠른 해소 여유). 엣지 장비가 저사양이면 K를 올리기 전에 측정 필수.
