# 운영 가이드 — Edge Agent

에이전트 운영에 필요한 명령어 모음. 값은 `docker-compose.yml`·`.env.example` 기본값 기준
(Mosquitto `1883/9001`, PostgreSQL `5435`, Redis `6379`, 관측성 HTTP `9100`).

> **인증·인가(dev 기본값 — 운영 배포 시 교체):**
> - MQTT 계정: `agent`/`agentmqttpw`(에이전트·운영, `device/+/msg` 구독+발행), `device`/`devicemqttpw`(단말 공용, **ACL로 자기 clientId 토픽 `device/<clientId>/msg`에만 발행 가능**). 익명 차단, 메시지 64KB 제한.
> - Redis: `agentredispw`(requirepass). 클라이언트는 URL에 포함: `mqtt://agent:agentmqttpw@localhost:1883`, `redis://:agentredispw@localhost:6379`.
> - **포트 노출**: 외부 공개는 MQTT(1883/9001)뿐. PG(5435)·Redis(6379)·관측성(9100)은 호스트 `127.0.0.1` 전용.
> - 크리덴셜 교체: `.env`에 `POSTGRES_PASSWORD`/`REDIS_PASSWORD`/`MQTT_PASSWORD` 지정(compose가 주입) + MQTT는 passwd 재생성: `docker run --rm -v ./docker/mosquitto/config:/cfg eclipse-mosquitto:2 mosquitto_passwd -b /cfg/passwd <user> <pw>` 후 mosquitto 재기동.

---

## 1. 기동 / 종료

```bash
# 의존성 기동 (Mosquitto + PostgreSQL + Redis)
docker compose up -d
docker compose ps                  # 상태 확인
docker compose logs -f mosquitto   # 개별 로그 (postgres | redis 동일)

# 에이전트 빌드·기동 (기동 시 스키마 자동 적용)
npm run build
npm start

# 에이전트 종료: Ctrl+C (SIGINT) — graceful shutdown (수신 중단 → 워커 드레인 → 연결 정리)

# (운영 권장) 에이전트를 컨테이너로 실행 — 크래시 시 자동 재기동 + /health 헬스체크
docker compose --profile agent up -d --build
docker logs -f edge-agent
docker compose --profile agent rm -sf agent   # 중지·제거
# 주의: npm start(로컬)와 동시 실행 금지 — 같은 MQTT_CLIENT_ID·9100 포트 충돌

# 의존성 종료 (데이터 볼륨은 유지)
docker compose down
```

## 2. 상태 확인 (헬스체크 / 지표 / 로그)

```bash
# 헬스체크: MQTT·Redis·PG 모두 정상이면 200, 하나라도 실패면 503
curl http://localhost:9100/health
# → {"ok":true,"mqtt":true,"redis":true,"pg":true}

# Prometheus 지표
curl http://localhost:9100/metrics
```

| 지표 | 의미 | 경보 기준(권장) |
|---|---|---|
| `agent_stream_backlog` | Redis 스트림 미처리 잔량(XLEN) | 지속 증가 시 PG 적재 지연 의심 |
| `agent_stream_pending` | 워커가 claim했지만 미ack | 고착 시 워커/PG 장애 의심 |
| `agent_dlq_depth` | DLQ(poison) 적재 건수 | > 0이면 §5 절차로 처리 |
| `agent_raw_error_rows` | `error_yn='Y'` 원본 건수 | 증가 시 §4 재처리 검토 |
| `agent_error_log_total{stage=...}` | 단계별 오류 건수 | stage별 원인 분석 |
| `agent_processed_total` | 처리 성공(ack) 누적 — 처리율 = `rate()` | 유입 대비 처리율 급락 시 병목 의심 |
| `agent_process_failed_total` | 처리 실패(재시도) 누적 | 증가 추세면 PG/파서 점검 |
| `agent_dlq_moved_total` | DLQ 이동 누적 | > 0이면 poison 발생 이력 |
| `agent_e2e_latency_ms_sum` | 수신→처리완료 지연 합 — 평균 = `rate(sum)/rate(processed_total)` | 평균 지연 급증 시 적재 지연 의심 |
| `agent_e2e_latency_ms_max` | 최대 지연(시작 이후) | 스파이크 원인 분석용 |

로그: `LOG_DIR`(기본 `./logs`)의 `agent.log` (JSON 한 줄씩, 날짜 변경 시 `agent-YYYY-MM-DD.log`로 백업).

```powershell
Get-Content $env:LOG_DIR\agent.log -Tail 50 -Wait   # 실시간 추적 (PowerShell)
```

## 3. 단말 등록

메시지가 파생 처리되려면 `imei`가 `devices`에 등록돼 있어야 한다(미등록은 원본+에러만 기록).

```bash
npm run device -- register IMEI-001   # 등록 (멱등, 재실행 안전)
npm run device -- list                # 등록 단말 목록
```

```sql
-- 미등록으로 쌓인 imei 확인 (등록 대상 파악) — psql: docker exec -it agent-postgres psql -U agent -d agent_db
SELECT imei, count(*) FROM messages_raw
WHERE error_yn='Y' AND device_id IS NULL GROUP BY imei;
```

등록 후 `npm run reprocess`(§4)를 실행하면 그동안 쌓인 원본이 파생까지 복구된다.

## 4. 원본 재처리 (단말 등록·파서 수정 후 복구)

```bash
npm run reprocess    # DATABASE_URL만 필요, 에이전트 기동 중에도 실행 가능(멱등)
# 완료 로그: {"msg":"reprocess done","scanned":N,"reprocessed":N,"stillUnregistered":N}
```

- `error_yn='Y'` 원본을 순회 — 미등록이었다가 등록된 단말은 device_id 매핑 + 파생 생성, projection 실패 건은 파생 재실행.
- 반복 실행해도 안전(도메인 INSERT 전부 ON CONFLICT DO NOTHING). 실패 건은 다시 `error_yn='Y'`로 남아 다음 실행에서 재시도.

## 5. DLQ 처리 (poison 메시지)

`maxRetry`(5회) 초과로 격리된 메시지. `agent_dlq_depth > 0`이면:

```bash
npm run dlq -- list          # 적재분 조회 (기본 20건, JSON 한 줄씩)
npm run dlq -- list 100      # 최대 100건
# → {"id":"1718...-0","msg":{"topic":"device/A/msg","payload":"...","receivedAt":"..."}}

# 원인(파서 버그, 스키마 등) 수정·배포 후 재투입 → 워커가 처음부터 다시 처리
npm run dlq -- requeue               # 전부
npm run dlq -- requeue 1718...-0     # 해당 건만
```

## 6. Redis 버퍼 점검

```bash
docker exec -it agent-redis redis-cli -a agentredispw

XLEN messages:stream                     # 미처리 잔량
XPENDING messages:stream agent-workers   # 미ack 요약 (건수/범위/컨슈머별)
XINFO GROUPS messages:stream             # consumer group 상태
XLEN messages:dlq                        # DLQ 적재량
XRANGE messages:dlq - + COUNT 10         # DLQ 내용 확인 (조회는 npm run dlq 권장)
```

## 7. PostgreSQL 점검 / 추적 조회

```sql
-- 오늘 수집 현황
SELECT count(*) AS total,
       count(*) FILTER (WHERE error_yn='Y') AS errors
FROM messages_raw WHERE received_at >= CURRENT_DATE;

-- 단계별 오류 분포
SELECT stage, count(*) FROM error_log GROUP BY stage ORDER BY 2 DESC;

-- 메시지 1건 전 구간 추적 (원본·단말·업무·위치·오류를 한 키로)
SELECT d.imei, r.message_code, r.error_yn, r.error_detail,
       f.ftp, f.sp, f.pcode, l.latitude, l.longitude,
       e.stage AS error_stage, e.detail AS error_log_detail
FROM messages_raw r
LEFT JOIN devices d USING (device_id)
LEFT JOIN domain_fault f USING (message_id)
LEFT JOIN domain_location l USING (message_id)
LEFT JOIN error_log e USING (message_id)
WHERE r.message_id = $1;
```

## 8. 부하 테스트 / 발행 테스트

```bash
node scripts/pub.mjs                                                   # 단건 발행 테스트
node scripts/load.mjs --count 2000 --imei load-001 --topic device/A/msg  # N건 순간 발행
```

두 스크립트 모두 `.env`의 `MQTT_URL`(브로커 인증 포함)을 자동 로드한다. 다른 브로커는 `--url mqtt://user:pw@host:1883`로 지정.

워커 수(`WORKER_CONCURRENCY`) 조정 기준과 K별 처리율 실측치는 [k-tuning.md](k-tuning.md) 참조.

검증: `messages_raw` 건수 == N, 중복 0, `error_yn='Y'` 0 (imei가 등록돼 있을 때).

## 9. 장애 시나리오별 대응

| 상황 | 증상 | 대응 |
|---|---|---|
| **PG 다운** | `/health` pg=false, `agent_stream_backlog` 증가 | PG 복구만 하면 됨 — 수신분은 Redis에 쌓였다가 워커가 자동 드레인(무손실). 장기화 시 Redis 메모리 감시 |
| **Redis 다운** | `/health` redis=false, enqueue 실패 로그 | Redis 복구(AOF로 잔량 복원). 미적재분은 MQTT ack 안 됐으므로 브로커가 재전송 |
| **브로커 다운** | `/health` mqtt=false | 브로커 복구 시 자동 재접속(reconnectPeriod 2s). durable 세션(clean:false)이라 미ack 분 재전송 |
| **에이전트 크래시** | 프로세스 종료 | 재기동만 하면 됨 — Redis pending은 reclaim(30s idle)으로 회수, 원본 저장 직후 크래시분은 중복 재수신 시 파생 자동 복구 |
| **poison 누적** | `agent_dlq_depth` 증가 | §5 (원인 수정 → requeue) |
| **미등록 단말 누적** | `agent_raw_error_rows` 증가, stage=device_lookup | §3 등록 → §4 재처리 |

> **주의:** `messages_raw`는 불변 원본(진실의 원천)이다. 운영 중 UPDATE/DELETE 금지 — 복구는 항상 재처리 도구(§4·§5)로 한다.
