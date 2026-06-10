# MQTT 사용법 (메시지 발행·구독·테스트)

단말기 시뮬레이션 — Mosquitto 브로커로 메시지를 발행해 에이전트가 PostgreSQL에 저장하는 전 과정을 테스트하는 방법.

- 브로커: `mqtt://localhost:1883` (docker-compose의 `agent-mosquitto`, 익명 허용)
- 에이전트 구독 토픽: `device/+/msg`

> Windows PowerShell 기준. (bash/Linux는 따옴표만 다름)

---

## 1. 전체 체인 한눈에

메시지가 DB에 저장되려면 **3가지**가 떠 있어야 합니다.

```
[발행]  scripts/pub.mjs / mosquitto_pub
   │ MQTT publish (device/+/msg)
   ▼
[Mosquitto]  docker-compose: agent-mosquitto (1883)
   │ subscribe
   ▼
[에이전트]  node dist/main.js
   │ 저장
   ▼
[PostgreSQL]  agent-postgres (5435, agent_db)
```

```powershell
# (1) 인프라 기동
docker compose up -d

# (2) 단말 등록 (한 번만) — 보낼 메시지의 imei와 동일해야 정상 저장
docker exec agent-postgres psql -U agent -d agent_db -c "INSERT INTO devices (imei) VALUES ('111222333') ON CONFLICT DO NOTHING;"

# (3) 에이전트 실행 (창1, 계속 떠 있음)
npm run build
node dist/main.js        # .env 자동 로드 → {"msg":"agent started"}

# (4) 메시지 발행 (창2)
node scripts/pub.mjs fault
```

---

## 2. 발행 방법

### A. Node 발행 스크립트 (권장) — `scripts/pub.mjs`

이미 설치된 `mqtt`를 사용. `process_dttm`을 현재 시각으로 자동 생성하므로 매 실행이 새 메시지(중복 아님).

```powershell
# 프리셋
node scripts/pub.mjs fault          # 등록 단말 고장(중첩 message) → domain_fault
node scripts/pub.mjs sensor         # 평면 범용(volt/air/status) → domain_generic(키별 행)
node scripts/pub.mjs unregistered   # 미등록 imei → error_yn=Y

# npm 으로도
npm run pub -- sensor

# 옵션
node scripts/pub.mjs fault --count 5            # 5번 반복
node scripts/pub.mjs sensor --imei 111222333    # imei 변경
node scripts/pub.mjs fault --topic device/Z/msg # 토픽 변경
node scripts/pub.mjs fault --url mqtt://localhost:1883

# 직접 JSON
node scripts/pub.mjs '{"imei":"111222333","messageCode":"Fault","message":{"ftp":"1","sp":"2","pcode":"P9"}}'
```

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--url` | env `MQTT_URL` 또는 `mqtt://localhost:1883` | 브로커 |
| `--topic` | `device/A/msg` | 발행 토픽 |
| `--imei` | `111222333` | 프리셋 imei |
| `--count` | `1` | 반복 횟수 |

### B. 컨테이너 내장 `mosquitto_pub` (설치 불필요)

```powershell
$msg = '{"imei":"111222333","messageCode":"Fault","process_dttm":"2026-06-10 12:00:00","message":{"ftp":"100","sp":"12","pcode":"P0001"},"latitude":"19.2","longitude":"203.1"}'
docker exec agent-mosquitto mosquitto_pub -t "device/A/msg" -q 1 -m $msg
```
- PowerShell에선 JSON을 **작은따옴표 변수**(`$msg = '...'`)에 담아 넘기는 게 안전.
- `docker exec`는 셸 없이 인자를 전달하므로 추가 escape 불필요.

### C. 구독해서 도달 확인 — `mosquitto_sub`

발행이 브로커에 도달하는지 먼저 확인할 때 (다른 창):
```powershell
docker exec agent-mosquitto mosquitto_sub -t "device/+/msg" -v
```
- `-v` : 토픽 + payload 함께 출력

### D. GUI 도구

**MQTTX(데스크톱)** 또는 **MQTT Explorer**: 연결 `localhost` / 포트 `1883` / 인증 없음 → 토픽 `device/A/msg`에 JSON 발행. 시각적으로 pub/sub 확인.

> ⚠️ **브라우저로 `http://localhost:1883` 접속은 안 됩니다** (`ERR_EMPTY_RESPONSE`). MQTT는 HTTP가 아니라 raw TCP입니다. 웹/브라우저 MQTT 도구를 쓰려면 Mosquitto에 WebSocket 리스너(예: 9001)를 별도로 열어야 합니다.

---

## 3. 토픽 규칙

- 에이전트는 **`device/+/msg`** 를 구독합니다. → 발행 토픽은 **`device/<무엇이든>/msg`** 형식이어야 합니다.
- 토픽의 두 번째 세그먼트(`<무엇이든>`)는 보조 식별자로만 쓰이며, **정식 단말 식별은 payload의 `imei`** 입니다.

---

## 4. 메시지 포맷

JSON. 공통 헤더 + 업무 본문으로 구성됩니다.

### 공통 헤더 (5개 키 — 에이전트가 인식)
| 키 | 용도 |
|---|---|
| `imei` | 단말 식별 (devices 조회 → device_id) |
| `messageCode` | 업무 라우팅 (`Fault`=전용 테이블, 그 외=범용) |
| `process_dttm` | 단말 처리 시각 `"YYYY-MM-DD HH:mm:ss"` |
| `latitude` / `longitude` | 공통 위치 → `domain_location` |

### 업무 본문 (위 5개를 제외한 나머지)
- **평면(flat):** 최상위에 업무 키가 직접 — 예 `{"imei":..,"messageCode":"Sensor","volt":"20","air":"100"}`
- **중첩(nested):** `message` 객체 안에 — 예 `{"imei":..,"messageCode":"Fault","message":{"ftp":"100"}}`
- 둘 다 지원: `message`가 있으면 그것을, 없으면 공통 5키 제외한 최상위를 업무 본문으로 사용.

### 라우팅
- `messageCode = "Fault"` → `domain_fault`(ftp/sp/pcode 전용 컬럼)
- 그 외 모든 코드 → `domain_generic`(본문을 키마다 한 행, EAV)

---

## 5. 시나리오별 예시

```powershell
# ① 등록 단말 고장 → domain_fault
node scripts/pub.mjs fault

# ② 평면 범용(Sensor) → domain_generic 키별 행(volt/air/status)
node scripts/pub.mjs sensor

# ③ 미등록 imei → messages_raw.error_yn='Y' + error_log(device_lookup)
node scripts/pub.mjs unregistered

# ④ 중복 — 동일 내용 2회 (message_key 충돌 → 1건만 저장)
$dup = '{"imei":"111222333","messageCode":"Fault","process_dttm":"2026-06-10 12:00:00","message":{"ftp":"1"}}'
node scripts/pub.mjs $dup
node scripts/pub.mjs $dup
```

---

## 6. 결과 확인 (DB)

```powershell
# 원본 + 에러 여부
docker exec agent-postgres psql -U agent -d agent_db -c "SELECT message_id, message_code, error_yn, error_detail FROM messages_raw ORDER BY message_id DESC LIMIT 5;"

# 고장(전용)
docker exec agent-postgres psql -U agent -d agent_db -c "SELECT * FROM domain_fault ORDER BY id DESC LIMIT 3;"

# 범용(EAV)
docker exec agent-postgres psql -U agent -d agent_db -c "SELECT message_id, key, value FROM domain_generic ORDER BY id DESC LIMIT 6;"

# 위치
docker exec agent-postgres psql -U agent -d agent_db -c "SELECT * FROM domain_location ORDER BY message_id DESC LIMIT 3;"

# 에러 추적
docker exec agent-postgres psql -U agent -d agent_db -c "SELECT message_id, stage, imei, detail FROM error_log ORDER BY id DESC LIMIT 5;"
```

---

## 7. 트러블슈팅

| 증상 | 원인 / 해결 |
|---|---|
| 브라우저 `ERR_EMPTY_RESPONSE` | 1883은 HTTP가 아님. `mosquitto_pub`/데스크톱 MQTT 클라이언트 사용 (또는 WebSocket 리스너 추가) |
| `No such container: agent-mosquitto` | `docker compose ps`로 이름 확인, 안 떠 있으면 `docker compose up -d` |
| 구독엔 보이는데 DB에 없음 | 에이전트 미실행 또는 토픽 패턴 불일치. 토픽 `device/+/msg`, 에이전트 로그 확인 |
| `error_yn=Y`, `unregistered imei` | 단말 미등록 → `devices`에 해당 imei 등록 후 재발행 |
| `error_log(stage=ingest)` | JSON 파싱 실패. payload 따옴표 깨짐 확인 |
| 발행은 되는데 처리 안 됨(보류) | DB 중지 상태. 복구 후 재연결 시 재처리(미ACK 메시지는 durable 세션으로 보존) |

---

관련: 아키텍처·규칙은 [CLAUDE.md](../CLAUDE.md), 설계는 [docs/superpowers/specs/](superpowers/specs/) 참조.
