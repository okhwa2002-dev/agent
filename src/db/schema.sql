-- 단말 마스터 — device_id(숫자 PK) ↔ imei(자연키)
CREATE TABLE IF NOT EXISTS devices (
  device_id   BIGSERIAL PRIMARY KEY,
  imei        TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE  devices            IS '단말 마스터: 단말 관리 키(device_id)와 하드웨어 식별자(imei) 매핑';
COMMENT ON COLUMN devices.device_id  IS '단말 관리 키 (숫자 surrogate, 전 계층 매핑에 사용)';
COMMENT ON COLUMN devices.imei       IS '단말 하드웨어 식별자 (payload의 imei로 조회)';
COMMENT ON COLUMN devices.created_at IS '생성일시';

-- 원본 적재 (bronze, 불변) — 숫자 PK(message_id) + 멱등 키(message_key)
CREATE TABLE IF NOT EXISTS messages_raw (
  message_id    BIGSERIAL PRIMARY KEY,
  message_key   TEXT NOT NULL UNIQUE,
  device_id     BIGINT REFERENCES devices(device_id),
  imei          TEXT,
  message_code  TEXT NOT NULL,
  process_dttm  TIMESTAMPTZ,
  latitude      NUMERIC,
  longitude     NUMERIC,
  raw_payload   JSONB NOT NULL,
  status        TEXT NOT NULL DEFAULT 'received',
  received_at   TIMESTAMPTZ NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_status ON messages_raw (status) WHERE status <> 'parsed';
CREATE INDEX IF NOT EXISTS idx_raw_code   ON messages_raw (message_code, received_at);
CREATE INDEX IF NOT EXISTS idx_raw_device ON messages_raw (device_id, received_at);
CREATE INDEX IF NOT EXISTS idx_raw_imei   ON messages_raw (imei, received_at);
COMMENT ON TABLE  messages_raw              IS '원본 적재 테이블(불변): 단말 원본 JSON + 공통 헤더. 전 계층 추적 기준';
COMMENT ON COLUMN messages_raw.message_id   IS '숫자 surrogate PK = 도메인/에러 테이블 매핑 키';
COMMENT ON COLUMN messages_raw.message_key  IS '멱등 키 (에이전트 결정적 생성, 재전송 중복 흡수)';
COMMENT ON COLUMN messages_raw.device_id    IS 'imei 조회 결과. 미등록 단말이면 NULL';
COMMENT ON COLUMN messages_raw.imei         IS '단말 하드웨어 식별자 (payload에서 추출, 미등록 추적용)';
COMMENT ON COLUMN messages_raw.message_code IS '업무 라우팅 구분자 (예: Fault)';
COMMENT ON COLUMN messages_raw.process_dttm IS '단말 처리 시각 (payload)';
COMMENT ON COLUMN messages_raw.latitude     IS '공통 위치 - 위도 (payload)';
COMMENT ON COLUMN messages_raw.longitude    IS '공통 위치 - 경도 (payload)';
COMMENT ON COLUMN messages_raw.raw_payload  IS '단말 원본 JSON (무변형 보존)';
COMMENT ON COLUMN messages_raw.status       IS '처리 상태: received | parsed | parse_error | unregistered_device';
COMMENT ON COLUMN messages_raw.received_at  IS '에이전트 수신 시각';
COMMENT ON COLUMN messages_raw.created_at   IS '생성일시';

-- 업무 도메인 테이블 (silver, 파생). message_id(숫자)로 원본 매핑, device_id 보유.
-- messageCode = "Fault"
CREATE TABLE IF NOT EXISTS domain_fault (
  id          BIGSERIAL PRIMARY KEY,
  message_id  BIGINT NOT NULL UNIQUE REFERENCES messages_raw(message_id),
  device_id   BIGINT NOT NULL REFERENCES devices(device_id),
  ftp         TEXT,
  sp          TEXT,
  pcode       TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fault_device ON domain_fault (device_id);
COMMENT ON TABLE  domain_fault            IS '업무(고장) 파생 테이블: messageCode=Fault의 message 본문';
COMMENT ON COLUMN domain_fault.id         IS '업무단 자체 시퀀스 PK';
COMMENT ON COLUMN domain_fault.message_id IS '원본(messages_raw.message_id) 참조용 보관 (UNIQUE, 멱등)';
COMMENT ON COLUMN domain_fault.device_id  IS '단말 식별 (단말별 조회용)';
COMMENT ON COLUMN domain_fault.ftp        IS '업무 필드 (message.ftp)';
COMMENT ON COLUMN domain_fault.sp         IS '업무 필드 (message.sp)';
COMMENT ON COLUMN domain_fault.pcode      IS '업무 필드 (message.pcode)';
COMMENT ON COLUMN domain_fault.created_at IS '생성일시';

-- 공통 위치 (모든 등록 단말 메시지에서 분리). messageCode 무관.
CREATE TABLE IF NOT EXISTS domain_location (
  id          BIGSERIAL PRIMARY KEY,
  message_id  BIGINT NOT NULL UNIQUE REFERENCES messages_raw(message_id),
  device_id   BIGINT NOT NULL REFERENCES devices(device_id),
  latitude    NUMERIC,
  longitude   NUMERIC,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_location_device ON domain_location (device_id);
COMMENT ON TABLE  domain_location            IS '공통 위치 파생 테이블: 등록 단말 메시지의 위/경도 (messageCode 무관)';
COMMENT ON COLUMN domain_location.id         IS '업무단 자체 시퀀스 PK';
COMMENT ON COLUMN domain_location.message_id IS '원본(messages_raw.message_id) 참조용 보관 (UNIQUE, 멱등)';
COMMENT ON COLUMN domain_location.device_id  IS '단말 식별 (단말별 조회용)';
COMMENT ON COLUMN domain_location.latitude   IS '위도';
COMMENT ON COLUMN domain_location.longitude  IS '경도';
COMMENT ON COLUMN domain_location.created_at IS '생성일시';

-- 범용 업무 테이블 (catch-all, EAV). 전용 파서 없는 코드의 본문을 키마다 한 행씩 저장.
CREATE TABLE IF NOT EXISTS domain_generic (
  id           BIGSERIAL PRIMARY KEY,
  message_id   BIGINT NOT NULL REFERENCES messages_raw(message_id),
  device_id    BIGINT NOT NULL REFERENCES devices(device_id),
  message_code TEXT NOT NULL,
  key          TEXT NOT NULL,
  value        TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (message_id, key)
);
CREATE INDEX IF NOT EXISTS idx_generic_message ON domain_generic (message_id);
CREATE INDEX IF NOT EXISTS idx_generic_device  ON domain_generic (device_id);
CREATE INDEX IF NOT EXISTS idx_generic_key     ON domain_generic (key);
COMMENT ON TABLE  domain_generic              IS '범용 업무 파생 테이블(catch-all, EAV): 전용 파서 없는 코드의 본문을 키:값 한 행씩 저장';
COMMENT ON COLUMN domain_generic.id           IS '자체 시퀀스 PK';
COMMENT ON COLUMN domain_generic.message_id   IS '원본(messages_raw.message_id) 참조용 보관';
COMMENT ON COLUMN domain_generic.device_id    IS '단말 식별 (단말별 조회용)';
COMMENT ON COLUMN domain_generic.message_code IS '업무 구분자';
COMMENT ON COLUMN domain_generic.key          IS '업무 본문 키';
COMMENT ON COLUMN domain_generic.value        IS '업무 본문 값 (객체면 JSON 문자열)';
COMMENT ON COLUMN domain_generic.created_at   IS '생성일시';
-- (message_id, key) UNIQUE → 재처리 멱등

-- 전용 에러 테이블 — 단계별 오류 추적
CREATE TABLE IF NOT EXISTS error_log (
  id           BIGSERIAL PRIMARY KEY,
  message_id   BIGINT,
  message_key  TEXT,
  stage        TEXT NOT NULL,
  message_code TEXT,
  imei         TEXT,
  detail       TEXT NOT NULL,
  raw_text     TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_error_message ON error_log (message_id);
CREATE INDEX IF NOT EXISTS idx_error_stage   ON error_log (stage, created_at);
COMMENT ON TABLE  error_log              IS '단계별 오류 추적 테이블 (message_id/message_key로 원본과 매핑)';
COMMENT ON COLUMN error_log.id           IS '에러 일련번호';
COMMENT ON COLUMN error_log.message_id   IS '원본(messages_raw.message_id) 매핑 키 (원본 저장 전 오류면 NULL)';
COMMENT ON COLUMN error_log.message_key  IS '멱등 키 (있을 경우)';
COMMENT ON COLUMN error_log.stage        IS '오류 단계: ingest | device_lookup | projection | location';
COMMENT ON COLUMN error_log.message_code IS '업무 구분자 (있을 경우)';
COMMENT ON COLUMN error_log.imei         IS '단말 식별자 (있을 경우)';
COMMENT ON COLUMN error_log.detail       IS '오류 내용';
COMMENT ON COLUMN error_log.raw_text     IS '원본 텍스트 (파싱 실패 시 보존)';
COMMENT ON COLUMN error_log.created_at   IS '생성일시';
