CREATE TABLE IF NOT EXISTS devices (
  device_id     TEXT PRIMARY KEY,
  imei          TEXT NOT NULL UNIQUE,
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS messages_raw (
  message_id    TEXT PRIMARY KEY,
  device_id     TEXT REFERENCES devices(device_id),
  message_code  TEXT NOT NULL,
  process_dttm  TIMESTAMPTZ,
  latitude      NUMERIC,
  longitude     NUMERIC,
  raw_payload   JSONB NOT NULL,
  status        TEXT NOT NULL DEFAULT 'received',
  received_at   TIMESTAMPTZ NOT NULL,
  stored_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_status ON messages_raw (status) WHERE status <> 'parsed';
CREATE INDEX IF NOT EXISTS idx_raw_code   ON messages_raw (message_code, received_at);
CREATE INDEX IF NOT EXISTS idx_raw_device ON messages_raw (device_id, received_at);

CREATE TABLE IF NOT EXISTS domain_fault (
  message_id   TEXT PRIMARY KEY REFERENCES messages_raw(message_id),
  device_id    TEXT NOT NULL REFERENCES devices(device_id),
  ftp          TEXT,
  sp           TEXT,
  pcode        TEXT
);
CREATE INDEX IF NOT EXISTS idx_fault_device ON domain_fault (device_id);

CREATE TABLE IF NOT EXISTS domain_location (
  message_id   TEXT PRIMARY KEY REFERENCES messages_raw(message_id),
  device_id    TEXT NOT NULL REFERENCES devices(device_id),
  latitude     NUMERIC,
  longitude    NUMERIC
);
CREATE INDEX IF NOT EXISTS idx_location_device ON domain_location (device_id);

CREATE TABLE IF NOT EXISTS error_log (
  id           BIGSERIAL PRIMARY KEY,
  message_id   TEXT,
  stage        TEXT NOT NULL,
  message_code TEXT,
  imei         TEXT,
  detail       TEXT NOT NULL,
  raw_text     TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_error_message ON error_log (message_id);
CREATE INDEX IF NOT EXISTS idx_error_stage   ON error_log (stage, created_at);
