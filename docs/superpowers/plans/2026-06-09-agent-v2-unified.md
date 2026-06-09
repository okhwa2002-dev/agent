# 통합 에이전트 Implementation Plan (v2: 서버 흡수)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or subagent-driven-development) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 기존 에이전트(Redis 버퍼 + HTTP 전송)를 개정하여, 단일 프로세스가 MQTT 수신 → PG 원본 저장 → imei로 device_id 조회 → 업무별 파생 저장까지 직접 수행하고, 각 단계 오류를 전용 `error_log`에 message_id로 기록한다.

**Architecture:** MQTT(QoS1, manual ack) → 원본 `messages_raw` INSERT(내구성 지점, 성공 후 ack) → imei→device_id 조회 → messageCode 분기 파서 → `domain_*` 저장. Redis/HTTP/서버 제거. PostgreSQL은 엣지 로컬/근접.

**Tech Stack:** Node.js 20+, TypeScript, mqtt.js(5.x), pg, Vitest, @testcontainers/postgresql. 패키지 매니저 npm.

설계 출처: [2026-06-09-realtime-message-pipeline-design.md](../specs/2026-06-09-realtime-message-pipeline-design.md) (개정판)

> **기존 코드 전제:** main 브랜치에 Redis/HTTP 기반 에이전트가 구현돼 있다. 이 계획은 그것을 개정한다. 유지: `ingest/messageId.ts`. 개정: `ingest/MqttSubscriber.ts`, `config`, `main`, `types`. 제거: `queue/`, `dispatch/`, `pipeline/`.

---

## File Structure (최종)

| 파일 | 상태 | 책임 |
|---|---|---|
| `src/types.ts` | 개정 | `RawMessage`, `Clock` (ServerRecord 제거) |
| `src/ingest/messageId.ts` | 유지 | 결정적 messageId |
| `src/header.ts` | 신규 | rawPayload → 공통 헤더 추출(순수) |
| `src/db/pool.ts` | 신규 | pg Pool 팩토리 |
| `src/db/schema.sql` | 신규 | devices + messages_raw + domain_fault + error_log |
| `src/db/applySchema.ts` | 신규 | 스키마 적용 헬퍼 |
| `src/repo/deviceRepo.ts` | 신규 | imei → device_id |
| `src/repo/rawRepo.ts` | 신규 | messages_raw UPSERT/상태변경 |
| `src/repo/domainRepo.ts` | 신규 | domain_fault INSERT |
| `src/repo/errorRepo.ts` | 신규 | error_log 기록 |
| `src/parsers/types.ts` | 신규 | DomainParser 인터페이스 |
| `src/parsers/faultParser.ts` | 신규 | Fault 파서 |
| `src/parsers/registry.ts` | 신규 | messageCode → 파서 |
| `src/service/projectionService.ts` | 신규 | raw → 도메인 + status/error |
| `src/service/messageProcessor.ts` | 신규 | 수신 1건 오케스트레이션 |
| `src/ingest/MqttSubscriber.ts` | 개정 | manual ack(handleMessage 오버라이드) |
| `src/config/config.ts` | 개정 | PG 설정 (Redis/server URL 제거) |
| `src/main.ts` | 개정 | MQTT + 처리 파이프라인 조립 |
| `src/queue/`, `src/dispatch/`, `src/pipeline/` | 제거 | — |

---

## Task 1: 의존성 교체 & 구 모듈 제거

**Files:**
- Modify: `package.json`, `src/types.ts`
- Delete: `src/queue/`, `src/dispatch/`, `src/pipeline/`

- [ ] **Step 1: package.json dependencies/devDependencies 교체**

`dependencies`를 다음으로 교체:
```json
  "dependencies": {
    "mqtt": "^5.10.1",
    "pg": "^8.13.0"
  },
```
`devDependencies`를 다음으로 교체:
```json
  "devDependencies": {
    "@testcontainers/postgresql": "^10.13.2",
    "@types/node": "^20.16.5",
    "@types/pg": "^8.11.10",
    "typescript": "^5.6.2",
    "vitest": "^2.1.1"
  }
```
(ioredis, undici, @testcontainers/redis 제거 / pg, @types/pg, @testcontainers/postgresql 추가)

- [ ] **Step 2: 구 모듈 디렉터리 삭제**

Run:
```bash
git rm -r src/queue src/dispatch src/pipeline
```
Expected: 6개 파일(각 .ts + .test.ts) 삭제됨

- [ ] **Step 3: types.ts에서 ServerRecord 제거**

`src/types.ts` 전체를 다음으로 교체:
```ts
// 단말에서 수신한 원본 메시지 (파싱 직후)
export interface RawMessage {
  topic: string;
  deviceId: string;      // MQTT 토픽에서 추출(보조)
  payload: unknown;      // 파싱된 JSON (원본 구조 그대로, 변형 금지)
  rawText: string;       // 수신한 원본 텍스트
  receivedAt: string;    // 에이전트 수신 시각 (ISO8601)
}

// 시각 주입용 (테스트 가능성)
export interface Clock {
  now(): Date;
}

export const systemClock: Clock = { now: () => new Date() };
```

- [ ] **Step 4: 의존성 재설치**

Run: `npm install`
Expected: ioredis/undici 제거, pg 설치. 오류 없음

- [ ] **Step 5: Commit**

```bash
git add package.json package-lock.json src/types.ts
git commit -m "refactor: drop Redis/HTTP modules, switch deps to pg"
```

---

## Task 2: DB 스키마 & 풀 & 적용 헬퍼

**Files:**
- Create: `src/db/schema.sql`, `src/db/pool.ts`, `src/db/applySchema.ts`

- [ ] **Step 1: schema.sql 작성**

```sql
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
  ftp          TEXT,
  sp           TEXT,
  pcode        TEXT
);

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
```

- [ ] **Step 2: pool.ts 작성**

```ts
import { Pool } from 'pg';

/** 연결 문자열로 pg Pool 생성. */
export function createPool(connectionString: string): Pool {
  return new Pool({ connectionString });
}
```

- [ ] **Step 3: applySchema.ts 작성**

```ts
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import type { Pool } from 'pg';

const here = dirname(fileURLToPath(import.meta.url));

/** schema.sql 실행 (IF NOT EXISTS라 멱등). */
export async function applySchema(pool: Pool): Promise<void> {
  const sql = readFileSync(join(here, 'schema.sql'), 'utf8');
  await pool.query(sql);
}
```

- [ ] **Step 4: 컴파일 확인**

Run: `npx tsc --noEmit`
Expected: 오류 없음

- [ ] **Step 5: Commit**

```bash
git add src/db/
git commit -m "feat: db schema (devices/raw/domain/error), pool, applySchema"
```

---

## Task 3: 헤더 추출

**Files:**
- Create: `src/header.ts`
- Test: `src/header.test.ts`

- [ ] **Step 1: 실패하는 테스트 작성**

```ts
import { describe, it, expect } from 'vitest';
import { extractHeader } from './header.js';

describe('extractHeader', () => {
  it('rawPayload에서 공통 헤더를 추출한다', () => {
    const h = extractHeader({
      imei: '356938035643809', messageCode: 'Fault',
      process_dttm: '2026-06-09 09:03:00', latitude: '19.23222', longitude: '203.12121',
      message: { ftp: '100' },
    });
    expect(h).toEqual({
      imei: '356938035643809', messageCode: 'Fault',
      processDttm: '2026-06-09 09:03:00', latitude: '19.23222', longitude: '203.12121',
    });
  });

  it('누락 필드는 null로 처리한다', () => {
    const h = extractHeader({ imei: '123', messageCode: 'Fault' });
    expect(h.processDttm).toBeNull();
    expect(h.latitude).toBeNull();
  });
});
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `npx vitest run src/header.test.ts`
Expected: FAIL — "Cannot find module './header.js'"

- [ ] **Step 3: header.ts 작성**

```ts
export interface Header {
  imei: string;
  messageCode: string;
  processDttm: string | null;
  latitude: string | null;
  longitude: string | null;
}

/** rawPayload(JSON)에서 공통 헤더 필드 추출. 원본은 변형하지 않는다. */
export function extractHeader(rawPayload: unknown): Header {
  const p = (rawPayload ?? {}) as Record<string, unknown>;
  const str = (v: unknown): string | null => (v == null ? null : String(v));
  return {
    imei: String(p.imei ?? ''),
    messageCode: String(p.messageCode ?? ''),
    processDttm: str(p.process_dttm),
    latitude: str(p.latitude),
    longitude: str(p.longitude),
  };
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `npx vitest run src/header.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/header.ts src/header.test.ts
git commit -m "feat: header extraction from rawPayload"
```

---

## Task 4: 리포지토리 (device/raw/domain/error) + 통합 테스트

**Files:**
- Create: `src/repo/deviceRepo.ts`, `src/repo/rawRepo.ts`, `src/repo/domainRepo.ts`, `src/repo/errorRepo.ts`
- Test: `src/repo/repo.test.ts`

- [ ] **Step 1: deviceRepo.ts 작성**

```ts
import type { Pool } from 'pg';

export class DeviceRepo {
  constructor(private readonly pool: Pool) {}

  /** imei로 device_id 조회. 미등록이면 null. */
  async findDeviceIdByImei(imei: string): Promise<string | null> {
    const res = await this.pool.query<{ device_id: string }>(
      'SELECT device_id FROM devices WHERE imei = $1', [imei],
    );
    return res.rows[0]?.device_id ?? null;
  }

  /** 단말 등록 (테스트·운영용). */
  async register(deviceId: string, imei: string): Promise<void> {
    await this.pool.query(
      'INSERT INTO devices (device_id, imei) VALUES ($1, $2) ON CONFLICT DO NOTHING',
      [deviceId, imei],
    );
  }
}
```

- [ ] **Step 2: rawRepo.ts 작성**

```ts
import type { Pool } from 'pg';
import type { Header } from '../header.js';

export interface RawInsert {
  messageId: string;
  deviceId: string | null;
  header: Header;
  rawPayload: unknown;
  status: string;
  receivedAt: string;
}

export class RawRepo {
  constructor(private readonly pool: Pool) {}

  /** 원본 멱등 적재. 신규면 true, 중복(message_id 충돌)이면 false. */
  async insert(r: RawInsert): Promise<boolean> {
    const res = await this.pool.query(
      `INSERT INTO messages_raw
         (message_id, device_id, message_code, process_dttm, latitude, longitude, raw_payload, status, received_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (message_id) DO NOTHING
       RETURNING message_id`,
      [
        r.messageId, r.deviceId, r.header.messageCode,
        r.header.processDttm, r.header.latitude, r.header.longitude,
        JSON.stringify(r.rawPayload), r.status, r.receivedAt,
      ],
    );
    return (res.rowCount ?? 0) > 0;
  }

  /** status 전이 (received | parsed | parse_error | unregistered_device). */
  async markStatus(messageId: string, status: string): Promise<void> {
    await this.pool.query(
      'UPDATE messages_raw SET status = $2 WHERE message_id = $1',
      [messageId, status],
    );
  }
}
```

- [ ] **Step 3: domainRepo.ts 작성**

```ts
import type { Pool } from 'pg';

export interface FaultRecord {
  ftp: string | null;
  sp: string | null;
  pcode: string | null;
}

export class DomainRepo {
  constructor(private readonly pool: Pool) {}

  /** domain_fault 멱등 INSERT. */
  async insertFault(messageId: string, rec: FaultRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO domain_fault (message_id, ftp, sp, pcode)
       VALUES ($1,$2,$3,$4) ON CONFLICT (message_id) DO NOTHING`,
      [messageId, rec.ftp, rec.sp, rec.pcode],
    );
  }
}
```

- [ ] **Step 4: errorRepo.ts 작성**

```ts
import type { Pool } from 'pg';

export type ErrorStage = 'ingest' | 'device_lookup' | 'projection';

export interface ErrorEntry {
  messageId: string | null;
  stage: ErrorStage;
  messageCode?: string | null;
  imei?: string | null;
  detail: string;
  rawText?: string | null;
}

export class ErrorRepo {
  constructor(private readonly pool: Pool) {}

  /** error_log에 단계별 오류 기록. */
  async log(e: ErrorEntry): Promise<void> {
    await this.pool.query(
      `INSERT INTO error_log (message_id, stage, message_code, imei, detail, raw_text)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [e.messageId, e.stage, e.messageCode ?? null, e.imei ?? null, e.detail, e.rawText ?? null],
    );
  }
}
```

- [ ] **Step 5: repo.test.ts 작성 (통합 테스트)**

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { DeviceRepo } from './deviceRepo.js';
import { RawRepo } from './rawRepo.js';
import { DomainRepo } from './domainRepo.js';
import { ErrorRepo } from './errorRepo.js';
import type { Header } from '../header.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;

const header: Header = {
  imei: '356938035643809', messageCode: 'Fault',
  processDttm: '2026-06-09 09:03:00', latitude: '19.23222', longitude: '203.12121',
};

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
});

afterAll(async () => {
  await pool.end();
  await container.stop();
});

describe('repositories', () => {
  it('deviceRepo: imei로 device_id 조회', async () => {
    const repo = new DeviceRepo(pool);
    await repo.register('DEV-100', '356938035643809');
    expect(await repo.findDeviceIdByImei('356938035643809')).toBe('DEV-100');
    expect(await repo.findDeviceIdByImei('000')).toBeNull();
  });

  it('rawRepo: 신규 true, 중복 false + markStatus', async () => {
    const repo = new RawRepo(pool);
    const base = { deviceId: 'DEV-100', header, rawPayload: { messageCode: 'Fault' }, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' };
    expect(await repo.insert({ messageId: 'raw-1', ...base })).toBe(true);
    expect(await repo.insert({ messageId: 'raw-1', ...base })).toBe(false);
    await repo.markStatus('raw-1', 'parsed');
    const r = await pool.query('SELECT status FROM messages_raw WHERE message_id = $1', ['raw-1']);
    expect(r.rows[0].status).toBe('parsed');
  });

  it('domainRepo: fault 멱등 INSERT', async () => {
    const raw = new RawRepo(pool);
    await raw.insert({ messageId: 'raw-2', deviceId: 'DEV-100', header, rawPayload: {}, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
    const repo = new DomainRepo(pool);
    await repo.insertFault('raw-2', { ftp: '100', sp: '12', pcode: 'P0001' });
    await repo.insertFault('raw-2', { ftp: '100', sp: '12', pcode: 'P0001' });
    const res = await pool.query('SELECT ftp FROM domain_fault WHERE message_id = $1', ['raw-2']);
    expect(res.rows[0].ftp).toBe('100');
  });

  it('errorRepo: 단계별 오류 기록', async () => {
    const repo = new ErrorRepo(pool);
    await repo.log({ messageId: 'raw-1', stage: 'projection', messageCode: 'Fault', detail: 'boom' });
    const res = await pool.query('SELECT stage, detail FROM error_log WHERE message_id = $1', ['raw-1']);
    expect(res.rows[0]).toMatchObject({ stage: 'projection', detail: 'boom' });
  });
});
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `npx vitest run src/repo/repo.test.ts`
Expected: PASS (4 tests). Docker 필요.

- [ ] **Step 7: Commit**

```bash
git add src/repo/
git commit -m "feat: device/raw/domain/error repositories"
```

---

## Task 5: 파서 + 레지스트리

**Files:**
- Create: `src/parsers/types.ts`, `src/parsers/faultParser.ts`, `src/parsers/registry.ts`
- Test: `src/parsers/faultParser.test.ts`

- [ ] **Step 1: types.ts 작성**

```ts
import type { DomainRepo } from '../repo/domainRepo.js';

/** 업무 타입별 파서. parse는 순수 변환, insert는 도메인 저장. */
export interface DomainParser<T> {
  readonly messageCode: string;
  parse(rawPayload: unknown): T;
  insert(repo: DomainRepo, messageId: string, parsed: T): Promise<void>;
}
```

- [ ] **Step 2: faultParser.test.ts 작성 (실패하는 테스트)**

```ts
import { describe, it, expect } from 'vitest';
import { faultParser } from './faultParser.js';

describe('faultParser', () => {
  it('message.* 를 fault 레코드로 변환', () => {
    expect(faultParser.parse({ message: { ftp: '100', sp: '12', pcode: 'P0001' } }))
      .toEqual({ ftp: '100', sp: '12', pcode: 'P0001' });
  });

  it('message 누락 시 null 필드', () => {
    expect(faultParser.parse({})).toEqual({ ftp: null, sp: null, pcode: null });
  });

  it('messageCode는 Fault', () => {
    expect(faultParser.messageCode).toBe('Fault');
  });
});
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `npx vitest run src/parsers/faultParser.test.ts`
Expected: FAIL — "Cannot find module './faultParser.js'"

- [ ] **Step 4: faultParser.ts 작성**

```ts
import type { DomainParser } from './types.js';
import type { DomainRepo, FaultRecord } from '../repo/domainRepo.js';

/** messageCode = "Fault": rawPayload.message → domain_fault */
export const faultParser: DomainParser<FaultRecord> = {
  messageCode: 'Fault',

  parse(rawPayload: unknown): FaultRecord {
    const msg = ((rawPayload as Record<string, unknown> | null)?.message ?? {}) as Record<string, unknown>;
    const str = (v: unknown): string | null => (v == null ? null : String(v));
    return { ftp: str(msg.ftp), sp: str(msg.sp), pcode: str(msg.pcode) };
  },

  insert(repo: DomainRepo, messageId: string, parsed: FaultRecord): Promise<void> {
    return repo.insertFault(messageId, parsed);
  },
};
```

- [ ] **Step 5: registry.ts 작성**

```ts
import type { DomainParser } from './types.js';
import { faultParser } from './faultParser.js';

/** messageCode → 파서 매핑. 새 업무 코드는 여기에 등록. */
export class ParserRegistry {
  private readonly parsers = new Map<string, DomainParser<unknown>>();

  constructor(parsers: DomainParser<unknown>[]) {
    for (const p of parsers) this.parsers.set(p.messageCode, p);
  }

  get(messageCode: string): DomainParser<unknown> | undefined {
    return this.parsers.get(messageCode);
  }
}

export function defaultRegistry(): ParserRegistry {
  return new ParserRegistry([faultParser as DomainParser<unknown>]);
}
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `npx vitest run src/parsers/faultParser.test.ts`
Expected: PASS (3 tests)

- [ ] **Step 7: Commit**

```bash
git add src/parsers/
git commit -m "feat: domain parsers and registry"
```

---

## Task 6: projectionService

**Files:**
- Create: `src/service/projectionService.ts`
- Test: `src/service/projectionService.test.ts`

- [ ] **Step 1: projectionService.ts 작성**

```ts
import type { RawRepo } from '../repo/rawRepo.js';
import type { DomainRepo } from '../repo/domainRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { ParserRegistry } from '../parsers/registry.js';

/** raw → 업무 코드 분기 → 도메인 파생. 실패 시 status=parse_error + error_log. */
export class ProjectionService {
  constructor(
    private readonly registry: ParserRegistry,
    private readonly domainRepo: DomainRepo,
    private readonly rawRepo: RawRepo,
    private readonly errorRepo: ErrorRepo,
  ) {}

  async project(messageId: string, messageCode: string, rawPayload: unknown): Promise<void> {
    const parser = this.registry.get(messageCode);
    if (!parser) {
      await this.rawRepo.markStatus(messageId, 'parse_error');
      await this.errorRepo.log({ messageId, stage: 'projection', messageCode, detail: `no parser for messageCode=${messageCode}` });
      return;
    }
    try {
      const parsed = parser.parse(rawPayload);
      await parser.insert(this.domainRepo, messageId, parsed);
      await this.rawRepo.markStatus(messageId, 'parsed');
    } catch (err) {
      await this.rawRepo.markStatus(messageId, 'parse_error');
      await this.errorRepo.log({ messageId, stage: 'projection', messageCode, detail: String(err) });
    }
  }
}
```

- [ ] **Step 2: projectionService.test.ts 작성 (통합 테스트)**

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { RawRepo } from '../repo/rawRepo.js';
import { DomainRepo } from '../repo/domainRepo.js';
import { ErrorRepo } from '../repo/errorRepo.js';
import { defaultRegistry } from '../parsers/registry.js';
import { ProjectionService } from './projectionService.js';
import type { Header } from '../header.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let svc: ProjectionService;
let rawRepo: RawRepo;

const header: Header = { imei: '123', messageCode: 'Fault', processDttm: null, latitude: null, longitude: null };

async function seed(messageId: string, code: string, rawPayload: unknown): Promise<void> {
  await rawRepo.insert({ messageId, deviceId: null, header: { ...header, messageCode: code }, rawPayload, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
}

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  rawRepo = new RawRepo(pool);
  svc = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, new ErrorRepo(pool));
});

afterAll(async () => {
  await pool.end();
  await container.stop();
});

describe('ProjectionService', () => {
  it('Fault 파생 + status=parsed', async () => {
    await seed('p-1', 'Fault', { message: { ftp: '100', sp: '12', pcode: 'P0001' } });
    await svc.project('p-1', 'Fault', { message: { ftp: '100', sp: '12', pcode: 'P0001' } });
    expect((await pool.query('SELECT pcode FROM domain_fault WHERE message_id=$1', ['p-1'])).rows[0].pcode).toBe('P0001');
    expect((await pool.query('SELECT status FROM messages_raw WHERE message_id=$1', ['p-1'])).rows[0].status).toBe('parsed');
  });

  it('미등록 코드 → parse_error + error_log', async () => {
    await seed('p-2', 'Unknown', { messageCode: 'Unknown' });
    await svc.project('p-2', 'Unknown', { messageCode: 'Unknown' });
    expect((await pool.query('SELECT status FROM messages_raw WHERE message_id=$1', ['p-2'])).rows[0].status).toBe('parse_error');
    const e = await pool.query('SELECT detail FROM error_log WHERE message_id=$1 AND stage=$2', ['p-2', 'projection']);
    expect(e.rows[0].detail).toContain('Unknown');
  });
});
```

- [ ] **Step 3: 테스트 통과 확인**

Run: `npx vitest run src/service/projectionService.test.ts`
Expected: PASS (2 tests). Docker 필요.

- [ ] **Step 4: Commit**

```bash
git add src/service/projectionService.ts src/service/projectionService.test.ts
git commit -m "feat: projectionService with parse_error + error_log"
```

---

## Task 7: messageProcessor (수신 1건 오케스트레이션)

**Files:**
- Create: `src/service/messageProcessor.ts`
- Test: `src/service/messageProcessor.test.ts`

오케스트레이션 규칙: 성공/중복/논리오류 → 정상 반환(호출자 ack). PG 인프라 오류 → throw(호출자 ack 안 함 → 재전송).

- [ ] **Step 1: messageProcessor.ts 작성**

```ts
import type { DeviceRepo } from '../repo/deviceRepo.js';
import type { RawRepo } from '../repo/rawRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { ProjectionService } from './projectionService.js';
import type { Clock } from '../types.js';
import { extractHeader } from '../header.js';
import { deriveMessageId } from '../ingest/messageId.js';

/** 수신 1건의 전 단계 처리. 정상 반환=ack 가능, throw=인프라 오류로 재전송 유도. */
export class MessageProcessor {
  constructor(
    private readonly deviceRepo: DeviceRepo,
    private readonly rawRepo: RawRepo,
    private readonly projection: ProjectionService,
    private readonly errorRepo: ErrorRepo,
    private readonly clock: Clock,
  ) {}

  /** @param topic MQTT 토픽, @param rawBuffer 페이로드 바이트 */
  async handle(topic: string, rawBuffer: Buffer): Promise<void> {
    const rawText = rawBuffer.toString('utf8');
    let payload: unknown;
    try {
      payload = JSON.parse(rawText);
    } catch {
      // 파싱 불가 — 재전송 무의미. error_log에 보존하고 ack.
      await this.errorRepo.log({ messageId: null, stage: 'ingest', detail: 'json parse failed', rawText });
      return;
    }

    const deviceIdFromTopic = topic.split('/')[1] ?? 'unknown';
    const header = extractHeader(payload);
    const messageId = deriveMessageId(deviceIdFromTopic, payload, rawText);

    // 단말 조회 (PG 오류면 throw → 재전송)
    const deviceId = await this.deviceRepo.findDeviceIdByImei(header.imei);
    const status = deviceId ? 'received' : 'unregistered_device';

    // 원본 적재 (내구성 지점). PG 오류면 throw → 재전송
    const isNew = await this.rawRepo.insert({
      messageId, deviceId, header, rawPayload: payload, status,
      receivedAt: this.clock.now().toISOString(),
    });
    if (!isNew) return; // 중복 → ack

    if (!deviceId) {
      // 미등록 단말: 원본 보존, 파생 보류, 추적 기록
      await this.errorRepo.log({ messageId, stage: 'device_lookup', imei: header.imei, messageCode: header.messageCode, detail: `unregistered imei: ${header.imei}` });
      return;
    }

    await this.projection.project(messageId, header.messageCode, payload);
  }
}
```

- [ ] **Step 2: messageProcessor.test.ts 작성 (통합 테스트)**

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { DeviceRepo } from '../repo/deviceRepo.js';
import { RawRepo } from '../repo/rawRepo.js';
import { DomainRepo } from '../repo/domainRepo.js';
import { ErrorRepo } from '../repo/errorRepo.js';
import { defaultRegistry } from '../parsers/registry.js';
import { ProjectionService } from './projectionService.js';
import { MessageProcessor } from './messageProcessor.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let proc: MessageProcessor;

const clock = { now: () => new Date('2026-06-09T09:03:00.000Z') };
const buf = (o: unknown) => Buffer.from(JSON.stringify(o));
const msg = (imei: string) => ({ imei, messageCode: 'Fault', process_dttm: '2026-06-09 09:03:00', message: { ftp: '100', sp: '12', pcode: 'P0001' }, latitude: '19.2', longitude: '203.1' });

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  const deviceRepo = new DeviceRepo(pool);
  await deviceRepo.register('DEV-1', 'imei-ok');
  const rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo);
  proc = new MessageProcessor(deviceRepo, rawRepo, projection, errorRepo, clock);
});

afterAll(async () => {
  await pool.end();
  await container.stop();
});

describe('MessageProcessor', () => {
  it('등록 단말: raw + device_id + 도메인 파생', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-ok')));
    const raw = await pool.query('SELECT message_id, device_id, status FROM messages_raw');
    const row = raw.rows.find((r) => r.device_id === 'DEV-1');
    expect(row.status).toBe('parsed');
    const dom = await pool.query('SELECT pcode FROM domain_fault WHERE message_id=$1', [row.message_id]);
    expect(dom.rows[0].pcode).toBe('P0001');
  });

  it('중복은 한 번만 저장(멱등)', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-ok'))); // 동일 내용 재처리
    const cnt = await pool.query("SELECT count(*)::int AS c FROM messages_raw WHERE device_id='DEV-1'");
    expect(cnt.rows[0].c).toBe(1);
  });

  it('미등록 imei → unregistered_device + error_log(device_lookup), 도메인 없음', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-unknown')));
    const raw = await pool.query("SELECT message_id, device_id, status FROM messages_raw WHERE status='unregistered_device'");
    expect(raw.rows[0].device_id).toBeNull();
    const e = await pool.query('SELECT stage FROM error_log WHERE message_id=$1', [raw.rows[0].message_id]);
    expect(e.rows[0].stage).toBe('device_lookup');
  });

  it('JSON 파싱 실패 → error_log(ingest), 원본 미저장', async () => {
    await proc.handle('device/dev/msg', Buffer.from('not-json'));
    const e = await pool.query("SELECT raw_text FROM error_log WHERE stage='ingest'");
    expect(e.rows[0].raw_text).toBe('not-json');
  });
});
```

- [ ] **Step 3: 테스트 통과 확인**

Run: `npx vitest run src/service/messageProcessor.test.ts`
Expected: PASS (4 tests). Docker 필요.

- [ ] **Step 4: Commit**

```bash
git add src/service/messageProcessor.ts src/service/messageProcessor.test.ts
git commit -m "feat: messageProcessor orchestration (raw->lookup->projection->error)"
```

---

## Task 8: MqttSubscriber(manual ack) 개정 + config + main + 빌드

**Files:**
- Modify: `src/ingest/MqttSubscriber.ts`, `src/ingest/MqttSubscriber.test.ts`, `src/config/config.ts`, `src/config/config.test.ts`, `src/main.ts`, `package.json`(build에 schema.sql 복사)

- [ ] **Step 1: MqttSubscriber.ts 전체 교체 (handleMessage 오버라이드로 ack 제어)**

```ts
import mqtt, { type MqttClient, type IClientPublishOptions } from 'mqtt';
import type { IPublishPacket } from 'mqtt-packet';

/** 메시지 처리기: 정상 반환=ack, throw=ack 안 함(재전송 유도). */
export type Handler = (topic: string, payload: Buffer) => Promise<void>;

export interface MqttSubscriberOptions {
  brokerUrl: string;
  topic: string;
  clientId: string;
  qos: 0 | 1 | 2;
}

/**
 * Mosquitto 구독. handleMessage 오버라이드로 QoS1 puback을 처리 성공 후에만 전송한다.
 * clean:false + 안정 clientId로 미ack 메시지 재전송 보장.
 */
export class MqttSubscriber {
  private client?: MqttClient;
  constructor(
    private readonly opts: MqttSubscriberOptions,
    private readonly handler: Handler,
  ) {}

  async start(): Promise<void> {
    const client = mqtt.connect(this.opts.brokerUrl, {
      clientId: this.opts.clientId,
      clean: false,            // durable session — 미ack QoS1 재전송
      reconnectPeriod: 2000,
    });
    this.client = client;

    // 처리 성공 후 cb() → puback 전송. 실패 시 cb(err) → puback 안 함 → 재전송.
    client.handleMessage = (packet: IPublishPacket, cb: (err?: Error) => void): void => {
      this.handler(packet.topic, packet.payload as Buffer)
        .then(() => cb())
        .catch((err: unknown) => {
          console.error(JSON.stringify({ level: 'error', msg: 'process failed (will redeliver)', err: String(err) }));
          cb(err instanceof Error ? err : new Error(String(err)));
        });
    };

    await new Promise<void>((resolve, reject) => {
      client.once('connect', () => resolve());
      client.once('error', reject);
    });
    await client.subscribeAsync(this.opts.topic, { qos: this.opts.qos });
  }

  async stop(): Promise<void> {
    await this.client?.endAsync();
  }
}

// (사용하지 않지만 타입 참조 유지를 위한 re-export 방지용)
export type { IClientPublishOptions };
```

> 참고: `handleMessage` 기본 구현은 'message' 이벤트를 emit하고 cb를 호출한다. 이를 오버라이드하면 'message' 이벤트 대신 직접 처리하며, QoS1 puback은 cb 호출 시점에 전송된다. cb(err) 시 puback이 전송되지 않아 durable 세션에서 재전송된다.

- [ ] **Step 2: MqttSubscriber.test.ts 전체 교체 (handleMessage ack 로직 단위 검증, 브로커 불필요)**

```ts
import { describe, it, expect, vi } from 'vitest';
import mqtt from 'mqtt';
import { MqttSubscriber, type Handler } from './MqttSubscriber.js';
import type { IPublishPacket } from 'mqtt-packet';

// mqtt.connect를 가짜 클라이언트로 대체 — handleMessage 오버라이드 동작만 검증
function fakeClient() {
  const client: any = {
    handleMessage: undefined,
    once: (ev: string, cb: (...a: any[]) => void) => { if (ev === 'connect') setTimeout(cb, 0); return client; },
    subscribeAsync: vi.fn().mockResolvedValue(undefined),
    endAsync: vi.fn().mockResolvedValue(undefined),
  };
  return client;
}

const packet = (topic: string, payload: string): IPublishPacket =>
  ({ cmd: 'publish', topic, payload: Buffer.from(payload), qos: 1, dup: false, retain: false } as IPublishPacket);

describe('MqttSubscriber handleMessage ack', () => {
  it('처리 성공 시 cb()를 인자 없이 호출(=ack)', async () => {
    const client = fakeClient();
    vi.spyOn(mqtt, 'connect').mockReturnValue(client);
    const handler: Handler = vi.fn().mockResolvedValue(undefined);
    const sub = new MqttSubscriber({ brokerUrl: 'mqtt://x', topic: 't', clientId: 'c', qos: 1 }, handler);
    await sub.start();

    const cb = vi.fn();
    await new Promise<void>((r) => { client.handleMessage(packet('device/d/msg', '{}'), (e?: Error) => { cb(e); r(); }); });
    expect(handler).toHaveBeenCalledWith('device/d/msg', Buffer.from('{}'));
    expect(cb).toHaveBeenCalledWith(undefined); // ack
  });

  it('처리 실패 시 cb(err) 호출(=ack 안 함)', async () => {
    const client = fakeClient();
    vi.spyOn(mqtt, 'connect').mockReturnValue(client);
    const handler: Handler = vi.fn().mockRejectedValue(new Error('pg down'));
    const sub = new MqttSubscriber({ brokerUrl: 'mqtt://x', topic: 't', clientId: 'c', qos: 1 }, handler);
    await sub.start();

    const cb = vi.fn();
    await new Promise<void>((r) => { client.handleMessage(packet('device/d/msg', '{}'), (e?: Error) => { cb(e); r(); }); });
    expect(cb.mock.calls[0][0]).toBeInstanceOf(Error); // no-ack
  });
});
```

- [ ] **Step 3: MqttSubscriber 테스트 통과 확인**

Run: `npx vitest run src/ingest/MqttSubscriber.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 4: config.ts 전체 교체 (PG 설정)**

```ts
export interface AppConfig {
  databaseUrl: string;
  mqttUrl: string;
  mqttTopic: string;
  mqttClientId: string;
  qos: 0 | 1 | 2;
}

function required(env: Record<string, string | undefined>, key: string): string {
  const v = env[key];
  if (!v) throw new Error(`missing required env: ${key}`);
  return v;
}

export function loadConfig(env: Record<string, string | undefined>): AppConfig {
  return {
    databaseUrl: required(env, 'DATABASE_URL'),
    mqttUrl: required(env, 'MQTT_URL'),
    mqttTopic: required(env, 'MQTT_TOPIC'),
    mqttClientId: env.MQTT_CLIENT_ID ?? 'edge-agent',
    qos: 1,
  };
}
```

- [ ] **Step 5: config.test.ts 전체 교체**

```ts
import { describe, it, expect } from 'vitest';
import { loadConfig } from './config.js';

describe('loadConfig', () => {
  it('환경변수에서 설정을 로드한다', () => {
    const cfg = loadConfig({
      DATABASE_URL: 'postgres://localhost/db', MQTT_URL: 'mqtt://localhost:1883', MQTT_TOPIC: 'device/+/msg',
    });
    expect(cfg.databaseUrl).toBe('postgres://localhost/db');
    expect(cfg.qos).toBe(1);
    expect(cfg.mqttClientId).toBe('edge-agent');
  });

  it('필수 변수 누락 시 throw', () => {
    expect(() => loadConfig({})).toThrow(/DATABASE_URL/);
  });
});
```

- [ ] **Step 6: config 테스트 통과 확인**

Run: `npx vitest run src/config/config.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 7: main.ts 전체 교체 (PG 직접 처리 조립)**

```ts
import { loadConfig } from './config/config.js';
import { createPool } from './db/pool.js';
import { applySchema } from './db/applySchema.js';
import { DeviceRepo } from './repo/deviceRepo.js';
import { RawRepo } from './repo/rawRepo.js';
import { DomainRepo } from './repo/domainRepo.js';
import { ErrorRepo } from './repo/errorRepo.js';
import { defaultRegistry } from './parsers/registry.js';
import { ProjectionService } from './service/projectionService.js';
import { MessageProcessor } from './service/messageProcessor.js';
import { MqttSubscriber } from './ingest/MqttSubscriber.js';
import { systemClock } from './types.js';

async function main(): Promise<void> {
  const cfg = loadConfig(process.env);
  const pool = createPool(cfg.databaseUrl);
  await applySchema(pool); // IF NOT EXISTS — 멱등

  const deviceRepo = new DeviceRepo(pool);
  const rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo);
  const processor = new MessageProcessor(deviceRepo, rawRepo, projection, errorRepo, systemClock);

  const subscriber = new MqttSubscriber(
    { brokerUrl: cfg.mqttUrl, topic: cfg.mqttTopic, clientId: cfg.mqttClientId, qos: cfg.qos },
    (topic, payload) => processor.handle(topic, payload),
  );
  await subscriber.start();
  console.log(JSON.stringify({ level: 'info', msg: 'agent started', clientId: cfg.mqttClientId }));

  const shutdown = async () => {
    await subscriber.stop();
    await pool.end();
    console.log(JSON.stringify({ level: 'info', msg: 'agent stopped' }));
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

main().catch((err) => {
  console.error(JSON.stringify({ level: 'fatal', err: String(err) }));
  process.exit(1);
});
```

- [ ] **Step 8: package.json build에 schema.sql 복사 추가**

`scripts.build`를 교체:
```json
"build": "tsc && node -e \"require('fs').mkdirSync('dist/db',{recursive:true});require('fs').copyFileSync('src/db/schema.sql','dist/db/schema.sql')\"",
```

- [ ] **Step 9: 빌드 확인**

Run: `npm run build`
Expected: `dist/main.js`, `dist/db/schema.sql` 생성, 컴파일 오류 없음

- [ ] **Step 10: 전체 테스트 실행**

Run: `npm test`
Expected: 모든 테스트 PASS (Docker 필요)

- [ ] **Step 11: 수동 통합 확인 (선택, 로컬 Mosquitto+PG 필요)**

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=agent postgres:16-alpine
docker run -d -p 1883:1883 eclipse-mosquitto:2 \
  sh -c "printf 'listener 1883\nallow_anonymous true\n' > /mosquitto/config/mosquitto.conf && mosquitto -c /mosquitto/config/mosquitto.conf"

DATABASE_URL=postgres://postgres:pw@localhost:5432/agent \
  MQTT_URL=mqtt://localhost:1883 MQTT_TOPIC='device/+/msg' npm start
```
Expected: "agent started" 로그. (devices에 단말 등록 후) 메시지 발행 → messages_raw + domain_fault 저장 확인

- [ ] **Step 12: Commit**

```bash
git add src/ingest/MqttSubscriber.ts src/ingest/MqttSubscriber.test.ts src/config/ src/main.ts package.json
git commit -m "feat: manual-ack MQTT subscriber, PG config, main wiring"
```

---

## Self-Review 결과

- **Spec 커버리지 (개정판):** 단일 프로세스 MQTT→PG(T7,T8), manual ack 원본저장후(T8), imei→device_id(T4,T7), 원본 멱등 적재(T4,T7), messageCode 라우팅+파서(T5), 동기 파생(T6,T7), parse_error/unregistered_device 격리(T6,T7), 전용 error_log 단계별 기록(T4,T6,T7), Redis/HTTP 제거(T1) — 모두 매핑.
- **타입 일관성:** `Header`(header.ts), `RawInsert`, `FaultRecord`, `DomainParser<T>`, `ErrorEntry/ErrorStage`, `Handler`, `AppConfig` 시그니처가 전 태스크에서 일치. 리포 메서드명(`findDeviceIdByImei`, `insert`, `markStatus`, `insertFault`, `log`) 호출부 일치.
- **무손실:** 원본 INSERT 성공→cb()(ack), 실패→cb(err)(no-ack)+durable 세션 재전송. message_id UNIQUE로 재전송 중복 흡수.
- **주의:** ① schema.sql은 build에서 dist 복사(T8 Step 8). ② projection 단계의 PG 일시 오류는 parse_error로 잘못 기록될 수 있음(인프라/논리 오류 구분은 후속 개선). ③ process_dttm은 서버 TZ 가정 캐스팅. ④ MQTT manual-ack 실동작은 T8 Step 11 수동 통합으로 최종 확인.

---

## 다음 단계

구현 완료 후, Mosquitto+PG+에이전트 docker-compose E2E로 단말→DB 전 구간(원본/도메인/미등록/parse_error/중복) 검증(설계 §6.4).
