# 업무 테이블 device_id + 위치 분리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. 체크박스(`- [ ]`) 추적.

**Goal:** 모든 업무 도메인 테이블에 `device_id`를 추가하고, 모든 등록 단말 메시지의 공통 위치(lat/lon)를 `domain_location`으로 분리 저장한다. 미등록 단말은 원본+에러만 남기고 도메인/위치 파생을 하지 않는다.

**Architecture:** 기존 단일 에이전트(MQTT→PG) 확장. 파서 `insert`에 device_id 전파, 신규 `LocationProjector`/`LocationRepo`, messageProcessor 흐름에서 device_id 조회→raw 저장→(등록 시) 위치+업무 파생.

**Tech Stack:** 기존과 동일 (TS, pg, mqtt, vitest, @testcontainers/postgresql).

설계: [2026-06-09-realtime-message-pipeline-design.md](../specs/2026-06-09-realtime-message-pipeline-design.md) §4–5

---

## Task 1: 스키마 — domain_fault에 device_id 추가 + domain_location 신규

**Files:** Modify `src/db/schema.sql`

- [ ] **Step 1:** `domain_fault` 정의를 교체하고 `domain_location` 추가

```sql
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
```

- [ ] **Step 2:** Commit `git add src/db/schema.sql && git commit -m "feat: domain tables carry device_id, add domain_location"`

---

## Task 2: domainRepo + locationRepo (device_id 반영)

**Files:** Modify `src/repo/domainRepo.ts`; Create `src/repo/locationRepo.ts`; Modify `src/repo/repo.test.ts`

- [ ] **Step 1:** `domainRepo.ts`의 `insertFault` 시그니처에 deviceId 추가

```ts
import type { Pool } from 'pg';

export interface FaultRecord {
  ftp: string | null;
  sp: string | null;
  pcode: string | null;
}

export class DomainRepo {
  constructor(private readonly pool: Pool) {}

  async insertFault(messageId: string, deviceId: string, rec: FaultRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO domain_fault (message_id, device_id, ftp, sp, pcode)
       VALUES ($1,$2,$3,$4,$5) ON CONFLICT (message_id) DO NOTHING`,
      [messageId, deviceId, rec.ftp, rec.sp, rec.pcode],
    );
  }
}
```

- [ ] **Step 2:** `locationRepo.ts` 작성

```ts
import type { Pool } from 'pg';

export class LocationRepo {
  constructor(private readonly pool: Pool) {}

  /** domain_location 멱등 INSERT. */
  async insert(messageId: string, deviceId: string, latitude: string | null, longitude: string | null): Promise<void> {
    await this.pool.query(
      `INSERT INTO domain_location (message_id, device_id, latitude, longitude)
       VALUES ($1,$2,$3,$4) ON CONFLICT (message_id) DO NOTHING`,
      [messageId, deviceId, latitude, longitude],
    );
  }
}
```

- [ ] **Step 3:** `repo.test.ts`의 domainRepo 테스트를 deviceId 인자 반영으로 수정하고 locationRepo 테스트 추가

domainRepo 테스트의 `insertFault('raw-2', { ... })` 두 줄을 다음으로 교체:
```ts
    await repo.insertFault('raw-2', 'DEV-100', { ftp: '100', sp: '12', pcode: 'P0001' });
    await repo.insertFault('raw-2', 'DEV-100', { ftp: '100', sp: '12', pcode: 'P0001' });
```

import에 LocationRepo 추가:
```ts
import { LocationRepo } from './locationRepo.js';
```

`errorRepo` 테스트 다음에 추가:
```ts
  it('locationRepo: 위치 멱등 INSERT', async () => {
    const raw = new RawRepo(pool);
    await raw.insert({ messageId: 'raw-3', deviceId: 'DEV-100', header, rawPayload: {}, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
    const repo = new LocationRepo(pool);
    await repo.insert('raw-3', 'DEV-100', '19.2', '203.1');
    await repo.insert('raw-3', 'DEV-100', '19.2', '203.1');
    const res = await pool.query('SELECT latitude, device_id FROM domain_location WHERE message_id=$1', ['raw-3']);
    expect(res.rows[0].device_id).toBe('DEV-100');
  });
```

- [ ] **Step 4:** Run `npx vitest run src/repo/repo.test.ts` — Expected: PASS (5 tests, Docker 필요)
- [ ] **Step 5:** Commit `git add src/repo/ && git commit -m "feat: domainRepo device_id + locationRepo"`

---

## Task 3: 파서 insert에 device_id 전파

**Files:** Modify `src/parsers/types.ts`, `src/parsers/faultParser.ts`

- [ ] **Step 1:** `types.ts`의 DomainParser.insert에 deviceId 추가

```ts
import type { DomainRepo } from '../repo/domainRepo.js';

export interface DomainParser<T> {
  readonly messageCode: string;
  parse(rawPayload: unknown): T;
  insert(repo: DomainRepo, messageId: string, deviceId: string, parsed: T): Promise<void>;
}
```

- [ ] **Step 2:** `faultParser.ts`의 insert 시그니처 수정

`insert` 메서드를 다음으로 교체:
```ts
  insert(repo: DomainRepo, messageId: string, deviceId: string, parsed: FaultRecord): Promise<void> {
    return repo.insertFault(messageId, deviceId, parsed);
  },
```

- [ ] **Step 3:** Run `npx vitest run src/parsers/faultParser.test.ts` — Expected: PASS (3 tests, parse는 불변)
- [ ] **Step 4:** Commit `git add src/parsers/ && git commit -m "feat: parser insert propagates device_id"`

---

## Task 4: projectionService — device_id 전파

**Files:** Modify `src/service/projectionService.ts`, `src/service/projectionService.test.ts`

- [ ] **Step 1:** `project` 시그니처에 deviceId 추가, parser.insert 호출에 전파

`project` 메서드를 다음으로 교체:
```ts
  async project(messageId: string, deviceId: string, messageCode: string, rawPayload: unknown): Promise<void> {
    const parser = this.registry.get(messageCode);
    if (!parser) {
      await this.rawRepo.markStatus(messageId, 'parse_error');
      await this.errorRepo.log({ messageId, stage: 'projection', messageCode, detail: `no parser for messageCode=${messageCode}` });
      return;
    }
    try {
      const parsed = parser.parse(rawPayload);
      await parser.insert(this.domainRepo, messageId, deviceId, parsed);
      await this.rawRepo.markStatus(messageId, 'parsed');
    } catch (err) {
      await this.rawRepo.markStatus(messageId, 'parse_error');
      await this.errorRepo.log({ messageId, stage: 'projection', messageCode, detail: String(err) });
    }
  }
```

- [ ] **Step 2:** `projectionService.test.ts`의 `svc.project(...)` 호출에 deviceId 인자 추가

`svc.project('p-1', 'Fault', {...})` → `svc.project('p-1', 'DEV-1', 'Fault', {...})`
`svc.project('p-2', 'Unknown', {...})` → `svc.project('p-2', 'DEV-1', 'Unknown', {...})`
그리고 seed에서 사용하는 device를 등록하기 위해, beforeAll에 다음을 추가:
```ts
  await new DeviceRepo(pool).register('DEV-1', 'imei-proj');
```
import 추가: `import { DeviceRepo } from '../repo/deviceRepo.js';`
seed의 deviceId를 'DEV-1'로: `rawRepo.insert({ messageId, deviceId: 'DEV-1', ... })`

- [ ] **Step 3:** Run `npx vitest run src/service/projectionService.test.ts` — Expected: PASS (2 tests, Docker 필요)
- [ ] **Step 4:** Commit `git add src/service/projectionService.ts src/service/projectionService.test.ts && git commit -m "feat: projectionService propagates device_id"`

---

## Task 5: LocationProjector

**Files:** Create `src/service/locationProjector.ts`, `src/service/locationProjector.test.ts`

- [ ] **Step 1:** `locationProjector.ts` 작성

```ts
import type { LocationRepo } from '../repo/locationRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { Header } from '../header.js';

/** lat/lon이 모두 있으면 domain_location에 저장(등록 단말 전용, messageCode 무관). 실패는 비치명. */
export class LocationProjector {
  constructor(
    private readonly locationRepo: LocationRepo,
    private readonly errorRepo: ErrorRepo,
  ) {}

  async project(messageId: string, deviceId: string, header: Header): Promise<void> {
    if (header.latitude == null || header.longitude == null) return; // 위치 없음 → 스킵(오류 아님)
    try {
      await this.locationRepo.insert(messageId, deviceId, header.latitude, header.longitude);
    } catch (err) {
      await this.errorRepo.log({ messageId, stage: 'location', detail: String(err) });
    }
  }
}
```

- [ ] **Step 2:** `errorRepo.ts`의 `ErrorStage`에 `'location'` 추가

`export type ErrorStage = 'ingest' | 'device_lookup' | 'projection';` →
`export type ErrorStage = 'ingest' | 'device_lookup' | 'projection' | 'location';`

- [ ] **Step 3:** `locationProjector.test.ts` 작성 (통합)

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { DeviceRepo } from '../repo/deviceRepo.js';
import { RawRepo } from '../repo/rawRepo.js';
import { LocationRepo } from '../repo/locationRepo.js';
import { ErrorRepo } from '../repo/errorRepo.js';
import { LocationProjector } from './locationProjector.js';
import type { Header } from '../header.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let proj: LocationProjector;
let rawRepo: RawRepo;

const base: Header = { imei: 'i', messageCode: 'Fault', processDttm: null, latitude: '19.2', longitude: '203.1' };

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  await new DeviceRepo(pool).register('DEV-1', 'imei-loc');
  rawRepo = new RawRepo(pool);
  proj = new LocationProjector(new LocationRepo(pool), new ErrorRepo(pool));
});

afterAll(async () => { await pool.end(); await container.stop(); });

async function seed(id: string) {
  await rawRepo.insert({ messageId: id, deviceId: 'DEV-1', header: base, rawPayload: {}, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
}

describe('LocationProjector', () => {
  it('lat/lon 있으면 domain_location에 저장', async () => {
    await seed('loc-1');
    await proj.project('loc-1', 'DEV-1', base);
    const r = await pool.query('SELECT latitude, device_id FROM domain_location WHERE message_id=$1', ['loc-1']);
    expect(r.rows[0].device_id).toBe('DEV-1');
  });

  it('lat/lon 없으면 스킵(저장 안 함)', async () => {
    await seed('loc-2');
    await proj.project('loc-2', 'DEV-1', { ...base, latitude: null, longitude: null });
    const r = await pool.query('SELECT 1 FROM domain_location WHERE message_id=$1', ['loc-2']);
    expect(r.rowCount).toBe(0);
  });
});
```

- [ ] **Step 4:** Run `npx vitest run src/service/locationProjector.test.ts` — Expected: PASS (2 tests, Docker 필요)
- [ ] **Step 5:** Commit `git add src/service/locationProjector.ts src/repo/errorRepo.ts src/service/locationProjector.test.ts && git commit -m "feat: LocationProjector + location error stage"`

---

## Task 6: messageProcessor — device_id lookup 우선 + 위치/도메인 분기

**Files:** Modify `src/service/messageProcessor.ts`, `src/service/messageProcessor.test.ts`

- [ ] **Step 1:** `messageProcessor.ts` 교체 (lookup→raw→분기, locationProjector 주입)

```ts
import type { DeviceRepo } from '../repo/deviceRepo.js';
import type { RawRepo } from '../repo/rawRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { ProjectionService } from './projectionService.js';
import type { LocationProjector } from './locationProjector.js';
import type { Clock } from '../types.js';
import { extractHeader } from '../header.js';
import { deriveMessageId } from '../ingest/messageId.js';

/** 수신 1건의 전 단계 처리. 정상 반환=ack 가능, throw=인프라 오류로 재전송 유도. */
export class MessageProcessor {
  constructor(
    private readonly deviceRepo: DeviceRepo,
    private readonly rawRepo: RawRepo,
    private readonly projection: ProjectionService,
    private readonly location: LocationProjector,
    private readonly errorRepo: ErrorRepo,
    private readonly clock: Clock,
  ) {}

  async handle(topic: string, rawBuffer: Buffer): Promise<void> {
    const rawText = rawBuffer.toString('utf8');
    let payload: unknown;
    try {
      payload = JSON.parse(rawText);
    } catch {
      await this.errorRepo.log({ messageId: null, stage: 'ingest', detail: 'json parse failed', rawText });
      return;
    }

    const deviceIdFromTopic = topic.split('/')[1] ?? 'unknown';
    const header = extractHeader(payload);
    const messageId = deriveMessageId(deviceIdFromTopic, payload, rawText);

    // imei로 device_id 조회 (PG 오류면 throw → 재전송)
    const deviceId = await this.deviceRepo.findDeviceIdByImei(header.imei);
    const status = deviceId ? 'received' : 'unregistered_device';

    // 원본 적재 (내구성 지점). PG 오류면 throw → 재전송
    const isNew = await this.rawRepo.insert({
      messageId, deviceId, header, rawPayload: payload, status,
      receivedAt: this.clock.now().toISOString(),
    });
    if (!isNew) return; // 중복 → ack

    if (!deviceId) {
      // 미등록 단말: 원본만 보존, 도메인/위치 저장 안 함
      await this.errorRepo.log({ messageId, stage: 'device_lookup', imei: header.imei, messageCode: header.messageCode, detail: `unregistered imei: ${header.imei}` });
      return;
    }

    // 등록 단말: 공통 위치 + messageCode 업무 파생
    await this.location.project(messageId, deviceId, header);
    await this.projection.project(messageId, deviceId, header.messageCode, payload);
  }
}
```

- [ ] **Step 2:** `messageProcessor.test.ts` 수정 — LocationProjector 주입 + 위치 저장/미등록 미저장 검증

import 추가:
```ts
import { LocationRepo } from '../repo/locationRepo.js';
import { LocationProjector } from './locationProjector.js';
```
beforeAll의 processor 생성부를 다음으로 교체:
```ts
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo);
  const location = new LocationProjector(new LocationRepo(pool), errorRepo);
  proc = new MessageProcessor(deviceRepo, rawRepo, projection, location, errorRepo, clock);
```
첫 테스트(등록 단말)에 도메인+위치 확인 추가 — 기존 it 블록 끝에 다음 줄 추가:
```ts
    const loc = await pool.query('SELECT device_id FROM domain_location WHERE message_id=$1', [row.message_id]);
    expect(loc.rows[0].device_id).toBe('DEV-1');
```
미등록 테스트에 위치 미저장 확인 추가 — 기존 미등록 it 블록 끝에:
```ts
    const loc = await pool.query('SELECT 1 FROM domain_location WHERE message_id=$1', [raw.rows[0].message_id]);
    expect(loc.rowCount).toBe(0);
```

- [ ] **Step 3:** Run `npx vitest run src/service/messageProcessor.test.ts` — Expected: PASS (4 tests, Docker 필요)
- [ ] **Step 4:** Commit `git add src/service/messageProcessor.ts src/service/messageProcessor.test.ts && git commit -m "feat: messageProcessor lookup-first + location/domain branch"`

---

## Task 7: main 조립 수정 + 전체 빌드/테스트

**Files:** Modify `src/main.ts`

- [ ] **Step 1:** `main.ts`에 LocationRepo/LocationProjector 조립 추가

import 추가:
```ts
import { LocationRepo } from './repo/locationRepo.js';
import { LocationProjector } from './service/locationProjector.js';
```
processor 생성부를 다음으로 교체:
```ts
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo);
  const location = new LocationProjector(new LocationRepo(pool), errorRepo);
  const processor = new MessageProcessor(deviceRepo, rawRepo, projection, location, errorRepo, systemClock);
```

- [ ] **Step 2:** Run `npm run build` — Expected: 컴파일 오류 없음, dist 생성
- [ ] **Step 3:** Run `npm test` — Expected: 전체 PASS (Docker 필요)
- [ ] **Step 4:** Commit `git add src/main.ts && git commit -m "feat: wire LocationProjector into main"`

---

## Self-Review 결과

- **Spec 커버리지:** domain_fault device_id(T1,T2,T3,T4), domain_location(T1,T2,T5), 위치 projection 등록단말 전용(T5,T6), 미등록 단말 도메인/위치 미저장(T6), location 에러 stage(T5), device_id 조회 우선(T6) — 모두 매핑.
- **타입 일관성:** `insertFault(messageId, deviceId, rec)`, `project(messageId, deviceId, messageCode, payload)`, `DomainParser.insert(repo, messageId, deviceId, parsed)`, `LocationProjector.project(messageId, deviceId, header)` 전 호출부 일치.
- **주의:** 위치는 등록 단말만(domain_location.device_id NOT NULL). 미등록은 원본+에러만. lat/lon 없으면 위치 스킵(정상).
