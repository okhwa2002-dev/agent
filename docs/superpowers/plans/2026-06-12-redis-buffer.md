# Redis Streams 로컬 버퍼 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans (또는 subagent-driven-development). 체크박스(`- [ ]`) 추적.

**Goal:** MQTT 수신을 Redis Streams 버퍼에 즉시 적재(ack)하고, 인프로세스 다중 워커가 버퍼를 PG로 드레인하여 순간 폭주 시 브로커 드롭(메시지 유실)을 제거한다.

**Architecture:** 수신부(MqttSubscriber)는 메시지를 `XADD messages:stream` 후 MQTT ack(내구성 지점 이동). K개 워커가 consumer group으로 `XREADGROUP`→`messageProcessor.handle`→`XACK`. 실패는 재시도(XAUTOCLAIM), poison은 DLQ. `messageProcessor`·repo·스키마는 무변경 재사용.

**Tech Stack:** Node.js/TypeScript(ESM/NodeNext), ioredis, mqtt 5, pg, Vitest, @testcontainers/redis + @testcontainers/postgresql.

설계 출처: [2026-06-12-redis-buffer-design.md](../specs/2026-06-12-redis-buffer-design.md)

---

## File Structure

| 파일 | 상태 | 책임 |
|---|---|---|
| `src/buffer/redisPool.ts` | 신규 | ioredis 인스턴스 팩토리 |
| `src/buffer/RedisStreamQueue.ts` | 신규 | XADD/XREADGROUP/XACK/XAUTOCLAIM/XDEL/DLQ 래핑 |
| `src/buffer/WorkerPool.ts` | 신규 | K개 워커 루프 기동·정지, 재시도·DLQ |
| `src/ingest/MqttSubscriber.ts` | 변경 | handler 호출 → enqueue(XADD), ack=XADD 성공 |
| `src/config/config.ts` | 변경 | `redisUrl`, `workerConcurrency` 추가 |
| `src/db/pool.ts` | 변경 | pg Pool `max` 옵션 |
| `src/main.ts` | 변경 | Redis 큐 + 워커풀 조립 |
| `.env.example` | 변경 | `REDIS_URL`, `WORKER_CONCURRENCY` |
| `docker-compose.yml` | 변경 | Redis 서비스 추가 |

> 유지(무변경): `messageProcessor`, 모든 repo, mapper, parser, schema, logger.

---

## Task 1: 의존성 추가 + Redis 서비스 + 설정

**Files:** Modify `package.json`, `docker-compose.yml`, `.env.example`, `src/config/config.ts`, `src/config/config.test.ts`

- [ ] **Step 1: 의존성 설치**

Run:
```bash
npm install ioredis
npm install -D @testcontainers/redis
```
Expected: 설치 성공, package.json에 `ioredis`(dependencies), `@testcontainers/redis`(devDependencies) 추가.

- [ ] **Step 2: docker-compose.yml에 Redis 서비스 추가**

`services:` 아래에 추가(mosquitto/postgres와 동일 레벨):
```yaml
  redis:
    image: redis:7-alpine
    container_name: agent-redis
    command: ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec"]
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    restart: unless-stopped
```
그리고 파일 맨 아래 `volumes:` 블록에 `redis-data:` 추가:
```yaml
volumes:
  mosquitto-data:
  postgres-data:
  redis-data:
```

- [ ] **Step 3: .env.example에 추가**

`LOG_DIR=...` 줄 아래에 추가:
```
# Redis 버퍼
REDIS_URL=redis://localhost:6379

# 인프로세스 워커 수 (기본 4)
WORKER_CONCURRENCY=4
```

- [ ] **Step 4: config.test.ts에 실패 테스트 추가**

`src/config/config.test.ts`의 첫 번째 `it('환경변수에서 설정을 로드한다', ...)` 안 `expect`들 뒤에 추가:
```ts
    expect(cfg.redisUrl).toBe('redis://localhost:6379');
    expect(cfg.workerConcurrency).toBe(4);
```
그리고 그 테스트의 `loadConfig({...})` 인자에 다음 키를 추가:
```ts
      REDIS_URL: 'redis://localhost:6379', WORKER_CONCURRENCY: '4',
```

- [ ] **Step 5: 테스트 실패 확인**

Run: `npx vitest run src/config/config.test.ts`
Expected: FAIL — `cfg.redisUrl`/`cfg.workerConcurrency` undefined.

- [ ] **Step 6: config.ts 수정**

`AppConfig` 인터페이스에 추가:
```ts
  redisUrl: string;
  workerConcurrency: number;
```
`loadConfig` 반환 객체에 추가(`qos: 1,` 위/아래 아무 곳):
```ts
    redisUrl: required(env, 'REDIS_URL'),
    workerConcurrency: Number(env.WORKER_CONCURRENCY ?? '4'),
```

- [ ] **Step 7: 테스트 통과 확인**

Run: `npx vitest run src/config/config.test.ts`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add package.json package-lock.json docker-compose.yml .env.example src/config/config.ts src/config/config.test.ts
git commit -m "chore: add redis dep, service, and config (REDIS_URL/WORKER_CONCURRENCY)"
```

---

## Task 2: redisPool 팩토리

**Files:** Create `src/buffer/redisPool.ts`

- [ ] **Step 1: 작성**

```ts
import { Redis } from 'ioredis';

/** ioredis 인스턴스 생성. maxRetriesPerRequest:null 로 블로킹 명령(XREADGROUP BLOCK) 허용. */
export function createRedis(url: string): Redis {
  return new Redis(url, { maxRetriesPerRequest: null });
}
```

- [ ] **Step 2: 컴파일 확인**

Run: `npx tsc --noEmit`
Expected: 오류 없음.

- [ ] **Step 3: Commit**

```bash
git add src/buffer/redisPool.ts
git commit -m "feat: redis instance factory"
```

---

## Task 3: RedisStreamQueue

**Files:** Create `src/buffer/RedisStreamQueue.ts`; Test `src/buffer/RedisStreamQueue.test.ts`

- [ ] **Step 1: 실패하는 통합 테스트 작성**

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { RedisStreamQueue } from './RedisStreamQueue.js';

let container: StartedRedisContainer;
let redis: Redis;

beforeAll(async () => {
  container = await new RedisContainer('redis:7-alpine').start();
  redis = new Redis(container.getConnectionUrl(), { maxRetriesPerRequest: null });
});
afterAll(async () => { await redis.quit(); await container.stop(); });

function newQueue(stream: string) {
  return new RedisStreamQueue(redis, { stream, group: 'g', dlqStream: `${stream}:dlq` });
}

describe('RedisStreamQueue', () => {
  it('enqueue → claim 하면 원본 필드를 돌려준다', async () => {
    const q = newQueue('s1'); await q.init();
    await q.enqueue({ topic: 'device/a/msg', payload: '{"x":1}', receivedAt: '2026-06-12T00:00:00.000Z' });
    const claimed = await q.claim('worker-0', 10, 50);
    expect(claimed).toHaveLength(1);
    expect(claimed[0].msg).toEqual({ topic: 'device/a/msg', payload: '{"x":1}', receivedAt: '2026-06-12T00:00:00.000Z' });
  });

  it('ack 후에는 다시 claim/회수되지 않고 스트림에서 삭제된다', async () => {
    const q = newQueue('s2'); await q.init();
    await q.enqueue({ topic: 't', payload: 'p', receivedAt: 'r' });
    const [e] = await q.claim('worker-0', 10, 50);
    await q.ack(e.id);
    const again = await q.claim('worker-0', 10, 50);
    expect(again).toHaveLength(0);
    expect(await redis.xlen('s2')).toBe(0); // XDEL 됨
  });

  it('ack 안 한 엔트리는 reclaim(XAUTOCLAIM)으로 회수된다', async () => {
    const q = newQueue('s3'); await q.init();
    await q.enqueue({ topic: 't', payload: 'p', receivedAt: 'r' });
    await q.claim('worker-0', 10, 50);        // claim 후 ack 안 함
    const reclaimed = await q.reclaim('worker-1', 0, 10);
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0].deliveries).toBeGreaterThanOrEqual(2);
  });

  it('toDlq는 원본을 DLQ로 옮기고 ack한다', async () => {
    const q = newQueue('s4'); await q.init();
    await q.enqueue({ topic: 't', payload: 'poison', receivedAt: 'r' });
    const [e] = await q.claim('worker-0', 10, 50);
    await q.toDlq(e);
    expect(await redis.xlen('s4:dlq')).toBe(1);
    expect(await redis.xlen('s4')).toBe(0);
  });
});
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `npx vitest run src/buffer/RedisStreamQueue.test.ts`
Expected: FAIL — "Cannot find module './RedisStreamQueue.js'".

- [ ] **Step 3: 구현 작성**

```ts
import type { Redis } from 'ioredis';

export interface QueueMessage {
  topic: string;
  payload: string;     // 원본 텍스트
  receivedAt: string;
}

export interface ClaimedEntry {
  id: string;          // 스트림 엔트리 ID
  msg: QueueMessage;
  deliveries: number;  // 전달 횟수(재시도 판정용)
}

export interface RedisStreamQueueOptions {
  stream: string;
  group: string;
  dlqStream: string;
}

/** Redis Streams + Consumer Group 기반 버퍼. */
export class RedisStreamQueue {
  constructor(private readonly redis: Redis, private readonly opts: RedisStreamQueueOptions) {}

  /** 그룹 생성(이미 있으면 무시). MKSTREAM으로 스트림도 생성. */
  async init(): Promise<void> {
    try {
      await this.redis.xgroup('CREATE', this.opts.stream, this.opts.group, '$', 'MKSTREAM');
    } catch (err) {
      if (!(err instanceof Error) || !err.message.includes('BUSYGROUP')) throw err;
    }
  }

  /** 수신 메시지 적재. 성공 후에만 MQTT ack 해야 함. */
  async enqueue(m: QueueMessage): Promise<void> {
    await this.redis.xadd(this.opts.stream, '*', 'topic', m.topic, 'payload', m.payload, 'receivedAt', m.receivedAt);
  }

  /** 신규 엔트리를 consumer로 claim (BLOCK ms 대기). */
  async claim(consumer: string, count: number, blockMs: number): Promise<ClaimedEntry[]> {
    const res = await this.redis.xreadgroup(
      'GROUP', this.opts.group, consumer, 'COUNT', count, 'BLOCK', blockMs,
      'STREAMS', this.opts.stream, '>',
    );
    if (!Array.isArray(res) || res.length === 0) return [];
    const entries = (res[0] as [string, [string, string[]][]])[1] ?? [];
    return entries.map(([id, fields]) => ({ id, msg: parseFields(fields), deliveries: 1 }));
  }

  /** idle 초과한 미ACK 엔트리를 이 consumer로 회수. */
  async reclaim(consumer: string, idleMs: number, count: number): Promise<ClaimedEntry[]> {
    const res = (await this.redis.xautoclaim(
      this.opts.stream, this.opts.group, consumer, idleMs, '0', 'COUNT', count,
    )) as [string, [string, string[] | null][], string[]];
    const entries = res[1] ?? [];
    const out: ClaimedEntry[] = [];
    for (const [id, fields] of entries) {
      if (!fields) continue; // 이미 삭제된 엔트리
      out.push({ id, msg: parseFields(fields), deliveries: 2 });
    }
    return out;
  }

  /** 처리 완료: XACK + XDEL(스트림에서 제거). */
  async ack(id: string): Promise<void> {
    await this.redis.xack(this.opts.stream, this.opts.group, id);
    await this.redis.xdel(this.opts.stream, id);
  }

  /** poison: DLQ로 옮기고 원본 ack. */
  async toDlq(e: ClaimedEntry): Promise<void> {
    await this.redis.xadd(this.opts.dlqStream, '*', 'topic', e.msg.topic, 'payload', e.msg.payload, 'receivedAt', e.msg.receivedAt);
    await this.ack(e.id);
  }
}

function parseFields(fields: string[]): QueueMessage {
  const m: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) m[fields[i]] = fields[i + 1];
  return { topic: m.topic, payload: m.payload, receivedAt: m.receivedAt };
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `npx vitest run src/buffer/RedisStreamQueue.test.ts`
Expected: PASS (4 tests). Docker 필요.

- [ ] **Step 5: Commit**

```bash
git add src/buffer/RedisStreamQueue.ts src/buffer/RedisStreamQueue.test.ts
git commit -m "feat: RedisStreamQueue (xadd/claim/ack-xdel/reclaim/dlq)"
```

---

## Task 4: WorkerPool

**Files:** Create `src/buffer/WorkerPool.ts`; Test `src/buffer/WorkerPool.test.ts`

처리기는 `messageProcessor.handle(topic, Buffer)` 시그니처를 따른다. WorkerPool은 큐와 처리기만 알면 되도록 처리기를 함수로 주입한다.

- [ ] **Step 1: 실패하는 테스트 작성 (fake 큐로 단위 검증)**

```ts
import { describe, it, expect, vi } from 'vitest';
import { WorkerPool, type Handler } from './WorkerPool.js';
import type { ClaimedEntry, RedisStreamQueue } from './RedisStreamQueue.js';

function entry(id: string, payload: string, deliveries = 1): ClaimedEntry {
  return { id, msg: { topic: 'device/a/msg', payload, receivedAt: 'r' }, deliveries };
}

/** claim을 한 번만 주고 이후 빈 배열을 주는 fake 큐. */
function fakeQueue(first: ClaimedEntry[]): RedisStreamQueue & { acked: string[]; dlq: string[] } {
  let served = false;
  const acked: string[] = []; const dlq: string[] = [];
  return {
    acked, dlq,
    init: vi.fn().mockResolvedValue(undefined),
    enqueue: vi.fn(),
    claim: vi.fn(async () => { if (served) return []; served = true; return first; }),
    reclaim: vi.fn(async () => []),
    ack: vi.fn(async (id: string) => { acked.push(id); }),
    toDlq: vi.fn(async (e: ClaimedEntry) => { dlq.push(e.id); acked.push(e.id); }),
  } as unknown as RedisStreamQueue & { acked: string[]; dlq: string[] };
}

describe('WorkerPool', () => {
  it('성공 처리 시 ack한다', async () => {
    const q = fakeQueue([entry('1-0', '{"ok":1}')]);
    const handler: Handler = vi.fn().mockResolvedValue(undefined);
    const pool = new WorkerPool(q, handler, { concurrency: 1, maxRetry: 3, blockMs: 10, idleReclaimMs: 30000 });
    await pool.start();
    await vi.waitFor(() => expect(q.acked).toContain('1-0'));
    await pool.stop();
    expect(handler).toHaveBeenCalledWith('device/a/msg', Buffer.from('{"ok":1}'));
  });

  it('처리 실패 + delivery 초과 시 DLQ로 보낸다', async () => {
    const q = fakeQueue([entry('2-0', 'poison', 5)]); // deliveries 5 > maxRetry 3
    const handler: Handler = vi.fn().mockRejectedValue(new Error('boom'));
    const pool = new WorkerPool(q, handler, { concurrency: 1, maxRetry: 3, blockMs: 10, idleReclaimMs: 30000 });
    await pool.start();
    await vi.waitFor(() => expect(q.dlq).toContain('2-0'));
    await pool.stop();
  });

  it('처리 실패 + delivery 미만이면 ack/DLQ 둘 다 안 한다(재시도 위임)', async () => {
    const q = fakeQueue([entry('3-0', 'x', 1)]); // deliveries 1 <= maxRetry 3
    const handler: Handler = vi.fn().mockRejectedValue(new Error('temp'));
    const pool = new WorkerPool(q, handler, { concurrency: 1, maxRetry: 3, blockMs: 10, idleReclaimMs: 30000 });
    await pool.start();
    await vi.waitFor(() => expect(handler).toHaveBeenCalled());
    await pool.stop();
    expect(q.acked).not.toContain('3-0');
    expect(q.dlq).not.toContain('3-0');
  });
});
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `npx vitest run src/buffer/WorkerPool.test.ts`
Expected: FAIL — "Cannot find module './WorkerPool.js'".

- [ ] **Step 3: 구현 작성**

```ts
import type { RedisStreamQueue, ClaimedEntry } from './RedisStreamQueue.js';
import { logger } from '../logger.js';

export type Handler = (topic: string, payload: Buffer) => Promise<void>;

export interface WorkerPoolOptions {
  concurrency: number;   // K
  maxRetry: number;      // delivery 초과 시 DLQ
  blockMs: number;       // XREADGROUP BLOCK
  idleReclaimMs: number; // XAUTOCLAIM idle 임계
}

/** K개 워커가 큐를 드레인. 각 워커는 claim→처리→ack 루프. */
export class WorkerPool {
  private running = false;
  private loops: Promise<void>[] = [];

  constructor(
    private readonly queue: RedisStreamQueue,
    private readonly handler: Handler,
    private readonly opts: WorkerPoolOptions,
  ) {}

  async start(): Promise<void> {
    this.running = true;
    for (let i = 0; i < this.opts.concurrency; i++) {
      this.loops.push(this.loop(`worker-${i}`));
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await Promise.all(this.loops);
  }

  private async loop(consumer: string): Promise<void> {
    while (this.running) {
      try {
        const reclaimed = await this.queue.reclaim(consumer, this.opts.idleReclaimMs, 10);
        for (const e of reclaimed) await this.process(e);
        const claimed = await this.queue.claim(consumer, 50, this.opts.blockMs);
        for (const e of claimed) await this.process(e);
      } catch (err) {
        logger.error({ msg: 'worker loop error', consumer, err: String(err) });
        await sleep(this.opts.blockMs);
      }
    }
  }

  private async process(e: ClaimedEntry): Promise<void> {
    try {
      await this.handler(e.msg.topic, Buffer.from(e.msg.payload));
      await this.queue.ack(e.id);
    } catch (err) {
      if (e.deliveries > this.opts.maxRetry) {
        logger.error({ msg: 'message moved to DLQ', id: e.id, deliveries: e.deliveries, err: String(err) });
        await this.queue.toDlq(e);
      } else {
        // ack 안 함 → 다음 reclaim(XAUTOCLAIM)에서 재처리
        logger.error({ msg: 'process failed (will retry)', id: e.id, deliveries: e.deliveries, err: String(err) });
      }
    }
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `npx vitest run src/buffer/WorkerPool.test.ts`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/buffer/WorkerPool.ts src/buffer/WorkerPool.test.ts
git commit -m "feat: WorkerPool (K workers, retry, DLQ)"
```

---

## Task 5: MqttSubscriber → enqueue (XADD 후 ack)

**Files:** Modify `src/ingest/MqttSubscriber.ts`, `src/ingest/MqttSubscriber.test.ts`

기존 `Handler`는 `(topic, payload) => Promise<void>` 인데, 이제 그 핸들러가 "Redis enqueue"가 된다. 시그니처는 그대로(처리 성공=ack 유지). 즉 MqttSubscriber 코드는 **변경 없음** — main에서 주입하는 핸들러만 enqueue로 바뀐다.

- [ ] **Step 1: 확인 — MqttSubscriber는 변경 불필요**

`MqttSubscriber`는 핸들러가 성공(resolve)하면 ack, 실패(reject)하면 ack 안 함. enqueue(XADD)를 핸들러로 주면 "XADD 성공 후 ack"가 그대로 성립한다. 코드 수정 없음.

- [ ] **Step 2: 기존 테스트가 여전히 통과하는지 확인**

Run: `npx vitest run src/ingest/MqttSubscriber.test.ts`
Expected: PASS (2 tests, 무변경).

> Task 5는 별도 커밋 없음(변경 없음). main 조립(Task 6)에서 enqueue 핸들러를 연결한다.

---

## Task 6: main 조립 (Redis 큐 + 워커풀)

**Files:** Modify `src/main.ts`, `src/db/pool.ts`

- [ ] **Step 1: pool.ts에 max 옵션 추가**

`src/db/pool.ts` 전체를 다음으로 교체:
```ts
import { Pool } from 'pg';

/** 연결 문자열로 pg Pool 생성. max로 동시 연결 수 제어(워커 동시성 대응). */
export function createPool(connectionString: string, max = 10): Pool {
  return new Pool({ connectionString, max });
}
```

- [ ] **Step 2: main.ts 수정 — Redis 큐 + 워커풀 조립**

import 추가(상단, logger 아래):
```ts
import { createRedis } from './buffer/redisPool.js';
import { RedisStreamQueue } from './buffer/RedisStreamQueue.js';
import { WorkerPool } from './buffer/WorkerPool.js';
```

`main()` 내부에서 pool 생성을 워커 수에 맞춰 수정:
```ts
  const pool = createPool(cfg.databaseUrl, cfg.workerConcurrency + 4);
```

`subscriber` 생성 직전에 Redis 큐 + 워커풀을 만들고, subscriber 핸들러를 enqueue로 바꾼다. 기존:
```ts
  const subscriber = new MqttSubscriber(
    { brokerUrl: cfg.mqttUrl, topic: cfg.mqttTopic, clientId: cfg.mqttClientId, qos: cfg.qos },
    (topic, payload) => processor.handle(topic, payload),
  );
  await subscriber.start();
```
를 다음으로 교체:
```ts
  // Redis 버퍼 + 워커풀
  const redis = createRedis(cfg.redisUrl);
  const queue = new RedisStreamQueue(redis, { stream: 'messages:stream', group: 'agent-workers', dlqStream: 'messages:dlq' });
  await queue.init();
  const workers = new WorkerPool(queue, (topic, payload) => processor.handle(topic, payload), {
    concurrency: cfg.workerConcurrency, maxRetry: 5, blockMs: 1000, idleReclaimMs: 30000,
  });
  await workers.start();

  // 수신부: 메시지를 Redis에 적재(성공 후 MQTT ack)
  const subscriber = new MqttSubscriber(
    { brokerUrl: cfg.mqttUrl, topic: cfg.mqttTopic, clientId: cfg.mqttClientId, qos: cfg.qos },
    (topic, payload) => queue.enqueue({ topic, payload: payload.toString('utf8'), receivedAt: systemClock.now().toISOString() }),
  );
  await subscriber.start();
```

`shutdown`에 워커·redis 정리 추가(기존 `await subscriber.stop();` 다음 줄):
```ts
    await workers.stop();
    await redis.quit();
```

`logger.info({ msg: 'agent started', ... })`에 동시성 표기 추가:
```ts
  logger.info({ msg: 'agent started', clientId: cfg.mqttClientId, workers: cfg.workerConcurrency });
```

- [ ] **Step 3: 빌드 확인**

Run: `npm run build`
Expected: 컴파일 오류 없음.

- [ ] **Step 4: 전체 테스트**

Run: `npm test`
Expected: 전체 PASS (Docker 필요 — Redis/PG testcontainers).

- [ ] **Step 5: Commit**

```bash
git add src/main.ts src/db/pool.ts
git commit -m "feat: wire Redis buffer + worker pool into main"
```

---

## Task 7: 부하 재검증 (무손실 입증)

**Files:** 없음(실행 검증). docker-compose의 Redis 포함 기동 필요.

- [ ] **Step 1: 인프라 기동(Redis 포함) + 빌드**

```bash
docker compose up -d          # mosquitto + postgres + redis
npm run build
```

- [ ] **Step 2: 단말 등록 + 에이전트 기동**

```bash
docker exec agent-postgres psql -U agent -d agent_db -c "INSERT INTO devices (imei) VALUES ('load-001') ON CONFLICT DO NOTHING;"
# .env에 REDIS_URL=redis://localhost:6379, WORKER_CONCURRENCY=4 확인 후
node dist/main.js
```

- [ ] **Step 3: 2000건 폭주 발행**

```bash
node scripts/load.mjs --count 2000 --imei load-001
```

- [ ] **Step 4: 전량 저장 확인 (드롭 0)**

폴링 후:
```bash
docker exec agent-postgres psql -U agent -d agent_db -c "SELECT count(*) AS raw, count(*) FILTER (WHERE error_yn='Y') AS err, count(*)-count(DISTINCT message_key) AS dup FROM messages_raw WHERE imei='load-001';"
```
Expected: `raw=2000, err=0, dup=0`. Mosquitto 로그에 `dropped` 없음.

- [ ] **Step 5: 정리**

```bash
docker exec agent-postgres psql -U agent -d agent_db -c "DELETE FROM domain_generic g USING messages_raw r WHERE g.message_id=r.message_id AND r.imei='load-001'; DELETE FROM messages_raw WHERE imei='load-001'; DELETE FROM devices WHERE imei='load-001';"
```

---

## Self-Review 결과

- **Spec 커버리지:** Redis 서비스/의존성(T1), redisPool(T2), 큐 XADD/claim/ack-xdel/reclaim/dlq(T3), 워커풀 K·재시도·DLQ(T4), 내구성 지점 이동=enqueue 후 ack(T5,T6), main 조립+pg풀 사이징(T6), 부하 무손실 재검증(T7) — 모두 매핑. `messageProcessor`/repo/스키마 무변경 재사용 확인.
- **타입 일관성:** `QueueMessage{topic,payload,receivedAt}`, `ClaimedEntry{id,msg,deliveries}`, `RedisStreamQueue`(init/enqueue/claim/reclaim/ack/toDlq), `WorkerPool`(start/stop), `Handler=(topic,Buffer)=>Promise<void>` 전 태스크 일치. `createPool(url, max)` T6에서 일관.
- **주의:** AOF everysec은 docker-compose Redis 옵션(T1). 워커 throw는 인프라 오류뿐(논리 오류는 messageProcessor 내부 흡수)이라 재시도/DLQ가 적정. 순서 비보장(메시지 독립).

---

## 다음 단계

구현 완료 후, 필요 시 관측성(스트림 길이/PEL/DLQ 메트릭)·DB 장시간 중지 시나리오(워커 재시도 누적) 추가 검증.
