import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Redis } from 'ioredis';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { RawRepo } from '../repo/rawRepo.js';
import { ErrorRepo } from '../repo/errorRepo.js';
import { RedisStreamQueue } from '../buffer/RedisStreamQueue.js';
import { StatsCollector } from './statsCollector.js';
import { AgentCounters } from './counters.js';
import { extractHeader } from '../header.js';

let redisContainer: StartedRedisContainer;
let pgContainer: StartedPostgreSqlContainer;
let redis: Redis;
let pool: Pool;
let collector: StatsCollector;

const OPTS = { stream: 'm:stream', group: 'g', dlqStream: 'm:dlq' };

beforeAll(async () => {
  [redisContainer, pgContainer] = await Promise.all([
    new RedisContainer('redis:7-alpine').start(),
    new PostgreSqlContainer('postgres:16-alpine').start(),
  ]);
  redis = new Redis(redisContainer.getConnectionUrl(), { maxRetriesPerRequest: null });
  pool = createPool(pgContainer.getConnectionUri());
  await applySchema(pool);

  const rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  collector = new StatsCollector(
    { redis, pool, rawRepo, errorRepo, isMqttConnected: () => true },
    OPTS,
  );

  // Redis: 3건 적재 → 3건 claim → 1건 ack, 1건 DLQ → 잔량 1·미ack 1·DLQ 1
  const q = new RedisStreamQueue(redis, OPTS);
  await q.init();
  for (const p of ['a', 'b', 'c']) await q.enqueue({ topic: 't', payload: p, receivedAt: 'r' });
  const claimed = await q.claim('w0', 10, 50);
  await q.ack(claimed[0].id);
  await q.toDlq(claimed[1]);

  // PG: error_yn='Y' 1건 + error_log(ingest 2건, projection 1건)
  const payload = { imei: 'x', messageCode: 'Fault' };
  await rawRepo.insert({
    messageKey: 'k1', deviceId: null, header: extractHeader(payload), rawPayload: payload,
    errorYn: 'Y', errorDetail: 'unregistered', receivedAt: '2026-07-07T10:00:00.000Z',
  });
  await errorRepo.log({ stage: 'ingest', detail: 'e1' });
  await errorRepo.log({ stage: 'ingest', detail: 'e2' });
  await errorRepo.log({ stage: 'projection', detail: 'e3' });
});

afterAll(async () => {
  await pool.end();
  await redis.quit();
  await Promise.all([redisContainer.stop(), pgContainer.stop()]);
});

describe('StatsCollector', () => {
  it('Redis 스트림 잔량·미ack·DLQ 건수를 수집한다', async () => {
    const s = await collector.collect();
    expect(s.streamBacklog).toBe(1); // 3 - ack 1(XDEL) - dlq 1(XDEL)
    expect(s.streamPending).toBe(1); // claim 3 - ack 1 - dlq 1
    expect(s.dlqDepth).toBe(1);
  });

  it('PG 에러 건수(error_yn=Y, error_log 단계별)를 수집한다', async () => {
    const s = await collector.collect();
    expect(s.rawErrorRows).toBe(1);
    expect(s.errorLogByStage).toEqual({ ingest: 2, projection: 1 });
  });

  it('주입된 counters(처리량·지연)를 collect 결과에 포함한다', async () => {
    const counters = new AgentCounters();
    counters.processedTotal = 7;
    counters.processFailedTotal = 2;
    counters.dlqMovedTotal = 1;
    counters.e2eLatencySumMs = 1400;
    counters.e2eLatencyMaxMs = 500;
    const withCounters = new StatsCollector(
      { redis, pool, rawRepo: new RawRepo(pool), errorRepo: new ErrorRepo(pool), isMqttConnected: () => true, counters },
      OPTS,
    );
    const s = await withCounters.collect();
    expect(s.processedTotal).toBe(7);
    expect(s.processFailedTotal).toBe(2);
    expect(s.dlqMovedTotal).toBe(1);
    expect(s.e2eLatencySumMs).toBe(1400);
    expect(s.e2eLatencyMaxMs).toBe(500);
  });

  it('health: redis/pg/mqtt 모두 정상이면 ok=true', async () => {
    const h = await collector.health();
    expect(h).toEqual({ ok: true, mqtt: true, redis: true, pg: true });
  });

  it('health: mqtt 끊김이면 ok=false, 나머지 상태는 유지', async () => {
    const rawRepo = new RawRepo(pool);
    const errorRepo = new ErrorRepo(pool);
    const down = new StatsCollector(
      { redis, pool, rawRepo, errorRepo, isMqttConnected: () => false },
      OPTS,
    );
    const h = await down.health();
    expect(h.ok).toBe(false);
    expect(h.mqtt).toBe(false);
    expect(h.redis).toBe(true);
    expect(h.pg).toBe(true);
  });
});
