import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import Redis from 'ioredis';
import { RedisStreamQueue } from './RedisStreamQueue.js';
import type { ServerRecord } from '../types.js';

let container: StartedRedisContainer;
let redis: Redis;

const rec = (id: string): ServerRecord => ({
  messageId: id, agentId: 'edge-01', deviceId: 'dev-1',
  receivedAt: '2026-06-09T09:03:00.000Z', rawPayload: { messageCode: 'Fault' },
});

beforeAll(async () => {
  container = await new RedisContainer('redis:7-alpine').start();
  redis = new Redis(container.getConnectionUrl());
});

afterAll(async () => {
  await redis.quit();
  await container.stop();
});

describe('RedisStreamQueue', () => {
  it('enqueue한 레코드를 claim해서 원본 그대로 돌려준다', async () => {
    const q = new RedisStreamQueue(redis, { stream: 's1', group: 'g1', consumer: 'c1' });
    await q.init();
    await q.enqueue(rec('m1'));
    const claimed = await q.claimBatch(10);
    expect(claimed).toHaveLength(1);
    expect(claimed[0].record).toEqual(rec('m1'));
  });

  it('ack하면 다시 claim되지 않는다', async () => {
    const q = new RedisStreamQueue(redis, { stream: 's2', group: 'g2', consumer: 'c2' });
    await q.init();
    await q.enqueue(rec('m2'));
    const claimed = await q.claimBatch(10);
    await q.ack(claimed.map((c) => c.entryId));
    const again = await q.claimBatch(10);
    expect(again).toHaveLength(0);
  });

  it('ack 안 한 엔트리는 reclaimStale로 회수된다 (크래시 복구)', async () => {
    const q = new RedisStreamQueue(redis, { stream: 's3', group: 'g3', consumer: 'c3' });
    await q.init();
    await q.enqueue(rec('m3'));
    await q.claimBatch(10);             // claim했지만 ack 안 함 (크래시 가정)
    const reclaimed = await q.reclaimStale(0, 10);
    expect(reclaimed.map((c) => c.record.messageId)).toContain('m3');
  });
});
