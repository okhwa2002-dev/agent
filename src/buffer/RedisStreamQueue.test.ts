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
