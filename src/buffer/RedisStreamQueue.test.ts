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

  it('reclaim은 실제 delivery 횟수를 보고한다(재시도마다 증가 → maxRetry 초과 판정 가능)', async () => {
    const q = newQueue('s5'); await q.init();
    await q.enqueue({ topic: 't', payload: 'poison', receivedAt: 'r' });
    await q.claim('worker-0', 10, 50); // 1차 전달(ack 안 함)
    await q.reclaim('worker-1', 0, 10); // 2차 전달
    await q.reclaim('worker-1', 0, 10); // 3차 전달
    const [e] = await q.reclaim('worker-1', 0, 10); // 4차 전달
    expect(e.deliveries).toBe(4);
  });

  it('그룹 생성 전에 적재된 엔트리도 claim된다(그룹 재생성 시 잔량 유실 방지)', async () => {
    const q = newQueue('s6');
    await q.enqueue({ topic: 't', payload: 'before-group', receivedAt: 'r' }); // 그룹 없이 XADD
    await q.init(); // 그룹을 나중에 생성해도 기존 잔량이 보여야 함
    const claimed = await q.claim('worker-0', 10, 50);
    expect(claimed).toHaveLength(1);
    expect(claimed[0].msg.payload).toBe('before-group');
  });

  it('워커의 블로킹 claim(BLOCK)이 enqueue를 지연시키지 않는다(전용 연결)', async () => {
    const q = newQueue('s10'); await q.init();
    const wq = q.forWorker(); // 워커 전용 연결
    const claimP = wq.claim('w0', 10, 1500); // 빈 스트림에서 BLOCK 1500ms
    await new Promise((r) => setTimeout(r, 100)); // claim이 실제 블록 상태에 들어가도록
    const t0 = performance.now();
    await q.enqueue({ topic: 't', payload: 'fast', receivedAt: 'r' });
    expect(performance.now() - t0).toBeLessThan(500); // 연결 공유 시 ~1400ms 대기
    const claimed = await claimP; // 블록 중이던 claim이 새 엔트리를 즉시 수신
    expect(claimed).toHaveLength(1);
    await wq.close();
  });

  it('toDlq는 원본을 DLQ로 옮기고 ack한다', async () => {
    const q = newQueue('s4'); await q.init();
    await q.enqueue({ topic: 't', payload: 'poison', receivedAt: 'r' });
    const [e] = await q.claim('worker-0', 10, 50);
    await q.toDlq(e);
    expect(await redis.xlen('s4:dlq')).toBe(1);
    expect(await redis.xlen('s4')).toBe(0);
  });

  it('listDlq는 DLQ 적재분을 원본 필드 그대로 반환한다', async () => {
    const q = newQueue('s7'); await q.init();
    await q.enqueue({ topic: 'device/a/msg', payload: 'p1', receivedAt: 'r1' });
    const [e] = await q.claim('worker-0', 10, 50);
    await q.toDlq(e);
    const list = await q.listDlq(10);
    expect(list).toHaveLength(1);
    expect(list[0].msg).toEqual({ topic: 'device/a/msg', payload: 'p1', receivedAt: 'r1' });
  });

  it('requeueDlq는 전부 원본 스트림으로 재투입하고 DLQ를 비운다(재claim 가능)', async () => {
    const q = newQueue('s8'); await q.init();
    for (const p of ['p1', 'p2']) {
      await q.enqueue({ topic: 't', payload: p, receivedAt: 'r' });
    }
    const claimed = await q.claim('worker-0', 10, 50);
    for (const e of claimed) await q.toDlq(e);
    expect(await redis.xlen('s8:dlq')).toBe(2);

    const moved = await q.requeueDlq();
    expect(moved).toBe(2);
    expect(await redis.xlen('s8:dlq')).toBe(0);
    const again = await q.claim('worker-0', 10, 50);
    expect(again.map((e) => e.msg.payload).sort()).toEqual(['p1', 'p2']);
  });

  it('requeueDlq에 id를 주면 해당 건만 재투입한다', async () => {
    const q = newQueue('s9'); await q.init();
    for (const p of ['p1', 'p2']) {
      await q.enqueue({ topic: 't', payload: p, receivedAt: 'r' });
    }
    const claimed = await q.claim('worker-0', 10, 50);
    for (const e of claimed) await q.toDlq(e);
    const [first] = await q.listDlq(10);

    const moved = await q.requeueDlq(first.id);
    expect(moved).toBe(1);
    expect(await redis.xlen('s9:dlq')).toBe(1); // 나머지 1건은 그대로
    const again = await q.claim('worker-0', 10, 50);
    expect(again).toHaveLength(1);
    expect(again[0].msg.payload).toBe(first.msg.payload);
  });
});
