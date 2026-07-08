import { describe, it, expect, vi } from 'vitest';
import { WorkerPool, type Handler } from './WorkerPool.js';
import type { ClaimedEntry, RedisStreamQueue } from './RedisStreamQueue.js';
import { AgentCounters } from '../metrics/counters.js';

function entry(id: string, payload: string, deliveries = 1): ClaimedEntry {
  return { id, msg: { topic: 'device/a/msg', payload, receivedAt: 'r' }, deliveries };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** claim을 한 번만 주고 이후 빈 배열을 주는 fake 큐. (BLOCK 흉내로 지연 → 핫루프 방지) */
function fakeQueue(first: ClaimedEntry[]): RedisStreamQueue & { acked: string[]; dlq: string[] } {
  let served = false;
  const acked: string[] = []; const dlq: string[] = [];
  return {
    acked, dlq,
    init: vi.fn().mockResolvedValue(undefined),
    enqueue: vi.fn(),
    claim: vi.fn(async () => { if (served) { await sleep(15); return []; } served = true; return first; }),
    reclaim: vi.fn(async () => { await sleep(5); return []; }),
    ack: vi.fn(async (id: string) => { acked.push(id); }),
    toDlq: vi.fn(async (e: ClaimedEntry) => { dlq.push(e.id); acked.push(e.id); }),
    forWorker(this: unknown) { return this; }, // 단위 테스트: 전용 연결 없이 자신을 반환
    close: vi.fn().mockResolvedValue(undefined),
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

  it('counters에 처리 성공·지연(receivedAt→완료)·실패·DLQ 이동을 기록한다', async () => {
    const ok: ClaimedEntry = { id: 'c-1', msg: { topic: 't', payload: 'ok', receivedAt: '2026-07-07T00:00:00.000Z' }, deliveries: 1 };
    const fail: ClaimedEntry = { id: 'c-2', msg: { topic: 't', payload: 'fail', receivedAt: '2026-07-07T00:00:00.000Z' }, deliveries: 1 };
    const poison: ClaimedEntry = { id: 'c-3', msg: { topic: 't', payload: 'poison', receivedAt: '2026-07-07T00:00:00.000Z' }, deliveries: 9 };
    const q = fakeQueue([ok, fail, poison]);
    const counters = new AgentCounters();
    const clock = { now: () => new Date('2026-07-07T00:00:00.250Z') }; // 지연 250ms
    const handler: Handler = vi.fn(async (_t, payload) => {
      if (payload.toString() !== 'ok') throw new Error('boom');
    });
    const pool = new WorkerPool(q, handler, { concurrency: 1, maxRetry: 3, blockMs: 10, idleReclaimMs: 30000 }, counters, clock);
    await pool.start();
    await vi.waitFor(() => expect(q.dlq).toContain('c-3'));
    await pool.stop();
    expect(counters.processedTotal).toBe(1);
    expect(counters.e2eLatencySumMs).toBe(250);
    expect(counters.e2eLatencyMaxMs).toBe(250);
    expect(counters.processFailedTotal).toBe(1);  // c-2 (재시도 위임)
    expect(counters.dlqMovedTotal).toBe(1);       // c-3
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
