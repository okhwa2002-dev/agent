import { describe, it, expect, vi } from 'vitest';
import { Dispatcher } from './Dispatcher.js';
import type { MessageQueue, ClaimedRecord } from '../queue/MessageQueue.js';
import type { ServerRecord } from '../types.js';

const rec = (id: string): ServerRecord => ({
  messageId: id, agentId: 'edge-01', deviceId: 'dev-1',
  receivedAt: '2026-06-09T09:03:00.000Z', rawPayload: {},
});

function fakeQueue(initial: ClaimedRecord[]): MessageQueue & { acked: string[] } {
  let pending = [...initial];
  const acked: string[] = [];
  return {
    acked,
    enqueue: vi.fn(),
    claimBatch: async () => { const b = pending; pending = []; return b; },
    ack: async (ids) => { acked.push(...ids); },
    reclaimStale: async () => [],
  };
}

describe('Dispatcher', () => {
  it('claim한 배치를 전송하고 성공 시 ack한다', async () => {
    const queue = fakeQueue([{ entryId: '1-0', record: rec('m1') }]);
    const client = { sendBatch: vi.fn().mockResolvedValue(undefined) };
    const d = new Dispatcher(queue, client as any, { batchSize: 10, idleReclaimMs: 30_000 });
    await d.tick();
    expect(client.sendBatch).toHaveBeenCalledOnce();
    expect(queue.acked).toEqual(['1-0']);
  });

  it('전송 실패 시 ack하지 않는다 (큐에 잔류)', async () => {
    const queue = fakeQueue([{ entryId: '1-0', record: rec('m1') }]);
    const client = { sendBatch: vi.fn().mockRejectedValue(new Error('down')) };
    const d = new Dispatcher(queue, client as any, { batchSize: 10, idleReclaimMs: 30_000 });
    await expect(d.tick()).rejects.toThrow('down');
    expect(queue.acked).toEqual([]);
  });
});
