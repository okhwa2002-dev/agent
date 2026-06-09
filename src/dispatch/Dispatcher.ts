import type { MessageQueue } from '../queue/MessageQueue.js';
import type { ServerClient } from './ServerClient.js';
import type { ServerRecord } from '../types.js';

export interface DispatcherOptions {
  batchSize: number;
  idleReclaimMs: number;
}

/** 큐에서 배치를 claim → 서버 전송 → 성공 시 ack. 실패는 throw(큐에 잔류). */
export class Dispatcher {
  constructor(
    private readonly queue: MessageQueue,
    private readonly client: ServerClient,
    private readonly opts: DispatcherOptions,
  ) {}

  /** 한 번의 처리 사이클. 신규 + 회수(reclaim) 배치를 처리한다. */
  async tick(): Promise<void> {
    const reclaimed = await this.queue.reclaimStale(this.opts.idleReclaimMs, this.opts.batchSize);
    if (reclaimed.length > 0) await this.flush(reclaimed.map((c) => c.entryId), reclaimed.map((c) => c.record));

    const claimed = await this.queue.claimBatch(this.opts.batchSize);
    if (claimed.length > 0) await this.flush(claimed.map((c) => c.entryId), claimed.map((c) => c.record));
  }

  private async flush(entryIds: string[], records: ServerRecord[]): Promise<void> {
    // 멱등키 = 정렬된 messageId 집합의 결정적 표현 (동일 배치 재전송 식별)
    const idempotencyKey = records.map((r) => r.messageId).sort().join(',').slice(0, 200);
    await this.client.sendBatch(records, idempotencyKey); // 실패 시 throw → ack 안 함
    await this.queue.ack(entryIds);
  }
}
