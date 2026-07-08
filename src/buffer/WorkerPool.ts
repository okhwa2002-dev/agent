import type { RedisStreamQueue, ClaimedEntry } from './RedisStreamQueue.js';
import { logger } from '../logger.js';
import { AgentCounters } from '../metrics/counters.js';
import { systemClock, type Clock } from '../types.js';

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
    private readonly counters: AgentCounters = new AgentCounters(), // 처리량·지연 지표 (미주입 시 자체 보관)
    private readonly clock: Clock = systemClock,
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
    // 워커 전용 연결: 블로킹 claim(XREADGROUP BLOCK)이 enqueue·타 워커 명령을 막지 않게 한다.
    const queue = this.queue.forWorker();
    try {
      while (this.running) {
        try {
          const reclaimed = await queue.reclaim(consumer, this.opts.idleReclaimMs, 10);
          for (const e of reclaimed) await this.process(queue, e);
          const claimed = await queue.claim(consumer, 50, this.opts.blockMs);
          for (const e of claimed) await this.process(queue, e);
        } catch (err) {
          logger.error({ msg: 'worker loop error', consumer, err: String(err) });
          await sleep(this.opts.blockMs);
        }
      }
    } finally {
      await queue.close();
    }
  }

  private async process(queue: RedisStreamQueue, e: ClaimedEntry): Promise<void> {
    try {
      await this.handler(e.msg.topic, Buffer.from(e.msg.payload));
      await queue.ack(e.id);
      this.counters.recordProcessed(this.clock.now().getTime() - Date.parse(e.msg.receivedAt));
    } catch (err) {
      if (e.deliveries > this.opts.maxRetry) {
        logger.error({ msg: 'message moved to DLQ', id: e.id, deliveries: e.deliveries, err: String(err) });
        await queue.toDlq(e);
        this.counters.recordDlqMoved();
      } else {
        // ack 안 함 → 다음 reclaim(XAUTOCLAIM)에서 재처리
        logger.error({ msg: 'process failed (will retry)', id: e.id, deliveries: e.deliveries, err: String(err) });
        this.counters.recordFailed();
      }
    }
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
