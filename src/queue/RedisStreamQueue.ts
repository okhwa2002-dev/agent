import type Redis from 'ioredis';
import type { MessageQueue, ClaimedRecord } from './MessageQueue.js';
import type { ServerRecord } from '../types.js';

export interface RedisStreamQueueOptions {
  stream: string;
  group: string;
  consumer: string;
}

const FIELD = 'data';

/** Redis Streams + Consumer Group 기반 at-least-once 큐. */
export class RedisStreamQueue implements MessageQueue {
  constructor(
    private readonly redis: Redis,
    private readonly opts: RedisStreamQueueOptions,
  ) {}

  /** Consumer Group 생성 (이미 있으면 무시). MKSTREAM으로 스트림도 함께 생성. */
  async init(): Promise<void> {
    try {
      await this.redis.xgroup('CREATE', this.opts.stream, this.opts.group, '$', 'MKSTREAM');
    } catch (err) {
      if (!(err instanceof Error) || !err.message.includes('BUSYGROUP')) throw err;
    }
  }

  async enqueue(record: ServerRecord): Promise<void> {
    await this.redis.xadd(this.opts.stream, '*', FIELD, JSON.stringify(record));
  }

  async claimBatch(count: number): Promise<ClaimedRecord[]> {
    const res = await this.redis.xreadgroup(
      'GROUP', this.opts.group, this.opts.consumer,
      'COUNT', count, 'STREAMS', this.opts.stream, '>',
    );
    return this.parseStreamRead(res);
  }

  async ack(entryIds: string[]): Promise<void> {
    if (entryIds.length === 0) return;
    await this.redis.xack(this.opts.stream, this.opts.group, ...entryIds);
  }

  async reclaimStale(idleMs: number, count: number): Promise<ClaimedRecord[]> {
    // XAUTOCLAIM: 다른 consumer의 idle PEL 엔트리를 이 consumer로 회수
    const res = (await this.redis.xautoclaim(
      this.opts.stream, this.opts.group, this.opts.consumer,
      idleMs, '0', 'COUNT', count,
    )) as [string, [string, string[]][], string[]];
    const entries = res[1] ?? [];
    return this.parseEntries(entries);
  }

  // ---- 내부 파서 ----

  private parseStreamRead(res: unknown): ClaimedRecord[] {
    if (!Array.isArray(res) || res.length === 0) return [];
    const first = res[0] as [string, [string, string[]][]];
    return this.parseEntries(first[1] ?? []);
  }

  private parseEntries(entries: [string, string[]][]): ClaimedRecord[] {
    const out: ClaimedRecord[] = [];
    for (const [entryId, fields] of entries) {
      if (!fields) continue; // XAUTOCLAIM이 삭제된 엔트리에 null을 줄 수 있음
      const idx = fields.indexOf(FIELD);
      if (idx === -1) continue;
      out.push({ entryId, record: JSON.parse(fields[idx + 1]) as ServerRecord });
    }
    return out;
  }
}
