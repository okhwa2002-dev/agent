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
