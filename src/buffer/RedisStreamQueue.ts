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

  /** 그룹 생성(이미 있으면 무시). MKSTREAM으로 스트림도 생성.
   *  시작점 '0': 그룹 생성 전 적재된 잔량(그룹 삭제 후 재생성 등)도 놓치지 않는다. */
  async init(): Promise<void> {
    try {
      await this.redis.xgroup('CREATE', this.opts.stream, this.opts.group, '0', 'MKSTREAM');
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

  /** idle 초과한 미ACK 엔트리를 이 consumer로 회수. deliveries는 XPENDING의 실제 전달 횟수. */
  async reclaim(consumer: string, idleMs: number, count: number): Promise<ClaimedEntry[]> {
    const res = (await this.redis.xautoclaim(
      this.opts.stream, this.opts.group, consumer, idleMs, '0', 'COUNT', count,
    )) as [string, [string, string[] | null][], string[]];
    const entries = (res[1] ?? []).filter((e): e is [string, string[]] => e[1] != null); // null=이미 삭제된 엔트리
    if (entries.length === 0) return [];
    const counts = await this.deliveryCounts(entries.map(([id]) => id));
    return entries.map(([id, fields]) => ({ id, msg: parseFields(fields), deliveries: counts.get(id) ?? 2 }));
  }

  /** XPENDING으로 엔트리별 실제 delivery 횟수 조회(XAUTOCLAIM 증가분 반영). */
  private async deliveryCounts(ids: string[]): Promise<Map<string, number>> {
    const pipe = this.redis.pipeline();
    for (const id of ids) pipe.xpending(this.opts.stream, this.opts.group, id, id, 1);
    const results = (await pipe.exec()) ?? [];
    const counts = new Map<string, number>();
    for (const [err, rows] of results) {
      if (err || !Array.isArray(rows)) continue;
      for (const row of rows as [string, string, number, number][]) counts.set(row[0], row[3]);
    }
    return counts;
  }

  /** 처리 완료: XACK + XDEL을 MULTI로 원자 실행(중간 크래시 시 acked 엔트리 잔류 방지). */
  async ack(id: string): Promise<void> {
    await this.redis.multi()
      .xack(this.opts.stream, this.opts.group, id)
      .xdel(this.opts.stream, id)
      .exec();
  }

  /** 워커 전용 큐 생성: 블로킹 XREADGROUP이 다른 명령(enqueue/ack)을 막지 않도록 전용 연결 사용. */
  forWorker(): RedisStreamQueue {
    const q = new RedisStreamQueue(this.redis.duplicate(), this.opts);
    q.owned = true;
    return q;
  }

  /** forWorker로 만든 전용 연결 해제(원본 큐의 공유 연결은 건드리지 않음). */
  async close(): Promise<void> {
    if (this.owned) await this.redis.quit();
  }

  private owned = false; // duplicate()로 만든 전용 연결 소유 여부

  /** DLQ 적재분 조회(오래된 순). */
  async listDlq(count: number): Promise<{ id: string; msg: QueueMessage }[]> {
    const res = (await this.redis.xrange(this.opts.dlqStream, '-', '+', 'COUNT', count)) as [string, string[]][];
    return res.map(([id, fields]) => ({ id, msg: parseFields(fields) }));
  }

  /** DLQ → 원본 스트림 재투입(원인 수정 후). id 지정 시 해당 건만, 생략 시 전부. 이동 건수 반환. */
  async requeueDlq(id?: string): Promise<number> {
    let moved = 0;
    for (;;) {
      const entries = id
        ? ((await this.redis.xrange(this.opts.dlqStream, id, id)) as [string, string[]][])
        : ((await this.redis.xrange(this.opts.dlqStream, '-', '+', 'COUNT', 100)) as [string, string[]][]);
      if (entries.length === 0) break;
      for (const [entryId, fields] of entries) {
        const m = parseFields(fields);
        await this.redis.multi() // 재투입+삭제 원자 실행(중복 재투입 방지)
          .xadd(this.opts.stream, '*', 'topic', m.topic, 'payload', m.payload, 'receivedAt', m.receivedAt)
          .xdel(this.opts.dlqStream, entryId)
          .exec();
        moved++;
      }
      if (id) break; // 단건 모드는 1회로 종료
    }
    return moved;
  }

  /** poison: DLQ 적재 + 원본 XACK/XDEL을 MULTI로 원자 실행(DLQ 중복 적재 방지). */
  async toDlq(e: ClaimedEntry): Promise<void> {
    await this.redis.multi()
      .xadd(this.opts.dlqStream, '*', 'topic', e.msg.topic, 'payload', e.msg.payload, 'receivedAt', e.msg.receivedAt)
      .xack(this.opts.stream, this.opts.group, e.id)
      .xdel(this.opts.stream, e.id)
      .exec();
  }
}

function parseFields(fields: string[]): QueueMessage {
  const m: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) m[fields[i]] = fields[i + 1];
  return { topic: m.topic, payload: m.payload, receivedAt: m.receivedAt };
}
