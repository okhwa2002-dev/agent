import type { Redis } from 'ioredis';
import type { Pool } from 'pg';
import type { RawRepo } from '../repo/rawRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { AgentStats, HealthStatus } from './stats.js';

export interface StatsCollectorDeps {
  redis: Redis;
  pool: Pool;
  rawRepo: RawRepo;
  errorRepo: ErrorRepo;
  isMqttConnected: () => boolean;
}

export interface StatsCollectorOptions {
  stream: string;
  group: string;
  dlqStream: string;
}

/** Redis(스트림 적체·DLQ) + PG(에러 건수) 지표 수집 및 헬스체크. */
export class StatsCollector {
  constructor(
    private readonly deps: StatsCollectorDeps,
    private readonly opts: StatsCollectorOptions,
  ) {}

  async collect(): Promise<AgentStats> {
    const { redis, rawRepo, errorRepo } = this.deps;
    const [streamBacklog, dlqDepth, pending, rawErrorRows, errorLogByStage] = await Promise.all([
      redis.xlen(this.opts.stream),
      redis.xlen(this.opts.dlqStream),
      redis.xpending(this.opts.stream, this.opts.group) as Promise<[number, ...unknown[]]>,
      rawRepo.countErrors(),
      errorRepo.countByStage(),
    ]);
    return {
      streamBacklog,
      streamPending: Number(pending?.[0] ?? 0),
      dlqDepth,
      rawErrorRows,
      errorLogByStage,
    };
  }

  async health(): Promise<HealthStatus> {
    const mqtt = this.deps.isMqttConnected();
    const [redisOk, pgOk] = await Promise.all([
      this.deps.redis.ping().then(() => true).catch(() => false),
      this.deps.pool.query('SELECT 1').then(() => true).catch(() => false), // 연결 프로브(업무 쿼리 아님)
    ]);
    return { ok: mqtt && redisOk && pgOk, mqtt, redis: redisOk, pg: pgOk };
  }
}
