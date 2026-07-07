import 'dotenv/config'; // 프로젝트 루트 .env 로드
import { logger } from './logger.js';
import { createRedis } from './buffer/redisPool.js';
import { RedisStreamQueue } from './buffer/RedisStreamQueue.js';

/**
 * DLQ 운영 CLI (에이전트와 별개 실행, REDIS_URL만 필요).
 * 사용:
 *   npm run dlq -- list [n]        # 적재분 조회(오래된 순, 기본 20건)
 *   npm run dlq -- requeue [id]    # 원인 수정 후 스트림 재투입. id 생략 시 전부
 */
async function main(): Promise<void> {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) throw new Error('REDIS_URL is required');
  const [cmd = 'list', arg] = process.argv.slice(2);

  const redis = createRedis(redisUrl);
  try {
    const queue = new RedisStreamQueue(redis, { stream: 'messages:stream', group: 'agent-workers', dlqStream: 'messages:dlq' });
    if (cmd === 'list') {
      const entries = await queue.listDlq(Number(arg ?? '20'));
      for (const e of entries) console.log(JSON.stringify(e));
      logger.info({ msg: 'dlq list', shown: entries.length });
    } else if (cmd === 'requeue') {
      const moved = await queue.requeueDlq(arg);
      logger.info({ msg: 'dlq requeue done', moved });
    } else {
      throw new Error(`unknown command: ${cmd} (use: list [n] | requeue [id])`);
    }
  } finally {
    await redis.quit();
  }
}

main().catch((err) => {
  logger.fatal({ msg: 'dlq command failed', err: String(err) });
  process.exit(1);
});
