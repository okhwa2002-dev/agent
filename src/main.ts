import 'dotenv/config'; // 프로젝트 루트 .env 로드 (loadConfig·logger보다 먼저)
import { logger } from './logger.js';
import { loadConfig } from './config/config.js';
import { createPool } from './db/pool.js';
import { applySchema } from './db/applySchema.js';
import { DeviceRepo } from './repo/deviceRepo.js';
import { RawRepo } from './repo/rawRepo.js';
import { DomainRepo } from './repo/domainRepo.js';
import { ErrorRepo } from './repo/errorRepo.js';
import { LocationRepo } from './repo/locationRepo.js';
import { GenericRepo } from './repo/genericRepo.js';
import { defaultRegistry } from './parsers/registry.js';
import { ProjectionService } from './service/projectionService.js';
import { LocationProjector } from './service/locationProjector.js';
import { MessageProcessor } from './service/messageProcessor.js';
import { MqttSubscriber } from './ingest/MqttSubscriber.js';
import { createRedis } from './buffer/redisPool.js';
import { RedisStreamQueue } from './buffer/RedisStreamQueue.js';
import { WorkerPool } from './buffer/WorkerPool.js';
import { systemClock } from './types.js';

async function main(): Promise<void> {
  const cfg = loadConfig(process.env);
  const pool = createPool(cfg.databaseUrl, cfg.workerConcurrency + 4);
  await applySchema(pool); // IF NOT EXISTS — 멱등

  const deviceRepo = new DeviceRepo(pool);
  const rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo, new GenericRepo(pool));
  const location = new LocationProjector(new LocationRepo(pool), errorRepo);
  const processor = new MessageProcessor(deviceRepo, rawRepo, projection, location, errorRepo, systemClock);

  // Redis 버퍼 + 워커풀
  const redis = createRedis(cfg.redisUrl);
  const queue = new RedisStreamQueue(redis, { stream: 'messages:stream', group: 'agent-workers', dlqStream: 'messages:dlq' });
  await queue.init();
  const workers = new WorkerPool(queue, (topic, payload) => processor.handle(topic, payload), {
    concurrency: cfg.workerConcurrency, maxRetry: 5, blockMs: 1000, idleReclaimMs: 30000,
  });
  await workers.start();

  // 수신부: 메시지를 Redis에 적재(성공 후 MQTT ack)
  const subscriber = new MqttSubscriber(
    { brokerUrl: cfg.mqttUrl, topic: cfg.mqttTopic, clientId: cfg.mqttClientId, qos: cfg.qos },
    (topic, payload) => queue.enqueue({ topic, payload: payload.toString('utf8'), receivedAt: systemClock.now().toISOString() }),
  );
  await subscriber.start();
  logger.info({ msg: 'agent started', clientId: cfg.mqttClientId, workers: cfg.workerConcurrency });

  const shutdown = async () => {
    await subscriber.stop();
    await workers.stop();
    await redis.quit();
    await pool.end();
    logger.info({ msg: 'agent stopped' });
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

main().catch((err) => {
  logger.fatal({ err: String(err) });
  process.exit(1);
});
