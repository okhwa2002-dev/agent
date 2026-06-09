import { Redis } from 'ioredis';
import { loadConfig } from './config/config.js';
import { RedisStreamQueue } from './queue/RedisStreamQueue.js';
import { Pipeline } from './pipeline/Pipeline.js';
import { ServerClient } from './dispatch/ServerClient.js';
import { Dispatcher } from './dispatch/Dispatcher.js';
import { MqttSubscriber } from './ingest/MqttSubscriber.js';
import { systemClock } from './types.js';

async function main(): Promise<void> {
  const cfg = loadConfig(process.env);
  const redis = new Redis(cfg.redisUrl, { maxRetriesPerRequest: null });

  const queue = new RedisStreamQueue(redis, { stream: cfg.stream, group: cfg.group, consumer: cfg.consumer });
  await queue.init();

  const pipeline = new Pipeline(cfg.agentId);
  const client = new ServerClient({
    baseUrl: cfg.serverUrl, agentId: cfg.agentId,
    maxRetries: cfg.maxRetries, baseDelayMs: cfg.baseDelayMs,
  });
  const dispatcher = new Dispatcher(queue, client, {
    batchSize: cfg.batchSize, idleReclaimMs: cfg.idleReclaimMs,
  });

  // 수신: pipeline으로 ServerRecord 만들어 enqueue. enqueue 성공 후에만 QoS1 ack됨.
  const subscriber = new MqttSubscriber(
    { brokerUrl: cfg.mqttUrl, topic: cfg.mqttTopic, qos: 1 },
    async (raw) => { await queue.enqueue(pipeline.process(raw)); },
    systemClock,
  );
  await subscriber.start();

  // 디스패치 루프
  let running = true;
  const loop = (async () => {
    while (running) {
      try { await dispatcher.tick(); }
      catch (err) { console.error(JSON.stringify({ level: 'error', msg: 'tick failed', err: String(err) })); }
      await new Promise((r) => setTimeout(r, cfg.tickIntervalMs));
    }
  })();

  console.log(JSON.stringify({ level: 'info', msg: 'agent started', agentId: cfg.agentId }));

  const shutdown = async () => {
    running = false;
    await subscriber.stop();   // 신규 수신 중단
    await loop;                // 진행 중 tick 완료
    await redis.quit();
    console.log(JSON.stringify({ level: 'info', msg: 'agent stopped' }));
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

main().catch((err) => {
  console.error(JSON.stringify({ level: 'fatal', err: String(err) }));
  process.exit(1);
});
