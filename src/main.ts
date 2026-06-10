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
import { systemClock } from './types.js';

async function main(): Promise<void> {
  const cfg = loadConfig(process.env);
  const pool = createPool(cfg.databaseUrl);
  await applySchema(pool); // IF NOT EXISTS — 멱등

  const deviceRepo = new DeviceRepo(pool);
  const rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo, new GenericRepo(pool));
  const location = new LocationProjector(new LocationRepo(pool), errorRepo);
  const processor = new MessageProcessor(deviceRepo, rawRepo, projection, location, errorRepo, systemClock);

  const subscriber = new MqttSubscriber(
    { brokerUrl: cfg.mqttUrl, topic: cfg.mqttTopic, clientId: cfg.mqttClientId, qos: cfg.qos },
    (topic, payload) => processor.handle(topic, payload),
  );
  await subscriber.start();
  console.log(JSON.stringify({ level: 'info', msg: 'agent started', clientId: cfg.mqttClientId }));

  const shutdown = async () => {
    await subscriber.stop();
    await pool.end();
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
