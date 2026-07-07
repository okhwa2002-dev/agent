import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import mqtt, { type MqttClient } from 'mqtt';
import type { Pool } from 'pg';
import type { Redis } from 'ioredis';
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
import { createRedis } from './buffer/redisPool.js';
import { RedisStreamQueue } from './buffer/RedisStreamQueue.js';
import { WorkerPool } from './buffer/WorkerPool.js';
import { MqttSubscriber } from './ingest/MqttSubscriber.js';
import { systemClock } from './types.js';

// 운영 mosquitto.conf와 동일한 폭주 수용 설정
const MOSQUITTO_CONF = `listener 1883
allow_anonymous true
max_queued_messages 0
max_inflight_messages 1000
`;

let mosquitto: StartedTestContainer;
let pgContainer: StartedPostgreSqlContainer;
let redisContainer: StartedRedisContainer;
let pool: Pool;
let redis: Redis;
let subscriber: MqttSubscriber;
let workers: WorkerPool;
let publisher: MqttClient;

beforeAll(async () => {
  [mosquitto, pgContainer, redisContainer] = await Promise.all([
    new GenericContainer('eclipse-mosquitto:2')
      .withCopyContentToContainer([{ content: MOSQUITTO_CONF, target: '/mosquitto/config/mosquitto.conf' }])
      .withExposedPorts(1883)
      .withWaitStrategy(Wait.forLogMessage(/mosquitto version .+ running/))
      .start(),
    new PostgreSqlContainer('postgres:16-alpine').start(),
    new RedisContainer('redis:7-alpine').start(),
  ]);

  pool = createPool(pgContainer.getConnectionUri());
  await applySchema(pool);
  await new DeviceRepo(pool).register('imei-e2e');

  // 에이전트 조립 — main.ts와 동일 배선
  const deviceRepo = new DeviceRepo(pool);
  const rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo, new GenericRepo(pool));
  const location = new LocationProjector(new LocationRepo(pool), errorRepo);
  const processor = new MessageProcessor(deviceRepo, rawRepo, projection, location, errorRepo, systemClock);

  redis = createRedis(redisContainer.getConnectionUrl());
  const queue = new RedisStreamQueue(redis, { stream: 'messages:stream', group: 'agent-workers', dlqStream: 'messages:dlq' });
  await queue.init();
  workers = new WorkerPool(queue, (topic, payload) => processor.handle(topic, payload), {
    concurrency: 4, maxRetry: 5, blockMs: 200, idleReclaimMs: 5000,
  });
  await workers.start();

  const mqttUrl = `mqtt://${mosquitto.getHost()}:${mosquitto.getMappedPort(1883)}`;
  subscriber = new MqttSubscriber(
    { brokerUrl: mqttUrl, topic: 'device/+/msg', clientId: 'e2e-agent', qos: 1 },
    (topic, payload) => queue.enqueue({ topic, payload: payload.toString('utf8'), receivedAt: systemClock.now().toISOString() }),
  );
  await subscriber.start();

  publisher = mqtt.connect(mqttUrl, { clientId: 'e2e-pub' });
  await new Promise<void>((resolve, reject) => {
    publisher.once('connect', () => resolve());
    publisher.once('error', reject);
  });
}, 180_000);

afterAll(async () => {
  await publisher?.endAsync();
  await subscriber?.stop();
  await workers?.stop();
  await redis?.quit();
  await pool?.end();
  await Promise.all([mosquitto?.stop(), pgContainer?.stop(), redisContainer?.stop()]);
}, 120_000);

describe('E2E: 단말 → MQTT → Redis 버퍼 → 워커 → PG', () => {
  const N = 300;

  it(`${N}건 순간 발행 → 전량 저장(손실 0·중복 0·에러 0) + 파생 생성`, async () => {
    const pubs: Promise<unknown>[] = [];
    for (let i = 0; i < N; i++) {
      const payload = JSON.stringify({
        imei: 'imei-e2e', messageCode: 'Fault', process_dttm: '2026-07-07 12:00:00',
        message: { ftp: '1', sp: '2', pcode: `P${i}` },
        latitude: '19.2', longitude: '203.1', seq: i,
      });
      pubs.push(publisher.publishAsync('device/e2e/msg', payload, { qos: 1 }));
    }
    await Promise.all(pubs); // 순간 폭주 발행
    const publishedAt = performance.now();

    await vi.waitFor(async () => {
      const r = await pool.query('SELECT count(*)::int AS c FROM messages_raw');
      expect(r.rows[0].c).toBe(N); // 손실 0 · 중복 0 (message_key 멱등)
    }, { timeout: 60_000, interval: 500 });
    console.log(`e2e: ${N} messages stored in ${Math.round(performance.now() - publishedAt)}ms`);

    const errs = await pool.query("SELECT count(*)::int AS c FROM messages_raw WHERE error_yn='Y'");
    expect(errs.rows[0].c).toBe(0);
    const fault = await pool.query('SELECT count(*)::int AS c FROM domain_fault');
    expect(fault.rows[0].c).toBe(N);
    const loc = await pool.query('SELECT count(*)::int AS c FROM domain_location');
    expect(loc.rows[0].c).toBe(N);
    expect(await redis.xlen('messages:dlq')).toBe(0); // poison 없음
  }, 90_000);
});
