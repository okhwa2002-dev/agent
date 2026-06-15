import { describe, it, expect } from 'vitest';
import { loadConfig } from './config.js';

describe('loadConfig', () => {
  it('환경변수에서 설정을 로드한다', () => {
    const cfg = loadConfig({
      DATABASE_URL: 'postgres://localhost/db', MQTT_URL: 'mqtt://localhost:1883', MQTT_TOPIC: 'device/+/msg',
      REDIS_URL: 'redis://localhost:6379', WORKER_CONCURRENCY: '4',
    });
    expect(cfg.databaseUrl).toBe('postgres://localhost/db');
    expect(cfg.qos).toBe(1);
    expect(cfg.mqttClientId).toBe('edge-agent');
    expect(cfg.redisUrl).toBe('redis://localhost:6379');
    expect(cfg.workerConcurrency).toBe(4);
  });

  it('필수 변수 누락 시 throw', () => {
    expect(() => loadConfig({})).toThrow(/DATABASE_URL/);
  });
});
