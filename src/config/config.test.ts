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
    expect(cfg.metricsPort).toBe(9100); // 기본값
  });

  it('필수 변수 누락 시 throw', () => {
    expect(() => loadConfig({})).toThrow(/DATABASE_URL/);
  });

  it('METRICS_PORT 지정·비활성(0)을 지원한다', () => {
    const base = {
      DATABASE_URL: 'postgres://localhost/db', MQTT_URL: 'mqtt://localhost:1883', MQTT_TOPIC: 'device/+/msg',
      REDIS_URL: 'redis://localhost:6379',
    };
    expect(loadConfig({ ...base, METRICS_PORT: '9200' }).metricsPort).toBe(9200);
    expect(loadConfig({ ...base, METRICS_PORT: '0' }).metricsPort).toBe(0);
  });

  it('METRICS_HOST 기본은 127.0.0.1(로컬 전용), 지정 시 그대로', () => {
    const base = {
      DATABASE_URL: 'postgres://localhost/db', MQTT_URL: 'mqtt://localhost:1883', MQTT_TOPIC: 'device/+/msg',
      REDIS_URL: 'redis://localhost:6379',
    };
    expect(loadConfig(base).metricsHost).toBe('127.0.0.1');
    expect(loadConfig({ ...base, METRICS_HOST: '0.0.0.0' }).metricsHost).toBe('0.0.0.0');
  });
});
