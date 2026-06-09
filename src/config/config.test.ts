import { describe, it, expect } from 'vitest';
import { loadConfig } from './config.js';

describe('loadConfig', () => {
  it('환경변수에서 설정을 로드한다', () => {
    const cfg = loadConfig({
      AGENT_ID: 'edge-01', MQTT_URL: 'mqtt://localhost:1883', MQTT_TOPIC: 'device/+/msg',
      REDIS_URL: 'redis://localhost:6379', SERVER_URL: 'http://localhost:3000',
    });
    expect(cfg.agentId).toBe('edge-01');
    expect(cfg.batchSize).toBeGreaterThan(0); // 기본값
  });

  it('필수 변수 누락 시 throw한다', () => {
    expect(() => loadConfig({})).toThrow(/AGENT_ID/);
  });
});
