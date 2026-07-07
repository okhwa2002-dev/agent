import { describe, it, expect, afterEach } from 'vitest';
import { MetricsServer, type StatsSource } from './metricsServer.js';
import type { AgentStats, HealthStatus } from './stats.js';

const stats: AgentStats = {
  streamBacklog: 7, streamPending: 0, dlqDepth: 0, rawErrorRows: 0, errorLogByStage: {},
};

function source(health: HealthStatus): StatsSource {
  return { collect: async () => stats, health: async () => health };
}

let server: MetricsServer | undefined;
afterEach(async () => { await server?.stop(); server = undefined; });

describe('MetricsServer', () => {
  it('/health: 모두 정상이면 200 + JSON', async () => {
    server = new MetricsServer(source({ ok: true, mqtt: true, redis: true, pg: true }), 0);
    const port = await server.start();
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true, mqtt: true, redis: true, pg: true });
  });

  it('/health: 비정상이면 503', async () => {
    server = new MetricsServer(source({ ok: false, mqtt: false, redis: true, pg: true }), 0);
    const port = await server.start();
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    expect(res.status).toBe(503);
    expect((await res.json()).mqtt).toBe(false);
  });

  it('/metrics: Prometheus 텍스트를 반환한다', async () => {
    server = new MetricsServer(source({ ok: true, mqtt: true, redis: true, pg: true }), 0);
    const port = await server.start();
    const res = await fetch(`http://127.0.0.1:${port}/metrics`);
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/plain');
    expect(await res.text()).toContain('agent_stream_backlog 7');
  });

  it('그 외 경로는 404', async () => {
    server = new MetricsServer(source({ ok: true, mqtt: true, redis: true, pg: true }), 0);
    const port = await server.start();
    const res = await fetch(`http://127.0.0.1:${port}/nope`);
    expect(res.status).toBe(404);
  });
});
