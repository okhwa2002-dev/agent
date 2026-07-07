import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import { formatPrometheus } from './stats.js';
import type { AgentStats, HealthStatus } from './stats.js';

/** MetricsServer가 의존하는 수집 인터페이스 (StatsCollector가 구현). */
export interface StatsSource {
  collect(): Promise<AgentStats>;
  health(): Promise<HealthStatus>;
}

/** /health(JSON, 200/503) + /metrics(Prometheus 텍스트) HTTP 서버. */
export class MetricsServer {
  private server?: Server;

  constructor(
    private readonly source: StatsSource,
    private readonly port: number,
    private readonly host: string = '0.0.0.0', // 운영 기본은 config에서 127.0.0.1로 제한
  ) {}

  /** 리슨 시작. 실제 바인딩된 포트를 반환(테스트에서 port 0 사용). */
  async start(): Promise<number> {
    const server = createServer((req, res) => void this.route(req, res));
    this.server = server;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(this.port, this.host, () => resolve());
    });
    const addr = server.address();
    return typeof addr === 'object' && addr != null ? addr.port : this.port;
  }

  async stop(): Promise<void> {
    const server = this.server;
    if (!server) return;
    this.server = undefined;
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }

  private async route(req: IncomingMessage, res: ServerResponse): Promise<void> {
    try {
      if (req.url === '/health') {
        const h = await this.source.health();
        res.writeHead(h.ok ? 200 : 503, { 'content-type': 'application/json' });
        res.end(JSON.stringify(h));
      } else if (req.url === '/metrics') {
        const body = formatPrometheus(await this.source.collect());
        res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4; charset=utf-8' });
        res.end(body);
      } else {
        res.writeHead(404, { 'content-type': 'application/json' });
        res.end('{"error":"not found"}');
      }
    } catch (err) {
      res.writeHead(500, { 'content-type': 'text/plain' });
      res.end(String(err));
    }
  }
}
