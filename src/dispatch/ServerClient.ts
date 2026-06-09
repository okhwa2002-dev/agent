import { request } from 'undici';
import type { ServerRecord } from '../types.js';

export interface ServerClientOptions {
  baseUrl: string;
  agentId: string;
  maxRetries: number;
  baseDelayMs: number;
}

/** 서버 일시 장애를 나타내는 에러. Dispatcher가 ack하지 않고 큐에 남기도록 함. */
export class ServerUnavailableError extends Error {}

/** 배치를 서버에 POST한다. 5xx/네트워크 오류는 지수 백오프로 재시도. */
export class ServerClient {
  constructor(private readonly opts: ServerClientOptions) {}

  async sendBatch(records: ServerRecord[], idempotencyKey: string): Promise<void> {
    const url = `${this.opts.baseUrl}/api/v1/messages/batch`;
    const body = JSON.stringify({ agentId: this.opts.agentId, messages: records });

    let lastErr: unknown;
    for (let attempt = 0; attempt <= this.opts.maxRetries; attempt++) {
      if (attempt > 0) await sleep(this.opts.baseDelayMs * 2 ** (attempt - 1));
      try {
        const res = await request(url, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
          body,
        });
        if (res.statusCode >= 200 && res.statusCode < 300) {
          await res.body.dump(); // 본문 소비(소켓 누수 방지)
          return;
        }
        await res.body.dump();
        // 4xx(잘못된 데이터)는 재시도 무의미 → 즉시 실패
        if (res.statusCode >= 400 && res.statusCode < 500) {
          throw new Error(`server rejected batch: ${res.statusCode}`);
        }
        lastErr = new ServerUnavailableError(`server ${res.statusCode}`);
      } catch (err) {
        if (err instanceof Error && err.message.startsWith('server rejected')) throw err;
        lastErr = err; // 네트워크 오류 → 재시도
      }
    }
    throw new ServerUnavailableError(`batch failed after retries: ${String(lastErr)}`);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
