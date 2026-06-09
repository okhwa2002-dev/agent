import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { ServerClient } from './ServerClient.js';
import type { ServerRecord } from '../types.js';

let server: Server;
let baseUrl: string;
let received: unknown[] = [];
let failTimes = 0;

beforeAll(async () => {
  server = createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      if (failTimes > 0) { failTimes--; res.statusCode = 503; res.end('busy'); return; }
      received.push({ idemKey: req.headers['idempotency-key'], body: JSON.parse(body) });
      res.statusCode = 200;
      res.end(JSON.stringify({ accepted: [], duplicated: [] }));
    });
  });
  await new Promise<void>((r) => server.listen(0, r));
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(() => new Promise<void>((r) => server.close(() => r())));

const rec: ServerRecord = {
  messageId: 'm1', agentId: 'edge-01', deviceId: 'dev-1',
  receivedAt: '2026-06-09T09:03:00.000Z', rawPayload: { messageCode: 'Fault' },
};

describe('ServerClient', () => {
  it('배치를 멱등키 헤더와 함께 POST한다', async () => {
    received = []; failTimes = 0;
    const client = new ServerClient({ baseUrl, agentId: 'edge-01', maxRetries: 3, baseDelayMs: 1 });
    await client.sendBatch([rec], 'batch-key-1');
    expect(received).toHaveLength(1);
    expect((received[0] as any).idemKey).toBe('batch-key-1');
    expect((received[0] as any).body.messages[0].messageId).toBe('m1');
  });

  it('5xx면 백오프 후 재시도해 성공한다', async () => {
    received = []; failTimes = 2; // 처음 2번 503, 3번째 성공
    const client = new ServerClient({ baseUrl, agentId: 'edge-01', maxRetries: 5, baseDelayMs: 1 });
    await client.sendBatch([rec], 'batch-key-2');
    expect(received).toHaveLength(1);
  });
});
