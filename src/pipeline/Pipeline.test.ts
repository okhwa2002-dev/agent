import { describe, it, expect } from 'vitest';
import { Pipeline } from './Pipeline.js';
import type { RawMessage } from '../types.js';

const raw: RawMessage = {
  topic: 'device/dev-1/msg',
  deviceId: 'dev-1',
  payload: { messageCode: 'Fault', message: { ftp: '100' }, latitude: '19.2' },
  rawText: '{"messageCode":"Fault","message":{"ftp":"100"},"latitude":"19.2"}',
  receivedAt: '2026-06-09T09:03:00.000Z',
};

describe('Pipeline', () => {
  it('원본 payload를 무변형으로 rawPayload에 담는다', () => {
    const pipeline = new Pipeline('edge-01');
    const record = pipeline.process(raw);
    expect(record.rawPayload).toEqual(raw.payload); // 변형 없음
    expect(record.agentId).toBe('edge-01');
    expect(record.deviceId).toBe('dev-1');
    expect(record.receivedAt).toBe('2026-06-09T09:03:00.000Z');
  });

  it('결정적 messageId를 부여한다', () => {
    const pipeline = new Pipeline('edge-01');
    expect(pipeline.process(raw).messageId).toBe(pipeline.process(raw).messageId);
  });
});
