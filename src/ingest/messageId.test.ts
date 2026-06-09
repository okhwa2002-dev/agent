import { describe, it, expect } from 'vitest';
import { deriveMessageId } from './messageId.js';

describe('deriveMessageId', () => {
  it('단말이 messageId를 제공하면 그대로 사용한다', () => {
    const id = deriveMessageId('dev-1', { messageId: 'abc-123' }, '{"messageId":"abc-123"}');
    expect(id).toBe('abc-123');
  });

  it('messageId가 없으면 deviceId+원본텍스트 해시로 유도한다 (결정적)', () => {
    const text = '{"messageCode":"Fault"}';
    const a = deriveMessageId('dev-1', { messageCode: 'Fault' }, text);
    const b = deriveMessageId('dev-1', { messageCode: 'Fault' }, text);
    expect(a).toBe(b);              // 동일 입력 → 동일 ID
    expect(a).toHaveLength(64);     // sha256 hex
  });

  it('deviceId가 다르면 다른 ID', () => {
    const text = '{"messageCode":"Fault"}';
    expect(deriveMessageId('dev-1', {}, text)).not.toBe(deriveMessageId('dev-2', {}, text));
  });
});
