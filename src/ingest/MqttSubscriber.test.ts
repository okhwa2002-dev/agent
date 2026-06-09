import { describe, it, expect, vi } from 'vitest';
import { handleMessage } from './MqttSubscriber.js';
import type { RawMessage } from '../types.js';

describe('handleMessage', () => {
  it('유효한 JSON을 RawMessage로 변환해 onMessage를 호출한다', async () => {
    const onMessage = vi.fn<(msg: RawMessage) => Promise<void>>().mockResolvedValue(undefined);
    const clock = { now: () => new Date('2026-06-09T09:03:00.000Z') };
    await handleMessage('device/dev-1/msg', Buffer.from('{"messageCode":"Fault"}'), onMessage, clock);
    const msg = onMessage.mock.calls[0][0];
    expect(msg.deviceId).toBe('dev-1');
    expect(msg.payload).toEqual({ messageCode: 'Fault' });
    expect(msg.receivedAt).toBe('2026-06-09T09:03:00.000Z');
  });

  it('파싱 불가 JSON은 onMessage를 호출하지 않고 false를 반환한다', async () => {
    const onMessage = vi.fn().mockResolvedValue(undefined);
    const clock = { now: () => new Date('2026-06-09T09:03:00.000Z') };
    const ok = await handleMessage('device/dev-1/msg', Buffer.from('not-json'), onMessage, clock);
    expect(ok).toBe(false);
    expect(onMessage).not.toHaveBeenCalled();
  });
});
