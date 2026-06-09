import { describe, it, expect, vi } from 'vitest';
import mqtt from 'mqtt';
import { MqttSubscriber, type Handler } from './MqttSubscriber.js';
import type { IPublishPacket } from 'mqtt-packet';

// mqtt.connect를 가짜 클라이언트로 대체 — handleMessage 오버라이드 동작만 검증
function fakeClient() {
  const client: any = {
    handleMessage: undefined,
    once: (ev: string, cb: (...a: any[]) => void) => { if (ev === 'connect') setTimeout(cb, 0); return client; },
    subscribeAsync: vi.fn().mockResolvedValue(undefined),
    endAsync: vi.fn().mockResolvedValue(undefined),
  };
  return client;
}

const packet = (topic: string, payload: string): IPublishPacket =>
  ({ cmd: 'publish', topic, payload: Buffer.from(payload), qos: 1, dup: false, retain: false } as IPublishPacket);

describe('MqttSubscriber handleMessage ack', () => {
  it('처리 성공 시 cb()를 인자 없이 호출(=ack)', async () => {
    const client = fakeClient();
    vi.spyOn(mqtt, 'connect').mockReturnValue(client);
    const handler: Handler = vi.fn().mockResolvedValue(undefined);
    const sub = new MqttSubscriber({ brokerUrl: 'mqtt://x', topic: 't', clientId: 'c', qos: 1 }, handler);
    await sub.start();

    const cb = vi.fn();
    await new Promise<void>((r) => { client.handleMessage(packet('device/d/msg', '{}'), (e?: Error) => { cb(e); r(); }); });
    expect(handler).toHaveBeenCalledWith('device/d/msg', Buffer.from('{}'));
    expect(cb).toHaveBeenCalledWith(undefined); // ack
  });

  it('처리 실패 시 cb(err) 호출(=ack 안 함)', async () => {
    const client = fakeClient();
    vi.spyOn(mqtt, 'connect').mockReturnValue(client);
    const handler: Handler = vi.fn().mockRejectedValue(new Error('pg down'));
    const sub = new MqttSubscriber({ brokerUrl: 'mqtt://x', topic: 't', clientId: 'c', qos: 1 }, handler);
    await sub.start();

    const cb = vi.fn();
    await new Promise<void>((r) => { client.handleMessage(packet('device/d/msg', '{}'), (e?: Error) => { cb(e); r(); }); });
    expect(cb.mock.calls[0][0]).toBeInstanceOf(Error); // no-ack
  });
});
