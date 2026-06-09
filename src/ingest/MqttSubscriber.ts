import mqtt, { type MqttClient } from 'mqtt';
import type { RawMessage, Clock } from '../types.js';
import { systemClock } from '../types.js';

export type OnMessage = (msg: RawMessage) => Promise<void>;

/** 토픽 device/<deviceId>/... 에서 deviceId 추출 */
function extractDeviceId(topic: string): string {
  return topic.split('/')[1] ?? 'unknown';
}

/**
 * 수신 메시지 1건 처리(순수 로직, 테스트 대상).
 * @returns 정상 처리 true / 파싱 실패 false
 */
export async function handleMessage(
  topic: string, raw: Buffer, onMessage: OnMessage, clock: Clock,
): Promise<boolean> {
  const rawText = raw.toString('utf8');
  let payload: unknown;
  try {
    payload = JSON.parse(rawText);
  } catch {
    return false; // 파싱 불가 → 호출자가 별도 처리(로그/격리)
  }
  await onMessage({
    topic,
    deviceId: extractDeviceId(topic),
    payload,
    rawText,
    receivedAt: clock.now().toISOString(),
  });
  return true;
}

export interface MqttSubscriberOptions {
  brokerUrl: string;
  topic: string;
  qos: 0 | 1 | 2;
}

/** Mosquitto 구독. 수신 시 handleMessage로 위임. QoS 1로 at-least-once. */
export class MqttSubscriber {
  private client?: MqttClient;
  constructor(
    private readonly opts: MqttSubscriberOptions,
    private readonly onMessage: OnMessage,
    private readonly clock: Clock = systemClock,
  ) {}

  async start(): Promise<void> {
    this.client = mqtt.connect(this.opts.brokerUrl, { reconnectPeriod: 2000 });
    await new Promise<void>((resolve, reject) => {
      this.client!.once('connect', () => resolve());
      this.client!.once('error', reject);
    });
    await this.client.subscribeAsync(this.opts.topic, { qos: this.opts.qos });
    this.client.on('message', (topic, payload) => {
      // onMessage(enqueue) 성공 후에만 처리 완료 — 실패 시 throw되어 QoS1 재전송 유도
      void handleMessage(topic, payload, this.onMessage, this.clock).catch((err) => {
        console.error(JSON.stringify({ level: 'error', msg: 'ingest failed', err: String(err) }));
      });
    });
  }

  async stop(): Promise<void> {
    await this.client?.endAsync();
  }
}
