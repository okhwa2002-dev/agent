import mqtt, { type MqttClient } from 'mqtt';
import type { IPublishPacket } from 'mqtt-packet';
import { logger } from '../logger.js';

/** 메시지 처리기: 정상 반환=ack, throw=ack 안 함(재전송 유도). */
export type Handler = (topic: string, payload: Buffer) => Promise<void>;

export interface MqttSubscriberOptions {
  brokerUrl: string;
  topic: string;
  clientId: string;
  qos: 0 | 1 | 2;
}

/**
 * Mosquitto 구독. handleMessage 오버라이드로 QoS1 puback을 처리 성공 후에만 전송한다.
 * clean:false + 안정 clientId로 미ack 메시지 재전송 보장.
 */
export class MqttSubscriber {
  private client?: MqttClient;
  constructor(
    private readonly opts: MqttSubscriberOptions,
    private readonly handler: Handler,
  ) {}

  async start(): Promise<void> {
    const client = mqtt.connect(this.opts.brokerUrl, {
      clientId: this.opts.clientId,
      clean: false,            // durable session — 미ack QoS1 재전송
      reconnectPeriod: 2000,
    });
    this.client = client;

    // 처리 성공 후 cb() → puback 전송. 실패 시 cb(err) → puback 안 함 → 재전송.
    client.handleMessage = (packet: IPublishPacket, cb: (err?: Error) => void): void => {
      this.handler(packet.topic, packet.payload as Buffer)
        .then(() => cb())
        .catch((err: unknown) => {
          logger.error({ msg: 'process failed (will redeliver)', err: String(err) });
          cb(err instanceof Error ? err : new Error(String(err)));
        });
    };

    await new Promise<void>((resolve, reject) => {
      client.once('connect', () => resolve());
      client.once('error', reject);
    });
    await client.subscribeAsync(this.opts.topic, { qos: this.opts.qos });
  }

  /** 브로커 연결 상태 (헬스체크용). */
  isConnected(): boolean {
    return this.client?.connected ?? false;
  }

  async stop(): Promise<void> {
    await this.client?.endAsync();
  }
}
