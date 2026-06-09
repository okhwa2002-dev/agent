import type { DeviceRepo } from '../repo/deviceRepo.js';
import type { RawRepo } from '../repo/rawRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { ProjectionService } from './projectionService.js';
import type { LocationProjector } from './locationProjector.js';
import type { Clock } from '../types.js';
import { extractHeader } from '../header.js';
import { deriveMessageId } from '../ingest/messageId.js';

/** 수신 1건의 전 단계 처리. 정상 반환=ack 가능, throw=인프라 오류로 재전송 유도. */
export class MessageProcessor {
  constructor(
    private readonly deviceRepo: DeviceRepo,
    private readonly rawRepo: RawRepo,
    private readonly projection: ProjectionService,
    private readonly location: LocationProjector,
    private readonly errorRepo: ErrorRepo,
    private readonly clock: Clock,
  ) {}

  /** @param topic MQTT 토픽, @param rawBuffer 페이로드 바이트 */
  async handle(topic: string, rawBuffer: Buffer): Promise<void> {
    const rawText = rawBuffer.toString('utf8');
    let payload: unknown;
    try {
      payload = JSON.parse(rawText);
    } catch {
      // 파싱 불가 — 재전송 무의미. error_log에 보존하고 ack.
      await this.errorRepo.log({ messageId: null, stage: 'ingest', detail: 'json parse failed', rawText });
      return;
    }

    const deviceIdFromTopic = topic.split('/')[1] ?? 'unknown';
    const header = extractHeader(payload);
    const messageId = deriveMessageId(deviceIdFromTopic, payload, rawText);

    // 단말 조회 (PG 오류면 throw → 재전송)
    const deviceId = await this.deviceRepo.findDeviceIdByImei(header.imei);
    const status = deviceId ? 'received' : 'unregistered_device';

    // 원본 적재 (내구성 지점). PG 오류면 throw → 재전송
    const isNew = await this.rawRepo.insert({
      messageId, deviceId, header, rawPayload: payload, status,
      receivedAt: this.clock.now().toISOString(),
    });
    if (!isNew) return; // 중복 → ack

    if (!deviceId) {
      // 미등록 단말: 원본 보존, 파생 보류, 추적 기록
      await this.errorRepo.log({ messageId, stage: 'device_lookup', imei: header.imei, messageCode: header.messageCode, detail: `unregistered imei: ${header.imei}` });
      return;
    }

    // 등록 단말: 공통 위치 + messageCode 업무 파생
    await this.location.project(messageId, deviceId, header);
    await this.projection.project(messageId, deviceId, header.messageCode, payload);
  }
}
