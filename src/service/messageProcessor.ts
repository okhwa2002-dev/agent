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

  async handle(topic: string, rawBuffer: Buffer): Promise<void> {
    const rawText = rawBuffer.toString('utf8');
    let payload: unknown;
    try {
      payload = JSON.parse(rawText);
    } catch {
      // 파싱 불가 — 재전송 무의미. error_log에 보존하고 ack.
      await this.errorRepo.log({ stage: 'ingest', detail: 'json parse failed', rawText });
      return;
    }

    const deviceIdFromTopic = topic.split('/')[1] ?? 'unknown';
    const header = extractHeader(payload);
    const messageKey = deriveMessageId(deviceIdFromTopic, payload, rawText);

    // imei로 device_id 조회 (PG 오류면 throw → 재전송)
    const deviceId = await this.deviceRepo.findDeviceIdByImei(header.imei);
    const unregisteredDetail = deviceId ? null : `unregistered imei: ${header.imei}`;

    // 원본 적재 (내구성 지점). 신규면 message_id 반환, 중복(message_key)이면 null.
    const messageId = await this.rawRepo.insert({
      messageKey, deviceId, header, rawPayload: payload,
      errorYn: deviceId ? 'N' : 'Y', errorDetail: unregisteredDetail,
      receivedAt: this.clock.now().toISOString(),
    });
    if (messageId == null) {
      // 중복(재전송/재처리). 원본 저장 직후 크래시했다면 파생이 누락됐을 수 있어
      // 기존 message_id로 파생을 재실행한다(도메인 INSERT는 전부 ON CONFLICT DO NOTHING → 멱등).
      if (!deviceId) return; // 미등록은 원래 파생 없음
      const existingId = await this.rawRepo.findIdByKey(messageKey);
      if (existingId == null) return;
      await this.location.project(existingId, deviceId, header);
      await this.projection.project(existingId, deviceId, header.messageCode, payload);
      return;
    }

    if (!deviceId) {
      // 미등록 단말: 원본만 보존(error_yn=Y), 도메인/위치 저장 안 함, 추적 기록
      await this.errorRepo.log({ messageId, messageKey, stage: 'device_lookup', imei: header.imei, messageCode: header.messageCode, detail: unregisteredDetail! });
      return;
    }

    // 등록 단말: 공통 위치 + messageCode 업무 파생
    await this.location.project(messageId, deviceId, header);
    await this.projection.project(messageId, deviceId, header.messageCode, payload);
  }
}
