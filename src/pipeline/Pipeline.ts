import type { RawMessage, ServerRecord } from '../types.js';
import { deriveMessageId } from '../ingest/messageId.js';

/** RawMessage를 서버 전송용 ServerRecord로 변환한다. 원본 payload는 변형하지 않는다. */
export class Pipeline {
  constructor(private readonly agentId: string) {}

  process(raw: RawMessage): ServerRecord {
    return {
      messageId: deriveMessageId(raw.deviceId, raw.payload, raw.rawText),
      agentId: this.agentId,
      deviceId: raw.deviceId,
      receivedAt: raw.receivedAt,
      rawPayload: raw.payload, // 무변형
    };
  }
}
