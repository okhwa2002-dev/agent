// 단말에서 수신한 원본 메시지 (파싱 직후)
export interface RawMessage {
  topic: string;
  deviceId: string;      // MQTT 토픽에서 추출
  payload: unknown;      // 파싱된 JSON (원본 구조 그대로, 변형 금지)
  rawText: string;       // 수신한 원본 텍스트 (해시·디버깅용)
  receivedAt: string;    // 에이전트 수신 시각 (ISO8601)
}

// 서버로 전송할 레코드 (envelope + 원본 payload)
export interface ServerRecord {
  messageId: string;     // 결정적 멱등 키
  agentId: string;
  deviceId: string;
  receivedAt: string;
  rawPayload: unknown;   // 단말 원본 JSON 무변형
}

// 시각 주입용 (테스트 가능성)
export interface Clock {
  now(): Date;
}

export const systemClock: Clock = { now: () => new Date() };
