// 단말에서 수신한 원본 메시지 (파싱 직후)
export interface RawMessage {
  topic: string;
  deviceId: string;      // MQTT 토픽에서 추출(보조)
  payload: unknown;      // 파싱된 JSON (원본 구조 그대로, 변형 금지)
  rawText: string;       // 수신한 원본 텍스트
  receivedAt: string;    // 에이전트 수신 시각 (ISO8601)
}

// 시각 주입용 (테스트 가능성)
export interface Clock {
  now(): Date;
}

export const systemClock: Clock = { now: () => new Date() };
