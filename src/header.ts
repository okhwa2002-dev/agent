export interface Header {
  imei: string;
  messageCode: string;
  processDttm: string | null;
  latitude: string | null;
  longitude: string | null;
}

/** rawPayload(JSON)에서 공통 헤더 필드 추출. 원본은 변형하지 않는다. */
export function extractHeader(rawPayload: unknown): Header {
  const p = (rawPayload ?? {}) as Record<string, unknown>;
  const str = (v: unknown): string | null => (v == null ? null : String(v));
  return {
    imei: String(p.imei ?? ''),
    messageCode: String(p.messageCode ?? ''),
    processDttm: str(p.process_dttm),
    latitude: str(p.latitude),
    longitude: str(p.longitude),
  };
}
