export interface Header {
  imei: string;
  messageCode: string;
  processDttm: string | null;
  latitude: string | null;
  longitude: string | null;
}

/** 공통 헤더 키 집합 (업무 본문에서 제외). extractHeader가 읽는 필드와 일치. */
export const COMMON_KEYS = new Set(['imei', 'messageCode', 'process_dttm', 'latitude', 'longitude']);

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

/**
 * 업무 본문(키:값) 추출. 두 형식 모두 지원:
 * - 중첩: `message` 객체가 있으면 그것을 업무 본문으로 사용
 * - 평면: 없으면 공통 헤더 키를 제외한 나머지 최상위 키를 업무 본문으로 사용
 */
export function extractBusinessBody(rawPayload: unknown): Record<string, unknown> {
  const p = (rawPayload ?? {}) as Record<string, unknown>;
  const msg = p.message;
  if (msg && typeof msg === 'object' && !Array.isArray(msg)) {
    return msg as Record<string, unknown>;
  }
  const body: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(p)) {
    if (!COMMON_KEYS.has(k)) body[k] = v;
  }
  return body;
}
