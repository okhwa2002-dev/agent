import { createHash } from 'node:crypto';

/**
 * 결정적 messageId 유도.
 * - payload.messageId 가 있으면 그대로 사용
 * - 없으면 deviceId + 원본 텍스트의 sha256 (재전송 시 동일 ID 재현 → 멱등 보장)
 *
 * 주의: 단말 스펙에서 고유 메시지 ID 필드가 확정되면 그 필드를 우선 사용하도록 조정.
 */
export function deriveMessageId(deviceId: string, payload: unknown, rawText: string): string {
  const provided = (payload as { messageId?: unknown } | null)?.messageId;
  if (typeof provided === 'string' && provided.length > 0) return provided;
  return createHash('sha256').update(`${deviceId} ${rawText}`).digest('hex');
}
