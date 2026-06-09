import type { DomainParser } from './types.js';
import type { DomainRepo, FaultRecord } from '../repo/domainRepo.js';

/**
 * messageCode = "Fault" 샘플 파서: rawPayload.message → domain_fault.
 * message 본문은 코드별로 키가 다른 가변 구조다. Fault는 ftp/sp/pcode를 사용한다.
 * 새 업무 코드는 파서 1개 + 도메인 테이블 1개를 추가한다(기존 미수정, 개방-폐쇄).
 */
export const faultParser: DomainParser<FaultRecord> = {
  messageCode: 'Fault',

  parse(rawPayload: unknown): FaultRecord {
    const msg = ((rawPayload as Record<string, unknown> | null)?.message ?? {}) as Record<string, unknown>;
    const str = (v: unknown): string | null => (v == null ? null : String(v));
    return { ftp: str(msg.ftp), sp: str(msg.sp), pcode: str(msg.pcode) };
  },

  insert(repo: DomainRepo, messageId: string, deviceId: string, parsed: FaultRecord): Promise<void> {
    return repo.insertFault(messageId, deviceId, parsed);
  },
};
