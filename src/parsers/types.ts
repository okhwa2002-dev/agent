import type { DomainRepo } from '../repo/domainRepo.js';

/** 업무 타입별 파서. parse는 순수 변환, insert는 도메인 저장. */
export interface DomainParser<T> {
  readonly messageCode: string;
  parse(rawPayload: unknown): T;
  insert(repo: DomainRepo, messageId: string, parsed: T): Promise<void>;
}
