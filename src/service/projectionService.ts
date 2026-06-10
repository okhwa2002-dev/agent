import type { RawRepo } from '../repo/rawRepo.js';
import type { DomainRepo } from '../repo/domainRepo.js';
import type { GenericRepo } from '../repo/genericRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { ParserRegistry } from '../parsers/registry.js';
import { extractBusinessBody } from '../header.js';

/**
 * raw → 업무 코드 분기 → 도메인 파생.
 * - 전용 파서 있음(예: Fault) → 타입 컬럼 테이블
 * - 없음(catch-all) → domain_generic에 본문 키마다 한 행씩(EAV) 저장
 * 실제 파싱/저장 예외 시에만 status=parse_error + error_log.
 */
export class ProjectionService {
  constructor(
    private readonly registry: ParserRegistry,
    private readonly domainRepo: DomainRepo,
    private readonly rawRepo: RawRepo,
    private readonly errorRepo: ErrorRepo,
    private readonly genericRepo: GenericRepo,
  ) {}

  async project(messageId: string, deviceId: string, messageCode: string, rawPayload: unknown): Promise<void> {
    const parser = this.registry.get(messageCode);
    try {
      if (parser) {
        const parsed = parser.parse(rawPayload);
        await parser.insert(this.domainRepo, messageId, deviceId, parsed);
      } else {
        // catch-all: 본문(키:값) 추출(평면/중첩 통일) → 키마다 한 행씩 저장
        const body = extractBusinessBody(rawPayload);
        await this.genericRepo.insertMany(messageId, deviceId, messageCode, body);
      }
      // 성공: error_yn은 INSERT 시 'N' 그대로 유지 (별도 작업 없음)
    } catch (err) {
      await this.rawRepo.markError(messageId, String(err));
      await this.errorRepo.log({ messageId, stage: 'projection', messageCode, detail: String(err) });
    }
  }
}
