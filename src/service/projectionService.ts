import type { RawRepo } from '../repo/rawRepo.js';
import type { DomainRepo } from '../repo/domainRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { ParserRegistry } from '../parsers/registry.js';

/** raw → 업무 코드 분기 → 도메인 파생. 실패 시 status=parse_error + error_log. */
export class ProjectionService {
  constructor(
    private readonly registry: ParserRegistry,
    private readonly domainRepo: DomainRepo,
    private readonly rawRepo: RawRepo,
    private readonly errorRepo: ErrorRepo,
  ) {}

  async project(messageId: string, deviceId: string, messageCode: string, rawPayload: unknown): Promise<void> {
    const parser = this.registry.get(messageCode);
    if (!parser) {
      await this.rawRepo.markStatus(messageId, 'parse_error');
      await this.errorRepo.log({ messageId, stage: 'projection', messageCode, detail: `no parser for messageCode=${messageCode}` });
      return;
    }
    try {
      const parsed = parser.parse(rawPayload);
      await parser.insert(this.domainRepo, messageId, deviceId, parsed);
      await this.rawRepo.markStatus(messageId, 'parsed');
    } catch (err) {
      await this.rawRepo.markStatus(messageId, 'parse_error');
      await this.errorRepo.log({ messageId, stage: 'projection', messageCode, detail: String(err) });
    }
  }
}
