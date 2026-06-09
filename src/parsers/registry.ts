import type { DomainParser } from './types.js';
import { faultParser } from './faultParser.js';

/** messageCode → 파서 매핑. 새 업무 코드는 여기에 등록. */
export class ParserRegistry {
  private readonly parsers = new Map<string, DomainParser<unknown>>();

  constructor(parsers: DomainParser<unknown>[]) {
    for (const p of parsers) this.parsers.set(p.messageCode, p);
  }

  get(messageCode: string): DomainParser<unknown> | undefined {
    return this.parsers.get(messageCode);
  }
}

export function defaultRegistry(): ParserRegistry {
  return new ParserRegistry([faultParser as DomainParser<unknown>]);
}
