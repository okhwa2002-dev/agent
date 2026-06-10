import type { Pool } from 'pg';
import { getQuery } from '../db/mapper.js';

export interface FaultRecord {
  ftp: string | null;
  sp: string | null;
  pcode: string | null;
}

export class DomainRepo {
  constructor(private readonly pool: Pool) {}

  /** domain_fault 멱등 INSERT. */
  async insertFault(messageId: string, deviceId: string, rec: FaultRecord): Promise<void> {
    const { text, values } = getQuery('domain', 'insertFault', {
      messageId, deviceId, ftp: rec.ftp, sp: rec.sp, pcode: rec.pcode,
    });
    await this.pool.query(text, values);
  }
}
