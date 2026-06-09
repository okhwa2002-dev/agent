import type { Pool } from 'pg';

export interface FaultRecord {
  ftp: string | null;
  sp: string | null;
  pcode: string | null;
}

export class DomainRepo {
  constructor(private readonly pool: Pool) {}

  /** domain_fault 멱등 INSERT. */
  async insertFault(messageId: string, deviceId: string, rec: FaultRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO domain_fault (message_id, device_id, ftp, sp, pcode)
       VALUES ($1,$2,$3,$4,$5) ON CONFLICT (message_id) DO NOTHING`,
      [messageId, deviceId, rec.ftp, rec.sp, rec.pcode],
    );
  }
}
