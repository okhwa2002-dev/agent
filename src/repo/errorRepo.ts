import type { Pool } from 'pg';

export type ErrorStage = 'ingest' | 'device_lookup' | 'projection';

export interface ErrorEntry {
  messageId: string | null;
  stage: ErrorStage;
  messageCode?: string | null;
  imei?: string | null;
  detail: string;
  rawText?: string | null;
}

export class ErrorRepo {
  constructor(private readonly pool: Pool) {}

  /** error_log에 단계별 오류 기록. */
  async log(e: ErrorEntry): Promise<void> {
    await this.pool.query(
      `INSERT INTO error_log (message_id, stage, message_code, imei, detail, raw_text)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [e.messageId, e.stage, e.messageCode ?? null, e.imei ?? null, e.detail, e.rawText ?? null],
    );
  }
}
