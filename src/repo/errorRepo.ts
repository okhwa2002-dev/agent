import type { Pool } from 'pg';

export type ErrorStage = 'ingest' | 'device_lookup' | 'projection' | 'location';

export interface ErrorEntry {
  messageId?: string | null;     // messages_raw.message_id (있을 때)
  messageKey?: string | null;    // 멱등 키 (있을 때)
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
      `INSERT INTO error_log (message_id, message_key, stage, message_code, imei, detail, raw_text)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [e.messageId ?? null, e.messageKey ?? null, e.stage, e.messageCode ?? null, e.imei ?? null, e.detail, e.rawText ?? null],
    );
  }
}
