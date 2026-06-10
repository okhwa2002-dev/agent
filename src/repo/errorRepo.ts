import type { Pool } from 'pg';
import { getQuery } from '../db/mapper.js';

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
    const { text, values } = getQuery('error', 'log', {
      messageId: e.messageId ?? null,
      messageKey: e.messageKey ?? null,
      stage: e.stage,
      messageCode: e.messageCode ?? null,
      imei: e.imei ?? null,
      detail: e.detail,
      rawText: e.rawText ?? null,
    });
    await this.pool.query(text, values);
  }
}
