import type { Pool } from 'pg';

export class GenericRepo {
  constructor(private readonly pool: Pool) {}

  /** domain_generic 멱등 INSERT (message 본문을 JSONB로 저장). */
  async insert(messageId: string, deviceId: string, messageCode: string, data: unknown): Promise<void> {
    await this.pool.query(
      `INSERT INTO domain_generic (message_id, device_id, message_code, data)
       VALUES ($1,$2,$3,$4) ON CONFLICT (message_id) DO NOTHING`,
      [messageId, deviceId, messageCode, JSON.stringify(data ?? {})],
    );
  }
}
