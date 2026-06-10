import type { Pool } from 'pg';

export class GenericRepo {
  constructor(private readonly pool: Pool) {}

  /** message 본문(키:값)을 키마다 한 행씩 멱등 INSERT. */
  async insertMany(messageId: string, deviceId: string, messageCode: string, body: Record<string, unknown>): Promise<void> {
    for (const [key, v] of Object.entries(body)) {
      const value = v == null ? null : (typeof v === 'object' ? JSON.stringify(v) : String(v));
      await this.pool.query(
        `INSERT INTO domain_generic (message_id, device_id, message_code, key, value)
         VALUES ($1,$2,$3,$4,$5) ON CONFLICT (message_id, key) DO NOTHING`,
        [messageId, deviceId, messageCode, key, value],
      );
    }
  }
}
