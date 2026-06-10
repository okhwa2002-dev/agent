import type { Pool } from 'pg';
import { getQuery } from '../db/mapper.js';

export class GenericRepo {
  constructor(private readonly pool: Pool) {}

  /** message 본문(키:값)을 키마다 한 행씩 멱등 INSERT. */
  async insertMany(messageId: string, deviceId: string, body: Record<string, unknown>): Promise<void> {
    for (const [key, v] of Object.entries(body)) {
      const value = v == null ? null : (typeof v === 'object' ? JSON.stringify(v) : String(v));
      const { text, values } = getQuery('generic', 'insert', { messageId, deviceId, key, value });
      await this.pool.query(text, values);
    }
  }
}
