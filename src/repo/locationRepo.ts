import type { Pool } from 'pg';
import { getQuery } from '../db/mapper.js';

export class LocationRepo {
  constructor(private readonly pool: Pool) {}

  /** domain_location 멱등 INSERT. */
  async insert(messageId: string, deviceId: string, latitude: string | null, longitude: string | null): Promise<void> {
    const { text, values } = getQuery('location', 'insert', { messageId, deviceId, latitude, longitude });
    await this.pool.query(text, values);
  }
}
