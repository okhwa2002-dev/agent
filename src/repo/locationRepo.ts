import type { Pool } from 'pg';

export class LocationRepo {
  constructor(private readonly pool: Pool) {}

  /** domain_location 멱등 INSERT. */
  async insert(messageId: string, deviceId: string, latitude: string | null, longitude: string | null): Promise<void> {
    await this.pool.query(
      `INSERT INTO domain_location (message_id, device_id, latitude, longitude)
       VALUES ($1,$2,$3,$4) ON CONFLICT (message_id) DO NOTHING`,
      [messageId, deviceId, latitude, longitude],
    );
  }
}
