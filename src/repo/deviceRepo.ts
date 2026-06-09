import type { Pool } from 'pg';

export class DeviceRepo {
  constructor(private readonly pool: Pool) {}

  /** imei로 device_id 조회. 미등록이면 null. */
  async findDeviceIdByImei(imei: string): Promise<string | null> {
    const res = await this.pool.query<{ device_id: string }>(
      'SELECT device_id FROM devices WHERE imei = $1', [imei],
    );
    return res.rows[0]?.device_id ?? null;
  }

  /** 단말 등록 (테스트·운영용). */
  async register(deviceId: string, imei: string): Promise<void> {
    await this.pool.query(
      'INSERT INTO devices (device_id, imei) VALUES ($1, $2) ON CONFLICT DO NOTHING',
      [deviceId, imei],
    );
  }
}
