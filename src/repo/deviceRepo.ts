import type { Pool } from 'pg';

export class DeviceRepo {
  constructor(private readonly pool: Pool) {}

  /** imei로 device_id 조회. 미등록이면 null. (BIGINT는 문자열로 반환) */
  async findDeviceIdByImei(imei: string): Promise<string | null> {
    const res = await this.pool.query<{ device_id: string }>(
      'SELECT device_id FROM devices WHERE imei = $1', [imei],
    );
    return res.rows[0]?.device_id ?? null;
  }

  /** 단말 등록(또는 기존 조회). 생성/기존 device_id를 반환. */
  async register(imei: string): Promise<string> {
    const res = await this.pool.query<{ device_id: string }>(
      `INSERT INTO devices (imei) VALUES ($1)
       ON CONFLICT (imei) DO UPDATE SET imei = EXCLUDED.imei
       RETURNING device_id`,
      [imei],
    );
    return res.rows[0].device_id;
  }
}
