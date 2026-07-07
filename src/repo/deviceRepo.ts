import type { Pool } from 'pg';
import { getQuery } from '../db/mapper.js';

export class DeviceRepo {
  constructor(private readonly pool: Pool) {}

  /** imei로 device_id 조회. 미등록이면 null. (BIGINT는 문자열로 반환) */
  async findDeviceIdByImei(imei: string): Promise<string | null> {
    const { text, values } = getQuery('device', 'findDeviceIdByImei', { imei });
    const res = await this.pool.query<{ device_id: string }>(text, values);
    return res.rows[0]?.device_id ?? null;
  }

  /** 단말 등록(또는 기존 조회). 생성/기존 device_id를 반환. */
  async register(imei: string): Promise<string> {
    const { text, values } = getQuery('device', 'register', { imei });
    const res = await this.pool.query<{ device_id: string }>(text, values);
    return res.rows[0].device_id;
  }

  /** 등록 단말 목록(운영 CLI용). */
  async list(): Promise<{ deviceId: string; imei: string; createdAt: string }[]> {
    const { text, values } = getQuery('device', 'list');
    const res = await this.pool.query<{ device_id: string; imei: string; created_at: string }>(text, values);
    return res.rows.map((r) => ({ deviceId: r.device_id, imei: r.imei, createdAt: String(r.created_at) }));
  }
}
