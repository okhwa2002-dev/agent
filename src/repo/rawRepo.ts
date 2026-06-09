import type { Pool } from 'pg';
import type { Header } from '../header.js';

export interface RawInsert {
  messageKey: string;            // 멱등 키 (에이전트 결정적 생성)
  deviceId: string | null;       // imei 조회 결과 (BIGINT 문자열) 또는 null
  header: Header;
  rawPayload: unknown;
  status: string;
  receivedAt: string;
}

export class RawRepo {
  constructor(private readonly pool: Pool) {}

  /**
   * 원본 멱등 적재. 신규면 생성된 message_id(BIGINT 문자열), 중복(message_key 충돌)이면 null.
   */
  async insert(r: RawInsert): Promise<string | null> {
    const res = await this.pool.query<{ message_id: string }>(
      `INSERT INTO messages_raw
         (message_key, device_id, message_code, process_dttm, latitude, longitude, raw_payload, status, received_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (message_key) DO NOTHING
       RETURNING message_id`,
      [
        r.messageKey, r.deviceId, r.header.messageCode,
        r.header.processDttm, r.header.latitude, r.header.longitude,
        JSON.stringify(r.rawPayload), r.status, r.receivedAt,
      ],
    );
    return res.rows[0]?.message_id ?? null;
  }

  /** status 전이 (received | parsed | parse_error | unregistered_device). */
  async markStatus(messageId: string, status: string): Promise<void> {
    await this.pool.query(
      'UPDATE messages_raw SET status = $2 WHERE message_id = $1',
      [messageId, status],
    );
  }
}
