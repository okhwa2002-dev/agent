import type { Pool } from 'pg';
import type { Header } from '../header.js';

export interface RawInsert {
  messageId: string;
  deviceId: string | null;
  header: Header;
  rawPayload: unknown;
  status: string;
  receivedAt: string;
}

export class RawRepo {
  constructor(private readonly pool: Pool) {}

  /** 원본 멱등 적재. 신규면 true, 중복(message_id 충돌)이면 false. */
  async insert(r: RawInsert): Promise<boolean> {
    const res = await this.pool.query(
      `INSERT INTO messages_raw
         (message_id, device_id, message_code, process_dttm, latitude, longitude, raw_payload, status, received_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (message_id) DO NOTHING
       RETURNING message_id`,
      [
        r.messageId, r.deviceId, r.header.messageCode,
        r.header.processDttm, r.header.latitude, r.header.longitude,
        JSON.stringify(r.rawPayload), r.status, r.receivedAt,
      ],
    );
    return (res.rowCount ?? 0) > 0;
  }

  /** status 전이 (received | parsed | parse_error | unregistered_device). */
  async markStatus(messageId: string, status: string): Promise<void> {
    await this.pool.query(
      'UPDATE messages_raw SET status = $2 WHERE message_id = $1',
      [messageId, status],
    );
  }
}
