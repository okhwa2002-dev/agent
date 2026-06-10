import type { Pool } from 'pg';
import type { Header } from '../header.js';
import { getQuery } from '../db/mapper.js';

export interface RawInsert {
  messageKey: string;            // 멱등 키 (에이전트 결정적 생성)
  deviceId: string | null;       // imei 조회 결과 (BIGINT 문자열) 또는 null
  header: Header;
  rawPayload: unknown;
  errorYn: 'Y' | 'N';            // 에러 여부
  errorDetail: string | null;    // 에러 내용 (errorYn='Y'일 때)
  receivedAt: string;
}

export class RawRepo {
  constructor(private readonly pool: Pool) {}

  /**
   * 원본 멱등 적재. 신규면 생성된 message_id(BIGINT 문자열), 중복(message_key 충돌)이면 null.
   */
  async insert(r: RawInsert): Promise<string | null> {
    const { text, values } = getQuery('raw', 'insert', {
      messageKey: r.messageKey,
      deviceId: r.deviceId,
      imei: r.header.imei,
      messageCode: r.header.messageCode,
      processDttm: r.header.processDttm,
      latitude: r.header.latitude,
      longitude: r.header.longitude,
      rawPayload: JSON.stringify(r.rawPayload),
      errorYn: r.errorYn,
      errorDetail: r.errorDetail,
      receivedAt: r.receivedAt,
    });
    const res = await this.pool.query<{ message_id: string }>(text, values);
    return res.rows[0]?.message_id ?? null;
  }

  /** 에러 표시: error_yn='Y' + error_detail 기록. */
  async markError(messageId: string, errorDetail: string): Promise<void> {
    const { text, values } = getQuery('raw', 'markError', { messageId, errorDetail });
    await this.pool.query(text, values);
  }
}
