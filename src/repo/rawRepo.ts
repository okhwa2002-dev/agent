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

  /** 재처리 대상 조회: error_yn='Y' 행을 message_id keyset으로 배치 조회. */
  async findErrorRows(afterMessageId: string, limit: number): Promise<ErrorRawRow[]> {
    const { text, values } = getQuery('raw', 'findErrorRows', { afterMessageId, limit });
    const res = await this.pool.query<{ message_id: string; device_id: string | null; imei: string | null; message_code: string; raw_payload: unknown }>(text, values);
    return res.rows.map((r) => ({
      messageId: r.message_id, deviceId: r.device_id, imei: r.imei,
      messageCode: r.message_code, rawPayload: r.raw_payload,
    }));
  }

  /** 단말 매핑 갱신(뒤늦게 등록된 단말) + 에러 해제. */
  async assignDevice(messageId: string, deviceId: string): Promise<void> {
    const { text, values } = getQuery('raw', 'assignDevice', { messageId, deviceId });
    await this.pool.query(text, values);
  }

  /** 에러 해제: error_yn='N' + error_detail 제거 (재처리 직전 초기화). */
  async clearError(messageId: string): Promise<void> {
    const { text, values } = getQuery('raw', 'clearError', { messageId });
    await this.pool.query(text, values);
  }

  /** 멱등 키로 기존 message_id 조회(중복 재수신 시 파생 복구용). 없으면 null. */
  async findIdByKey(messageKey: string): Promise<string | null> {
    const { text, values } = getQuery('raw', 'findIdByKey', { messageKey });
    const res = await this.pool.query<{ message_id: string }>(text, values);
    return res.rows[0]?.message_id ?? null;
  }

  /** 지표: error_yn='Y' 행 수. */
  async countErrors(): Promise<number> {
    const { text, values } = getQuery('raw', 'countErrors');
    const res = await this.pool.query<{ c: number }>(text, values);
    return res.rows[0].c;
  }
}

export interface ErrorRawRow {
  messageId: string;
  deviceId: string | null;
  imei: string | null;
  messageCode: string;
  rawPayload: unknown;  // JSONB → 파싱된 객체
}
