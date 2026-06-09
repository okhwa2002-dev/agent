import type { ServerRecord } from '../types.js';

/** 큐에서 claim한 항목: 스트림 엔트리 ID + 레코드 */
export interface ClaimedRecord {
  entryId: string;       // 큐 구현체의 엔트리 식별자 (ack에 사용)
  record: ServerRecord;
}

/**
 * 영속 메시지 큐 추상화. 구현체는 at-least-once를 보장해야 한다:
 * claim 후 ack 전에 죽으면 reclaimStale로 회수 가능해야 한다.
 */
export interface MessageQueue {
  /** 레코드를 큐에 영속 적재. 성공 후에만 MQTT ack 해야 함. */
  enqueue(record: ServerRecord): Promise<void>;
  /** 미처리 레코드를 최대 count개 claim (처리중 상태로 전이). */
  claimBatch(count: number): Promise<ClaimedRecord[]>;
  /** 전송 성공한 엔트리들을 ack(완료 처리). */
  ack(entryIds: string[]): Promise<void>;
  /** idleMs 이상 ack되지 않은 엔트리를 회수(크래시 복구). */
  reclaimStale(idleMs: number, count: number): Promise<ClaimedRecord[]>;
}
