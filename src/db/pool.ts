import { Pool } from 'pg';

/** 연결 문자열로 pg Pool 생성. max로 동시 연결 수 제어(워커 동시성 대응). */
export function createPool(connectionString: string, max = 10): Pool {
  return new Pool({ connectionString, max });
}
