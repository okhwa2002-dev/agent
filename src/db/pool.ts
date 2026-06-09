import { Pool } from 'pg';

/** 연결 문자열로 pg Pool 생성. */
export function createPool(connectionString: string): Pool {
  return new Pool({ connectionString });
}
