import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import type { Pool } from 'pg';

const here = dirname(fileURLToPath(import.meta.url));

/** schema.sql 실행 (IF NOT EXISTS라 멱등). */
export async function applySchema(pool: Pool): Promise<void> {
  const sql = readFileSync(join(here, 'schema.sql'), 'utf8');
  await pool.query(sql);
}
