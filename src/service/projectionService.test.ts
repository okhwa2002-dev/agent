import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { RawRepo } from '../repo/rawRepo.js';
import { DomainRepo } from '../repo/domainRepo.js';
import { GenericRepo } from '../repo/genericRepo.js';
import { DeviceRepo } from '../repo/deviceRepo.js';
import { ErrorRepo } from '../repo/errorRepo.js';
import { defaultRegistry } from '../parsers/registry.js';
import { ProjectionService } from './projectionService.js';
import type { Header } from '../header.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let svc: ProjectionService;
let rawRepo: RawRepo;
let deviceId: string;

const header: Header = { imei: '123', messageCode: 'Fault', processDttm: null, latitude: null, longitude: null };

async function seed(messageKey: string, code: string, rawPayload: unknown): Promise<string> {
  const id = await rawRepo.insert({ messageKey, deviceId, header: { ...header, messageCode: code }, rawPayload, errorYn: 'N', errorDetail: null, receivedAt: '2026-06-09T09:03:00.000Z' });
  return id!;
}

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  deviceId = await new DeviceRepo(pool).register('imei-proj');
  rawRepo = new RawRepo(pool);
  svc = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, new ErrorRepo(pool), new GenericRepo(pool));
});

afterAll(async () => {
  await pool.end();
  await container.stop();
});

describe('ProjectionService', () => {
  it('Fault 파생 + error_yn=N', async () => {
    const mid = await seed('pk-1', 'Fault', { message: { ftp: '100', sp: '12', pcode: 'P0001' } });
    await svc.project(mid, deviceId, 'Fault', { message: { ftp: '100', sp: '12', pcode: 'P0001' } });
    expect((await pool.query('SELECT pcode FROM domain_fault WHERE message_id=$1', [mid])).rows[0].pcode).toBe('P0001');
    expect((await pool.query('SELECT error_yn FROM messages_raw WHERE message_id=$1', [mid])).rows[0].error_yn).toBe('N');
  });

  it('전용 파서 없는 코드 → domain_generic 키별 행(EAV) + parsed', async () => {
    const mid = await seed('pk-2', 'Sensor', { volt: '20', air: '100', status: '0' });
    await svc.project(mid, deviceId, 'Sensor', { messageCode: 'Sensor', volt: '20', air: '100', status: '0' });
    const g = await pool.query('SELECT key, value FROM domain_generic WHERE message_id=$1 ORDER BY key', [mid]);
    expect(g.rows).toEqual([
      { key: 'air', value: '100' },
      { key: 'status', value: '0' },
      { key: 'volt', value: '20' },
    ]);
    expect((await pool.query('SELECT error_yn FROM messages_raw WHERE message_id=$1', [mid])).rows[0].error_yn).toBe('N');
  });

  it('catch-all 본문 키 수가 상한(200)을 넘으면 저장하지 않고 error_yn=Y + error_log(projection)', async () => {
    const huge: Record<string, string> = { messageCode: 'Bulk' };
    for (let i = 0; i < 201; i++) huge[`k${i}`] = String(i); // 본문 키 201개 (공통 키 제외)
    const mid = await seed('pk-3', 'Bulk', huge);
    await svc.project(mid, deviceId, 'Bulk', huge);
    expect((await pool.query('SELECT count(*)::int AS c FROM domain_generic WHERE message_id=$1', [mid])).rows[0].c).toBe(0);
    expect((await pool.query('SELECT error_yn FROM messages_raw WHERE message_id=$1', [mid])).rows[0].error_yn).toBe('Y');
    const e = await pool.query("SELECT detail FROM error_log WHERE message_id=$1 AND stage='projection'", [mid]);
    expect(e.rows[0].detail).toContain('200');
  });
});
