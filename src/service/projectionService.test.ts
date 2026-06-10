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
  const id = await rawRepo.insert({ messageKey, deviceId, header: { ...header, messageCode: code }, rawPayload, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
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
  it('Fault 파생 + status=parsed', async () => {
    const mid = await seed('pk-1', 'Fault', { message: { ftp: '100', sp: '12', pcode: 'P0001' } });
    await svc.project(mid, deviceId, 'Fault', { message: { ftp: '100', sp: '12', pcode: 'P0001' } });
    expect((await pool.query('SELECT pcode FROM domain_fault WHERE message_id=$1', [mid])).rows[0].pcode).toBe('P0001');
    expect((await pool.query('SELECT status FROM messages_raw WHERE message_id=$1', [mid])).rows[0].status).toBe('parsed');
  });

  it('전용 파서 없는 코드 → domain_generic(JSONB) + parsed (catch-all)', async () => {
    const mid = await seed('pk-2', 'Sensor', { message: { temp: '25', hum: '60' } });
    await svc.project(mid, deviceId, 'Sensor', { message: { temp: '25', hum: '60' } });
    const g = await pool.query('SELECT message_code, data, device_id FROM domain_generic WHERE message_id=$1', [mid]);
    expect(g.rows[0].message_code).toBe('Sensor');
    expect(g.rows[0].data).toEqual({ temp: '25', hum: '60' });
    expect(g.rows[0].device_id).toBe(deviceId);
    expect((await pool.query('SELECT status FROM messages_raw WHERE message_id=$1', [mid])).rows[0].status).toBe('parsed');
  });
});
