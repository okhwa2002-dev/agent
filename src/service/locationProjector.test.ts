import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { DeviceRepo } from '../repo/deviceRepo.js';
import { RawRepo } from '../repo/rawRepo.js';
import { LocationRepo } from '../repo/locationRepo.js';
import { ErrorRepo } from '../repo/errorRepo.js';
import { LocationProjector } from './locationProjector.js';
import type { Header } from '../header.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let proj: LocationProjector;
let rawRepo: RawRepo;
let deviceId: string;

const base: Header = { imei: 'i', messageCode: 'Fault', processDttm: null, latitude: '19.2', longitude: '203.1' };

async function seed(messageKey: string): Promise<string> {
  const id = await rawRepo.insert({ messageKey, deviceId, header: base, rawPayload: {}, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
  return id!;
}

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  deviceId = await new DeviceRepo(pool).register('imei-loc');
  rawRepo = new RawRepo(pool);
  proj = new LocationProjector(new LocationRepo(pool), new ErrorRepo(pool));
});

afterAll(async () => { await pool.end(); await container.stop(); });

describe('LocationProjector', () => {
  it('lat/lon 있으면 domain_location에 저장', async () => {
    const mid = await seed('lk-1');
    await proj.project(mid, deviceId, base);
    const r = await pool.query('SELECT latitude, device_id FROM domain_location WHERE message_id=$1', [mid]);
    expect(r.rows[0].device_id).toBe(deviceId);
  });

  it('lat/lon 없으면 스킵(저장 안 함)', async () => {
    const mid = await seed('lk-2');
    await proj.project(mid, deviceId, { ...base, latitude: null, longitude: null });
    const r = await pool.query('SELECT 1 FROM domain_location WHERE message_id=$1', [mid]);
    expect(r.rowCount).toBe(0);
  });
});
