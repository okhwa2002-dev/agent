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

const base: Header = { imei: 'i', messageCode: 'Fault', processDttm: null, latitude: '19.2', longitude: '203.1' };

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  await new DeviceRepo(pool).register('DEV-1', 'imei-loc');
  rawRepo = new RawRepo(pool);
  proj = new LocationProjector(new LocationRepo(pool), new ErrorRepo(pool));
});

afterAll(async () => { await pool.end(); await container.stop(); });

async function seed(id: string): Promise<void> {
  await rawRepo.insert({ messageId: id, deviceId: 'DEV-1', header: base, rawPayload: {}, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
}

describe('LocationProjector', () => {
  it('lat/lon 있으면 domain_location에 저장', async () => {
    await seed('loc-1');
    await proj.project('loc-1', 'DEV-1', base);
    const r = await pool.query('SELECT latitude, device_id FROM domain_location WHERE message_id=$1', ['loc-1']);
    expect(r.rows[0].device_id).toBe('DEV-1');
  });

  it('lat/lon 없으면 스킵(저장 안 함)', async () => {
    await seed('loc-2');
    await proj.project('loc-2', 'DEV-1', { ...base, latitude: null, longitude: null });
    const r = await pool.query('SELECT 1 FROM domain_location WHERE message_id=$1', ['loc-2']);
    expect(r.rowCount).toBe(0);
  });
});
