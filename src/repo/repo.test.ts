import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { DeviceRepo } from './deviceRepo.js';
import { RawRepo } from './rawRepo.js';
import { DomainRepo } from './domainRepo.js';
import { ErrorRepo } from './errorRepo.js';
import { LocationRepo } from './locationRepo.js';
import type { Header } from '../header.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;

const header: Header = {
  imei: '356938035643809', messageCode: 'Fault',
  processDttm: '2026-06-09 09:03:00', latitude: '19.23222', longitude: '203.12121',
};

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
});

afterAll(async () => {
  await pool.end();
  await container.stop();
});

describe('repositories', () => {
  it('deviceRepo: imei로 device_id 조회', async () => {
    const repo = new DeviceRepo(pool);
    await repo.register('DEV-100', '356938035643809');
    expect(await repo.findDeviceIdByImei('356938035643809')).toBe('DEV-100');
    expect(await repo.findDeviceIdByImei('000')).toBeNull();
  });

  it('rawRepo: 신규 true, 중복 false + markStatus', async () => {
    const repo = new RawRepo(pool);
    const base = { deviceId: 'DEV-100', header, rawPayload: { messageCode: 'Fault' }, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' };
    expect(await repo.insert({ messageId: 'raw-1', ...base })).toBe(true);
    expect(await repo.insert({ messageId: 'raw-1', ...base })).toBe(false);
    await repo.markStatus('raw-1', 'parsed');
    const r = await pool.query('SELECT status FROM messages_raw WHERE message_id = $1', ['raw-1']);
    expect(r.rows[0].status).toBe('parsed');
  });

  it('domainRepo: fault 멱등 INSERT', async () => {
    const raw = new RawRepo(pool);
    await raw.insert({ messageId: 'raw-2', deviceId: 'DEV-100', header, rawPayload: {}, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
    const repo = new DomainRepo(pool);
    await repo.insertFault('raw-2', 'DEV-100', { ftp: '100', sp: '12', pcode: 'P0001' });
    await repo.insertFault('raw-2', 'DEV-100', { ftp: '100', sp: '12', pcode: 'P0001' });
    const res = await pool.query('SELECT ftp FROM domain_fault WHERE message_id = $1', ['raw-2']);
    expect(res.rows[0].ftp).toBe('100');
  });

  it('errorRepo: 단계별 오류 기록', async () => {
    const repo = new ErrorRepo(pool);
    await repo.log({ messageId: 'raw-1', stage: 'projection', messageCode: 'Fault', detail: 'boom' });
    const res = await pool.query('SELECT stage, detail FROM error_log WHERE message_id = $1', ['raw-1']);
    expect(res.rows[0]).toMatchObject({ stage: 'projection', detail: 'boom' });
  });

  it('locationRepo: 위치 멱등 INSERT', async () => {
    const raw = new RawRepo(pool);
    await raw.insert({ messageId: 'raw-3', deviceId: 'DEV-100', header, rawPayload: {}, status: 'received', receivedAt: '2026-06-09T09:03:00.000Z' });
    const repo = new LocationRepo(pool);
    await repo.insert('raw-3', 'DEV-100', '19.2', '203.1');
    await repo.insert('raw-3', 'DEV-100', '19.2', '203.1');
    const res = await pool.query('SELECT latitude, device_id FROM domain_location WHERE message_id=$1', ['raw-3']);
    expect(res.rows[0].device_id).toBe('DEV-100');
  });
});
