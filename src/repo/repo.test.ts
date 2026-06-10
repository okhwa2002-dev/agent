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
  it('deviceRepo: register는 device_id를 반환하고 imei로 조회된다', async () => {
    const repo = new DeviceRepo(pool);
    const id = await repo.register('356938035643809');
    expect(id).toBeTruthy();
    expect(await repo.findDeviceIdByImei('356938035643809')).toBe(id);
    expect(await repo.register('356938035643809')).toBe(id); // 멱등
    expect(await repo.findDeviceIdByImei('000')).toBeNull();
  });

  it('rawRepo: 신규는 message_id 반환, 중복(message_key)은 null + markStatus', async () => {
    const repo = new RawRepo(pool);
    const base = { deviceId: null, header, rawPayload: { messageCode: 'Fault' }, errorYn: 'N' as const, errorDetail: null, receivedAt: '2026-06-09T09:03:00.000Z' };
    const id1 = await repo.insert({ messageKey: 'key-1', ...base });
    expect(id1).not.toBeNull();
    expect(await repo.insert({ messageKey: 'key-1', ...base })).toBeNull(); // 중복
    await repo.markError(id1!, 'boom');
    const r = await pool.query('SELECT error_yn, error_detail FROM messages_raw WHERE message_id = $1', [id1]);
    expect(r.rows[0].error_yn).toBe('Y');
    expect(r.rows[0].error_detail).toBe('boom');
  });

  it('domainRepo: fault 멱등 INSERT (자체 id + message_id 참조)', async () => {
    const deviceId = await new DeviceRepo(pool).register('imei-dom');
    const raw = new RawRepo(pool);
    const mid = await raw.insert({ messageKey: 'key-2', deviceId, header, rawPayload: {}, errorYn: 'N', errorDetail: null, receivedAt: '2026-06-09T09:03:00.000Z' });
    const repo = new DomainRepo(pool);
    await repo.insertFault(mid!, deviceId, { ftp: '100', sp: '12', pcode: 'P0001' });
    await repo.insertFault(mid!, deviceId, { ftp: '100', sp: '12', pcode: 'P0001' }); // 중복 무시
    const res = await pool.query('SELECT ftp, device_id FROM domain_fault WHERE message_id = $1', [mid]);
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].ftp).toBe('100');
    expect(res.rows[0].device_id).toBe(deviceId);
  });

  it('locationRepo: 위치 멱등 INSERT', async () => {
    const deviceId = await new DeviceRepo(pool).register('imei-loc');
    const raw = new RawRepo(pool);
    const mid = await raw.insert({ messageKey: 'key-3', deviceId, header, rawPayload: {}, errorYn: 'N', errorDetail: null, receivedAt: '2026-06-09T09:03:00.000Z' });
    const repo = new LocationRepo(pool);
    await repo.insert(mid!, deviceId, '19.2', '203.1');
    await repo.insert(mid!, deviceId, '19.2', '203.1');
    const res = await pool.query('SELECT latitude, device_id FROM domain_location WHERE message_id = $1', [mid]);
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].device_id).toBe(deviceId);
  });

  it('errorRepo: 단계별 오류 기록', async () => {
    const raw = new RawRepo(pool);
    const mid = await raw.insert({ messageKey: 'key-4', deviceId: null, header, rawPayload: {}, errorYn: 'N', errorDetail: null, receivedAt: '2026-06-09T09:03:00.000Z' });
    const repo = new ErrorRepo(pool);
    await repo.log({ messageId: mid, messageKey: 'key-4', stage: 'projection', messageCode: 'Fault', detail: 'boom' });
    const res = await pool.query('SELECT stage, detail FROM error_log WHERE message_id = $1', [mid]);
    expect(res.rows[0]).toMatchObject({ stage: 'projection', detail: 'boom' });
  });
});
