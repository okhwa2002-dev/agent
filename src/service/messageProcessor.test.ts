import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { createPool } from '../db/pool.js';
import { applySchema } from '../db/applySchema.js';
import { DeviceRepo } from '../repo/deviceRepo.js';
import { RawRepo } from '../repo/rawRepo.js';
import { DomainRepo } from '../repo/domainRepo.js';
import { ErrorRepo } from '../repo/errorRepo.js';
import { LocationRepo } from '../repo/locationRepo.js';
import { GenericRepo } from '../repo/genericRepo.js';
import { defaultRegistry } from '../parsers/registry.js';
import { ProjectionService } from './projectionService.js';
import { LocationProjector } from './locationProjector.js';
import { MessageProcessor } from './messageProcessor.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let proc: MessageProcessor;
let deviceId: string;

const clock = { now: () => new Date('2026-06-09T09:03:00.000Z') };
const buf = (o: unknown) => Buffer.from(JSON.stringify(o));
const msg = (imei: string) => ({ imei, messageCode: 'Fault', process_dttm: '2026-06-09 09:03:00', message: { ftp: '100', sp: '12', pcode: 'P0001' }, latitude: '19.2', longitude: '203.1' });

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  const deviceRepo = new DeviceRepo(pool);
  deviceId = await deviceRepo.register('imei-ok');
  const rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo, new GenericRepo(pool));
  const location = new LocationProjector(new LocationRepo(pool), errorRepo);
  proc = new MessageProcessor(deviceRepo, rawRepo, projection, location, errorRepo, clock);
});

afterAll(async () => {
  await pool.end();
  await container.stop();
});

describe('MessageProcessor', () => {
  it('등록 단말: raw + device_id 매핑 + 도메인/위치 파생', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-ok')));
    const raw = await pool.query('SELECT message_id, device_id, imei, status FROM messages_raw WHERE device_id=$1', [deviceId]);
    const row = raw.rows[0];
    expect(row.status).toBe('parsed');
    expect(row.imei).toBe('imei-ok');
    const dom = await pool.query('SELECT pcode, device_id FROM domain_fault WHERE message_id=$1', [row.message_id]);
    expect(dom.rows[0].pcode).toBe('P0001');
    expect(dom.rows[0].device_id).toBe(deviceId);
    const loc = await pool.query('SELECT device_id FROM domain_location WHERE message_id=$1', [row.message_id]);
    expect(loc.rows[0].device_id).toBe(deviceId);
  });

  it('중복은 한 번만 저장(멱등)', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-ok'))); // 동일 내용 → 동일 message_key
    const cnt = await pool.query('SELECT count(*)::int AS c FROM messages_raw WHERE device_id=$1', [deviceId]);
    expect(cnt.rows[0].c).toBe(1);
  });

  it('미등록 imei → unregistered_device + error_log(device_lookup), 도메인/위치 없음', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-unknown')));
    const raw = await pool.query("SELECT message_id, device_id, imei FROM messages_raw WHERE status='unregistered_device'");
    expect(raw.rows[0].device_id).toBeNull();
    expect(raw.rows[0].imei).toBe('imei-unknown'); // 미등록도 imei로 추적 가능
    const e = await pool.query('SELECT stage FROM error_log WHERE message_id=$1', [raw.rows[0].message_id]);
    expect(e.rows[0].stage).toBe('device_lookup');
    const loc = await pool.query('SELECT 1 FROM domain_location WHERE message_id=$1', [raw.rows[0].message_id]);
    expect(loc.rowCount).toBe(0);
  });

  it('Fault 외 코드(Sensor, 평면) → domain_generic 키별 행(EAV)', async () => {
    await proc.handle('device/dev/msg', buf({ imei: 'imei-ok', messageCode: 'Sensor', volt: '20', air: '100', status: '0' }));
    const raw = await pool.query("SELECT message_id FROM messages_raw WHERE message_code='Sensor'");
    const g = await pool.query('SELECT key, value, device_id FROM domain_generic WHERE message_id=$1 ORDER BY key', [raw.rows[0].message_id]);
    expect(g.rows.map((r) => r.key)).toEqual(['air', 'status', 'volt']);
    expect(g.rows.find((r) => r.key === 'volt').value).toBe('20');
    expect(g.rows[0].device_id).toBe(deviceId);
  });

  it('JSON 파싱 실패 → error_log(ingest), 원본 미저장', async () => {
    await proc.handle('device/dev/msg', Buffer.from('not-json'));
    const e = await pool.query("SELECT raw_text FROM error_log WHERE stage='ingest'");
    expect(e.rows[0].raw_text).toBe('not-json');
  });
});
