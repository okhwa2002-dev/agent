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
import { ReprocessService } from './reprocessService.js';
import { extractHeader } from '../header.js';
import { deriveMessageId } from '../ingest/messageId.js';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let deviceRepo: DeviceRepo;
let rawRepo: RawRepo;
let proc: MessageProcessor;
let reproc: ReprocessService;

const clock = { now: () => new Date('2026-07-07T10:00:00.000Z') };
const buf = (o: unknown) => Buffer.from(JSON.stringify(o));
const msg = (imei: string) => ({ imei, messageCode: 'Fault', process_dttm: '2026-07-07 10:00:00', message: { ftp: '100', sp: '12', pcode: 'P0001' }, latitude: '19.2', longitude: '203.1' });

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:16-alpine').start();
  pool = createPool(container.getConnectionUri());
  await applySchema(pool);
  deviceRepo = new DeviceRepo(pool);
  rawRepo = new RawRepo(pool);
  const errorRepo = new ErrorRepo(pool);
  const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo, new GenericRepo(pool));
  const location = new LocationProjector(new LocationRepo(pool), errorRepo);
  proc = new MessageProcessor(deviceRepo, rawRepo, projection, location, errorRepo, clock);
  reproc = new ReprocessService(deviceRepo, rawRepo, projection, location);
});

afterAll(async () => {
  await pool.end();
  await container.stop();
});

describe('ReprocessService', () => {
  it('미등록 단말 → 등록 후 재처리하면 device_id 매핑 + 파생 생성 + 에러 해제', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-late')));
    const before = await pool.query("SELECT message_id, device_id, error_yn FROM messages_raw WHERE imei='imei-late'");
    expect(before.rows[0].device_id).toBeNull();
    expect(before.rows[0].error_yn).toBe('Y');

    const deviceId = await deviceRepo.register('imei-late');
    const summary = await reproc.run();

    const after = await pool.query("SELECT message_id, device_id, error_yn, error_detail FROM messages_raw WHERE imei='imei-late'");
    expect(after.rows[0].device_id).toBe(deviceId);
    expect(after.rows[0].error_yn).toBe('N');
    expect(after.rows[0].error_detail).toBeNull();
    const dom = await pool.query('SELECT pcode, device_id FROM domain_fault WHERE message_id=$1', [after.rows[0].message_id]);
    expect(dom.rows[0].pcode).toBe('P0001');
    expect(dom.rows[0].device_id).toBe(deviceId);
    const loc = await pool.query('SELECT 1 FROM domain_location WHERE message_id=$1', [after.rows[0].message_id]);
    expect(loc.rowCount).toBe(1);
    expect(summary.reprocessed).toBeGreaterThanOrEqual(1);
  });

  it('여전히 미등록인 단말은 건너뛰고 error_yn=Y 유지', async () => {
    await proc.handle('device/dev/msg', buf(msg('imei-never')));
    const summary = await reproc.run();
    const row = await pool.query("SELECT device_id, error_yn FROM messages_raw WHERE imei='imei-never'");
    expect(row.rows[0].device_id).toBeNull();
    expect(row.rows[0].error_yn).toBe('Y');
    expect(summary.stillUnregistered).toBeGreaterThanOrEqual(1);
  });

  it('projection 실패 행(등록 단말) → 파서 수정 가정 재처리로 파생 생성 + 에러 해제', async () => {
    // projection 실패로 error_yn='Y'가 된 등록 단말 행을 재현
    const deviceId = await deviceRepo.register('imei-fixed');
    const payload = msg('imei-fixed');
    const rawText = JSON.stringify(payload);
    const messageId = await rawRepo.insert({
      messageKey: deriveMessageId('dev', payload, rawText),
      deviceId,
      header: extractHeader(payload),
      rawPayload: payload,
      errorYn: 'Y',
      errorDetail: 'projection: boom',
      receivedAt: clock.now().toISOString(),
    });

    const summary = await reproc.run();

    const after = await pool.query('SELECT error_yn, error_detail FROM messages_raw WHERE message_id=$1', [messageId]);
    expect(after.rows[0].error_yn).toBe('N');
    expect(after.rows[0].error_detail).toBeNull();
    const dom = await pool.query('SELECT pcode FROM domain_fault WHERE message_id=$1', [messageId]);
    expect(dom.rows[0].pcode).toBe('P0001');
    expect(summary.reprocessed).toBeGreaterThanOrEqual(1);
  });

  it('재처리는 멱등 — 다시 실행해도 파생 중복 없음', async () => {
    const cntBefore = await pool.query('SELECT count(*)::int AS c FROM domain_fault');
    await reproc.run();
    const cntAfter = await pool.query('SELECT count(*)::int AS c FROM domain_fault');
    expect(cntAfter.rows[0].c).toBe(cntBefore.rows[0].c);
  });
});
