import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync, writeFileSync, existsSync, utimesSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createLogger } from './logger.js';

let dir: string;
const ymd = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;

beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'agentlog-')); });
afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

describe('logger 일일 로테이션', () => {
  it('로그를 active 파일(agent.log)에 JSON 한 줄로 기록', () => {
    const log = createLogger(dir);
    log.info({ msg: 'hello' });
    const active = join(dir, 'agent.log');
    expect(existsSync(active)).toBe(true);
    const line = JSON.parse(readFileSync(active, 'utf8').trim());
    expect(line).toMatchObject({ level: 'info', msg: 'hello' });
  });

  it('같은 날이면 백업 없이 누적', () => {
    const log = createLogger(dir);
    log.info({ msg: 'a' });
    log.error({ msg: 'b' });
    expect(readdirSync(dir)).toEqual(['agent.log']); // 백업 파일 없음
    expect(readFileSync(join(dir, 'agent.log'), 'utf8').trim().split('\n')).toHaveLength(2);
  });

  it('날짜가 바뀐 옛 파일을 agent-<날짜>.log로 백업하고 새로 시작', () => {
    const log = createLogger(dir);
    const active = join(dir, 'agent.log');
    writeFileSync(active, '{"old":true}\n');
    const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
    utimesSync(active, yesterday, yesterday); // 어제 기록된 파일로 위장

    log.info({ msg: 'new-day' }); // 오늘 기록 → 로테이션 발생

    expect(existsSync(join(dir, `agent-${ymd(yesterday)}.log`))).toBe(true); // 어제분 백업됨
    expect(existsSync(active)).toBe(true);                                   // 새 active
    expect(readFileSync(active, 'utf8')).toContain('new-day');               // 새 내용만
  });
});
