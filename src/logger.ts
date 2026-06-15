import { mkdirSync, existsSync, statSync, renameSync, appendFileSync } from 'node:fs';
import { join } from 'node:path';

/** YYYY-MM-DD (로컬 시각) */
function ymd(d: Date): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

/** YYYY-MM-DD HH:mm:ss.SSS (로컬 시각) — 로그 타임스탬프 */
function localTs(d: Date = new Date()): string {
  const p = (n: number, len = 2) => String(n).padStart(len, '0');
  return `${ymd(d)} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

export interface Logger {
  info(obj: Record<string, unknown>): void;
  error(obj: Record<string, unknown>): void;
  fatal(obj: Record<string, unknown>): void;
  /** 날짜가 바뀐 active 파일을 백업(rename)한다. 테스트/주기 호출용. */
  rotate(now?: Date): void;
  activePath: string;
}

export interface LoggerOptions {
  /** idle 자정 대비 주기적 로테이션 체크(ms). 0/미지정이면 비활성 */
  autoRotateMs?: number;
}

/**
 * 일일 로테이션 파일 로거.
 * - 당일 로그: <dir>/agent.log 에 누적
 * - 날짜가 바뀌면: 직전 파일을 agent-<그 파일의 날짜>.log 로 백업 후 새 agent.log 시작
 * - 파일 + 콘솔(stdout/stderr) 동시 출력
 */
export function createLogger(dir: string, opts: LoggerOptions = {}): Logger {
  mkdirSync(dir, { recursive: true });
  const activePath = join(dir, 'agent.log');

  function rotate(now: Date = new Date()): void {
    if (!existsSync(activePath)) return;
    const fileDate = ymd(statSync(activePath).mtime); // 파일 내용의 날짜(마지막 기록 시각)
    const today = ymd(now);
    if (fileDate !== today) {
      renameSync(activePath, join(dir, `agent-${fileDate}.log`));
    }
  }

  function write(level: string, obj: Record<string, unknown>): void {
    rotate();
    const line = JSON.stringify({ ts: localTs(), level, ...obj });
    try {
      appendFileSync(activePath, line + '\n');
    } catch (err) {
      console.error(JSON.stringify({ ts: localTs(), level: 'error', msg: 'log write failed', err: String(err) }));
    }
    (level === 'error' || level === 'fatal' ? console.error : console.log)(line);
  }

  if (opts.autoRotateMs && opts.autoRotateMs > 0) {
    const t = setInterval(() => { try { rotate(); } catch { /* ignore */ } }, opts.autoRotateMs);
    t.unref?.(); // 종료를 막지 않도록
  }

  return {
    info: (o) => write('info', o),
    error: (o) => write('error', o),
    fatal: (o) => write('fatal', o),
    rotate,
    activePath,
  };
}

/** 앱 전역 로거. 위치는 LOG_DIR 환경변수(.env), 미설정 시 ./logs(실행 cwd 기준). */
const DEFAULT_LOG_DIR = process.env.LOG_DIR ?? 'logs';
export const logger = createLogger(DEFAULT_LOG_DIR, { autoRotateMs: 60_000 });
