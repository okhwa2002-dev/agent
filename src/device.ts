import 'dotenv/config'; // 프로젝트 루트 .env 로드
import { logger } from './logger.js';
import { createPool } from './db/pool.js';
import { DeviceRepo } from './repo/deviceRepo.js';

/**
 * 단말 등록 CLI (DATABASE_URL만 필요).
 * 사용:
 *   npm run device -- register <imei>   # 등록(멱등). 등록 후 npm run reprocess로 쌓인 원본 복구
 *   npm run device -- list              # 등록 단말 목록
 */
async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');
  const [cmd, imei] = process.argv.slice(2);

  const pool = createPool(databaseUrl);
  try {
    const repo = new DeviceRepo(pool);
    if (cmd === 'register') {
      if (!imei) throw new Error('usage: device register <imei>');
      const deviceId = await repo.register(imei);
      logger.info({ msg: 'device registered', imei, deviceId });
    } else if (cmd === 'list') {
      const rows = await repo.list();
      for (const r of rows) console.log(JSON.stringify(r));
      logger.info({ msg: 'device list', count: rows.length });
    } else {
      throw new Error(`unknown command: ${cmd ?? ''} (use: register <imei> | list)`);
    }
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  logger.fatal({ msg: 'device command failed', err: String(err) });
  process.exit(1);
});
