import 'dotenv/config'; // 프로젝트 루트 .env 로드
import { logger } from './logger.js';
import { createPool } from './db/pool.js';
import { DeviceRepo } from './repo/deviceRepo.js';
import { RawRepo } from './repo/rawRepo.js';
import { DomainRepo } from './repo/domainRepo.js';
import { ErrorRepo } from './repo/errorRepo.js';
import { LocationRepo } from './repo/locationRepo.js';
import { GenericRepo } from './repo/genericRepo.js';
import { defaultRegistry } from './parsers/registry.js';
import { ProjectionService } from './service/projectionService.js';
import { LocationProjector } from './service/locationProjector.js';
import { ReprocessService } from './service/reprocessService.js';

/**
 * 원본 재처리 배치 CLI (에이전트와 별개 실행, DB만 필요).
 * 단말 등록·파서 수정 후 실행하면 error_yn='Y' 원본에서 파생을 복구한다.
 * 사용: npm run reprocess
 */
async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');

  const pool = createPool(databaseUrl);
  try {
    const rawRepo = new RawRepo(pool);
    const errorRepo = new ErrorRepo(pool);
    const projection = new ProjectionService(defaultRegistry(), new DomainRepo(pool), rawRepo, errorRepo, new GenericRepo(pool));
    const location = new LocationProjector(new LocationRepo(pool), errorRepo);
    const reprocess = new ReprocessService(new DeviceRepo(pool), rawRepo, projection, location);

    const summary = await reprocess.run();
    logger.info({ msg: 'reprocess done', ...summary });
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  logger.fatal({ msg: 'reprocess failed', err: String(err) });
  process.exit(1);
});
