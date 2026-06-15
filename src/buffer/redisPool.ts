import { Redis } from 'ioredis';

/** ioredis 인스턴스 생성. maxRetriesPerRequest:null 로 블로킹 명령(XREADGROUP BLOCK) 허용. */
export function createRedis(url: string): Redis {
  return new Redis(url, { maxRetriesPerRequest: null });
}
