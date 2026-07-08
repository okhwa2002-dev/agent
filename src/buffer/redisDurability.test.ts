import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { GenericContainer, type StartedTestContainer } from 'testcontainers';
import { Redis } from 'ioredis';
import { RedisStreamQueue } from './RedisStreamQueue.js';

/**
 * Redis 장애/재시작 내구성 검증 (운영과 동일한 AOF everysec).
 * XADD 후 MQTT ack하는 설계의 전제 — "Redis가 죽어도 적재분은 AOF로 복구된다" — 를 실검증한다.
 * SHUTDOWN NOSAVE로 RDB 저장 없이 강제 종료해 AOF 복구만으로 살아나는지 확인한다.
 * (everysec 특성상 최대 1초 유실 창이 있으므로 적재 후 fsync 여유를 두고 종료한다)
 */

let container: StartedTestContainer;
let redis: Redis;
let queue: RedisStreamQueue;

const OPTS = { stream: 'messages:stream', group: 'agent-workers', dlqStream: 'messages:dlq' };

beforeAll(async () => {
  container = await new GenericContainer('redis:7-alpine')
    .withCommand(['redis-server', '--appendonly', 'yes', '--appendfsync', 'everysec'])
    // 고정 호스트 포트: 무작위 포트는 컨테이너 재시작 시 재배정되어 재접속이 불가능해짐
    .withExposedPorts({ container: 6379, host: 63790 })
    .start();
  redis = new Redis({ host: container.getHost(), port: 63790, maxRetriesPerRequest: null });
  redis.on('error', () => undefined); // 재시작 중 connection error는 예상된 동작
  queue = new RedisStreamQueue(redis, OPTS);
  await queue.init();
}, 120_000);

afterAll(async () => {
  redis?.disconnect(); // quit()은 오프라인 큐에 걸려 hang 가능 — 즉시 종료
  await container?.stop();
}, 60_000);

describe('Redis 재시작 내구성 (AOF everysec)', () => {
  it('강제 종료(SHUTDOWN NOSAVE) 후 재시작해도 스트림 잔량·미ack(pending)이 보존되고 처리 가능하다', async () => {
    // 적재 5건 + 2건 claim(미ack 상태로 crash 맞이)
    for (let i = 0; i < 5; i++) {
      await queue.enqueue({ topic: 'device/d/msg', payload: `p${i}`, receivedAt: 'r' });
    }
    const claimed = await queue.claim('w0', 2, 100);
    expect(claimed).toHaveLength(2);

    // AOF fsync(everysec) 여유 후 RDB 저장 없이 강제 종료 → 재시작 (AOF 복구만으로 기동)
    await new Promise((r) => setTimeout(r, 1500));
    await container.exec(['redis-cli', 'shutdown', 'nosave']).catch(() => undefined); // 연결 단절로 에러는 정상
    await container.restart();

    // 재시작 후: 데이터·consumer group·pending 상태 모두 보존
    expect(await redis.xlen(OPTS.stream)).toBe(5); // 적재분 무손실
    const reclaimed = await queue.reclaim('w1', 0, 10); // crash 전 미ack 2건 회수
    expect(reclaimed).toHaveLength(2);
    const rest = await queue.claim('w1', 10, 100); // 미전달 3건 계속 처리
    expect(rest).toHaveLength(3);

    for (const e of [...reclaimed, ...rest]) await queue.ack(e.id);
    expect(await redis.xlen(OPTS.stream)).toBe(0); // 전량 처리 완료
  }, 60_000);
});
