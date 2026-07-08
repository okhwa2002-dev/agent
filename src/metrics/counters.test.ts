import { describe, it, expect } from 'vitest';
import { AgentCounters } from './counters.js';

describe('AgentCounters', () => {
  it('처리 성공을 지연과 함께 누적한다 (sum/max)', () => {
    const c = new AgentCounters();
    c.recordProcessed(100);
    c.recordProcessed(300);
    c.recordProcessed(200);
    expect(c.processedTotal).toBe(3);
    expect(c.e2eLatencySumMs).toBe(600);
    expect(c.e2eLatencyMaxMs).toBe(300);
  });

  it('음수/비정상 지연은 0으로 취급한다 (시계 역행 방어)', () => {
    const c = new AgentCounters();
    c.recordProcessed(-50);
    c.recordProcessed(Number.NaN);
    expect(c.processedTotal).toBe(2);
    expect(c.e2eLatencySumMs).toBe(0);
    expect(c.e2eLatencyMaxMs).toBe(0);
  });

  it('실패·DLQ 이동을 누적한다', () => {
    const c = new AgentCounters();
    c.recordFailed();
    c.recordFailed();
    c.recordDlqMoved();
    expect(c.processFailedTotal).toBe(2);
    expect(c.dlqMovedTotal).toBe(1);
  });
});
