import { describe, it, expect } from 'vitest';
import { formatPrometheus, type AgentStats } from './stats.js';

const stats: AgentStats = {
  streamBacklog: 5,
  streamPending: 2,
  dlqDepth: 1,
  rawErrorRows: 3,
  errorLogByStage: { ingest: 2, projection: 1 },
  processedTotal: 42,
  processFailedTotal: 4,
  dlqMovedTotal: 1,
  e2eLatencySumMs: 8400,
  e2eLatencyMaxMs: 950,
};

describe('formatPrometheus', () => {
  it('gauge 지표를 Prometheus 텍스트로 출력한다', () => {
    const text = formatPrometheus(stats);
    expect(text).toContain('# TYPE agent_stream_backlog gauge');
    expect(text).toContain('agent_stream_backlog 5');
    expect(text).toContain('agent_stream_pending 2');
    expect(text).toContain('agent_dlq_depth 1');
    expect(text).toContain('agent_raw_error_rows 3');
  });

  it('error_log 단계별 건수를 stage 라벨로 출력한다', () => {
    const text = formatPrometheus(stats);
    expect(text).toContain('agent_error_log_total{stage="ingest"} 2');
    expect(text).toContain('agent_error_log_total{stage="projection"} 1');
  });

  it('처리량·지연 누적 카운터를 counter 타입으로 출력한다', () => {
    const text = formatPrometheus(stats);
    expect(text).toContain('# TYPE agent_processed_total counter');
    expect(text).toContain('agent_processed_total 42');
    expect(text).toContain('agent_process_failed_total 4');
    expect(text).toContain('agent_dlq_moved_total 1');
    expect(text).toContain('agent_e2e_latency_ms_sum 8400');
    expect(text).toContain('agent_e2e_latency_ms_max 950');
  });

  it('마지막 줄은 개행으로 끝난다(Prometheus 규격)', () => {
    expect(formatPrometheus(stats).endsWith('\n')).toBe(true);
  });
});
