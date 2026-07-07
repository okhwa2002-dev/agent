import { describe, it, expect } from 'vitest';
import { formatPrometheus, type AgentStats } from './stats.js';

const stats: AgentStats = {
  streamBacklog: 5,
  streamPending: 2,
  dlqDepth: 1,
  rawErrorRows: 3,
  errorLogByStage: { ingest: 2, projection: 1 },
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

  it('마지막 줄은 개행으로 끝난다(Prometheus 규격)', () => {
    expect(formatPrometheus(stats).endsWith('\n')).toBe(true);
  });
});
