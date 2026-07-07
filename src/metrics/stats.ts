/** 에이전트 운영 지표 스냅샷. */
export interface AgentStats {
  streamBacklog: number;                  // messages:stream XLEN (미처리 잔량)
  streamPending: number;                  // consumer group 미ack(XPENDING) 건수
  dlqDepth: number;                       // messages:dlq XLEN
  rawErrorRows: number;                   // messages_raw error_yn='Y' 건수
  errorLogByStage: Record<string, number>; // error_log 단계별 건수
}

/** 헬스체크 결과. ok = 세 구성요소 모두 정상. */
export interface HealthStatus {
  ok: boolean;
  mqtt: boolean;
  redis: boolean;
  pg: boolean;
}

/** AgentStats → Prometheus 텍스트 포맷(gauge). 순수 함수. */
export function formatPrometheus(s: AgentStats): string {
  const lines: string[] = [];
  const gauge = (name: string, help: string, value: number) => {
    lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} gauge`, `${name} ${value}`);
  };
  gauge('agent_stream_backlog', 'Redis stream length (XLEN, unprocessed)', s.streamBacklog);
  gauge('agent_stream_pending', 'Consumer group un-acked entries (XPENDING)', s.streamPending);
  gauge('agent_dlq_depth', 'DLQ stream length (XLEN)', s.dlqDepth);
  gauge('agent_raw_error_rows', "messages_raw rows with error_yn='Y'", s.rawErrorRows);
  lines.push('# HELP agent_error_log_total error_log rows by stage', '# TYPE agent_error_log_total gauge');
  for (const [stage, count] of Object.entries(s.errorLogByStage)) {
    lines.push(`agent_error_log_total{stage="${stage}"} ${count}`);
  }
  return lines.join('\n') + '\n';
}
