/** 에이전트 운영 지표 스냅샷. */
export interface AgentStats {
  streamBacklog: number;                  // messages:stream XLEN (미처리 잔량)
  streamPending: number;                  // consumer group 미ack(XPENDING) 건수
  dlqDepth: number;                       // messages:dlq XLEN
  rawErrorRows: number;                   // messages_raw error_yn='Y' 건수
  errorLogByStage: Record<string, number>; // error_log 단계별 건수
  processedTotal: number;                 // 처리 성공(ack) 누적 (프로세스 시작 이후)
  processFailedTotal: number;             // 처리 실패(재시도) 누적
  dlqMovedTotal: number;                  // DLQ 이동 누적
  e2eLatencySumMs: number;                // 수신→처리완료 지연 합(ms) — 평균 = sum/processedTotal
  e2eLatencyMaxMs: number;                // 지연 최대(ms)
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
  const counter = (name: string, help: string, value: number) => {
    lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} counter`, `${name} ${value}`);
  };
  counter('agent_processed_total', 'Messages processed (acked) since start', s.processedTotal);
  counter('agent_process_failed_total', 'Processing failures (will retry) since start', s.processFailedTotal);
  counter('agent_dlq_moved_total', 'Messages moved to DLQ since start', s.dlqMovedTotal);
  counter('agent_e2e_latency_ms_sum', 'Sum of receive-to-processed latency ms (avg = sum / agent_processed_total)', s.e2eLatencySumMs);
  gauge('agent_e2e_latency_ms_max', 'Max receive-to-processed latency ms since start', s.e2eLatencyMaxMs);
  return lines.join('\n') + '\n';
}
