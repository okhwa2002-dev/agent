/**
 * 처리량·지연 누적 카운터 (프로세스 시작 이후 누적, 무잠금 단일 스레드).
 * WorkerPool이 기록하고 StatsCollector가 /metrics로 노출한다.
 * 평균 지연 = e2eLatencySumMs / processedTotal (Prometheus에서는 rate(sum)/rate(count)).
 */
export class AgentCounters {
  processedTotal = 0;      // 처리 성공(ack) 누적
  processFailedTotal = 0;  // 처리 실패(재시도 예정) 누적
  dlqMovedTotal = 0;       // DLQ 이동 누적
  e2eLatencySumMs = 0;     // 수신(receivedAt)→처리 완료 지연 합(ms)
  e2eLatencyMaxMs = 0;     // 지연 최대(ms)

  recordProcessed(latencyMs: number): void {
    this.processedTotal++;
    const ms = Number.isFinite(latencyMs) && latencyMs > 0 ? latencyMs : 0; // NaN/시계 역행 방어
    this.e2eLatencySumMs += ms;
    if (ms > this.e2eLatencyMaxMs) this.e2eLatencyMaxMs = ms;
  }

  recordFailed(): void {
    this.processFailedTotal++;
  }

  recordDlqMoved(): void {
    this.dlqMovedTotal++;
  }
}
