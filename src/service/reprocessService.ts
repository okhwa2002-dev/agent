import type { DeviceRepo } from '../repo/deviceRepo.js';
import type { RawRepo, ErrorRawRow } from '../repo/rawRepo.js';
import type { ProjectionService } from './projectionService.js';
import type { LocationProjector } from './locationProjector.js';
import { extractHeader } from '../header.js';

export interface ReprocessSummary {
  scanned: number;            // 조회한 error_yn='Y' 행 수
  reprocessed: number;        // 파생 재실행까지 수행한 행 수
  stillUnregistered: number;  // 여전히 미등록이라 건너뛴 행 수
}

/**
 * 원본 재처리 배치: error_yn='Y'인 messages_raw를 순회하며
 * 단말 등록·파서 수정 후 원본(raw_payload)로부터 파생을 복구한다.
 * - 미등록 행(device_id NULL): imei 재조회 → 등록됐으면 device_id 매핑 + 파생 실행
 * - projection 실패 행(device_id 있음): 에러 해제 후 파생 재실행
 * 파생 실패는 기존 경로(projection/location 내부 catch)로 다시 error_yn='Y' 처리되어
 * 다음 실행에서 재시도된다. 도메인 INSERT는 전부 ON CONFLICT DO NOTHING이라 멱등.
 */
export class ReprocessService {
  constructor(
    private readonly deviceRepo: DeviceRepo,
    private readonly rawRepo: RawRepo,
    private readonly projection: ProjectionService,
    private readonly location: LocationProjector,
  ) {}

  async run(batchSize = 100): Promise<ReprocessSummary> {
    const summary: ReprocessSummary = { scanned: 0, reprocessed: 0, stillUnregistered: 0 };
    let cursor = '0'; // keyset — 실패로 error_yn='Y'가 유지돼도 커서는 전진하므로 무한 루프 없음
    for (;;) {
      const rows = await this.rawRepo.findErrorRows(cursor, batchSize);
      if (rows.length === 0) break;
      for (const row of rows) await this.reprocessRow(row, summary);
      cursor = rows[rows.length - 1].messageId;
    }
    return summary;
  }

  private async reprocessRow(row: ErrorRawRow, summary: ReprocessSummary): Promise<void> {
    summary.scanned++;
    let deviceId = row.deviceId;
    if (!deviceId) {
      deviceId = row.imei ? await this.deviceRepo.findDeviceIdByImei(row.imei) : null;
      if (!deviceId) {
        summary.stillUnregistered++;
        return;
      }
      await this.rawRepo.assignDevice(row.messageId, deviceId);
    } else {
      await this.rawRepo.clearError(row.messageId);
    }
    const header = extractHeader(row.rawPayload);
    await this.location.project(row.messageId, deviceId, header);
    await this.projection.project(row.messageId, deviceId, header.messageCode, row.rawPayload);
    summary.reprocessed++;
  }
}
