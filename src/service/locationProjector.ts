import type { LocationRepo } from '../repo/locationRepo.js';
import type { ErrorRepo } from '../repo/errorRepo.js';
import type { Header } from '../header.js';

/** lat/lon이 모두 있으면 domain_location에 저장(등록 단말 전용, messageCode 무관). 실패는 비치명. */
export class LocationProjector {
  constructor(
    private readonly locationRepo: LocationRepo,
    private readonly errorRepo: ErrorRepo,
  ) {}

  async project(messageId: string, deviceId: string, header: Header): Promise<void> {
    if (header.latitude == null || header.longitude == null) return; // 위치 없음 → 스킵(오류 아님)
    try {
      await this.locationRepo.insert(messageId, deviceId, header.latitude, header.longitude);
    } catch (err) {
      await this.errorRepo.log({ messageId, stage: 'location', detail: String(err) });
    }
  }
}
