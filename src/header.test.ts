import { describe, it, expect } from 'vitest';
import { extractHeader } from './header.js';

describe('extractHeader', () => {
  it('rawPayload에서 공통 헤더를 추출한다', () => {
    const h = extractHeader({
      imei: '356938035643809', messageCode: 'Fault',
      process_dttm: '2026-06-09 09:03:00', latitude: '19.23222', longitude: '203.12121',
      message: { ftp: '100' },
    });
    expect(h).toEqual({
      imei: '356938035643809', messageCode: 'Fault',
      processDttm: '2026-06-09 09:03:00', latitude: '19.23222', longitude: '203.12121',
    });
  });

  it('누락 필드는 null로 처리한다', () => {
    const h = extractHeader({ imei: '123', messageCode: 'Fault' });
    expect(h.processDttm).toBeNull();
    expect(h.latitude).toBeNull();
  });
});
