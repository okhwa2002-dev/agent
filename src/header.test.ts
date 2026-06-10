import { describe, it, expect } from 'vitest';
import { extractHeader, extractBusinessBody } from './header.js';

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

describe('extractBusinessBody', () => {
  it('평면: 공통 5키 제외한 나머지 최상위 키를 업무 본문으로', () => {
    const body = extractBusinessBody({
      imei: '123', messageCode: 'Fault', process_dttm: '2026-06-09 09:03:00',
      volt: '20', air: '100', status: '0', latitude: '19.2', longitude: '203.1',
    });
    expect(body).toEqual({ volt: '20', air: '100', status: '0' });
  });

  it('중첩: message 객체가 있으면 그것을 업무 본문으로', () => {
    const body = extractBusinessBody({ imei: '123', messageCode: 'Sensor', message: { temp: '25', hum: '60' } });
    expect(body).toEqual({ temp: '25', hum: '60' });
  });

  it('업무 키 없으면 빈 객체', () => {
    expect(extractBusinessBody({ imei: '123', messageCode: 'Ping' })).toEqual({});
  });
});
