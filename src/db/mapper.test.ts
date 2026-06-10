import { describe, it, expect } from 'vitest';
import { getQuery } from './mapper.js';

describe('getQuery (MyBatis XML → pg 파라미터)', () => {
  it('#{name}을 $1 위치 파라미터로 변환하고 값 배열을 만든다', () => {
    const q = getQuery('device', 'findDeviceIdByImei', { imei: '123' });
    expect(q.text).toContain('$1');
    expect(q.text).not.toContain('#{');
    expect(q.values).toEqual(['123']);
  });

  it('여러 파라미터를 등장 순서대로 바인딩한다', () => {
    const q = getQuery('raw', 'markStatus', { status: 'parsed', messageId: '7' });
    expect(q.text).toMatch(/status = \$1/);
    expect(q.text).toMatch(/message_id = \$2/);
    expect(q.values).toEqual(['parsed', '7']);
  });

  it('누락 파라미터는 throw', () => {
    expect(() => getQuery('device', 'findDeviceIdByImei', {})).toThrow(/missing param/);
  });

  it('없는 매퍼는 throw', () => {
    expect(() => getQuery('device', 'nope', {})).toThrow(/mapper not found/);
  });
});
