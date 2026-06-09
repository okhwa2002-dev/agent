import { describe, it, expect } from 'vitest';
import { faultParser } from './faultParser.js';

describe('faultParser', () => {
  it('message.* 를 fault 레코드로 변환', () => {
    expect(faultParser.parse({ message: { ftp: '100', sp: '12', pcode: 'P0001' } }))
      .toEqual({ ftp: '100', sp: '12', pcode: 'P0001' });
  });

  it('message 누락 시 null 필드', () => {
    expect(faultParser.parse({})).toEqual({ ftp: null, sp: null, pcode: null });
  });

  it('messageCode는 Fault', () => {
    expect(faultParser.messageCode).toBe('Fault');
  });
});
