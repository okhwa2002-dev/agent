#!/usr/bin/env node
// MQTT 발행 테스트 스크립트
//
// 사용법:
//   node scripts/pub.mjs [preset|JSON] [옵션]
//
//   preset : fault | sensor | unregistered   (기본: fault)
//   JSON   : 직접 보낼 JSON 문자열 (preset 대신)
//
// 옵션:
//   --url <mqtt://...>     브로커 URL (기본 env MQTT_URL 또는 mqtt://localhost:1883)
//   --topic <device/X/msg> 발행 토픽   (기본 device/A/msg)
//   --imei <imei>          preset의 imei 덮어쓰기 (기본 111222333)
//   --count <n>            n번 반복 발행 (기본 1)
//
// 예시:
//   node scripts/pub.mjs fault
//   node scripts/pub.mjs sensor --imei 111222333
//   node scripts/pub.mjs unregistered
//   node scripts/pub.mjs '{"imei":"111222333","messageCode":"Fault","message":{"ftp":"1"}}'
//   node scripts/pub.mjs sensor --count 5

import 'dotenv/config'; // .env의 MQTT_URL(브로커 인증 포함) 자동 로드
import mqtt from 'mqtt';

// ---- 인자 파싱 ----
const args = process.argv.slice(2);
const opts = { url: process.env.MQTT_URL || 'mqtt://localhost:1883', topic: 'device/A/msg', imei: '111222333', count: 1 };
let positional;
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === '--url') opts.url = args[++i];
  else if (a === '--topic') opts.topic = args[++i];
  else if (a === '--imei') opts.imei = args[++i];
  else if (a === '--count') opts.count = Number(args[++i]);
  else positional = a; // preset 이름 또는 JSON 문자열
}

// ---- 현재시각 "YYYY-MM-DD HH:mm:ss" ----
function nowDttm() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

// ---- preset → payload 객체 ----
function buildPayload(name) {
  switch (name) {
    case 'sensor': // 평면(flat) 범용 메시지 → domain_generic(키별 행)
      return { imei: opts.imei, messageCode: 'Sensor', process_dttm: nowDttm(), volt: '20', air: '100', status: '0', latitude: '19.23222', longitude: '203.12121' };
    case 'unregistered': // 미등록 imei → error_yn=Y
      return { imei: '000-unknown', messageCode: 'Fault', process_dttm: nowDttm(), message: { ftp: '5' } };
    case 'fault': // 등록 단말 고장(중첩 message) → domain_fault
    default:
      return { imei: opts.imei, messageCode: 'Fault', process_dttm: nowDttm(), message: { ftp: '100', sp: '12', pcode: 'P0001' }, latitude: '19.23222', longitude: '203.12121' };
  }
}

// positional이 JSON이면 그대로, 아니면 preset
function resolvePayload() {
  if (positional && positional.trim().startsWith('{')) {
    return positional; // 원본 JSON 문자열 그대로 발행
  }
  return JSON.stringify(buildPayload(positional || 'fault'));
}

// ---- 발행 ----
const client = mqtt.connect(opts.url, { connectTimeout: 5000 });

client.on('connect', async () => {
  for (let i = 0; i < opts.count; i++) {
    const payload = resolvePayload();
    await new Promise((resolve, reject) =>
      client.publish(opts.topic, payload, { qos: 1 }, (err) => (err ? reject(err) : resolve())),
    );
    console.log(`[pub ${i + 1}/${opts.count}] ${opts.topic}  ${payload}`);
  }
  await client.endAsync();
});

client.on('error', (err) => {
  console.error('connect/publish 실패:', String(err));
  process.exit(1);
});
