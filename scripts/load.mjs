#!/usr/bin/env node
// 대량 부하 발행 스크립트
//
// 사용법:
//   node scripts/load.mjs [--count 1000] [--imei load-001] [--url mqtt://localhost:1883] [--topic device/A/msg]
//
// 각 메시지는 고유 seq를 가져 message_key가 모두 달라 N건이 모두 별개로 저장된다.
// (검증: messages_raw 에서 해당 imei 건수 == count, 중복/에러 0 인지 psql로 확인)

import mqtt from 'mqtt';

const a = process.argv.slice(2);
const opt = { count: 1000, imei: 'load-001', url: process.env.MQTT_URL || 'mqtt://localhost:1883', topic: 'device/A/msg' };
for (let i = 0; i < a.length; i++) {
  if (a[i] === '--count') opt.count = Number(a[++i]);
  else if (a[i] === '--imei') opt.imei = a[++i];
  else if (a[i] === '--url') opt.url = a[++i];
  else if (a[i] === '--topic') opt.topic = a[++i];
}

const client = mqtt.connect(opt.url, { connectTimeout: 5000 });

client.on('connect', async () => {
  const t0 = Date.now();
  const batch = [];
  for (let i = 0; i < opt.count; i++) {
    const payload = JSON.stringify({ imei: opt.imei, messageCode: 'Common', seq: i, v1: String(i), v2: 'load' });
    batch.push(new Promise((res, rej) => client.publish(opt.topic, payload, { qos: 1 }, (e) => (e ? rej(e) : res()))));
  }
  await Promise.all(batch);
  const dt = (Date.now() - t0) / 1000;
  console.log(`published ${opt.count} msgs (imei=${opt.imei}) in ${dt.toFixed(2)}s = ${Math.round(opt.count / dt)}/s`);
  await client.endAsync();
});

client.on('error', (e) => { console.error('publish error:', String(e)); process.exit(1); });
