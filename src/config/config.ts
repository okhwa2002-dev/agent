export interface AppConfig {
  databaseUrl: string;
  mqttUrl: string;
  mqttTopic: string;
  mqttClientId: string;
  qos: 0 | 1 | 2;
  redisUrl: string;
  workerConcurrency: number;
  metricsPort: number;      // /health,/metrics HTTP 포트 (0=비활성)
  metricsHost: string;      // 바인드 주소 (기본 127.0.0.1=로컬 전용, 컨테이너는 0.0.0.0)
}

function required(env: Record<string, string | undefined>, key: string): string {
  const v = env[key];
  if (!v) throw new Error(`missing required env: ${key}`);
  return v;
}

export function loadConfig(env: Record<string, string | undefined>): AppConfig {
  return {
    databaseUrl: required(env, 'DATABASE_URL'),
    mqttUrl: required(env, 'MQTT_URL'),
    mqttTopic: required(env, 'MQTT_TOPIC'),
    mqttClientId: env.MQTT_CLIENT_ID ?? 'edge-agent',
    qos: 1,
    redisUrl: required(env, 'REDIS_URL'),
    workerConcurrency: Number(env.WORKER_CONCURRENCY ?? '4'),
    metricsPort: Number(env.METRICS_PORT ?? '9100'),
    metricsHost: env.METRICS_HOST ?? '127.0.0.1',
  };
}
