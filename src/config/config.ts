export interface AppConfig {
  agentId: string;
  mqttUrl: string;
  mqttTopic: string;
  redisUrl: string;
  serverUrl: string;
  stream: string;
  group: string;
  consumer: string;
  batchSize: number;
  idleReclaimMs: number;
  tickIntervalMs: number;
  maxRetries: number;
  baseDelayMs: number;
}

function required(env: Record<string, string | undefined>, key: string): string {
  const v = env[key];
  if (!v) throw new Error(`missing required env: ${key}`);
  return v;
}

export function loadConfig(env: Record<string, string | undefined>): AppConfig {
  return {
    agentId: required(env, 'AGENT_ID'),
    mqttUrl: required(env, 'MQTT_URL'),
    mqttTopic: required(env, 'MQTT_TOPIC'),
    redisUrl: required(env, 'REDIS_URL'),
    serverUrl: required(env, 'SERVER_URL'),
    stream: env.REDIS_STREAM ?? 'messages:stream',
    group: env.REDIS_GROUP ?? 'agent-workers',
    consumer: env.REDIS_CONSUMER ?? `worker-${process.pid}`,
    batchSize: Number(env.BATCH_SIZE ?? '100'),
    idleReclaimMs: Number(env.IDLE_RECLAIM_MS ?? '30000'),
    tickIntervalMs: Number(env.TICK_INTERVAL_MS ?? '500'),
    maxRetries: Number(env.MAX_RETRIES ?? '5'),
    baseDelayMs: Number(env.BASE_DELAY_MS ?? '200'),
  };
}
