export interface AppConfig {
  databaseUrl: string;
  mqttUrl: string;
  mqttTopic: string;
  mqttClientId: string;
  qos: 0 | 1 | 2;
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
  };
}
