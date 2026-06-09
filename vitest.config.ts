import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    testTimeout: 60_000, // testcontainers 기동 여유
    hookTimeout: 120_000, // beforeAll에서 컨테이너 이미지 풀+기동 여유
  },
});
