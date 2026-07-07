# Edge Agent 컨테이너 (docker compose --profile agent up -d --build)
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package*.json ./
RUN npm ci --omit=dev
COPY --from=build /app/dist ./dist
COPY mappers ./mappers
RUN mkdir -p /app/logs && chown -R node:node /app
USER node
EXPOSE 9100
# /health로 컨테이너 헬스체크 (restart 정책과 함께 supervision 구성)
HEALTHCHECK --interval=15s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:9100/health || exit 1
CMD ["node", "dist/main.js"]
