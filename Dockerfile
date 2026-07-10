# proxy-chain Dockerfile — 基于 proxy-chain v3.0.0 源码二次开发
# ============================================================
# Stage 1: 依赖安装（含 devDependencies，用于编译）
# ============================================================
FROM node:22-bookworm-slim AS deps-layer

WORKDIR /app

RUN npm config set registry https://registry.npmmirror.com && \
    npm config set audit false && \
    npm config set fund false

COPY package.json package-lock.json* ./

ENV NPM_CONFIG_CACHE=/root/.npm

RUN --mount=type=cache,target=/root/.npm,id=npm-cache-proxy-chain \
    npm install

# ============================================================
# Stage 2: TypeScript 编译
# ============================================================
FROM deps-layer AS build-layer

COPY tsconfig.json ./
COPY src/ src/
COPY forwarder.ts ./

RUN npx tsc

# ============================================================
# Stage 3: 运行时
# ============================================================
FROM node:22-bookworm-slim AS runtime-layer

RUN sed -i 's|http://deb.debian.org|http://mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's|http://security.debian.org|http://mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=en_US.UTF-8 \
    TZ=Asia/Shanghai \
    NODE_ENV=production \
    FORWARDER_PORT=3128

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

COPY --from=build-layer /app/dist/ dist/
COPY --from=build-layer /app/node_modules ./node_modules
COPY . .

EXPOSE 3128

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget -qO- http://127.0.0.1:${FORWARDER_PORT}/healthz || exit 1

STOPSIGNAL SIGTERM
CMD ["node", "dist/forwarder.js"]
