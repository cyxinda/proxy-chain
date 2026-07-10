# proxy-chain Dockerfile — 基于 proxy-chain v3.0.0 源码二次开发
# ============================================================
# Stage 1: Git clone (SSH)
# ============================================================
FROM alpine/git:v2.49.1 AS git-layer

ARG GIT_REPO_URL=git@github.com:cyxinda/proxy-chain.git
ARG GIT_TAG=dev
ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""
ARG NO_PROXY="localhost,127.0.0.1"
ARG CACHEBUST=1

WORKDIR /data

RUN --mount=type=ssh,id=git_ssh_key \
    if [ -z "${GIT_REPO_URL}" ]; then \
        echo "GIT_REPO_URL is empty, skipping git clone"; \
        echo '{"name":"proxy-chain","private":true}' > /data/package.json; \
        exit 0; \
    fi && \
    echo "CACHEBUST=${CACHEBUST}" > /dev/null && \
    mkdir -p -m 0700 ~/.ssh && \
    HOST=$(echo "${GIT_REPO_URL}" | sed -n 's|.*@\([^:/]*\).*|\1|p') && \
    ssh-keyscan "${HOST}" >> ~/.ssh/known_hosts 2>/dev/null && \
    export HTTP_PROXY="$HTTP_PROXY" HTTPS_PROXY="$HTTPS_PROXY" NO_PROXY="$NO_PROXY" \
           http_proxy="$HTTP_PROXY" https_proxy="$HTTPS_PROXY" no_proxy="$NO_PROXY" && \
    rm -rf /data/* && \
    git clone --depth 1 --branch ${GIT_TAG} ${GIT_REPO_URL} . && \
    echo "Latest commits:" && git log -4 --oneline

# ============================================================
# Stage 2: 依赖安装（含 devDependencies，用于 TypeScript 编译）
# ============================================================
FROM node:22-bookworm-slim AS deps-layer

WORKDIR /app

COPY --from=git-layer /data/package.json /data/package-lock.json* ./
COPY package.json package-lock.json* ./

RUN npm config set registry https://registry.npmmirror.com/ && \
    npm config set audit false --global && \
    npm config set fund false --global

ENV NPM_CONFIG_CACHE=/root/.npm

RUN --mount=type=cache,target=/root/.npm,id=npm-cache-proxy-chain \
    echo "开始安装依赖..." && \
    npm install && \
    echo "依赖安装完成"

# ============================================================
# Stage 3: TypeScript 编译
# ============================================================
FROM deps-layer AS build-layer

COPY --from=git-layer /data/ ./
COPY . .

RUN npx tsc

# ============================================================
# Stage 4: 运行时
# ============================================================
FROM node:22-bookworm-slim AS runtime-layer

ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""

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
