# proxy-chain Dockerfile — 容器内编译（源码/依赖/编译全部在镜像内产出，不依赖本地 dist）
# ============================================================
# Stage 1: Git clone (SSH)
# ============================================================
FROM alpine/git:v2.49.1 AS git-layer

ARG GIT_REPO=git@github.com:cyxinda/proxy-chain.git
ARG GIT_REPO_URL=${GIT_REPO}
ARG GIT_TAG=master
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
# Stage 2: 安装全部依赖（含 dev，供 TypeScript 编译，跳过 postinstall）
# ============================================================
FROM node:22-bookworm-slim AS deps-layer

ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""

ENV HTTP_PROXY=$HTTP_PROXY \
    HTTPS_PROXY=$HTTPS_PROXY \
    PNPM_HOME=/pnpm \
    PATH=/pnpm:$PATH

RUN corepack enable

WORKDIR /app

COPY --from=git-layer /data/package.json /data/pnpm-lock.yaml* /data/pnpm-workspace.yaml* ./

RUN pnpm config set registry https://registry.npmmirror.com/ && \
    pnpm config set store-dir /pnpm/store

RUN --mount=type=cache,target=/pnpm/store,id=pnpm-store-proxy-chain \
    echo "开始安装编译依赖..." && \
    pnpm install --no-frozen-lockfile --ignore-scripts && \
    echo "编译依赖安装完成"

# ============================================================
# Stage 3: TypeScript 编译（源码来自 git-layer）
# ============================================================
FROM deps-layer AS build-layer

COPY --from=git-layer /data/ ./

# tsc 不编译纯 JS 源文件（未开 allowJs），需手动同步 src 下的 .js 到 dist
RUN pnpm exec tsc && \
    find src -name '*.js' -exec sh -c 'mkdir -p "dist/$(dirname "$1")" && cp "$1" "dist/$1"' _ {} \; && \
    ls -l dist/forwarder.js dist/src/config.js dist/src/nacosClient.js

# ============================================================
# Stage 4: 仅安装生产依赖
# ============================================================
FROM node:22-bookworm-slim AS prod-deps-layer

ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""

ENV HTTP_PROXY=$HTTP_PROXY \
    HTTPS_PROXY=$HTTPS_PROXY \
    PNPM_HOME=/pnpm \
    PATH=/pnpm:$PATH

RUN corepack enable

WORKDIR /app

COPY --from=git-layer /data/package.json /data/pnpm-lock.yaml* /data/pnpm-workspace.yaml* ./

RUN pnpm config set registry https://registry.npmmirror.com/ && \
    pnpm config set store-dir /pnpm/store

RUN --mount=type=cache,target=/pnpm/store,id=pnpm-store-proxy-chain-prod \
    echo "开始安装生产依赖..." && \
    pnpm install --prod --no-frozen-lockfile --ignore-scripts && \
    echo "生产依赖安装完成"

# ============================================================
# Stage 5: 运行时
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

COPY --from=prod-deps-layer /app/node_modules ./node_modules
COPY --from=build-layer /app/dist/ dist/
COPY --from=git-layer /data/config.yaml ./

EXPOSE 3128 3129

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget -qO- http://127.0.0.1:${FORWARDER_PORT}/healthz || exit 1

STOPSIGNAL SIGTERM
CMD ["node", "dist/forwarder.js"]
