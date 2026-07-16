/**
 * Unified DPS proxy forwarder v3
 *
 * 基于 proxy-chain v3.0.0 源码二次开发。
 * proxy-chain Server 负责 CONNECT 隧道建立和数据转发，
 * 本入口通过 prepareRequestFunction 回调选择 DPS IP。
 *
 * 配置来源：环境变量 > Nacos > 本地 config.yaml > 默认值
 * 启动时向 Nacos 注册服务实例，关闭时注销。
 *
 * 支持双模式:
 *   - shared (无 auth): per-host 短命 IP + 缓冲池, 供 web-archiver
 *   - session (有 auth): per-session sticky 长命 IP, 供 web-collector
 */

import os from 'node:os';
import http from 'node:http';
import net from 'node:net';
import { Server } from './src/server.js';
import { DpsApi } from './src/dps-api.js';
import { SharedPool } from './src/shared-pool.js';
import { SessionPool } from './src/session-pool.js';
// @ts-ignore - config.js is plain JS
import { config } from './src/config.js';
// @ts-ignore - nacosClient.js is plain JS
import { registerService, deregisterService, nacosConfig } from './src/nacosClient.js';

const TAG = '[Forwarder]';

// ── 配置 ──

const PORT = config.forwarder.port;
const SHARED_TTL_MS = config.forwarder.sharedTtlMs;
const SESSION_TTL_MS = config.forwarder.sessionTtlMs;
const VERBOSE = config.forwarder.verbose;
const MAX_RETRY_ATTEMPTS = config.forwarder.maxRetryAttempts ?? 5;
const INTERNAL_PORT = config.forwarder.internalPort ?? 3129;
const MAX_CONCURRENT_ACQUIRE = config.forwarder.maxConcurrentAcquire ?? 3;

// ── 并发限制 Semaphore ──

class Semaphore {
    private permits: number;
    private queue: (() => void)[] = [];
    constructor(permits: number) { this.permits = permits; }
    async acquire(): Promise<void> {
        if (this.permits > 0) { this.permits--; return; }
        return new Promise<void>(r => this.queue.push(r));
    }
    release(): void {
        if (this.queue.length > 0) {
            const next = this.queue.shift()!;
            next();
        } else {
            this.permits++;
        }
    }
}

const acquireSemaphore = new Semaphore(MAX_CONCURRENT_ACQUIRE);

// ── 初始化双订单 DPS API ──

const dpsApiShort = new DpsApi({
    secretId: config.dpsShort.secretId,
    secretKey: config.dpsShort.secretKey,
    proxyUsername: config.dpsShort.proxyUsername,
    proxyPassword: config.dpsShort.proxyPassword,
    apiEndpoint: config.dpsShort.apiEndpoint,
    orderKey: 'short',
});

const dpsApiLong = new DpsApi({
    secretId: config.dpsLong.secretId,
    secretKey: config.dpsLong.secretKey,
    proxyUsername: config.dpsLong.proxyUsername,
    proxyPassword: config.dpsLong.proxyPassword,
    apiEndpoint: config.dpsLong.apiEndpoint,
    orderKey: 'long',
});

const SHORT_PROXY_USER = config.dpsShort.proxyUsername;
const SHORT_PROXY_PASS = config.dpsShort.proxyPassword;
const LONG_PROXY_USER = config.dpsLong.proxyUsername;
const LONG_PROXY_PASS = config.dpsLong.proxyPassword;

// ── 初始化双池 ──

const sharedPool = new SharedPool({
    dpsApi: dpsApiShort,
    ttlMs: SHARED_TTL_MS,
    bufferSize: config.forwarder.bufferSize ?? 5,
    bufferRefillThreshold: config.forwarder.bufferRefillThreshold ?? 2,
    blockedIpTtlMs: config.forwarder.blockedIpTtlMs ?? 600_000,
});
const sessionPool = new SessionPool({
    dpsApi: dpsApiLong,
    ttlMs: SESSION_TTL_MS,
    failureThreshold: config.forwarder.sessionFailureThreshold,
});

// ── 健康检查: CONNECT to target:443 via DPS IP ──

function checkHealth(ipObj: { ip: string; port: number }, targetHost: string): Promise<boolean> {
    const target = targetHost || 'www.baidu.com';
    return new Promise<boolean>((resolve) => {
        const socket = net.connect({ host: ipObj.ip, port: ipObj.port });
        const cleanup = (ok: boolean) => { try { socket.destroy(); } catch {} resolve(ok); };
        const timer = setTimeout(() => cleanup(false), 4_000);
        socket.on('error', () => { clearTimeout(timer); cleanup(false); });
        socket.on('close', () => clearTimeout(timer));
        socket.once('connect', () => {
            const auth = Buffer.from(
                `${dpsApiShort.proxyUsername}:${dpsApiShort.proxyPassword || ''}`
            ).toString('base64');
            socket.write(
                `CONNECT ${target}:443 HTTP/1.1\r\n` +
                `Host: ${target}:443\r\n` +
                `Proxy-Authorization: Basic ${auth}\r\n\r\n`
            );
        });
        let buf = '';
        socket.on('data', (chunk: Buffer) => {
            buf += chunk.toString();
            const firstLine = buf.split('\r\n')[0];
            if (/^HTTP\/1\.[01] 2\d\d /.test(firstLine)) {
                clearTimeout(timer); cleanup(true);
            } else if (/^HTTP\/1\.[01] [3-5]\d\d /.test(firstLine)) {
                clearTimeout(timer); cleanup(false);
            }
        });
    });
}

// ── 获取本机 IP ──

function getLocalIp(): string {
    for (const ifaces of Object.values(os.networkInterfaces())) {
        if (!ifaces) continue;
        for (const iface of ifaces) {
            if (iface.family === 'IPv4' && !iface.internal) return iface.address;
        }
    }
    return '127.0.0.1';
}

// ── 创建 proxy-chain Server ──

const server = new Server({
    port: PORT,
    verbose: VERBOSE,
    healthCheckPath: '/healthz',
    prepareRequestFunction: async ({ username, hostname, port, isHttp }) => {
        try {
            const target = `${hostname}:${port}`;
            if (!username) {
                // Shared 模式: per-host IP with rotation retry
                const result = await acquireSharedWithRetry(hostname);
                if (!result) throw new Error('SharedPool: all IPs exhausted for ' + hostname);
                console.log(`${TAG} [shared] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} via ${result.ip}:${result.port}`);
                return {
                    upstreamProxyUrl: `http://${SHORT_PROXY_USER}:${SHORT_PROXY_PASS}@${result.ip}:${result.port}`,
                    customTag: { mode: 'shared' as const, host: hostname, ip: result.ip },
                };
            } else {
                // Session 模式: per-session sticky IP
                const entry = await sessionPool.getOrCreate(username, hostname);
                sessionPool.touchLastRequest(username);
                console.log(`${TAG} [session:${username}] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} via ${entry.ip}:${entry.port}`);
                return {
                    upstreamProxyUrl: `http://${LONG_PROXY_USER}:${LONG_PROXY_PASS}@${entry.ip}:${entry.port}`,
                    customTag: { mode: 'session' as const, id: username },
                };
            }
        } catch (err: any) {
            console.error(`${TAG} prepareRequestFunction error: ${err.message}`);
            throw err;
        }
    },
});

// ── Shared 模式: per-host IP 获取 + 健康检查 + 轮换重试 ──

async function acquireSharedWithRetry(host: string): Promise<{ ip: string; port: number } | null> {
    // 快速路径: host 已有有效 IP, 直接返回 (不做健康检查)
    const existing = await sharedPool.acquireForHost(host);
    if (existing) {
        sharedPool.touchLastRequest(host);
        return existing;
    }

    // 慢速路径: 并发限制 + 健康检查轮换重试
    await acquireSemaphore.acquire();
    try {
        return await acquireSlowPath(host);
    } finally {
        acquireSemaphore.release();
    }
}

async function acquireSlowPath(host: string): Promise<{ ip: string; port: number } | null> {
    for (let attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
        let ip = sharedPool.popFromBufferPool(host);
        if (!ip) {
            await sharedPool.refillBufferPool();
            ip = sharedPool.popFromBufferPool(host);
        }
        if (!ip) break;

        const healthy = await checkHealth({ ip: ip.ip, port: ip.port }, host);
        if (healthy) {
            sharedPool.bindHostIp(host, ip);
            sharedPool.touchLastRequest(host);
            console.log(`${TAG} [shared] ${host} acquired IP ${ip.ip}:${ip.port} (attempt ${attempt + 1})`);
            return ip;
        }

        console.warn(`${TAG} [shared] ${host} health check failed for ${ip.ip}:${ip.port} (attempt ${attempt + 1})`);
        sharedPool.addBlocked(host, ip.ip);
    }

    return null;
}

// ── 事件处理: 故障转移 ──

server.on('tunnelConnectFailed', async ({ customTag }: { customTag?: { mode: string; id?: string; host?: string; ip?: string } }) => {
    try {
        if (customTag?.mode === 'shared' && customTag.host) {
            sharedPool.invalidateHost(customTag.host, customTag.ip);
        } else if (customTag?.mode === 'session' && customTag.id) {
            await sessionPool.recordFailure(customTag.id);
        }
    } catch (err: any) {
        console.error(`${TAG} tunnelConnectFailed handler error: ${err.message}`);
    }
});

// ── 事件处理: 流量统计 ──

server.on('connectionClosed', ({ connectionId, stats }: { connectionId: number; stats: any }) => {
    console.log(`${TAG} [${connectionId}] srcTx=${stats.srcTxBytes} srcRx=${stats.srcRxBytes} trgTx=${stats.trgTxBytes} trgRx=${stats.trgRxBytes}`);
});

// ── 内部 HTTP 端点: 接收 web-archiver 封锁反馈 ──

const internalServer = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/healthz') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('OK');
        return;
    }

    if (req.method === 'POST' && req.url === '/internal/invalidate') {
        try {
            const chunks: Buffer[] = [];
            for await (const chunk of req) chunks.push(chunk as Buffer);
            const body = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
            if (!body.host) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ ok: false, error: 'host is required' }));
                return;
            }
            sharedPool.invalidateHost(body.host, body.ip);
            console.log(`${TAG} [internal] invalidated host=${body.host} ip=${body.ip ?? '(auto)'} reason=${body.reason ?? '(none)'}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
        } catch (err: any) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, error: err.message }));
        }
        return;
    }

    res.writeHead(404);
    res.end();
});

internalServer.listen(INTERNAL_PORT, () => {
    console.log(`${TAG} internal HTTP server listening on :${INTERNAL_PORT}`);
});

// ── 启动 ──

await server.listen();
console.log(`${TAG} listening on :${server.port}`);
console.log(`${TAG} shared TTL=${SHARED_TTL_MS}ms, session TTL=${SESSION_TTL_MS}ms`);
console.log(`${TAG} maxRetryAttempts=${MAX_RETRY_ATTEMPTS}, internalPort=${INTERNAL_PORT}`);
console.log(`${TAG} config source: Nacos ${process.env.NACOS_SERVER_ADDR || '172.16.11.229:38848'}`);

sharedPool.startIdleMonitor();
sharedPool.startHealthChecker();
sessionPool.startIdleMonitor();
sessionPool.startHealthChecker();

const localIp = getLocalIp();
registerService(nacosConfig.serviceName, localIp, PORT).catch(() => {});

const shutdown = async () => {
    console.log(`${TAG} shutting down...`);
    internalServer.close();
    await deregisterService(nacosConfig.serviceName, localIp, PORT).catch(() => {});
    await server.close(true);
    process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
