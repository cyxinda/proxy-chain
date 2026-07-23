/**
 * DPS proxy forwarder - Simple single IP mode
 *
 * Simple single IP management for web-archiver.
 * Uses one IP until it dies, then gets a new one.
 * Session mode for web-collector remains unchanged.
 *
 * Config: env vars > Nacos > local config.yaml > defaults
 */

import os from 'node:os';
import net from 'node:net';
import http from 'node:http';
import { Server } from './src/server.js';
import { DpsApi } from './src/dps-api.js';
import { SessionPool } from './src/session-pool.js';
// @ts-ignore
import { config } from './src/config.js';
// @ts-ignore
import { registerService, deregisterService, nacosConfig } from './src/nacosClient.js';

const TAG = '[Forwarder]';

const PORT = config.forwarder.port;
const SHARED_TTL_MS = config.forwarder.sharedTtlMs;
const SESSION_TTL_MS = config.forwarder.sessionTtlMs;
const VERBOSE = config.forwarder.verbose;
const FALLBACK_TO_DIRECT = config.forwarder.fallbackToDirect;
const DEGRADED_WINDOW_MS = config.forwarder.degradedWindowMs;

// ── DPS API ──

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

// ── Single IP state ──

let currentIp: { ip: string; port: number; acquiredAt: number } | null = null;
let ipPromise: Promise<{ ip: string; port: number }> | null = null;

/** 健康检查: 通过 DPS 代理 CONNECT 测试目标站点（复用 session-pool 逻辑） */
function checkIpHealth(ipObj: { ip: string; port: number }, targetHost: string): Promise<boolean> {
    const target = targetHost || 'www.baidu.com';
    return new Promise<boolean>((resolve) => {
        const socket = net.connect({ host: ipObj.ip, port: ipObj.port });
        const cleanup = (ok: boolean) => { try { socket.destroy(); } catch {} resolve(ok); };
        const timer = setTimeout(() => cleanup(false), 4_000);
        socket.on('error', () => { clearTimeout(timer); cleanup(false); });
        socket.on('close', () => clearTimeout(timer));
        socket.once('connect', () => {
            const auth = Buffer.from(`${SHORT_PROXY_USER}:${SHORT_PROXY_PASS}`).toString('base64');
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

async function getSharedIp(): Promise<{ ip: string; port: number }> {
    // Return existing IP if valid
    if (currentIp && Date.now() - currentIp.acquiredAt < SHARED_TTL_MS) {
        return currentIp;
    }

    // Dedup concurrent requests
    if (ipPromise) return ipPromise;

    ipPromise = (async () => {
        // 最多尝试 5 次获取健康 IP（DPS IP 池质量参差，死 IP 跳过）
        for (let i = 0; i < 5; i++) {
            const ips = await dpsApiShort.getDpsIps(1);
            if (!ips || ips.length === 0) throw new Error('DPS API: no IP returned');
            const candidate = { ip: ips[0].ip, port: ips[0].port };
            const healthy = await checkIpHealth(candidate, 'www.baidu.com');
            if (healthy) {
                currentIp = { ...candidate, acquiredAt: Date.now() };
                console.log(`${TAG} [shared] new IP ${currentIp.ip}:${currentIp.port} (healthy after ${i + 1} attempt(s))`);
                return currentIp;
            }
            console.warn(`${TAG} [shared] IP ${candidate.ip}:${candidate.port} health check failed (attempt ${i + 1}/5), skipping dead IP`);
        }
        throw new Error('DPS: failed to get healthy IP after 5 attempts');
    })();

    try {
        return await ipPromise;
    } finally {
        ipPromise = null;
    }
}

function invalidateSharedIp(): void {
    if (currentIp) {
        console.log(`${TAG} [shared] invalidated IP ${currentIp.ip}:${currentIp.port}`);
        currentIp = null;
    }
}

// ── Session pool (for web-collector) ──

const sessionPool = new SessionPool({
    dpsApi: dpsApiLong,
    ttlMs: SESSION_TTL_MS,
    failureThreshold: config.forwarder.sessionFailureThreshold,
    acquireAttempts: config.forwarder.sessionAcquireAttempts,
});

// ── Get local IP ──

function getLocalIp(): string {
    for (const ifaces of Object.values(os.networkInterfaces())) {
        if (!ifaces) continue;
        for (const iface of ifaces) {
            if (iface.family === 'IPv4' && !iface.internal) return iface.address;
        }
    }
    return '127.0.0.1';
}

// ── DPS fallback / circuit breaker ──
// 当快代理不可用时降级为本地直连,避免请求全部失败。
// 通过 prepareRequestFunction 返回不带 upstreamProxyUrl 的对象,
// 让 server 走 direct() 直连(src/server.ts 中 direct 分支)。
//
// 短命池(shared)和长命池(session)独立熔断:一个池挂了不影响另一个。
// 例如长命池没费用时,session 请求走直连,shared 请求继续用短命池。
//
// 状态机:healthy -> (5 次重试全失败) -> degraded -> (窗口过期试探)
//   -> 成功则 healthy / 失败则继续 degraded
// 降级窗口内不试 DPS,直接走直连,避免每个请求都等 5×4s 重试。

type PoolKey = 'shared' | 'session';
type BreakerState = { state: 'healthy' | 'degraded'; degradedUntil: number; degradedSince: number | null };
const breakers: Record<PoolKey, BreakerState> = {
    shared: { state: 'healthy', degradedUntil: 0, degradedSince: null },
    session: { state: 'healthy', degradedUntil: 0, degradedSince: null },
};

function isDpsDegraded(pool: PoolKey): boolean {
    const b = breakers[pool];
    return b.state === 'degraded' && Date.now() < b.degradedUntil;
}

function markDpsDegraded(pool: PoolKey, reason: string): void {
    const b = breakers[pool];
    const wasDegraded = b.state === 'degraded';
    b.state = 'degraded';
    b.degradedUntil = Date.now() + DEGRADED_WINDOW_MS;
    if (!wasDegraded) {
        b.degradedSince = Date.now();
        console.warn(`${TAG} [fallback] ${pool} pool degraded, switching to direct for ${DEGRADED_WINDOW_MS / 1000}s (reason: ${reason})`);
    } else {
        console.warn(`${TAG} [fallback] ${pool} pool still degraded, extending direct window (reason: ${reason})`);
    }
}

function markDpsHealthy(pool: PoolKey): void {
    const b = breakers[pool];
    if (b.state === 'degraded') {
        const dur = b.degradedSince ? Math.round((Date.now() - b.degradedSince) / 1000) : 0;
        console.log(`${TAG} [fallback] ${pool} pool recovered after ${dur}s degraded, back to proxy mode`);
        b.state = 'healthy';
        b.degradedSince = null;
    }
}

// ── Server ──

const server = new Server({
    port: PORT,
    verbose: VERBOSE,
    healthCheckPath: '/healthz',
    prepareRequestFunction: async ({ username, hostname, port, isHttp }) => {
        try {
            lastRequestAt = Date.now();
            const target = `${hostname}:${port}`;

            if (!username) {
                // Shared mode: single IP for all hosts (短命池)
                // 熔断检查:短命池降级窗口内直接走直连
                if (FALLBACK_TO_DIRECT && isDpsDegraded('shared')) {
                    console.log(`${TAG} [direct] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} (shared pool degraded, fallback to direct)`);
                    return { customTag: { mode: 'direct' as const } };
                }
                const ip = await getSharedIp();
                markDpsHealthy('shared');
                console.log(`${TAG} [shared] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} via ${ip.ip}:${ip.port}`);
                return {
                    upstreamProxyUrl: `http://${SHORT_PROXY_USER}:${SHORT_PROXY_PASS}@${ip.ip}:${ip.port}`,
                    customTag: { mode: 'shared' as const },
                };
            } else {
                // Session mode: sticky IP per session (长命池)
                // username 格式：${sessionId} 或 ${sessionId}|${areaCode}
                // areaCode 为省级行政区划代码（如 310000），让 DPS 分配对应区域 IP；
                // 缺省时不指定区域，DPS 全国随机分配。
                // 熔断检查:长命池降级窗口内直接走直连
                if (FALLBACK_TO_DIRECT && isDpsDegraded('session')) {
                    const sepIdx = username.indexOf('|');
                    const sessionId = sepIdx >= 0 ? username.substring(0, sepIdx) : username;
                    console.log(`${TAG} [direct] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} (session pool degraded, fallback to direct for ${sessionId})`);
                    return { customTag: { mode: 'direct' as const } };
                }
                const sepIdx = username.indexOf('|');
                const sessionId = sepIdx >= 0 ? username.substring(0, sepIdx) : username;
                const area = sepIdx >= 0 ? username.substring(sepIdx + 1) : undefined;
                const entry = await sessionPool.getOrCreate(sessionId, hostname, area);
                markDpsHealthy('session');
                sessionPool.touchLastRequest(sessionId);
                console.log(`${TAG} [session:${sessionId}${area ? ` area=${area}` : ''}] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} via ${entry.ip}:${entry.port}`);
                return {
                    upstreamProxyUrl: `http://${LONG_PROXY_USER}:${LONG_PROXY_PASS}@${entry.ip}:${entry.port}`,
                    customTag: { mode: 'session' as const, id: sessionId },
                };
            }
        } catch (err: any) {
            console.error(`${TAG} prepareRequestFunction error: ${err.message}`);
            // 开启降级时:根据当前模式进入对应池的降级窗口,本次请求走直连
            // (窗口过期后下一次请求会自动试探 DPS 是否恢复)
            if (FALLBACK_TO_DIRECT) {
                const pool: PoolKey = username ? 'session' : 'shared';
                markDpsDegraded(pool, err.message);
                return { customTag: { mode: 'direct' as const } };
            }
            throw err;
        }
    },
});

// ── tunnelConnectFailed: invalidate dead IP (with failure threshold + ratio) ──
// Single-site CONNECT failure should NOT invalidate the shared IP -- it may
// just be that one target site blocks this IP. Only invalidate when failures
// are frequent across multiple requests (indicating the IP itself is dead).
// Track failures with a sliding window: count failures, reset on success,
// invalidate when count reaches threshold.

let sharedIpFailures = 0;
let sharedIpSuccesses = 0;
const SHARED_IP_FAILURE_THRESHOLD = 5; // 5 consecutive failures (no success in between) -> invalidate

server.on('tunnelConnectFailed', async ({ customTag }: { customTag?: { mode: string; id?: string } }) => {
    try {
        if (customTag?.mode === 'shared') {
            if (!currentIp) return; // already invalidated, nothing to do
            sharedIpFailures++;
            if (sharedIpFailures >= SHARED_IP_FAILURE_THRESHOLD) {
                console.log(`${TAG} [shared] invalidated IP ${currentIp.ip}:${currentIp.port} (${sharedIpFailures} consecutive failures, ${sharedIpSuccesses} successes)`);
                invalidateSharedIp();
                sharedIpFailures = 0;
                sharedIpSuccesses = 0;
            } else {
                console.warn(`${TAG} [shared] CONNECT failed (${sharedIpFailures}/${SHARED_IP_FAILURE_THRESHOLD}, ${sharedIpSuccesses} ok), keeping IP ${currentIp?.ip}:${currentIp?.port}`);
            }
        } else if (customTag?.mode === 'session' && customTag.id) {
            await sessionPool.recordFailure(customTag.id);
        }
    } catch (err: any) {
        console.error(`${TAG} tunnelConnectFailed handler error: ${err.message}`);
    }
});

// 成功时重置失败计数
server.on('tunnelConnectResponded', () => {
    sharedIpFailures = 0;
    sharedIpSuccesses++;
});

// ── Connection stats ──

server.on('connectionClosed', ({ connectionId, stats }: { connectionId: number; stats: any }) => {
    if (VERBOSE) {
        console.log(`${TAG} [${connectionId}] srcTx=${stats.srcTxBytes} srcRx=${stats.srcRxBytes} trgTx=${stats.trgTxBytes} trgRx=${stats.trgRxBytes}`);
    }
});

// ── Proactive IP refresh (active mode only) ──
// Refresh based on DPS IP's actual TTL (SHARED_TTL_MS), not web-archiver's
// skip threshold. web-archiver no longer skips on expired IP (v2.1.5+),
// so we only need to rotate before DPS expires. Leave 10s safety margin
// to avoid using an IP that expires mid-request.
//
// In idle mode (no requests for 5min), stop rotating to save DPS quota.
// web-archiver will send requests even when IP is null/expired, which
// triggers prepareRequestFunction -> getSharedIp() on demand.

const REFRESH_INTERVAL_MS = Math.max(SHARED_TTL_MS - 10_000, 30_000); // DPS TTL - 10s safety margin
const IDLE_STOP_MS = 5 * 60_000;
let lastRequestAt = Date.now();

const ipRefreshTimer = setInterval(async () => {
    const idleMs = Date.now() - lastRequestAt;
    if (idleMs >= IDLE_STOP_MS) {
        // Idle: stop proactive refresh. Clear expired IP so web-archiver's
        // next request triggers getSharedIp() via prepareRequestFunction.
        if (currentIp && Date.now() - currentIp.acquiredAt >= SHARED_TTL_MS) {
            console.log(`${TAG} [shared] idle ${Math.round(idleMs / 1000)}s, clearing expired IP (will acquire on demand)`);
            currentIp = null;
        }
        return;
    }

    const age = currentIp ? Date.now() - currentIp.acquiredAt : Infinity;
    if (age >= REFRESH_INTERVAL_MS) {
        // 降级期间跳过主动刷新,避免浪费 DPS 配额和噪音日志;
        // 降级窗口结束后由 prepareRequestFunction 的按需获取试探恢复。
        if (FALLBACK_TO_DIRECT && isDpsDegraded('shared')) {
            return;
        }
        try {
            console.log(`${TAG} [shared] proactive refresh (age=${Math.round(age / 1000)}s >= ${Math.round(REFRESH_INTERVAL_MS / 1000)}s, idle=${Math.round(idleMs / 1000)}s)`);
            currentIp = null;
            const ip = await getSharedIp();
            markDpsHealthy('shared');
            console.log(`${TAG} [shared] refreshed IP ${ip.ip}:${ip.port}`);
        } catch (err: any) {
            console.error(`${TAG} [shared] proactive refresh failed: ${err.message}`);
            if (FALLBACK_TO_DIRECT) markDpsDegraded('shared', err.message);
        }
    }
}, Math.max(Math.floor(REFRESH_INTERVAL_MS / 2), 15_000));

// ── Status endpoint for web-archiver ──

const statusServer = http.createServer((req, res) => {
    if (req.url === '/status') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            ip: currentIp?.ip || null,
            port: currentIp?.port || null,
            acquiredAt: currentIp?.acquiredAt || null,
            fallbackToDirect: FALLBACK_TO_DIRECT,
            sharedPool: { state: breakers.shared.state, degradedUntil: breakers.shared.degradedUntil || null },
            sessionPool: { state: breakers.session.state, degradedUntil: breakers.session.degradedUntil || null },
        }));
        return;
    }
    res.writeHead(404);
    res.end();
});

const STATUS_PORT = PORT + 1;

statusServer.listen(STATUS_PORT, () => {
    console.log(`${TAG} status endpoint on :${STATUS_PORT}/status`);
});

// ── Start ──

await server.listen();
console.log(`${TAG} listening on :${server.port}`);
console.log(`${TAG} shared TTL=${SHARED_TTL_MS}ms, session TTL=${SESSION_TTL_MS}ms`);

sessionPool.startIdleMonitor();
sessionPool.startHealthChecker();

const localIp = getLocalIp();
registerService(nacosConfig.serviceName, localIp, PORT).catch(() => {});

const shutdown = async () => {
    console.log(`${TAG} shutting down...`);
    clearInterval(ipRefreshTimer);
    statusServer.close();
    await deregisterService(nacosConfig.serviceName, localIp, PORT).catch(() => {});
    await server.close(true);
    process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
