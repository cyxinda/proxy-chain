/**
 * Unified DPS proxy forwarder v4
 *
 * 共享 IP 池方案：所有 host 共享 IP 池，被封锁时快速轮换。
 *
 * 配置来源：环境变量 > Nacos > 本地 config.yaml > 默认值
 *
 * 双模式:
 *   - shared (无 auth): 共享 IP 池, 供 web-archiver
 *   - session (有 auth): per-session sticky 长命 IP, 供 web-collector
 */

import os from 'node:os';
import http from 'node:http';
import { Server } from './src/server.js';
import { DpsApi } from './src/dps-api.js';
import { SharedPool } from './src/shared-pool.js';
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
const INTERNAL_PORT = config.forwarder.internalPort ?? 3129;

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

// ── 共享 IP 池 ──

const sharedPool = new SharedPool({
    dpsApi: dpsApiShort,
    ttlMs: SHARED_TTL_MS,
    poolSize: config.forwarder.poolSize ?? 10,
    refillThreshold: config.forwarder.refillThreshold ?? 3,
    blockedIpTtlMs: config.forwarder.blockedIpTtlMs ?? 600_000,
});

const sessionPool = new SessionPool({
    dpsApi: dpsApiLong,
    ttlMs: SESSION_TTL_MS,
    failureThreshold: config.forwarder.sessionFailureThreshold,
});

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

// ── Server ──

const server = new Server({
    port: PORT,
    verbose: VERBOSE,
    healthCheckPath: '/healthz',
    prepareRequestFunction: async ({ username, hostname, port, isHttp }) => {
        try {
            const target = `${hostname}:${port}`;
            if (!username) {
                // Shared 模式：从共享池获取 IP（跳过被该 host 封锁的 IP）
                const ip = await sharedPool.acquireForHost(hostname);
                if (!ip) throw new Error('SharedPool: no IP available for ' + hostname);
                console.log(`${TAG} [shared] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} via ${ip.ip}:${ip.port}`);
                return {
                    upstreamProxyUrl: `http://${SHORT_PROXY_USER}:${SHORT_PROXY_PASS}@${ip.ip}:${ip.port}`,
                    customTag: { mode: 'shared' as const, host: hostname, ip: ip.ip },
                };
            } else {
                // Session 模式
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

// ── tunnelConnectFailed：CONNECT 隧道失败时删除死 IP ──

server.on('tunnelConnectFailed', async ({ customTag }: { customTag?: { mode: string; id?: string; host?: string; ip?: string } }) => {
    try {
        if (customTag?.mode === 'shared' && customTag.ip) {
            // TCP/TLS 层失败：从池中删除死 IP（不只是标记封锁）
            const port = parseInt(customTag.ip.split(':')[1] || '0');
            const ipOnly = customTag.ip.split(':')[0];
            sharedPool.removeIp(ipOnly, port || 0);
            console.log(`${TAG} tunnelConnectFailed: removed IP ${customTag.ip}`);
        } else if (customTag?.mode === 'session' && customTag.id) {
            await sessionPool.recordFailure(customTag.id);
        }
    } catch (err: any) {
        console.error(`${TAG} tunnelConnectFailed handler error: ${err.message}`);
    }
});

// ── 流量统计 ──

server.on('connectionClosed', ({ connectionId, stats }: { connectionId: number; stats: any }) => {
    console.log(`${TAG} [${connectionId}] srcTx=${stats.srcTxBytes} srcRx=${stats.srcRxBytes} trgTx=${stats.trgTxBytes} trgRx=${stats.trgRxBytes}`);
});

// ── 内部 HTTP 端点 ──

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
            console.log(`${TAG} [internal] blocked host=${body.host} ip=${body.ip ?? '(all)'} reason=${body.reason ?? '(none)'}`);
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
console.log(`${TAG} poolSize=${config.forwarder.poolSize ?? 10}, internalPort=${INTERNAL_PORT}`);

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
