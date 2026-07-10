/**
 * Unified DPS proxy forwarder v2
 *
 * 基于 proxy-chain v3.0.0 源码二次开发。
 * proxy-chain Server 负责 CONNECT 隧道建立和数据转发，
 * 本入口通过 prepareRequestFunction 回调选择 DPS IP。
 *
 * 配置来源：环境变量 > Nacos > 本地 config.yaml > 默认值
 * 启动时向 Nacos 注册服务实例，关闭时注销。
 *
 * 支持双模式:
 *   - shared (无 auth): 全局共享短命 IP, 供 web-archiver
 *   - session (有 auth): per-session sticky 长命 IP, 供 web-collector
 */

import os from 'node:os';
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

// ── 初始化双订单 DPS API ──

// 短命池订单: IP 寿命 1-2min, 供 web-archiver (shared 模式)
const dpsApiShort = new DpsApi({
    secretId: config.dpsShort.secretId,
    secretKey: config.dpsShort.secretKey,
    proxyUsername: config.dpsShort.proxyUsername,
    proxyPassword: config.dpsShort.proxyPassword,
    apiEndpoint: config.dpsShort.apiEndpoint,
    orderKey: 'short',
});

// 长命池订单: IP 寿命 15-20min, 供 web-collector (session 模式)
const dpsApiLong = new DpsApi({
    secretId: config.dpsLong.secretId,
    secretKey: config.dpsLong.secretKey,
    proxyUsername: config.dpsLong.proxyUsername,
    proxyPassword: config.dpsLong.proxyPassword,
    apiEndpoint: config.dpsLong.apiEndpoint,
    orderKey: 'long',
});

// DPS 代理凭证（短命池和长命池可能不同）
const SHORT_PROXY_USER = config.dpsShort.proxyUsername;
const SHORT_PROXY_PASS = config.dpsShort.proxyPassword;
const LONG_PROXY_USER = config.dpsLong.proxyUsername;
const LONG_PROXY_PASS = config.dpsLong.proxyPassword;

// ── 初始化双池 ──

const sharedPool = new SharedPool({ dpsApi: dpsApiShort, ttlMs: SHARED_TTL_MS });
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

// ── 创建 proxy-chain Server ──

const server = new Server({
    port: PORT,
    verbose: VERBOSE,
    prepareRequestFunction: async ({ username, hostname, port, isHttp }) => {
        try {
            const target = `${hostname}:${port}`;
            if (!username) {
                // Shared 模式: 从 SharedPool 获取 DPS IP（用短命池凭证）
                const ip = await sharedPool.acquire();
                if (!ip) throw new Error('SharedPool: failed to acquire DPS IP');
                sharedPool.touchLastRequest();
                console.log(`${TAG} [shared] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} via ${ip.ip}:${ip.port}`);
                return {
                    upstreamProxyUrl: `http://${SHORT_PROXY_USER}:${SHORT_PROXY_PASS}@${ip.ip}:${ip.port}`,
                    customTag: { mode: 'shared' as const },
                };
            } else {
                // Session 模式: 从 SessionPool 获取 sticky IP（用长命池凭证）
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
            throw err; // proxy-chain 会转为 502 响应
        }
    },
});

// ── 事件处理: 故障转移 ──

server.on('tunnelConnectFailed', async ({ customTag }: { customTag?: { mode: string; id?: string } }) => {
    try {
        if (customTag?.mode === 'shared') {
            await sharedPool.invalidateWithCooldown();
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

// ── 启动 ──

await server.listen();
console.log(`${TAG} listening on :${server.port}`);
console.log(`${TAG} shared TTL=${SHARED_TTL_MS}ms, session TTL=${SESSION_TTL_MS}ms`);
console.log(`${TAG} config source: Nacos ${process.env.NACOS_SERVER_ADDR || '172.16.11.229:38848'}`);

// 启动空闲监测 + 定期健康检查
sharedPool.startIdleMonitor();
sessionPool.startIdleMonitor();
sessionPool.startHealthChecker();

// Nacos 服务注册（服务名从 bootstrap.yaml 读取）
const localIp = getLocalIp();
registerService(nacosConfig.serviceName, localIp, PORT).catch(() => {});

// 优雅关闭
const shutdown = async () => {
    console.log(`${TAG} shutting down...`);
    await deregisterService(nacosConfig.serviceName, localIp, PORT).catch(() => {});
    await server.close(true);
    process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
