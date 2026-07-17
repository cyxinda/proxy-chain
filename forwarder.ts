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

async function getSharedIp(): Promise<{ ip: string; port: number }> {
    // Return existing IP if valid
    if (currentIp && Date.now() - currentIp.acquiredAt < SHARED_TTL_MS) {
        return currentIp;
    }

    // Dedup concurrent requests
    if (ipPromise) return ipPromise;

    ipPromise = (async () => {
        // Get new IP from DPS API
        const ips = await dpsApiShort.getDpsIps(1);
        if (!ips || ips.length === 0) throw new Error('DPS API: no IP returned');
        currentIp = { ip: ips[0].ip, port: ips[0].port, acquiredAt: Date.now() };
        console.log(`${TAG} [shared] new IP ${currentIp.ip}:${currentIp.port}`);
        return currentIp;
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

// ── Server ──

const server = new Server({
    port: PORT,
    verbose: VERBOSE,
    healthCheckPath: '/healthz',
    prepareRequestFunction: async ({ username, hostname, port, isHttp }) => {
        try {
            const target = `${hostname}:${port}`;

            if (!username) {
                // Shared mode: single IP for all hosts
                const ip = await getSharedIp();
                console.log(`${TAG} [shared] ${isHttp ? 'HTTP' : 'CONNECT'} ${target} via ${ip.ip}:${ip.port}`);
                return {
                    upstreamProxyUrl: `http://${SHORT_PROXY_USER}:${SHORT_PROXY_PASS}@${ip.ip}:${ip.port}`,
                    customTag: { mode: 'shared' as const },
                };
            } else {
                // Session mode: sticky IP per session
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

// ── tunnelConnectFailed: invalidate dead IP ──

server.on('tunnelConnectFailed', async ({ customTag }: { customTag?: { mode: string; id?: string } }) => {
    try {
        if (customTag?.mode === 'shared') {
            invalidateSharedIp();
        } else if (customTag?.mode === 'session' && customTag.id) {
            await sessionPool.recordFailure(customTag.id);
        }
    } catch (err: any) {
        console.error(`${TAG} tunnelConnectFailed handler error: ${err.message}`);
    }
});

// ── Connection stats ──

server.on('connectionClosed', ({ connectionId, stats }: { connectionId: number; stats: any }) => {
    if (VERBOSE) {
        console.log(`${TAG} [${connectionId}] srcTx=${stats.srcTxBytes} srcRx=${stats.srcRxBytes} trgTx=${stats.trgTxBytes} trgRx=${stats.trgRxBytes}`);
    }
});

// ── Proactive IP refresh ──
// Without this, when web-archiver stops requesting (e.g. all IPs expired),
// getSharedIp() is never called -> IP never refreshes -> deadlock.
// web-archiver skips when age >= ipMaxAgeMs (60s), but SHARED_TTL_MS is 90s,
// so we must force-refresh before web-archiver's threshold to avoid the skip.

const IP_MAX_AGE_MS = config.polling?.ipMaxAgeMs || 60_000;
const REFRESH_INTERVAL_MS = Math.max(Math.floor(IP_MAX_AGE_MS * 0.8), 30_000);
const ipRefreshTimer = setInterval(async () => {
    const age = currentIp ? Date.now() - currentIp.acquiredAt : Infinity;
    if (age >= REFRESH_INTERVAL_MS) {
        try {
            // Force invalidate so getSharedIp() fetches a new IP
            // (getSharedIp() returns old IP if age < SHARED_TTL_MS=90s)
            console.log(`${TAG} [shared] proactive refresh (age=${Math.round(age / 1000)}s >= ${Math.round(REFRESH_INTERVAL_MS / 1000)}s)`);
            currentIp = null;
            const ip = await getSharedIp();
            console.log(`${TAG} [shared] refreshed IP ${ip.ip}:${ip.port}`);
        } catch (err: any) {
            console.error(`${TAG} [shared] proactive refresh failed: ${err.message}`);
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
