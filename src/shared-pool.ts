import net from 'node:net';
import type { DpsApi } from './dps-api.js';

const TAG = '[SharedPool]';

interface CachedIp {
    ip: string;
    port: number;
    acquiredAt: number;
}

interface HostEntry extends CachedIp {
    lastRequestTime: number;
}

export class SharedPool {
    private dpsApi: DpsApi;
    private ttlMs: number;
    private bufferSize: number;
    private bufferRefillThreshold: number;
    private blockedIpTtlMs: number;

    private hostIps = new Map<string, HostEntry>();
    private bufferPool: CachedIp[] = [];
    private blockedIps = new Map<string, number>();

    private refillPromise: Promise<void> | null = null;
    private acquiring = new Map<string, Promise<CachedIp | null>>();

    private lastDpsCallAt = 0;
    private dpsBackoffUntil = 0;
    private ipExtractHistory: number[] = [];

    constructor({ dpsApi, ttlMs, bufferSize = 10, bufferRefillThreshold = 3, blockedIpTtlMs = 600_000 }: {
        dpsApi: DpsApi;
        ttlMs: number;
        bufferSize?: number;
        bufferRefillThreshold?: number;
        blockedIpTtlMs?: number;
    }) {
        this.dpsApi = dpsApi;
        this.ttlMs = ttlMs;
        this.bufferSize = bufferSize;
        this.bufferRefillThreshold = bufferRefillThreshold;
        this.blockedIpTtlMs = blockedIpTtlMs;
    }

    async acquireForHost(host: string): Promise<CachedIp | null> {
        const entry = this.hostIps.get(host);
        if (entry && !this.isExpired(entry)) {
            entry.lastRequestTime = Date.now();
            return entry;
        }

        if (this.acquiring.has(host)) {
            return this.acquiring.get(host)!;
        }

        const promise = this.acquireNewForHost(host);
        this.acquiring.set(host, promise);
        try {
            return await promise;
        } finally {
            this.acquiring.delete(host);
        }
    }

    touchLastRequest(host: string): void {
        const entry = this.hostIps.get(host);
        if (entry) entry.lastRequestTime = Date.now();
    }

    bindHostIp(host: string, ip: CachedIp): void {
        this.hostIps.set(host, { ...ip, lastRequestTime: Date.now() });
    }

    popFromBufferPool(host: string): CachedIp | null {
        const kept: CachedIp[] = [];
        let result: CachedIp | null = null;
        while (this.bufferPool.length > 0) {
            const ip = this.bufferPool.shift()!;
            if (this.isExpired(ip)) continue;
            if (!result && !this.isBlocked(host, ip.ip)) {
                result = ip;
            } else {
                kept.push(ip);
            }
        }
        this.bufferPool.push(...kept);
        return result;
    }

    getAnyAvailableIp(host: string): CachedIp | null {
        for (const [, entry] of this.hostIps) {
            if (this.isExpired(entry)) continue;
            if (this.isBlocked(host, entry.ip)) continue;
            return { ip: entry.ip, port: entry.port, acquiredAt: entry.acquiredAt };
        }
        return null;
    }

    async refillBufferPool(): Promise<void> {
        if (this.refillPromise) return this.refillPromise;
        this.refillPromise = this.doRefill();
        try {
            await this.refillPromise;
        } finally {
            this.refillPromise = null;
        }
    }

    invalidateHost(host: string, ip?: string): void {
        const entry = this.hostIps.get(host);
        const effectiveIp = ip ?? entry?.ip;
        this.hostIps.delete(host);
        if (effectiveIp) {
            this.blockedIps.set(`${host}:${effectiveIp}`, Date.now() + this.blockedIpTtlMs);
            console.log(`${TAG} invalidated ${host} IP=${effectiveIp}, blocked for ${this.blockedIpTtlMs / 1000}s`);
        } else {
            console.log(`${TAG} invalidated ${host} (no IP bound)`);
        }
    }

    addBlocked(host: string, ip: string): void {
        this.blockedIps.set(`${host}:${ip}`, Date.now() + this.blockedIpTtlMs);
    }

    startIdleMonitor(): void {
        setInterval(() => {
            const now = Date.now();
            for (const [host, entry] of this.hostIps) {
                const idle = now - entry.lastRequestTime;
                if (idle > this.ttlMs * 2) {
                    if (!this.isExpired(entry)) {
                        this.bufferPool.push({ ip: entry.ip, port: entry.port, acquiredAt: entry.acquiredAt });
                    }
                    this.hostIps.delete(host);
                    console.log(`${TAG} idle host ${host} released (idle=${Math.round(idle / 1000)}s)`);
                }
            }
        }, 60_000);
    }

    startHealthChecker(): void {
        setInterval(async () => {
            for (const [host, entry] of this.hostIps) {
                if (this.isExpired(entry)) {
                    this.hostIps.delete(host);
                    continue;
                }
                const alive = await this.checkHealth({ ip: entry.ip, port: entry.port }, host);
                if (!alive) {
                    console.log(`${TAG} periodic check failed for ${host}, discarding IP ${entry.ip}`);
                    this.hostIps.delete(host);
                }
            }
        }, 5 * 60_000);
    }

    // ---- internals ----

    private async acquireNewForHost(host: string): Promise<CachedIp | null> {
        let ip = this.popFromBufferPool(host);
        if (ip) return ip;

        await this.refillBufferPool();
        ip = this.popFromBufferPool(host);
        if (ip) return ip;

        const fallback = this.getAnyAvailableIp(host);
        if (fallback) {
            console.log(`${TAG} ${host} reusing shared IP ${fallback.ip}:${fallback.port} (buffer empty, DPS rate limited)`);
            return fallback;
        }

        console.error(`${TAG} no IP available for ${host} (buffer empty, no shared IP, DPS rate limited)`);
        return null;
    }

    private canCallDps(): boolean {
        const now = Date.now();
        if (now < this.dpsBackoffUntil) return false;
        if (now - this.lastDpsCallAt < 20_000) return false;
        const windowMs = 12 * 60 * 1000;
        this.ipExtractHistory = this.ipExtractHistory.filter(ts => now - ts < windowMs);
        return this.ipExtractHistory.length < 180;
    }

    private async doRefill(): Promise<void> {
        if (this.bufferPool.length >= this.bufferRefillThreshold) return;
        if (!this.canCallDps()) {
            console.warn(`${TAG} refill skipped: DPS API rate limited (extracted ${this.ipExtractHistory.length}/180 in 12min window)`);
            return;
        }
        const need = Math.min(this.bufferSize - this.bufferPool.length, 10);
        if (need <= 0) return;
        this.lastDpsCallAt = Date.now();
        try {
            const ips = await this.dpsApi.getDpsIps(need);
            this.ipExtractHistory.push(Date.now());
            for (const ip of ips) {
                this.bufferPool.push({ ip: ip.ip, port: ip.port, acquiredAt: Date.now() });
            }
            console.log(`${TAG} buffer refilled with ${ips.length} IPs (total=${this.bufferPool.length}, extracted=${this.ipExtractHistory.length}/180)`);
        } catch (err: any) {
            console.error(`${TAG} refill failed: ${err.message}`);
            if (/超限|最多|rate/i.test(err.message)) {
                this.dpsBackoffUntil = Date.now() + 300_000;
                console.warn(`${TAG} DPS API rate limited, backing off for 300s`);
            }
        }
    }

    private isBlocked(host: string, ip: string): boolean {
        const key = `${host}:${ip}`;
        const expiry = this.blockedIps.get(key);
        if (!expiry) return false;
        if (Date.now() >= expiry) {
            this.blockedIps.delete(key);
            return false;
        }
        return true;
    }

    private isExpired(entry: { acquiredAt: number }): boolean {
        return Date.now() - entry.acquiredAt >= this.ttlMs;
    }

    private checkHealth(ipObj: { ip: string; port: number }, targetHost: string): Promise<boolean> {
        const target = targetHost || 'www.baidu.com';
        return new Promise<boolean>((resolve) => {
            const socket = net.connect({ host: ipObj.ip, port: ipObj.port });
            const cleanup = (ok: boolean) => { try { socket.destroy(); } catch {} resolve(ok); };
            const timer = setTimeout(() => cleanup(false), 4_000);
            socket.on('error', () => { clearTimeout(timer); cleanup(false); });
            socket.on('close', () => clearTimeout(timer));
            socket.once('connect', () => {
                const auth = Buffer.from(
                    `${this.dpsApi.proxyUsername}:${this.dpsApi.proxyPassword || ''}`
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
}
