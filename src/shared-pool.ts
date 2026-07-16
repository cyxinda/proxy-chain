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

interface PendingInvalidation {
    host: string;
    ip: string;
    reason: string;
    queuedAt: number;
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

    private dpsBackoffUntil = 0;
    private ipExtractHistory: number[] = [];

    // 待删除队列：DPS 退避期间暂存的失效 IP，退避结束后处理
    private pendingInvalidations: PendingInvalidation[] = [];

    constructor({ dpsApi, ttlMs, bufferSize = 30, bufferRefillThreshold = 5, blockedIpTtlMs = 600_000 }: {
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
        if (entry && (!this.isExpired(entry) || !this.canCallDps())) {
            // 有效期内直接返回；退避期间即使过期也继续使用（无法获取新 IP）
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
        const expired: CachedIp[] = [];
        let result: CachedIp | null = null;
        while (this.bufferPool.length > 0) {
            const ip = this.bufferPool.shift()!;
            if (this.isExpired(ip)) {
                expired.push(ip);
            } else if (!result && !this.isBlocked(host, ip.ip)) {
                result = ip;
            } else {
                kept.push(ip);
            }
        }
        // 如果没有可用的未过期 IP, 尝试过期 IP (复用一次)
        if (!result && expired.length > 0) {
            for (const ip of expired) {
                if (!this.isBlocked(host, ip.ip)) {
                    result = ip;
                    console.log(`${TAG} reusing expired IP ${ip.ip}:${ip.port} for ${host} (age=${Math.round((Date.now() - ip.acquiredAt) / 1000)}s)`);
                    break;
                }
                kept.push(ip);
            }
        }
        if (!result) {
            kept.push(...expired.filter(ip => !kept.includes(ip)));
        }
        this.bufferPool.push(...kept);
        return result;
    }

    getAnyAvailableIp(host: string): CachedIp | null {
        const dpsBackoff = !this.canCallDps();
        for (const [, entry] of this.hostIps) {
            if (!dpsBackoff && this.isExpired(entry)) continue;
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

    /**
     * 封锁 host 的 IP。
     * - DPS API 可用时：立即从 hostIps 删除，加入 blockedIps
     * - DPS API 退避中：不删除 IP（保留当前 IP 继续使用），加入待处理队列
     *   退避结束后自动处理
     */
    invalidateHost(host: string, ip?: string): void {
        const entry = this.hostIps.get(host);
        const effectiveIp = ip ?? entry?.ip;

        if (this.canCallDps()) {
            // DPS API 可用：立即删除，下次请求可获取新 IP
            this.hostIps.delete(host);
            if (effectiveIp) {
                this.blockedIps.set(`${host}:${effectiveIp}`, Date.now() + this.blockedIpTtlMs);
                console.log(`${TAG} invalidated ${host} IP=${effectiveIp}, blocked for ${this.blockedIpTtlMs / 1000}s`);
            } else {
                console.log(`${TAG} invalidated ${host} (no IP bound)`);
            }
        } else if (effectiveIp) {
            // DPS API 退避中：暂不删除，加入待处理队列
            this.pendingInvalidations.push({
                host,
                ip: effectiveIp,
                reason: 'feedback',
                queuedAt: Date.now(),
            });
            console.log(`${TAG} deferred invalidation for ${host} IP=${effectiveIp} (DPS backoff active, pending=${this.pendingInvalidations.length})`);
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
            const dpsAvailable = this.canCallDps();
            for (const [host, entry] of this.hostIps) {
                if (this.isExpired(entry)) {
                    this.hostIps.delete(host);
                    continue;
                }
                const alive = await this.checkHealth({ ip: entry.ip, port: entry.port }, host);
                if (!alive) {
                    if (dpsAvailable) {
                        // DPS 可用：立即删除，可获取新 IP
                        console.log(`${TAG} periodic check failed for ${host}, discarding IP ${entry.ip}`);
                        this.hostIps.delete(host);
                    } else {
                        // DPS 退避中：暂不删除，加入待处理队列
                        this.pendingInvalidations.push({
                            host,
                            ip: entry.ip,
                            reason: 'health_check',
                            queuedAt: Date.now(),
                        });
                        console.log(`${TAG} periodic check failed for ${host} IP=${entry.ip}, deferred (DPS backoff active)`);
                    }
                }
            }
        }, 5 * 60_000);
    }

    /**
     * 定期处理待删除队列（每 60s）
     * 退避结束后，即使 bufferPool 不需要补充，也执行延迟的 invalidation
     */
    startPendingInvalidationProcessor(): void {
        setInterval(() => {
            this.processPendingInvalidations();
        }, 60_000);
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
        // 诊断模式：取消 20s 节流，允许快速获取 IP
        // if (now - this.lastDpsCallAt < 20_000) return false;
        const windowMs = 12 * 60 * 1000;
        this.ipExtractHistory = this.ipExtractHistory.filter(ts => now - ts < windowMs);
        return this.ipExtractHistory.length < 200;
    }

    /** 判断 DPS API 不可用的原因（用于日志） */
    private getDpsBlockReason(): string {
        const now = Date.now();
        if (now < this.dpsBackoffUntil) return `backoff until ${new Date(this.dpsBackoffUntil).toISOString()}`;
        const windowMs = 12 * 60 * 1000;
        const count = this.ipExtractHistory.filter(ts => now - ts < windowMs).length;
        if (count >= 200) return `extracted ${count}/200 in 12min window`;
        return 'available';
    }

    /**
     * 处理待删除队列：退避结束后，执行之前延迟的 invalidation
     */
    private processPendingInvalidations(): void {
        if (this.pendingInvalidations.length === 0) return;
        if (!this.canCallDps()) return;

        const now = Date.now();
        const processed: number[] = [];

        for (let i = 0; i < this.pendingInvalidations.length; i++) {
            const pending = this.pendingInvalidations[i];
            // 超过 5 分钟的待处理项过期丢弃（IP 可能已自然过期）
            if (now - pending.queuedAt > 5 * 60_000) {
                processed.push(i);
                continue;
            }

            const entry = this.hostIps.get(pending.host);
            // 只删除当前仍绑定同一 IP 的 host
            if (entry && entry.ip === pending.ip) {
                this.hostIps.delete(pending.host);
                this.blockedIps.set(`${pending.host}:${pending.ip}`, now + this.blockedIpTtlMs);
                console.log(`${TAG} processed deferred invalidation: ${pending.host} IP=${pending.ip} reason=${pending.reason}`);
            }
            processed.push(i);
        }

        // 从后往前删除，避免索引偏移
        for (let i = processed.length - 1; i >= 0; i--) {
            this.pendingInvalidations.splice(processed[i], 1);
        }

        if (processed.length > 0) {
            console.log(`${TAG} processed ${processed.length} pending invalidations, ${this.pendingInvalidations.length} remaining`);
        }
    }

    private async doRefill(): Promise<void> {
        // 退避结束后先处理待删除队列
        this.processPendingInvalidations();

        if (this.bufferPool.length >= this.bufferRefillThreshold) return;
        if (!this.canCallDps()) {
            console.warn(`${TAG} refill skipped: DPS API unavailable (${this.getDpsBlockReason()})`);
            return;
        }
        const need = Math.min(this.bufferSize - this.bufferPool.length, 50);
        if (need <= 0) return;
        try {
            const ips = await this.dpsApi.getDpsIps(need);
            this.ipExtractHistory.push(Date.now());
            for (const ip of ips) {
                this.bufferPool.push({ ip: ip.ip, port: ip.port, acquiredAt: Date.now() });
            }
            console.log(`${TAG} buffer refilled with ${ips.length} IPs (total=${this.bufferPool.length}, extracted=${this.ipExtractHistory.length}/200)`);
        } catch (err: any) {
            console.error(`${TAG} refill failed: ${err.message}`);
            if (/超限|最多|rate/i.test(err.message)) {
                this.dpsBackoffUntil = Date.now() + 300_000;
                console.warn(`${TAG} DPS API rate limited, backing off for 300s until ${new Date(this.dpsBackoffUntil).toISOString()}`);
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
