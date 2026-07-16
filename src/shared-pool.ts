import net from 'node:net';
import type { DpsApi } from './dps-api.js';

const TAG = '[SharedPool]';

interface CachedIp {
    ip: string;
    port: number;
    acquiredAt: number;
}

export class SharedPool {
    private dpsApi: DpsApi;
    private ttlMs: number;
    private poolSize: number;
    private refillThreshold: number;
    private blockedIpTtlMs: number;

    // IP 共享池：所有 host 共用
    private ipPool: CachedIp[] = [];
    // 被封锁的 host+IP 组合（10min TTL）
    private blockedIps = new Map<string, number>();

    private refillPromise: Promise<void> | null = null;
    private dpsBackoffUntil = 0;
    private ipExtractHistory: number[] = [];

    constructor({ dpsApi, ttlMs, poolSize = 10, refillThreshold = 3, blockedIpTtlMs = 600_000 }: {
        dpsApi: DpsApi;
        ttlMs: number;
        poolSize?: number;
        refillThreshold?: number;
        blockedIpTtlMs?: number;
    }) {
        this.dpsApi = dpsApi;
        this.ttlMs = ttlMs;
        this.poolSize = poolSize;
        this.refillThreshold = refillThreshold;
        this.blockedIpTtlMs = blockedIpTtlMs;
    }

    /**
     * 获取一个可用 IP 给指定 host
     * - 从共享池中取一个未过期、未被该 host 封锁的 IP
     * - 池空时自动补充
     * - 所有 host 共享同一个 IP 池
     */
    async acquireForHost(host: string): Promise<CachedIp | null> {
        // 1. 清理过期 IP
        this.removeExpiredIps();

        // 2. 从池中找一个未被该 host 封锁的 IP
        let ip = this.findAvailableIp(host);
        if (ip) return ip;

        // 3. 池空或所有 IP 都被该 host 封锁 → 补充
        await this.refillPool();
        ip = this.findAvailableIp(host);
        if (ip) return ip;

        // 4. 补充后仍无可用 IP → 尝试复用过期 IP
        ip = this.findExpiredIp(host);
        if (ip) {
            console.log(`${TAG} reusing expired IP ${ip.ip}:${ip.port} for ${host}`);
            return ip;
        }

        console.error(`${TAG} no IP available for ${host} (pool empty, all blocked or DPS limited)`);
        return null;
    }

    /**
     * 标记 host+IP 被封锁（10min 后自动解除）
     * 不删除 IP 本身，只是让该 host 跳过这个 IP
     */
    markBlocked(host: string, ip: string): void {
        const key = `${host}:${ip}`;
        if (!this.blockedIps.has(key)) {
            this.blockedIps.set(key, Date.now() + this.blockedIpTtlMs);
            console.log(`${TAG} blocked ${host}+${ip} for ${this.blockedIpTtlMs / 1000}s`);
        }
    }

    /**
     * 反馈接口：web-archiver 报告封锁
     * 只标记 blocked，不删除 IP（IP 可能对其他 host 仍可用）
     */
    invalidateHost(host: string, ip?: string): void {
        if (ip) {
            this.markBlocked(host, ip);
        } else {
            // 没有指定 IP，标记该 host 对所有当前 IP 都封锁（激进策略）
            for (const entry of this.ipPool) {
                this.markBlocked(host, entry.ip);
            }
            console.log(`${TAG} blocked all current IPs for ${host}`);
        }
    }

    /**
     * 从池中移除指定 IP（当 IP 确认不可用时调用）
     * 同时标记该 IP 对所有 host 封锁，防止被重新分配
     */
    removeIp(ip: string, port: number): void {
        const idx = this.ipPool.findIndex(p => p.ip === ip && p.port === port);
        if (idx >= 0) {
            this.ipPool.splice(idx, 1);
            console.log(`${TAG} removed dead IP ${ip}:${port} from pool (remaining=${this.ipPool.length})`);
        }
    }

    async refillPool(): Promise<void> {
        if (this.refillPromise) return this.refillPromise;
        this.refillPromise = this.doRefill();
        try {
            await this.refillPromise;
        } finally {
            this.refillPromise = null;
        }
    }

    startIdleMonitor(): void {
        setInterval(() => {
            this.removeExpiredIps();
            // 清理过期的 blockedIps
            const now = Date.now();
            for (const [key, expiry] of this.blockedIps) {
                if (now >= expiry) this.blockedIps.delete(key);
            }
        }, 60_000);
    }

    startHealthChecker(): void {
        setInterval(async () => {
            const toRemove: CachedIp[] = [];
            for (const ip of this.ipPool) {
                if (this.isExpired(ip)) {
                    toRemove.push(ip);
                    continue;
                }
                const alive = await this.checkHealth(ip);
                if (!alive) {
                    console.log(`${TAG} health check failed for ${ip.ip}:${ip.port}, removing`);
                    toRemove.push(ip);
                }
            }
            for (const ip of toRemove) {
                this.removeFromPool(ip);
            }
        }, 5 * 60_000);
    }

    // ---- internals ----

    private findAvailableIp(host: string): CachedIp | null {
        for (const ip of this.ipPool) {
            if (!this.isBlocked(host, ip.ip)) return ip;
        }
        return null;
    }

    private findExpiredIp(host: string): CachedIp | null {
        for (const ip of this.ipPool) {
            if (this.isExpired(ip) && !this.isBlocked(host, ip.ip)) return ip;
        }
        return null;
    }

    private removeExpiredIps(): void {
        const now = Date.now();
        this.ipPool = this.ipPool.filter(ip => now - ip.acquiredAt < this.ttlMs);
    }

    private removeFromPool(ip: CachedIp): void {
        const idx = this.ipPool.findIndex(p => p.ip === ip.ip && p.port === ip.port);
        if (idx >= 0) this.ipPool.splice(idx, 1);
    }

    private canCallDps(): boolean {
        const now = Date.now();
        if (now < this.dpsBackoffUntil) return false;
        const windowMs = 12 * 60 * 1000;
        this.ipExtractHistory = this.ipExtractHistory.filter(ts => now - ts < windowMs);
        return this.ipExtractHistory.length < 200;
    }

    private getDpsBlockReason(): string {
        const now = Date.now();
        if (now < this.dpsBackoffUntil) return `backoff until ${new Date(this.dpsBackoffUntil).toISOString()}`;
        const windowMs = 12 * 60 * 1000;
        const count = this.ipExtractHistory.filter(ts => now - ts < windowMs).length;
        if (count >= 200) return `extracted ${count}/200 in 12min window`;
        return 'available';
    }

    private async doRefill(): Promise<void> {
        if (this.ipPool.length >= this.refillThreshold) return;
        if (!this.canCallDps()) {
            console.warn(`${TAG} refill skipped: DPS API unavailable (${this.getDpsBlockReason()})`);
            return;
        }
        const need = Math.min(this.poolSize - this.ipPool.length, 10);
        if (need <= 0) return;
        try {
            const ips = await this.dpsApi.getDpsIps(need);
            this.ipExtractHistory.push(Date.now());
            for (const ip of ips) {
                this.ipPool.push({ ip: ip.ip, port: ip.port, acquiredAt: Date.now() });
            }
            console.log(`${TAG} pool refilled with ${ips.length} IPs (total=${this.ipPool.length}, extracted=${this.ipExtractHistory.length}/200)`);
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

    private checkHealth(ipObj: { ip: string; port: number }): Promise<boolean> {
        const target = 'www.baidu.com';
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
