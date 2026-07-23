const TAG = '[DpsApi]';

export class DpsApi {
    secretId: string;
    secretKey: string;
    proxyUsername: string;
    proxyPassword: string;
    orderKey: string;
    private apiEndpoint: string;
    private secretToken: string | null = null;
    private tokenFetchedAt = 0;
    private readonly TOKEN_TTL_MS = 50 * 60 * 1000; // 50min

    constructor({ secretId, secretKey, proxyUsername, proxyPassword, orderKey, apiEndpoint }: {
        secretId: string;
        secretKey: string;
        proxyUsername: string;
        proxyPassword: string;
        orderKey: string;
        apiEndpoint?: string;
    }) {
        if (!secretId || !secretKey) throw new Error(`[${orderKey}] secretId and secretKey are required`);
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.orderKey = orderKey;
        // 归一化 base URL：去掉末尾的 /getdps/ 或 /getdps
        this.apiEndpoint = (apiEndpoint || 'https://dps.kdlapi.com/api')
            .replace(/\/getdps\/?$/, '')
            .replace(/\/+$/, '');
    }

    async getDpsIp(area?: string): Promise<{ ip: string; port: number }> {
        const ips = await this.getDpsIps(1, area);
        return ips[0];
    }

    async getDpsIps(num: number, area?: string): Promise<{ ip: string; port: number }[]> {
        return this._getDpsIps(num, 0, area);
    }

    private async _getDpsIps(num: number, retry: number, area?: string): Promise<{ ip: string; port: number }[]> {
        const token = await this.ensureToken();
        const url = `${this.apiEndpoint}/getdps`;
        const params = new URLSearchParams({
            secret_id: this.secretId,
            signature: token,
            num: String(Math.max(1, num)),
            format: 'text',
            sep: '1',
        });
        // area 指定省级行政区划代码（如 310000=上海），让 DPS 分配对应区域的 IP。
        // 不传时 DPS 默认全国随机分配。传入无效代码时 DPS 返回错误，由调用方感知。
        if (area) params.set('area', area);

        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), 8_000);
        try {
            const res = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: params.toString(),
                signal: ctrl.signal,
            });
            const text = (await res.text()).trim();

            if (!res.ok) {
                if (/signature|token|expired/i.test(text)) this.secretToken = null;
                throw new Error(`getdps HTTP ${res.status} body=${truncate(text)}`);
            }

            if (text.startsWith('{')) {
                try {
                    const j = JSON.parse(text);
                    if (typeof j === 'object' && j.code != null && j.code !== 0) {
                        if (/signature|token/i.test(j.msg || '')) this.secretToken = null;
                        throw new Error(`getdps error: ${j.msg || `code=${j.code}`}`);
                    }
                } catch (e: any) {
                    if (e.code != null) throw e;
                }
            }

            if (/ERROR.*token.*expired|secret_token.*expired|signature.*invalid/i.test(text)) {
                if (retry >= 1) throw new Error(`getdps token still expired after refresh: ${truncate(text)}`);
                console.warn(`${TAG}[${this.orderKey}] token expired, refreshing and retrying`);
                this.secretToken = null;
                return this._getDpsIps(num, retry + 1, area);
            }

            const lines = text.split(/\n/).map(l => l.trim()).filter(Boolean);
            const results: { ip: string; port: number }[] = [];
            for (const line of lines) {
                const m = /^([0-9.]+):(\d+)$/.exec(line.split(/\s+/)[0]);
                if (m) results.push({ ip: m[1], port: parseInt(m[2], 10) });
            }
            if (results.length === 0) throw new Error(`getdps unexpected body: ${truncate(text)}`);
            return results;
        } finally {
            clearTimeout(timer);
        }
    }

    private async ensureToken(): Promise<string> {
        const now = Date.now();
        if (this.secretToken && now - this.tokenFetchedAt < this.TOKEN_TTL_MS) {
            return this.secretToken;
        }

        const url = `${this.apiEndpoint}/get_secret_token`;
        const params = new URLSearchParams({
            secret_id: this.secretId,
            secret_key: this.secretKey,
        });

        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params.toString(),
        });
        const text = (await res.text()).trim();

        if (res.ok && text) {
            if (text.startsWith('{')) {
                const j = JSON.parse(text);
                if (j.code === 0 && j.data?.secret_token) {
                    this.secretToken = j.data.secret_token;
                    this.tokenFetchedAt = now;
                    console.log(`${TAG}[${this.orderKey}] token refreshed`);
                    return this.secretToken!;
                }
                throw new Error(`get_secret_token error: ${text}`);
            } else if (!text.startsWith('ERROR')) {
                this.secretToken = text;
                this.tokenFetchedAt = now;
                return this.secretToken!;
            }
        }

        throw new Error(`[${this.orderKey}] get_secret_token failed: HTTP ${res.status}`);
    }
}

function truncate(s: string | null): string {
    if (s == null) return '';
    return s.length <= 200 ? s : s.slice(0, 200) + '...';
}
