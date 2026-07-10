/**
 * Nacos 客户端封装 -- 配置拉取（gRPC，nacos-config）+ 服务注册（v3 admin HTTP API）。
 *
 * 配置来源优先级：环境变量 > bootstrap.yaml > 硬编码默认值
 * 配置拉取沿用 nacos-config（gRPC，兼容 Nacos 3.x）。
 * 服务注册改为直接调用 Nacos 3.x 的 v3 admin naming HTTP API。
 */

import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { NacosConfigClient } from 'nacos';
import { load as yamlLoad } from 'js-yaml';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---- 加载 bootstrap.yaml ----

function loadBootstrap() {
  const candidates = [
    '/app/bootstrap.yaml',
    '/app/bootstrap.yml',
    resolve(__dirname, '..', 'bootstrap.yaml'),
    resolve(__dirname, '..', 'bootstrap.yml'),
  ];
  for (const p of candidates) {
    try {
      const raw = readFileSync(p, 'utf-8');
      console.log(`[nacos] bootstrap loaded from ${p}`);
      return yamlLoad(raw) ?? {};
    } catch { /* skip */ }
  }
  return {};
}

const bootstrap = loadBootstrap();
const bsNacos = bootstrap.nacos || {};
const bsConfig = bsNacos.config || {};
const bsDiscovery = bsNacos.discovery || {};

// ---- 解析 ${ENV_VAR:default} 语法 ----

/**
 * 解析 bootstrap.yaml 中的 ${VAR:default} 语法。
 * 如果环境变量存在则用环境变量，否则用 default 值。
 */
function resolveEnvVar(val) {
  if (typeof val !== 'string') return val;
  const m = /^\$\{([^:}]+)(?::([^}]*))?\}$/.exec(val);
  if (m) {
    return process.env[m[1]] ?? m[2] ?? '';
  }
  return val;
}

function resolveExpr(obj, key) {
  return resolveEnvVar(obj[key]);
}

// ---- 配置解析（环境变量 > bootstrap ${VAR:default} > 默认值）----

function env(key, fallback) {
  return process.env[key] || fallback;
}

export const serverAddr = env('NACOS_SERVER_ADDR', resolveExpr(bsNacos, 'server-addr') || '172.16.11.229:38848');
export const namespace = env('NACOS_NAMESPACE', resolveExpr(bsNacos, 'namespace') || 'dev');
const username = env('NACOS_USERNAME', resolveExpr(bsNacos, 'username') || 'nacos');
const password = env('NACOS_PASSWORD', resolveExpr(bsNacos, 'password') || 'nacos');

// Nacos 3.x 的 public 命名空间 id 为 'public'
const nsId = namespace || 'public';
const instanceApi = `http://${serverAddr}/nacos/v3/admin/ns/instance`;
const loginApi = `http://${serverAddr}/nacos/v1/auth/login`;

// 导出配置供 config.js 使用
export const nacosConfig = {
  dataId: env('NACOS_CONFIG_DATA_ID', resolveExpr(bsConfig, 'data-id') || 'proxy-chain-dev.yaml'),
  group: env('NACOS_CONFIG_GROUP', resolveExpr(bsConfig, 'group') || 'DEFAULT_GROUP'),
  sharedDataId: env('NACOS_SHARED_CONFIG_DATA_ID', resolveExpr(bsConfig, 'shared-data-id') || 'application-dev.yml'),
  sharedGroup: env('NACOS_SHARED_CONFIG_GROUP', resolveExpr(bsConfig, 'shared-group') || 'DEFAULT_GROUP'),
  serviceName: env('NACOS_SERVICE_NAME', resolveExpr(bsDiscovery, 'service-name') || 'proxy-chain'),
};

let configClient = null;

function getConfigClient() {
  if (!configClient) {
    configClient = new NacosConfigClient({ serverAddr, namespace, username, password });
  }
  return configClient;
}

// ---- 启动日志 ----

console.log(`[nacos] ====== Nacos 客户端初始化 ======`);
console.log(`[nacos] 服务器地址: ${serverAddr}`);
console.log(`[nacos] 命名空间: ${namespace || '(空/public)'}`);
console.log(`[nacos] 配置文件: ${nacosConfig.dataId}`);
console.log(`[nacos] 共享配置: ${nacosConfig.sharedDataId}`);
console.log(`[nacos] 服务名: ${nacosConfig.serviceName}`);
console.log(`[nacos] ================================`);

// ---- v3 admin API token 管理 ----
let token = null;
let tokenExpireAt = 0;

async function getToken() {
  if (token && Date.now() < tokenExpireAt - 60000) return token;
  const res = await fetch(loginApi, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: `username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`,
  });
  if (!res.ok) throw new Error(`nacos login http ${res.status}`);
  const j = await res.json();
  token = j.accessToken;
  tokenExpireAt = Date.now() + (j.tokenTtl || 18000) * 1000;
  return token;
}

async function v3Request(url, init) {
  const tk = await getToken();
  const sep = url.includes('?') ? '&' : '?';
  let res = await fetch(`${url}${sep}accessToken=${encodeURIComponent(tk)}`, init);
  if (res.status === 401) {
    token = null;
    const tk2 = await getToken();
    res = await fetch(`${url}${sep}accessToken=${encodeURIComponent(tk2)}`, init);
  }
  return res;
}

/**
 * 从 Nacos 拉取配置并解析为对象。
 * @param {string} [dataId]  默认用 bootstrap 配置
 * @param {string} [group]   默认用 bootstrap 配置
 * @returns {Promise<object|null>}
 */
export async function fetchNacosConfig(dataId, group) {
  const id = dataId || nacosConfig.dataId;
  const g = group || nacosConfig.group;
  try {
    const client = getConfigClient();
    console.log(`[nacos] 拉取配置: dataId=${id}, group=${g}`);

    const content = await client.getConfig(id, g);

    if (!content) {
      console.log(`[nacos] 配置内容为空`);
      return null;
    }

    console.log(`[nacos] 配置内容长度: ${content.length} 字节`);
    const parsed = yamlLoad(content) ?? {};
    console.log(`[nacos] 解析后配置项: ${Object.keys(parsed).join(', ')}`);
    return parsed;
  } catch (err) {
    console.warn(`[nacos] fetchNacosConfig(${id}) failed: ${err.message}`);
    return null;
  }
}

// ---- 服务注册（v3 admin naming HTTP API）----
const HEARTBEAT_INTERVAL_MS = 5000;
const heartbeats = new Map(); // `${serviceName}|${ip}|${port}` -> interval

function instanceParams(serviceName, ip, port, metadata = {}) {
  return new URLSearchParams({
    serviceName,
    groupName: 'DEFAULT_GROUP',
    namespaceId: nsId,
    ip,
    port: String(port),
    weight: '1',
    healthy: 'true',
    enabled: 'true',
    ephemeral: 'true',
    clusterName: 'DEFAULT',
    metadata: JSON.stringify({ version: '2.0.0', ...metadata }),
  });
}

async function registerOnce(serviceName, ip, port, metadata) {
  const params = instanceParams(serviceName, ip, port, metadata);
  const res = await v3Request(`${instanceApi}?${params}`, { method: 'POST' });
  const j = await res.json().catch(() => ({}));
  if (j.code !== 0) throw new Error(j.message || `nacos register http ${res.status}`);
}

/**
 * 注册服务实例到 Nacos（ephemeral），并启动心跳定时刷新。
 */
export async function registerService(serviceName, ip, port, metadata = {}) {
  const name = serviceName || nacosConfig.serviceName;
  try {
    await registerOnce(name, ip, port, metadata);
    console.log(`[nacos] registered ${name} ${ip}:${port}`);
    const key = `${name}|${ip}|${port}`;
    if (!heartbeats.has(key)) {
      const iv = setInterval(() => {
        registerOnce(name, ip, port, metadata).catch(() => {});
      }, HEARTBEAT_INTERVAL_MS);
      iv.unref?.();
      heartbeats.set(key, iv);
    }
  } catch (err) {
    console.warn(`[nacos] registerService failed: ${err.message || err.code || err}`);
  }
}

/**
 * 注销服务实例并停止心跳。
 */
export async function deregisterService(serviceName, ip, port) {
  const name = serviceName || nacosConfig.serviceName;
  const key = `${name}|${ip}|${port}`;
  if (heartbeats.has(key)) {
    clearInterval(heartbeats.get(key));
    heartbeats.delete(key);
  }
  try {
    const params = new URLSearchParams({
      serviceName: name,
      groupName: 'DEFAULT_GROUP',
      namespaceId: nsId,
      ip,
      port: String(port),
      clusterName: 'DEFAULT',
    });
    const res = await v3Request(`${instanceApi}?${params}`, { method: 'DELETE' });
    const j = await res.json().catch(() => ({}));
    if (j.code !== 0) throw new Error(j.message || `nacos deregister http ${res.status}`);
    console.log(`[nacos] deregistered ${name} ${ip}:${port}`);
  } catch (err) {
    console.warn(`[nacos] deregisterService failed: ${err.message || err.code || err}`);
  }
}

// ---- 服务发现（v3 admin naming HTTP API）----

/**
 * 从 Nacos 查询健康服务实例，返回随机一个实例的地址。
 */
export async function discoverService(serviceName, groupName = 'DEFAULT_GROUP') {
  const name = serviceName || nacosConfig.serviceName;
  try {
    const params = new URLSearchParams({
      serviceName: name,
      groupName,
      namespaceId: nsId,
      healthyOnly: 'true',
    });
    const res = await v3Request(`${instanceApi}/list?${params}`);
    const j = await res.json().catch(() => ({}));
    if (j.code !== 0) {
      console.warn(`[nacos] discoverService ${name} error: ${j.message}`);
      return null;
    }
    const instances = j.data?.hosts || j.data || j.service?.hosts || [];
    if (instances.length === 0) {
      console.warn(`[nacos] discoverService ${name}: no healthy instances`);
      return null;
    }
    const inst = instances[Math.floor(Math.random() * instances.length)];
    console.log(`[nacos] discovered ${name} -> ${inst.ip}:${inst.port} (${instances.length} instances)`);
    return { ip: inst.ip, port: inst.port };
  } catch (err) {
    console.warn(`[nacos] discoverService ${name} failed: ${err.message}`);
    return null;
  }
}
