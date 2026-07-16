// Centralized configuration for proxy-chain forwarder.
// 优先级：环境变量 > Nacos 远程配置 > 本地 config.yaml > 硬编码默认值。
// Nacos 连接参数从 bootstrap.yaml 读取（nacosClient.js 负责）。

import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { load as yamlLoad } from 'js-yaml';
import { fetchNacosConfig, nacosConfig } from './nacosClient.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

const num = (envVal, fileVal, defaultVal) => {
  if (envVal != null && envVal !== '') return Number(envVal);
  if (fileVal != null) return fileVal;
  return defaultVal;
};

const str = (envVal, fileVal, defaultVal) => {
  if (envVal != null && envVal !== '') return envVal;
  if (fileVal != null) return fileVal;
  return defaultVal;
};

/** 从 YAML 配置文件加载 */
function loadFileConfig() {
  const candidates = [
    '/app/config.yaml',
    '/app/config.yml',
    resolve(__dirname, '..', 'config.yaml'),
    resolve(__dirname, '..', 'config.yml'),
  ];
  for (const p of candidates) {
    try {
      const raw = readFileSync(p, 'utf-8');
      console.log(`[config] loaded from ${p}`);
      return yamlLoad(raw) ?? {};
    } catch { /* skip */ }
  }
  console.log('[config] no config.yaml found, using defaults');
  return {};
}

/** 加载配置：Nacos 优先，回退到本地文件 */
async function loadRemoteOrLocal() {
  // nacosConfig 由 nacosClient.js 从 bootstrap.yaml + 环境变量解析
  console.log(`[config] ====== 配置加载开始 ======`);

  try {
    const remote = await fetchNacosConfig(); // 使用 bootstrap 中的 dataId/group
    if (remote && Object.keys(remote).length > 0) {
      console.log(`[config] 从 Nacos 加载配置成功，配置项: ${Object.keys(remote).join(', ')}`);
      console.log(`[config] ====== 配置加载完成 (来源: Nacos) ======`);
      return remote;
    }
    console.log('[config] Nacos 返回空配置，回退到本地文件');
  } catch (err) {
    console.warn(`[config] Nacos 加载失败: ${err.message}，回退到本地文件`);
  }

  const localConfig = loadFileConfig();
  console.log(`[config] ====== 配置加载完成 (来源: 本地文件) ======`);
  return localConfig;
}

const file = await loadRemoteOrLocal();

// 共享凭证配置（application-dev.yml）：快代理 DPS 凭证的唯一来源。
// 支持双订单：short（短命池）和 long（长命池）
async function loadSharedConfig() {
  try {
    const remote = await fetchNacosConfig(nacosConfig.sharedDataId, nacosConfig.sharedGroup);
    if (remote && Object.keys(remote).length > 0) {
      console.log(`[config] 共享配置 ${nacosConfig.sharedDataId} 加载成功`);
      return remote;
    }
    console.log(`[config] 共享配置 ${nacosConfig.sharedDataId} 返回空`);
  } catch (err) {
    console.warn(`[config] 共享配置 ${nacosConfig.sharedDataId} 加载失败: ${err.message}`);
  }
  return {};
}

function mapShared(raw) {
  const root = raw || {};
  // 支持两种格式：新的 dps-short/dps-long 和旧的 kuaidaili
  const dpsShort = root['dps-short'] || root.dpsShort || {};
  const dpsLong = root['dps-long'] || root.dpsLong || {};
  const k = root.kuaidaili || {};

  return {
    dpsShort: {
      apiEndpoint: dpsShort['api-endpoint'] || dpsShort.apiEndpoint,
      secretId: dpsShort['secret-id'] || dpsShort.secretId,
      secretKey: dpsShort['secret-key'] || dpsShort.secretKey,
      proxyUsername: dpsShort['proxy-username'] || dpsShort.proxyUsername,
      proxyPassword: dpsShort['proxy-password'] || dpsShort.proxyPassword,
    },
    dpsLong: {
      apiEndpoint: dpsLong['api-endpoint'] || dpsLong.apiEndpoint,
      secretId: dpsLong['secret-id'] || dpsLong.secretId,
      secretKey: dpsLong['secret-key'] || dpsLong.secretKey,
      proxyUsername: dpsLong['proxy-username'] || dpsLong.proxyUsername,
      proxyPassword: dpsLong['proxy-password'] || dpsLong.proxyPassword,
    },
    // 旧格式兼容：kuaidaili 作为 fallback
    kuaidaili: {
      apiEndpoint: k['api-endpoint'],
      secretId: k['secret-id'],
      secretKey: k['secret-key'],
      proxyUsername: k['proxy-username'],
      proxyPassword: k['proxy-password'],
    },
  };
}

const shared = mapShared(await loadSharedConfig());

const fallback = shared.kuaidaili;

export const config = {
  log: {
    level: str(process.env.LOG_LEVEL, file.log?.level, 'info'),
  },
  dpsShort: {
    apiEndpoint: str(process.env.DPS_SHORT_API_ENDPOINT,
      shared.dpsShort?.apiEndpoint ?? file.dpsShort?.apiEndpoint ?? fallback?.apiEndpoint,
      'https://dps.kdlapi.com/api'),
    secretId: str(process.env.DPS_SHORT_SECRET_ID,
      shared.dpsShort?.secretId ?? file.dpsShort?.secretId ?? fallback?.secretId, ''),
    secretKey: str(process.env.DPS_SHORT_SECRET_KEY,
      shared.dpsShort?.secretKey ?? file.dpsShort?.secretKey ?? fallback?.secretKey, ''),
    proxyUsername: str(process.env.DPS_SHORT_PROXY_USERNAME,
      shared.dpsShort?.proxyUsername ?? file.dpsShort?.proxyUsername ?? fallback?.proxyUsername, ''),
    proxyPassword: str(process.env.DPS_SHORT_PROXY_PASSWORD,
      shared.dpsShort?.proxyPassword ?? file.dpsShort?.proxyPassword ?? fallback?.proxyPassword, ''),
  },
  dpsLong: {
    apiEndpoint: str(process.env.DPS_LONG_API_ENDPOINT,
      shared.dpsLong?.apiEndpoint ?? file.dpsLong?.apiEndpoint ?? fallback?.apiEndpoint,
      'https://dps.kdlapi.com/api'),
    secretId: str(process.env.DPS_LONG_SECRET_ID,
      shared.dpsLong?.secretId ?? file.dpsLong?.secretId ?? fallback?.secretId, ''),
    secretKey: str(process.env.DPS_LONG_SECRET_KEY,
      shared.dpsLong?.secretKey ?? file.dpsLong?.secretKey ?? fallback?.secretKey, ''),
    proxyUsername: str(process.env.DPS_LONG_PROXY_USERNAME,
      shared.dpsLong?.proxyUsername ?? file.dpsLong?.proxyUsername ?? fallback?.proxyUsername, ''),
    proxyPassword: str(process.env.DPS_LONG_PROXY_PASSWORD,
      shared.dpsLong?.proxyPassword ?? file.dpsLong?.proxyPassword ?? fallback?.proxyPassword, ''),
  },
  forwarder: {
    port: num(process.env.FORWARDER_PORT, file.forwarder?.port, 3128),
    verbose: str(process.env.FORWARDER_VERBOSE, file.forwarder?.verbose, 'false') === 'true',
    sharedTtlMs: num(process.env.SHARED_TTL_MS, file.forwarder?.sharedTtlMs, 90_000),
    sessionTtlMs: num(process.env.SESSION_TTL_MS, file.forwarder?.sessionTtlMs, 900_000),
    sessionFailureThreshold: num(process.env.SESSION_FAILURE_THRESHOLD, file.forwarder?.sessionFailureThreshold, 2),
    bufferSize: num(process.env.BUFFER_SIZE, file.forwarder?.bufferSize, 10),
    bufferRefillThreshold: num(process.env.BUFFER_REFILL_THRESHOLD, file.forwarder?.bufferRefillThreshold, 3),
    maxRetryAttempts: num(process.env.MAX_RETRY_ATTEMPTS, file.forwarder?.maxRetryAttempts, 5),
    blockedIpTtlMs: num(process.env.BLOCKED_IP_TTL_MS, file.forwarder?.blockedIpTtlMs, 600_000),
    internalPort: num(process.env.INTERNAL_PORT, file.forwarder?.internalPort, 3129),
    maxConcurrentAcquire: num(process.env.MAX_CONCURRENT_ACQUIRE, file.forwarder?.maxConcurrentAcquire, 3),
  },
};

export default config;
