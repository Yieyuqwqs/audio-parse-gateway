#!/usr/bin/env node
// 网关压测脚本（零依赖，node >= 18 自带 fetch）
//
//   node bench/load.js [base] [并发] [秒数] [unique|hot]
//
// unique（默认）：每个请求一个全新 id → 全部穿透到上游，测「竞速 + 超时」的最坏情况
// hot：所有请求同一个 id → 命中 L1/L2，测「缓存 + singleflight 合并」
//
// 压测时另开终端直接看 http://127.0.0.1:8080/metrics，
// 重点盯 lx_gateway_inflight_upstream ≤ 2000、lx_gateway_singleflight_keys ≤ 20000。

const base = process.argv[2] || 'http://127.0.0.1:8080';
const concurrency = Number(process.argv[3] || 64);
const seconds = Number(process.argv[4] || 10);
const mode = process.argv[5] || 'unique';

if (!Number.isFinite(concurrency) || concurrency < 1 || !Number.isFinite(seconds) || seconds < 1) {
  console.error('用法: node bench/load.js [base] [并发] [秒数] [unique|hot]');
  process.exit(1);
}

let seq = 0;
const latencies = [];
const errors = new Map();
let ok = 0;
let failed = 0;
const deadline = Date.now() + seconds * 1000;

const buildUrl = (id) => `${base}/api/parse?id=${encodeURIComponent(id)}&source=kw&quality=320k`;

async function worker() {
  while (Date.now() < deadline) {
    const id = mode === 'hot' ? 'bench-hot-1' : `bench-${(seq++).toString(36)}-${Date.now().toString(36)}`;
    const started = process.hrtime.bigint();
    try {
      const res = await fetch(buildUrl(id));
      const body = await res.json().catch(() => ({}));
      const ms = Number(process.hrtime.bigint() - started) / 1e6;
      latencies.push(ms);
      if (res.ok && body.code === 0) {
        ok++;
      } else {
        failed++;
        const key = `${res.status} ${String(body.msg || '').slice(0, 40)}`;
        errors.set(key, (errors.get(key) || 0) + 1);
      }
    } catch (err) {
      failed++;
      const key = `network: ${String(err.message).slice(0, 40)}`;
      errors.set(key, (errors.get(key) || 0) + 1);
    }
  }
}

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
}

(async () => {
  const begun = Date.now();
  await Promise.all(Array.from({ length: concurrency }, worker));
  const elapsed = (Date.now() - begun) / 1000;
  const total = ok + failed;
  latencies.sort((a, b) => a - b);

  console.log(`mode=${mode} concurrency=${concurrency} duration=${elapsed.toFixed(1)}s`);
  console.log(`requests=${total} ok=${ok} failed=${failed} rps=${(total / elapsed).toFixed(0)}`);
  console.log(
    `latency_ms p50=${percentile(latencies, 0.5).toFixed(1)} p90=${percentile(latencies, 0.9).toFixed(1)} ` +
      `p99=${percentile(latencies, 0.99).toFixed(1)} max=${(latencies[latencies.length - 1] || 0).toFixed(1)}`
  );
  if (errors.size) {
    console.log('errors:');
    for (const [key, count] of [...errors.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${key} x${count}`);
    }
  }
})();
