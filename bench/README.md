# 压测

零依赖，`node bench/load.js` 就能跑（node >= 18）。

```bash
# 1) 起网关（默认两个 mock 上游）
cargo run --release

# 2) 打一轮：64 并发、10 秒、每次都是新 id（全部穿透上游）
node bench/load.js http://127.0.0.1:8080 64 10

# 3) 再打一轮 hot：同一个 id（命中 L1/L2，测合并与缓存）
node bench/load.js http://127.0.0.1:8080 64 10 hot
```

压测时另开一个终端，直接看 `http://127.0.0.1:8080/metrics` 的这几个指标：

| 指标 | 期望 |
| --- | --- |
| `lx_gateway_inflight_upstream` | ≤ 2000（上游闸门上限；超了说明闸门没生效） |
| `lx_gateway_singleflight_keys` | ≤ 20000，且压完回落到 0（门闩泄漏就是这里不降） |
| `lx_gateway_coalesced_total` | `hot` 模式下应明显增长（同一 key 的并发被合并） |
| `lx_gateway_errors_total` | 与 mock 的 3% 失败率同量级；`unique` 模式下接近 0 说明竞速把失败兜住了 |

## 已知边界

- mock 上游带 0~120ms 随机抖动，所以 `unique` 模式 p50 约 100ms、p99 接近 200ms 属正常。
- 脚本是**客户端侧**计时（含本机 loopback 开销）；要纯服务端耗时就看看响应里的 `elapsed_ms` 字段。
- `unique` 模式会真的消耗上游配额（指向真实音源节点前先想清楚）。
