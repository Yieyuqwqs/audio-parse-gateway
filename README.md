# 落雪音乐（LX Music）解析后端 · Rust 核心骨架

`Axum + Tokio + reqwest 连接池 + moka(L1) + redis(L2) + singleflight + select! 竞速`

## 跑起来

```bash
cargo run --release
# 有 Redis 就带上（没有也能跑，自动降级为只用 L1）
REDIS_URL=redis://127.0.0.1:6379/ cargo run --release
# 换成真实音源节点
UPSTREAM_A_URL=https://node-a.example.com UPSTREAM_B_URL=https://node-b.example.com cargo run --release
```

默认会在 `127.0.0.1` 上自动拉起两个 mock 上游（随机端口），所以**不配任何环境变量也能直接跑通全链路**。

## 打几发

```bash
curl 'http://127.0.0.1:8080/api/parse?id=123456&source=kw&quality=320k'
curl 'http://127.0.0.1:8080/url/tx/0039MnYb0qxYhV?quality=flac'
curl 'http://127.0.0.1:8080/metrics'
```

响应（LX 约定：`code != 0` 即失败）：

```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "url": "https://cdn-upstream-a.lx-mock.example/kw/320k/123456.flac",
    "source": "kw",
    "quality": "320k",
    "provider": "upstream-a",
    "cache": "l1",
    "coalesced": false,
    "elapsed_ms": 0
  }
}
```

`cache` 字段直接暴露命中层级（`l1` / `l2` / `upstream`），`coalesced=true` 说明本次请求被 singleflight 合并了。

## 接到 LX Music 上

把下面脚本导入 LX Music 的「自定义源」（设置 → 自定义源 → 导入）：

```js
/**
 * @name 自建 Rust 解析源
 * @description 走自己的解析网关
 * @version 1.0.0
 */
const { EVENT_NAMES, request, on, send } = globalThis.lx

const BASE = 'http://192.168.1.10:8080' // ← 改成你的网关地址

const httpRequest = (url) => new Promise((resolve, reject) => {
  request(url, {}, (err, resp) => {
    if (err) return reject(err)
    try {
      const body = JSON.parse(resp.body)
      if (body.code !== 0) return reject(new Error(body.msg))
      resolve(body.data)
    } catch (e) {
      reject(e)
    }
  })
})

const sources = ['kw', 'kg', 'tx', 'wy', 'mg']

const apis = {}
for (const s of sources) {
  apis[s] = {
    musicUrl({ songmid, hash, strMediaMid, id, copyrightId }, quality) {
      const mid = songmid || hash || strMediaMid || copyrightId || id
      return httpRequest(`${BASE}/url/${s}/${mid}?quality=${quality}`).then((d) => d.url)
    },
  }
}

on(EVENT_NAMES.request, ({ source, action, info }) => {
  if (action !== 'musicUrl') return Promise.reject(new Error('unsupported action'))
  return apis[source].musicUrl(info.musicInfo, info.type)
})

send(EVENT_NAMES.inited, {
  sources: Object.fromEntries(
    sources.map((s) => [
      s,
      {
        name: `Rust源-${s}`,
        type: 'music',
        actions: ['musicUrl'],
        qualitys: ['128k', '320k', 'flac', 'flac24bit'],
      },
    ])
  ),
})
```

## 上游适配层（`src/upstream/`）

网关只做编排，**怎么拿到一条直链**收敛在 `Upstream` trait 后面：

```rust
#[async_trait::async_trait]
pub trait Upstream: Send + Sync + fmt::Debug {
    fn name(&self) -> &str;
    fn supports(&self, source: &str) -> bool;      // 竞速前先按音源过滤
    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError>;
}
```

现成三个实现：

| 实现 | 文件 | 说明 |
| --- | --- | --- |
| `NodeUpstream` | `src/upstream/node.rs` | 通用 HTTP 解析节点：路径支持 `{source}` `{id}` `{quality}` 模板，可带 Bearer token；响应兼容 `{url}` / `{code:0,data:{url}}` / `{data:{url}}` / `{data:[{url}]}`，有效期字段 `expires_at` / `expi` / `expires` 都认 |
| `NeteaseUpstream` | `src/upstream/netease.rs` | 网易云官方接口直连（源 `wy`）：音质 → 码率（128k/320k/flac），`expi` 写进 `expires_at`；付费/版权下架时接口返回 `url: null`，这里抛 `NoUrl` 让竞速换路，绝不把死链返回给播放器；带 `NETEASE_COOKIE` 才能拿 320k / flac |
| `MockUpstream` | `src/upstream/mock.rs` | 进程内假上游：0~120ms 抖动 + 3% 失败率，本地就能验证竞速与失败切换；测试里可以关掉 |

装配只看环境变量（`build_upstreams()`）：

| 环境变量 | 作用 |
| --- | --- |
| `UPSTREAM_A_URL` / `UPSTREAM_B_URL` | 解析节点地址（不配就跳过这一路） |
| `UPSTREAM_A_PATH` / `UPSTREAM_B_PATH` | 节点路径，默认 `/api/url`，支持 `{source}` `{id}` `{quality}` |
| `UPSTREAM_A_TOKEN` / `UPSTREAM_B_TOKEN` | 节点鉴权：`Authorization: Bearer <token>` |
| `UPSTREAM_A_COOKIE` / `UPSTREAM_B_COOKIE` | 节点会话 cookie（平台反爬/登录态靠它，逐字透传不改写） |
| `ENABLE_NETEASE=1` | 打开网易云直连适配器 |
| `NETEASE_COOKIE` | 网易云登录 cookie（解锁高音质） |
| `NETEASE_BASE` | 网易云接口反代地址，默认 `https://music.163.com` |

```bash
# 接自建节点 + 网易云直连
UPSTREAM_A_URL=https://node-a.example.com ENABLE_NETEASE=1 cargo run --release
# 只想本地跑通全链路：什么都不配，自动起两个 mock
cargo run --release
```

新增一个音源 = 实现 `Upstream` + 在 `build_upstreams()` 里 `push` 一项，
缓存 / singleflight / 竞速 / 指标这些主链路一行都不用改。

> 平台直连（kw / kg / tx / mg）目前都卡在各家的反爬会话或签名算法上：酷我 web 的 `playUrl`
> 没有合法 `Hm_Iuvt` 会话就只会回 `The request is illegal!`，旧的 `anti.s` 接口已经退化成返回同一个兜底文件。
> 所以这类源现阶段走**「节点 + cookie/token」**：节点自己做签名，网关负责缓存、竞速与容错。

## 测试

```bash
cargo test
```

共 35 个用例，覆盖：适配层错误分类与 HTTP 状态映射（504 / 502 / 503）、节点响应多形状解析、
节点请求实发校验（本地回显服务验证 query / Bearer / Cookie 三件套）、网易云音质→码率与空 url 判定、
竞速取最快 / 失败切换 / 全挂 502、singleflight 合并（6 并发只打 1 次上游）、L1 命中、过期直链不返回、
过载 503、Prometheus 文本、参数校验，以及 **Redis L2 命中**（只认 `REDIS_URL`，本地没起 redis 就自动跳过）。
CI 里 `cargo build --release`、`cargo test`、`cargo clippy` 都会跑，并且带一个 `redis:7-alpine` service 给 L2 用例用。

## 压测

零依赖脚本（node >= 18）：

```bash
cargo run --release
node bench/load.js http://127.0.0.1:8080 64 10        # 每次新 id，全穿透
node bench/load.js http://127.0.0.1:8080 64 10 hot    # 同一个 id，测缓存与合并
```

压测时盯 `/metrics` 的 `lx_gateway_inflight_upstream`（≤ 2000）、`lx_gateway_singleflight_keys`（≤ 20000 且压完回 0）、
`lx_gateway_coalesced_total`（hot 模式应上涨）。细节见 `bench/README.md`。

## 关键参数（改 main.rs 顶部常量即可）

| 常量 | 值 | 说明 |
| --- | --- | --- |
| `UPSTREAM_TIMEOUT` | 800ms | 单个上游请求硬超时 |
| `RACE_DEADLINE` | 850ms | 竞速整体硬上限 |
| `POOL_MAX_IDLE_PER_HOST` | 100 | 每 host 空闲长连接数 |
| `L1_CAPACITY` / `L1_TTL` | 10_000 / 30min | L1 容量与 TTL |
| `L2_TTL_SECS` | 1800 | Redis TTL |
| `UPSTREAM_MAX_INFLIGHT` | 2000 | 上游并发闸门，满了直接 503 |

## 说明

* 音源直链带签名、会过期，所以缓存里还存了 `expires_at`，命中后仍会做一次有效期校验（`SourceUrl::is_fresh`）。
* mock 上游有 3% 概率返回 500、延迟带随机抖动，用来验证「竞速 + 失败切换」确实生效。
* 真实的签名/加密逻辑（`md5`、AES、RSA）放在各上游节点里，网关只做编排，不碰各家私有协议。

## 监控（Prometheus）

`GET /metrics` 输出标准 Prometheus 文本格式（`text/plain; version=0.0.4`），Prometheus 直接抓就行：

```bash
curl -s http://127.0.0.1:8080/metrics
```

| 指标 | 类型 | 含义 |
| --- | --- | --- |
| `lx_gateway_requests_total` | counter | 收到的请求总数 |
| `lx_gateway_cache_hits_total{layer="l1"\|"l2"}` | counter | 分层命中次数 |
| `lx_gateway_cache_misses_total` | counter | 首查未命中的请求数（要走合并/上游） |
| `lx_gateway_upstream_requests_total` | counter | 真正打穿到上游并成功的次数（≈ 上游配额消耗） |
| `lx_gateway_coalesced_total` | counter | 被 singleflight 合并掉的次数（防击穿生效的证据） |
| `lx_gateway_errors_total` | counter | 上游失败 / 超时 |
| `lx_gateway_inflight_upstream` | gauge | 当前在飞的上游请求数（≤ 2000） |
| `lx_gateway_singleflight_keys` | gauge | 当前门闩表大小（≤ 20000） |

```yaml
# prometheus.yml
scrape_configs:
  - job_name: lx-gateway
    static_configs:
      - targets: ['127.0.0.1:8080']
```

## 内存账（凭什么「稳如老狗」）

| 结构 | 上界 |
| --- | --- |
| L1 moka | 10_000 条 × ~200B ≈ 2~4MB，LRU + TTL 淘汰 |
| singleflight 门闩表 | `MAX_FLIGHT_KEYS = 20_000`，超了 fail-open（放弃合并，直连上游） |
| 上游并发 | `UPSTREAM_MAX_INFLIGHT = 2000`，超了直接 503 |
| HTTP 连接池 | 每 host 100 条空闲连接，`pool_idle_timeout=90s` |
| 队列 | 全链路没有无界队列，只有背压 + 快速失败 |

唯一会随并发线性增长的是「同时在处理的请求本身」（每个连接一条 task），这是任何 web server 的固有成本；
要封顶就靠系统层：`ulimit -n`、反代 `worker_connections`、或前面挂一层限流。
压测时重点盯两个 gauge：`lx_gateway_inflight_upstream`（应 ≤ 2000）和 `lx_gateway_singleflight_keys`（应 ≤ 20000）。
