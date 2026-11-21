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

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `src/main.rs` | 路由、参数校验、`AppState::resolve` 主链路、适配器装配、启动与优雅退出 |
| `src/cache.rs` | L1（moka 无锁内存）、L2（Redis ConnectionManager + 熔断降级）、singleflight 门闩 |
| `src/metrics.rs` | Atomic 指标 + Prometheus 文本输出 |
| `src/quality.rs` | 音质阶梯：档位排序、降档、能力判定、`br`→档位映射 |
| `src/http.rs` | 全局连接池（进程内唯一 reqwest Client） |
| `src/upstream/` | 适配层：`Upstream` trait + 节点 / 网易云 / QQ / mock 四个实现 |

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
    "requested_quality": "flac",
    "downgraded": true,
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
| `QqUpstream` | `src/upstream/qq.rs` | QQ 音乐直连（源 `tx`）：`vkey.GetVkeyServer/CgiGetVkey`，一次请求带多个文件名取最好一档；`qm_keyst` + `o0<uin>` 才算登录 |
| `MockUpstream` | `src/upstream/mock.rs` | 进程内假上游：0~120ms 抖动 + 3% 失败率，本地就能验证竞速与失败切换；测试里可以关掉 |

装配只看环境变量（`build_upstreams()`）：

| 环境变量 | 作用 |
| --- | --- |
| `UPSTREAM_A_URL` / `UPSTREAM_B_URL` | 解析节点地址（不配就跳过这一路） |
| `UPSTREAM_A_PATH` / `UPSTREAM_B_PATH` | 节点路径，默认 `/api/url`，支持 `{source}` `{id}` `{quality}` |
| `UPSTREAM_A_TOKEN` / `UPSTREAM_B_TOKEN` | 节点鉴权：`Authorization: Bearer <token>` |
| `UPSTREAM_A_COOKIE` / `UPSTREAM_B_COOKIE` | 节点会话 cookie（平台反爬/登录态靠它，逐字透传不改写） |
| `ENABLE_NETEASE=1` | 打开网易云直连适配器 |
| `NETEASE_COOKIE` | 网易云登录 cookie：**只有带上它才可能拿到 flac / flac24bit**，否则最高 320k |
| `NETEASE_BASE` | 网易云接口反代地址，默认 `https://music.163.com` |
| `ENABLE_QQ=1` 或给 `TX_COOKIE` | 打开 QQ 音乐直连适配器（源 `tx`） |
| `TX_COOKIE` | QQ 整段 cookie（`qm_keyst=...; uin=o0<QQ号>`），没有就只有 128k |
| `TX_UIN` | QQ 号 —— 注意请求里要 `o0<QQ号>` 形式，适配器会自动补 |
| `REDIS_URL` | Redis 地址；不配或连不上就只用 L1（启动探测，请求路径零等待） |
| `RUST_LOG` | 日志级别，默认 `error,audio_parse_gateway=info` |

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

## 高音质（`flac` / `flac24bit`）

网关不会把「请求了 flac」当成「拿到了 flac」。音质走一条明确的阶梯：

```text
128k  <  320k  <  flac  <  flac24bit
```

两件事由 `src/quality.rs` + `Upstream::max_quality` 决定：

1. **能力协商**：每个适配器声明自己的天花板。请求档位超过所有适配器的天花板时，
   进门就沿阶梯降档（例如全部只到 320k，就直接按 320k 打，不会白打一次 flac 再失败）；
2. **如实上报**：上游可能连协商后的档位都没给到（网易云就会静默降级）。
   响应里的 `quality` 是**实际**档位，`requested_quality` 是请求档位，`downgraded` 标记本次是否降档。

网易云实测（2026-11-21，无登录 cookie）：

| 请求 | 接口参数 | 实际返回 |
| --- | --- | --- |
| `320k` | `br=320000` | 320k mp3（`level: exhigh`） |
| `flac` | `br=999000` | **静默降成 320k mp3**，不会报错 |
| `flac24bit` | `br=1999000` | 同上，没会员 cookie 拿不到 24bit |

所以想让 LX 真的放无损，只有两条路：给节点/网易云配**有会员的 cookie**（`NETEASE_COOKIE` / `UPSTREAM_A_COOKIE`），
或让自建节点自己解决签名。带上 cookie 后 `NeteaseUpstream::max_quality()` 会自动升到 `flac24bit`。

验证手上的 cookie 到底能不能拿无损（`url` 给了 `.flac` 才算真的行）：

```bash
curl -s "https://music.163.com/api/song/enhance/player/url?ids=%5B1330348068%5D&br=999000" \
  -H "Cookie: MUSIC_U=你的cookie" \
  -H "Referer: https://music.163.com/"
# 看 data[0].level：lossless / hires 才是无损，exhigh 说明只是 320k
```

### QQ 音乐（`tx`）实测

同一个 vkey 接口一次请求可以带**多个文件名**，网关就按「请求档位 → 128k」全带上，取能拿到的最好一档：

| 文件 | 游客 | 绿钻 | 豪华绿钻 |
| --- | --- | --- | --- |
| `M500….mp3`（128k） | ✅ 免费曲 | ✅ | ✅ |
| `M800….mp3`（320k） | ❌ `101404` | ✅ | ✅ |
| `F000….flac`（无损） | ❌ `101404` | ❌ `101404` | ✅ |
| `RS01….flac`（Hi-Res） | ❌ `101404` | ❌ `101404` | ✅ |

两个坑（都踩过）：

1. 现在有效的登录 cookie 是 **`qm_keyst`**，老的 `qqmusic_key` 已经不被认；
2. 请求体里的 `uin` 必须写成 **`o0<QQ号>`**（和 cookie 里一致），写成纯数字会被当游客 ——
   这一条不对，320k 就会莫名其妙地 `101404`。

```bash
# .env（compose 会自动读，且已被 .gitignore/.dockerignore 忽略）
ENABLE_QQ=1
TX_UIN=2310476390
TX_COOKIE='qm_keyst=...; uin=o02310476390'
```

降档次数进指标 `lx_gateway_quality_downgrades_total`；如果它一直涨、播放器又显示无损，
说明客户端把「请求档位」当成了实际档位 —— 那是播放器侧的问题，网关这边的语义是准的。

## 测试

```bash
cargo test
```

共 57 个用例，覆盖：适配层错误分类与 HTTP 状态映射（504 / 502 / 503）、节点响应多形状解析、
节点请求实发校验（本地回显服务验证 query / Bearer / Cookie 三件套）、网易云音质→码率与空 url 判定、
竞速取最快 / 失败切换 / 全挂 502、singleflight 合并（6 并发只打 1 次上游）、L1 命中、过期直链不返回、
过载 503、Prometheus 文本、参数校验、**音质协商与降档**（天花板筛选 / 静默降级如实上报 / 降档指标 / QQ 多档候选取最优），以及 **Redis L2 命中**（只认 `REDIS_URL`，本地没起 redis 就自动跳过）。
CI 里 `cargo build --release`、`cargo test`、`cargo clippy` 都会跑，并且带一个 `redis:7-alpine` service 给 L2 用例用。

## 下载 / 发布

二进制由 `Release` workflow 打（`.github/workflows/release.yml`），CI 只负责 build+test，不打产物：

| 触发方式 | 结果 |
| --- | --- |
| 打 tag：`git tag v0.1.0 && git push origin v0.1.0` | 三个平台的 `tar.gz` + `sha256` 自动挂到 **GitHub Release** |
| Actions → Release → **Run workflow** | 只打包，产物在本次 run 的 **Artifacts** 里（保留 30 天） |

产物命名：`audio-parse-gateway-<target>.tar.gz`（内含二进制 + README），旁边配 `.tar.gz.sha256`。

| target | 说明 |
| --- | --- |
| `x86_64-unknown-linux-gnu` | 主力产物：glibc 动态链接，Ubuntu/Debian 直接跑 |
| `x86_64-unknown-linux-musl` | 静态链接，拷到任何 Linux 都能跑（alpine 也行） |
| `aarch64-unknown-linux-gnu` | ARM64 服务器（Graviton / Ampere 之类） |

后两个在矩阵里标了 `experimental`：挂了不会挡住 gnu 产物，也不会把 workflow 带红，
去 run 日志里看缺哪个交叉工具链即可。`ring`（rustls 的加密实现）交叉编译时需要把 `CC` 指对，
workflow 里已经配了 `musl-gcc` / `aarch64-linux-gnu-gcc`。

## 部署

```bash
# 最省事：网关 + redis 一起起（没配真实音源时会用两个进程内 mock，适合冒烟）
docker compose up --build

# 或者只跑网关
docker build -t lx-gateway .
docker run --rm -p 8080:8080 lx-gateway

# 接真实音源（cookie / token 走环境变量，别写进仓库）
docker run --rm -p 8080:8080 \
  -e UPSTREAM_A_URL=https://node-a.example.com \
  -e UPSTREAM_A_TOKEN=xxxx \
  -e ENABLE_NETEASE=1 \
  -e NETEASE_COOKIE=MUSIC_U=... \
  lx-gateway
```

镜像是 `rust:1.88-bookworm` 多阶段构建 → `debian:bookworm-slim` 运行时：依赖层单独缓存，改 `src/` 只重编业务代码。
内置 `HEALTHCHECK` 打 `/health`；`REDIS_URL` 连不上会自动降级成 L1-only，不会拖垮主链路。`compose` 里的 redis 限制 256MB + `allkeys-lru`，只当直链缓存用、不持久化。

## 压测

零依赖脚本（node >= 18）：

```bash
cargo run --release
node bench/load.js http://127.0.0.1:8080 64 10        # 每次新 id，全穿透
node bench/load.js http://127.0.0.1:8080 64 10 hot    # 同一个 id，测缓存与合并
```

压测时盯 `/metrics` 的 `lx_gateway_inflight_upstream`（≤ 2000）、`lx_gateway_singleflight_keys`（≤ 20000 且压完回 0）、
`lx_gateway_coalesced_total`（hot 模式应上涨）。细节见 `bench/README.md`。

## 性能（延迟预算）

一次请求的预算排布：

| 环节 | 上限 | 说明 |
| --- | --- | --- |
| L2（Redis）读 | 0ms（关掉时）/ 150ms（开着） | 启动探测失败就全局关闭，请求路径只剩一次原子变量判断 |
| 单路上游 | **500ms** | 到点直接砍，不再等 800ms |
| 整场竞速 | **550ms** | 兜底，防止某个适配器漏了超时 |
| 建连 | **200ms** | 慢上游尽早出局，把预算让给能干活的那路 |
| 空闲连接保留 | 300s | 跨请求复用，避免每次重新握手 |

四件事决定体感：

1. **Redis 不装就不碰**：`L2Cache::probe()` 只在启动时试连一次，失败立刻 `enabled=false`，
   之后所有 `get` / `set` 都是原子变量判断 —— 不会再出现「每个请求等它超时 500ms」；
   运行期也只尝试建连一次（`attempted` 兜底），彻底没有「每 5 秒重试一次、每次卡一下」的抖动。
2. **连接池按「第二次请求零握手」配**：`pool_max_idle_per_host=200`、`pool_idle_timeout=300s`、
   `tcp_keepalive(30s)`、`tcp_nodelay`，并开启 HTTP/2（ALPN 协商 + 空闲 keep-alive ping）。
3. **日志只留两条**：默认 `RUST_LOG=error,audio_parse_gateway=info`，每请求只剩一行 `upstream resolved`
   和错误日志；缓存命中 / 合并这类 debug 噪音默认不打印（排查时 `RUST_LOG=debug,audio_parse_gateway=debug`）。
4. **超时砍到 500ms**：多路上游并发，谁先回来用谁，没回来的直接 abort（竞速整体 550ms 兜底）。

验证连接复用：

```bash
curl -o /dev/null -s -w '1st %{time_total}s\n' 'http://127.0.0.1:8080/api/parse?id=1&source=kw'
curl -o /dev/null -s -w '2nd %{time_total}s\n' 'http://127.0.0.1:8080/api/parse?id=2&source=kw'
```

第二次应明显低于第一次（省掉 TCP/TLS 握手）。冷启动第一发慢是正常的，别用第一发判断性能。

## 关键参数（改 main.rs 顶部常量即可）

| 常量 | 值 | 说明 |
| --- | --- | --- |
| `UPSTREAM_TIMEOUT` | 500ms | 单个上游请求硬超时（到点直接砍） |
| `RACE_DEADLINE` | 550ms | 竞速整体硬上限 |
| `CONNECT_TIMEOUT` | 200ms | 建连超时（`src/http.rs`） |
| `POOL_MAX_IDLE_PER_HOST` | 200 | 每 host 空闲长连接数 |
| `POOL_IDLE_TIMEOUT` | 300s | 空闲连接保留时长 |
| `BODY_DEADLINE` | 800ms | reqwest 兜底超时（`src/http.rs`） |
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
| `lx_gateway_quality_downgrades_total` | counter | 实际档位低于请求档位的次数（能力上限或接口静默降级） |
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
