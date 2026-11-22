#![forbid(unsafe_code)]
//! # 落雪音乐（LX Music）解析后端 · 工业级核心骨架
//!
//! ```text
//!  LX Music / 自定义源脚本 ──HTTP──▶ 本网关 ──竞速──▶ 上游音源节点 A / B ...
//!                                     │
//!                           L1(moka) ◀┴▶ L2(redis) ◀── singleflight 防击穿
//! ```
//!
//! 需求对照：
//! 1. 全局连接池：`once_cell::sync::Lazy<reqwest::Client>` 单例，TCP keep-alive + 连接复用，
//!    `pool_max_idle_per_host = 100`。
//! 2. 多级缓存：L1 = `moka::future::Cache`（容量 10_000 / TTL 30min，无锁并发读）；
//!    L2 = `redis::aio::ConnectionManager`（多路复用、自动重连，Redis 挂掉自动降级为 L1-only）。
//! 3. 超时与并发降级：每个上游请求 `tokio::time::timeout(800ms)`；全局信号量限流，超载快速失败。
//! 4. 并发竞速：`tokio::select!` 同时打两路上游，谁先成功用谁，输的那路立刻 abort（真取消）。
//! 5. 防缓存击穿：`SingleFlight`（DashMap + `tokio::sync::Mutex`），同一个 key 只有一个请求穿透上游，
//!    其余请求 await 等待并直接复用结果。
//! 6. 路由：`GET /api/parse?id=<musicId>[&source=kw][&quality=320k]`、`GET /url/{source}/{musicId}`。
//!
//! 监控与健康检查：`GET /metrics`（Prometheus 文本格式）、`GET /health`。
//!
//! 启动：`cargo run --release`（默认监听 0.0.0.0:8080；没配真实上游时自动起两个 mock）
//! 接真实音源：`UPSTREAM_A_URL=https://node.example.com cargo run --release`，`ENABLE_NETEASE=1` / `ENABLE_QQ=1` 打开网易云 / QQ 音乐；
//! 酷我 / 酷狗 / 咪咕（kw / kg / mg）**零凭据默认直连**，不配任何 env 也能解析；`REDIS_URL` 可选
//! 自测：`curl 'http://127.0.0.1:8080/api/parse?id=123456&source=kw&quality=320k'`

use std::{
    net::SocketAddr,
    sync::{
        atomic::Ordering,
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod cache;
mod http;
mod logfmt;
mod metrics;
mod proxy;
mod quality;
mod upstream;

use upstream::{
    KgUpstream, KwUpstream, MiguUpstream, MockUpstream, NeteaseUpstream, NodeUpstream, QqUpstream,
    TrackRequest, Upstream, UpstreamError,
};

pub(crate) use cache::{L2Cache, SingleFlight, L1_CACHE};
pub(crate) use quality::normalize as normalize_quality;
pub(crate) use metrics::{metrics_handler, Stats};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

/// 单个上游请求的硬超时：谁先回来用谁，500ms 还没回来的直接砍
const UPSTREAM_TIMEOUT: Duration = Duration::from_millis(500);
/// 竞速整体的硬上限（比单请求超时略宽，避免误杀「刚好在 500ms 返回」的赢家）
const RACE_DEADLINE: Duration = Duration::from_millis(550);
/// 音质降级链（flac24bit→flac→320k→128k）总共只允许花这么久
const FALLBACK_BUDGET: Duration = Duration::from_millis(1_200);
/// 同时在飞的上游请求上限（背压 / 过载保护）
const UPSTREAM_MAX_INFLIGHT: usize = 2_000;

/// singleflight 门闩表的硬上限：超过就放弃合并（fail-open），给内存兜底
const MAX_FLIGHT_KEYS: usize = 20_000;

/// L1 容量与 TTL（需求 2）
const L1_CAPACITY: u64 = 10_000;
const L1_TTL: Duration = Duration::from_secs(30 * 60);

/// L2（Redis）参数
const L2_KEY_PREFIX: &str = "lx:url:";
const L2_TTL_SECS: u64 = 30 * 60;
/// Redis 读写超时：L2 只是加速层，绝不允许拖慢主链路
const L2_OP_TIMEOUT: Duration = Duration::from_millis(150);
const L2_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

const DEFAULT_SOURCE: &str = "kw";
/// `?multi=1`：主源挂了就拿歌名+歌手去这几个**零凭据**平台试试（按顺序，谁先成功用谁）。
///
/// 这一条是「零 CK 也要出声」的关键：tx / wy 的高档位要靠会员 cookie，
/// 而 kw / kg / mg 匿名就能拿到可播直链，主源回 404/无权益时自动顶上。
const MULTI_SOURCE_LADDER: [&str; 3] = ["kg", "kw", "mg"];
const DEFAULT_QUALITY: &str = "320k";
/// LX 支持的源 key
const SUPPORTED_SOURCES: [&str; 6] = ["kw", "kg", "tx", "wy", "mg", "local"];


// ---------------------------------------------------------------------------
// 领域模型
// ---------------------------------------------------------------------------

/// 上游解析出来的播放地址（会被缓存，因此必须 `Serialize + Deserialize`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceUrl {
    /// 最终播放地址（带签名的直链，**始终是上游原链**：代拉的改写只发生在出口）
    url: String,
    /// 提供该地址的上游节点名，便于排查是哪一路赢的
    #[serde(default)]
    provider: String,
    /// 音质
    #[serde(default)]
    quality: String,
    /// 直链过期时间（unix 秒），0 表示不过期
    #[serde(default)]
    expires_at: u64,
}

impl SourceUrl {
    /// 直链是否还在有效期内。
    ///
    /// 音源直链通常带签名且 10~30 分钟过期，缓存命中后仍要校验一次，
    /// 否则会把「死链」返回给播放器（表现为播放器转圈/秒退）。
    pub(crate) fn is_fresh(&self) -> bool {
        self.expires_at == 0 || self.expires_at > now_unix()
    }
}

pub(crate) fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("invalid parameter: {0}")]
    BadRequest(String),
    #[error("upstream request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("upstream timed out after {0:?}")]
    Timeout(Duration),
    #[error("all upstreams failed: {0}")]
    Upstream(String),
    #[error("gateway is shedding load, please retry later")]
    Overloaded,
}

impl From<UpstreamError> for AppError {
    /// 适配层错误 → 对外 HTTP 语义：
    /// 超时 504、网络故障 502、其余（无直链 / 脏响应 / 没有可用上游）一律 502 并透出原因。
    fn from(err: UpstreamError) -> Self {
        match err {
            UpstreamError::Timeout { .. } => AppError::Timeout(UPSTREAM_TIMEOUT),
            UpstreamError::Transport { source, .. } => AppError::Http(source),
            other => AppError::Upstream(other.to_string()),
        }
    }
}

impl AppError {
    /// 对外 HTTP 状态码：响应头和访问日志共用同一套映射
    fn status(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Http(_) | AppError::Upstream(_) => StatusCode::BAD_GATEWAY,
            AppError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            AppError::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();

        if status.is_server_error() {
            warn!(error = %self, "request failed");
        } else {
            debug!(error = %self, "request rejected");
        }

        // LX 风格响应包：code != 0 即失败，msg 给出人话原因
        let body = ErrorBody { code: 1, msg: self.to_string() };
        (status, Json(body)).into_response()
    }
}

// ---------------------------------------------------------------------------
// 路由入参 / 出参（LX Music 自定义源约定）
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ParseQuery {
    /// 音乐 ID（必需，需求 5）
    id: Option<String>,
    /// 音源：kw / kg / tx / wy / mg / local，默认 kw
    source: Option<String>,
    /// 音质：128k / 320k / flac / flac24bit，默认 320k
    quality: Option<String>,
    /// `fallback=1`：主源彻底失败后允许去网易云二次搜索
    fallback: Option<String>,
    /// 搜索关键词；不给就用 `name` + `singer` 拼
    keyword: Option<String>,
    /// 歌曲名（跨平台回退用）
    name: Option<String>,
    /// `multi=1`：主源失败后依次试 kg / kw / mg（都是零凭据平台）
    multi: Option<String>,
    /// 歌手名（跨平台回退用）
    singer: Option<String>,
}

/// 一次解析的可选行为（跨平台回退开关 + 关键词）
#[derive(Debug, Clone, Default)]
struct ParseOptions {
    /// 主源失败后是否允许跨平台二次搜索
    fallback: bool,
    /// 主源失败后是否依次试 MULTI_SOURCE_LADDER
    multi: bool,
    keyword: Option<String>,
}

impl ParseOptions {
    fn from_query(query: &ParseQuery) -> Self {
        let fallback = query
            .fallback
            .as_deref()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        let keyword = query
            .keyword
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                let name = query.name.as_deref().unwrap_or("").trim();
                if name.is_empty() {
                    return None;
                }
                let singer = query.singer.as_deref().unwrap_or("").trim();
                Some(if singer.is_empty() {
                    name.to_string()
                } else {
                    format!("{name} {singer}")
                })
            });

        let multi = query
            .multi
            .as_deref()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        Self { fallback, multi, keyword }
    }

    /// 允许回退时才把关键词交出去
    fn fallback_keyword(&self) -> Option<String> {
        if self.fallback {
            self.keyword.clone()
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize)]
struct QualityQuery {
    quality: Option<String>,
}

/// LX 约定：`{ code: 0, msg: "success", data: ... }`
#[derive(Debug, Serialize)]
struct OkBody {
    code: i32,
    msg: &'static str,
    data: TrackData,
}

#[derive(Debug, Serialize)]
struct TrackData {
    /// 直链，自定义源脚本里直接 `return resp.data.url`
    url: String,
    source: String,
    /// **实际**拿到的音质档位（可能低于请求档位）
    quality: String,
    /// 客户端请求的档位
    requested_quality: String,
    /// 实际档位低于请求档位（例如没会员 cookie 时 flac 只能给 320k）
    downgraded: bool,
    /// 直链来自进程内 mock 上游（假地址，播放器一定放不出来）
    mock: bool,
    /// 哪个上游节点给的
    provider: String,
    /// 命中层级：l1 / l2 / upstream
    cache: &'static str,
    /// 本次是否被 singleflight 合并（说明有并发请求在等同一个 key）
    coalesced: bool,
    /// 网关内耗时（不含客户端网络）
    elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: i32,
    msg: String,
}

// ---------------------------------------------------------------------------
// 应用状态
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    /// 上游适配器注册表：节点 / 直连源 / mock 都实现同一个 `Upstream` trait
    pub(crate) upstreams: Arc<Vec<Arc<dyn Upstream>>>,
    pub(crate) l2: Arc<L2Cache>,
    pub(crate) flight: Arc<SingleFlight>,
    /// 上游并发闸门：过载时直接 503，保护自己也不压垮上游
    pub(crate) limiter: Arc<Semaphore>,
    pub(crate) stats: Arc<Stats>,
    /// 防盗链代拉的短链表
    pub(crate) proxy: Arc<proxy::ProxyStore>,
}

/// 一次解析的结果 + 命中信息
#[derive(Debug)]
struct Resolved {
    url: SourceUrl,
    /// l1 / l2 / upstream
    layer: &'static str,
    /// 客户端请求的音质档位（可能被协商降档）
    requested: &'static str,
    /// 是否等待了别人的在途请求
    coalesced: bool,
    /// 真正给出直链的音源（跨平台回退后可能是 `wy`）
    source: &'static str,
}

impl Resolved {
    fn new(
        url: SourceUrl,
        layer: &'static str,
        requested: &'static str,
        coalesced: bool,
        source: &'static str,
    ) -> Self {
        Self { url, layer, requested, coalesced, source }
    }

    /// 实际拿到的档位是否低于请求档位 —— 响应里要如实标出来，别让播放器以为拿到了 flac。
    fn downgraded(&self) -> bool {
        !self.url.quality.is_empty() && self.url.quality != self.requested
    }
}

impl AppState {
    /// 核心链路：音质协商 → L1 → L2 → singleflight → 竞速（含降级链）→ 回填缓存
    async fn resolve(
        &self,
        source: &'static str,
        music_id: &str,
        requested: &'static str,
    ) -> Result<Resolved, AppError> {
        // 高音质请求先做「能力协商」：没有上游能到这一档就沿阶梯往下走，
        // 免得每打一次 flac 都要先空跑一次、再靠失败来降档。
        let quality = self.effective_quality(source, requested);

        // 缓存 key 与适配层共用同一套拼法，避免两处漂移（按**协商后**的档位缓存）
        let key = TrackRequest::new(source, music_id, quality).cache_key();

        // ---- 1) L1 / L2：绝大多数请求在这里结束，纯内存、无锁读 ----
        if let Some((url, layer)) = self.cache_lookup(&key).await {
            return Ok(Resolved::new(url, layer, requested, false, source));
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);

        // ---- 2) 打不中 → 抢 singleflight 门闩（表满时 fail-open，直连上游） ----
        let gate = self.flight.gate(&key);

        let result = self
            .resolve_locked(&key, source, music_id, quality, requested, gate.as_ref())
            .await;

        // 无论成功失败都要摘门闩，否则表项会泄漏
        if let Some(gate) = gate.as_ref() {
            self.flight.release(&key, gate);
        }
        result
    }

    /// 能力协商：沿音质阶梯往下找，取「真的有上游能服务」的最高档。
    ///
    /// 全都够不着时返回原始档位，让竞速层照常报 `NoCandidate`，
    /// 免得把「参数/源配错」这种问题伪装成「降档成功」。
    fn effective_quality(&self, source: &str, requested: &'static str) -> &'static str {
        let mut candidate = requested;
        loop {
            if self
                .upstreams
                .iter()
                .any(|upstream| upstream.serves(source, candidate))
            {
                return candidate;
            }
            match quality::downgrade(candidate) {
                Some(next) => candidate = next,
                None => return requested,
            }
        }
    }

    /// 持锁之后的逻辑（锁在函数内部获取，保证任何返回路径都会释放）
    async fn resolve_locked(
        &self,
        key: &str,
        source: &'static str,
        music_id: &str,
        quality: &'static str,
        requested: &'static str,
        gate: Option<&Arc<Mutex<()>>>,
    ) -> Result<Resolved, AppError> {
        // try_lock 成功 → 我们就是 leader，**从这一步起一直持锁**，不存在竞态窗口；
        // try_lock 失败 → 已有 leader 在跑，挂起等待，醒来后靠下面的二次检查直接复用结果；
        // gate == None → singleflight 表已满，本次放弃合并，直接打上游（fail-open）。
        let mut coalesced = false;
        let _gate = match gate {
            Some(gate) => match gate.try_lock() {
                Ok(guard) => Some(guard),
                Err(_) => {
                    coalesced = true;
                    self.stats.coalesced.fetch_add(1, Ordering::Relaxed);
                    debug!(%key, "coalescing: waiting for the in-flight request");
                    Some(gate.lock().await)
                }
            },
            None => None,
        };

        // 二次检查：如果我们是等待者，leader 此刻已经把结果写进 L1/L2 了；
        // 如果 leader 挂了，我们顺势成为新的 leader。这就是「防击穿」的关键。
        if let Some((url, layer)) = self.cache_lookup(key).await {
            return Ok(Resolved::new(url, layer, requested, coalesced, source));
        }

        // ---- 3) 上游竞速（唯一的穿透点） ----
        // 过载保护：拿不到令牌就快速失败，而不是把请求堆在队列里拖垮自己
        let permit = self.limiter.try_acquire().map_err(|_| AppError::Overloaded)?;

        // 音质降级链：flac24bit → flac → 320k → 128k。
        // 只有「这一档拿不到」（NoUrl / 4xx）才值得降档；超时/过载直接放弃，别把预算烧光。
        let deadline_at = Instant::now() + FALLBACK_BUDGET;
        let mut candidate = quality;
        let mut winner: Option<SourceUrl> = None;
        let mut last_error: Option<UpstreamError> = None;

        loop {
            let remaining = deadline_at.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let slice = RACE_DEADLINE.min(remaining);
            let request = TrackRequest::new(source, music_id, candidate);
            match upstream::race(&self.upstreams, &request, slice).await {
                Ok(url) => {
                    winner = Some(url);
                    break;
                }
                Err(err) => {
                    let degradable = match &err {
                        UpstreamError::NoUrl { .. } | UpstreamError::Unsupported { .. } => true,
                        UpstreamError::Status { status, .. } => status.is_client_error(),
                        _ => false,
                    };
                    last_error = Some(err);
                    if !degradable {
                        break;
                    }
                    match quality::downgrade(candidate) {
                        Some(next) => candidate = next,
                        None => break,
                    }
                }
            }
        }
        drop(permit);

        let Some(mut url) = winner else {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            let err = last_error.unwrap_or(UpstreamError::NoCandidate {
                source_key: source.to_string(),
            });
            warn!(
                upstream = err.upstream(),
                source,
                id = music_id,
                error = %err,
                "all upstreams failed after the quality chain"
            );
            return Err(AppError::from(err));
        };

        // 上游可能连协商后的档位都没给到（网易云就可能静默降级）：
        // 空档位补成请求档位，其余以实际返回为准，响应里会标 `downgraded`。
        if url.quality.is_empty() {
            url.quality = candidate.to_string();
        }

        // 注意：这里**不再**改写直链。代拉短链只活 30 分钟，写进缓存就会出现
        // 「10 分钟后命中缓存 → 返回过期 token → 播放器 404 静音」，改写统一放到出口（respond）。

        self.stats.upstream_hits.fetch_add(1, Ordering::Relaxed);

        // ---- 4) 回填缓存：L1 同步写（极快），L2 丢后台 ----
        L1_CACHE.insert(key.to_owned(), Arc::new(url.clone())).await;

        {
            let l2 = Arc::clone(&self.l2);
            let key = key.to_owned();
            let payload = url.clone();
            tokio::spawn(async move { l2.set(&key, &payload).await });
        }

        Ok(Resolved::new(url, "upstream", requested, coalesced, source))
    }

    /// 跨平台回退：主源彻底失败时，拿关键词去网易云搜一次再解析。
    ///
    /// 目前只有网易云实现了真实搜索（kw/kg 的搜索接口都要反爬会话，见 README）。
    /// 跨平台回退：主源彻底失败时，拿歌名（或 keyword）去**每家并发搜一次**，谁先给出可播 id 用谁。
    ///
    /// 零 CK 的现实是：热门原曲在四家的免费库常常只有某一档能拿，甚至只有 Live/DJ 版。
    /// 所以这里不再只问网易云，而是 kg / kw / mg / wy 一起问。
    async fn cross_source(
        &self,
        keyword: &str,
        quality: &'static str,
    ) -> Result<Resolved, AppError> {
        const ORDER: [&str; 4] = ["kg", "kw", "mg", "wy"];

        let mut searchers: Vec<(&'static str, Arc<dyn Upstream>)> = Vec::new();
        for source in ORDER {
            if let Some(searcher) = self
                .upstreams
                .iter()
                .find(|upstream| upstream.supports(source))
                .cloned()
            {
                searchers.push((source, searcher));
            }
        }
        if searchers.is_empty() {
            return Err(AppError::Upstream(
                "cross-source fallback needs at least one searchable adapter".to_string(),
            ));
        }

        // 搜到 id 就立刻开始解析：用 futures 的 join_all 语义手写一个「先成功先用」
        let mut tasks = tokio::task::JoinSet::new();
        for (source, searcher) in searchers {
            let keyword = keyword.to_string();
            tasks.spawn(async move {
                let id = searcher.search_keyword(&keyword).await.map_err(AppError::from)?;
                Ok::<(&'static str, String), AppError>((source, id))
            });
        }

        let mut matched: Vec<(&'static str, String)> = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(pair)) => matched.push(pair),
                Ok(Err(err)) => debug!(error = %err, "a search adapter found nothing"),
                Err(err) => debug!(error = %err, "search task failed"),
            }
        }

        let mut last_error: Option<AppError> = None;
        for (source, song_id) in matched {
            debug!(search = keyword, source, song_id = %song_id, "cross-source fallback matched a track");
            match self.resolve(source, &song_id, quality).await {
                Ok(resolved) => return Ok(resolved),
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            AppError::Upstream(format!("no cross-source match for `{keyword}`"))
        }))
    }

    /// 多平台混音：主源失败时依次拿同一个 id 去零凭据平台试。
    ///
    /// 注意这里是「同一个 id」——只有 kw / wy / mg 的 id 有可能互通（都可能是纯数字）：
    /// tx 的 songmid、kg 的 hash 拿去问酷我必然是错的 id，所以非纯数字直接跳过。
    async fn multi_source(&self, music_id: &str, quality: &'static str) -> Option<Resolved> {
        if music_id.is_empty() || !music_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        for candidate in MULTI_SOURCE_LADDER {
            match self.resolve(candidate, music_id, quality).await {
                Ok(resolved) => {
                    debug!(source = candidate, "multi-source ladder served the request");
                    return Some(resolved);
                }
                Err(err) => {
                    debug!(source = candidate, error = %err, "multi-source candidate failed");
                }
            }
        }
        None
    }

    /// 这个源的直链要不要走网关代拉（`AUTO_PROXY`，默认只代拉 `tx`）。
    ///
    /// 只在**出口**用（`respond`）：缓存里永远是上游原链，这里决定了给客户端的是原链还是 `/stream`。
    fn should_proxy(&self, source: &str) -> bool {
        static AUTO_PROXY: Lazy<Vec<String>> = Lazy::new(|| {
            std::env::var("AUTO_PROXY")
                .unwrap_or_else(|_| "tx".to_string())
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect()
        });
        AUTO_PROXY.iter().any(|item| item == source)
    }

    /// L1 → L2 的读路径（带直链有效期校验），返回 `(直链, 命中层级)`
    async fn cache_lookup(&self, key: &str) -> Option<(SourceUrl, &'static str)> {
        // L1
        if let Some(hit) = L1_CACHE.get(key).await {
            if hit.is_fresh() {
                self.stats.l1_hits.fetch_add(1, Ordering::Relaxed);
                return Some(((*hit).clone(), "l1"));
            }
            // 直链已过期：主动剔除，避免继续返回死链
            L1_CACHE.invalidate(key).await;
            debug!(%key, "L1 entry expired (signed url), dropped");
        }

        // L2
        if let Some(hit) = self.l2.get(key).await {
            if !hit.is_fresh() {
                debug!(%key, "L2 entry expired (signed url), ignored");
                return None;
            }
            self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);
            // 回填 L1：下一次同 key 请求直接命中内存
            L1_CACHE.insert(key.to_owned(), Arc::new(hit.clone())).await;
            return Some((hit, "l2"));
        }

        None
    }
}

// ---------------------------------------------------------------------------
// 3 + 4. 竞速 + 超时
//
// 实现已经收敛到 `upstream::race`：每个适配器自带 800ms 硬超时（包住「发送 + 读 body」），
// 竞速整体 850ms 兜底，赢家返回后其余任务立即 abort —— 真取消，不浪费上游配额。
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 6. 路由与 handler
// ---------------------------------------------------------------------------

/// 一行访问日志 —— 这是进程里唯一的正常输出（其它 INFO/DEBUG 都被格式化器吞掉）
fn access_log(status: u16, source: &str, music_id: &str, quality: &str, started: Instant) {
    tracing::info!(
        target: logfmt::ACCESS_TARGET,
        status = status,
        source = source,
        id = music_id,
        quality = quality,
        elapsed_ms = started.elapsed().as_millis() as u64,
    );
}

/// `GET /api/parse?id=<musicId>&source=kw&quality=320k`
///
/// 跨平台回退示例：`?source=tx&id=<songmid>&fallback=1&name=海阔天空&singer=Beyond`
async fn parse_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ParseQuery>,
) -> Result<Json<OkBody>, AppError> {
    let source = normalize_source(query.source.as_deref())?;
    let quality = normalize_quality(query.quality.as_deref())?;
    let music_id = validate_music_id(query.id.as_deref())?;
    let options = ParseOptions::from_query(&query);
    respond(&state, public_base(&headers), source, music_id, quality, &options).await
}

/// `GET /url/{source}/{music_id}?quality=320k`（LX 风格的短链接口，方便脚本直连）
async fn lx_url_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((source, music_id)): Path<(String, String)>,
    Query(query): Query<QualityQuery>,
) -> Result<Json<OkBody>, AppError> {
    let source = normalize_source(Some(source.as_str()))?;
    let quality = normalize_quality(query.quality.as_deref())?;
    let music_id = validate_music_id(Some(music_id.as_str()))?;
    respond(
        &state,
        public_base(&headers),
        source,
        music_id,
        quality,
        &ParseOptions::default(),
    )
    .await
}

/// 两个入口共用的响应组装：解析（含降级链 / 跨平台回退） → LX 响应包 → 一行访问日志
async fn respond(
    state: &AppState,
    client_base: String,
    source: &'static str,
    music_id: &str,
    quality: &'static str,
    options: &ParseOptions,
) -> Result<Json<OkBody>, AppError> {
    let started = Instant::now();
    state.stats.total.fetch_add(1, Ordering::Relaxed);

    let resolved = match state.resolve(source, music_id, quality).await {
        Ok(resolved) => resolved,
        Err(primary) => {
            // 主源彻底失败：先试零凭据多平台（multi=1），再走网易云搜索回退（fallback=1）
            if options.multi {
                if let Some(resolved) = state.multi_source(music_id, quality).await {
                    return finish(state, client_base, resolved, music_id, quality, started).await;
                }
            }
            let Some(keyword) = options.fallback_keyword() else {
                access_log(primary.status().as_u16(), source, music_id, quality, started);
                return Err(primary);
            };
            match state.cross_source(&keyword, quality).await {
                Ok(resolved) => {
                    debug!(search = %keyword, "cross-source fallback served the request");
                    resolved
                }
                Err(fallback_err) => {
                    debug!(error = %fallback_err, "cross-source fallback also failed");
                    access_log(primary.status().as_u16(), source, music_id, quality, started);
                    return Err(primary);
                }
            }
        }
    };

    finish(state, client_base, resolved, music_id, quality, started).await
}

/// 五个平台各自有多少适配器在服务（启动自检与 `/health` 共用）
fn platform_matrix(upstreams: &[Arc<dyn Upstream>]) -> serde_json::Value {
    const PLATFORMS: [&str; 5] = ["tx", "kg", "kw", "wy", "mg"];
    let mut platforms = serde_json::Map::new();
    for source in PLATFORMS {
        let adapters: Vec<&str> = upstreams
            .iter()
            .filter(|upstream| upstream.supports(source))
            .map(|upstream| upstream.name())
            .collect();
        platforms.insert(
            source.to_string(),
            serde_json::json!({ "adapters": adapters, "ok": !adapters.is_empty() }),
        );
    }
    serde_json::Value::Object(platforms)
}

/// `GET /health`：五平台适配器矩阵 + L2 状态，直接给监控/压测脚本用
async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    const PLATFORMS: [&str; 5] = ["tx", "kg", "kw", "wy", "mg"];
    let platforms = platform_matrix(&state.upstreams);

    let missing: Vec<String> = PLATFORMS
        .iter()
        .filter(|source| {
            !platforms
                .get(**source)
                .and_then(|entry| entry.get("ok"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        })
        .map(|source| (*source).to_string())
        .collect();

    let status = if missing.is_empty() { "ok" } else { "degraded" };
    Json(serde_json::json!({
        "status": status,
        "l2": state.l2.is_enabled(),
        "adapters": state.upstreams.len(),
        "mock_upstreams": state
            .upstreams
            .iter()
            .filter(|upstream| upstream.is_mock())
            .count(),
        "platforms": platforms,
        "missing_platforms": missing,
    }))
}

/// 代拉短链的对外 base：**优先用请求自己的 Host**，其次 `PUBLIC_BASE_URL`，最后才是 BIND_ADDR。
///
/// 以前只看 BIND_ADDR，手机上推出的是 `http://127.0.0.1:8080` —— 客户端（PC 上的 LX、
/// 另一台手机、反代后面的浏览器）拿到这个地址会去连它自己，必然「解析成功但没声音」。
fn public_base(headers: &HeaderMap) -> String {
    let from_request = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(|host| {
            // 反代常见：X-Forwarded-Proto 表示客户端那侧的协议
            let scheme = headers
                .get("x-forwarded-proto")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|scheme| *scheme == "http" || *scheme == "https")
                .unwrap_or("http");
            format!("{scheme}://{host}")
        });

    if let Some(base) = from_request {
        return base;
    }
    if let Ok(base) = std::env::var("PUBLIC_BASE_URL") {
        if !base.trim().is_empty() {
            return base.trim().trim_end_matches('/').to_string();
        }
    }
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let port = bind.rsplit(':').next().unwrap_or("8080").to_string();
    format!("http://127.0.0.1:{port}")
}

fn sample_id(source: &str) -> Option<&'static str> {
    match source {
        "tx" => Some("002lzYCa14usQ5"),
        "wy" => Some("347230"),
        "kw" => Some("347230"),
        "mg" => Some("60090844416"),
        _ => None,
    }
}

/// 启动自检：先看五个平台各自有没有适配器，`STARTUP_PROBE=1` 时再五路并发真打一发
async fn startup_health_check(state: &AppState) {
    const PLATFORMS: [&str; 5] = ["tx", "kg", "kw", "wy", "mg"];
    let platforms = platform_matrix(&state.upstreams);
    info!(matrix = %platforms, "startup health check");

    for source in PLATFORMS {
        let ok = platforms
            .get(source)
            .and_then(|entry| entry.get("ok"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !ok {
            warn!(
                platform = source,
                "startup health check: no adapter for this platform（缺节点/cookie，只能靠跨平台回退）"
            );
        }
    }

    let probe_enabled = std::env::var("STARTUP_PROBE")
        .map(|value| !value.is_empty() && value != "0")
        .unwrap_or(false);
    if !probe_enabled {
        return;
    }

    let mut tasks = tokio::task::JoinSet::new();
    for source in PLATFORMS {
        let Some(sample) = sample_id(source) else {
            warn!(platform = source, "startup probe skipped: no sample track id");
            continue;
        };
        let state = state.clone();
        tasks.spawn(async move {
            let started = Instant::now();
            let probe = tokio::time::timeout(
                Duration::from_millis(800),
                state.resolve(source, sample, "128k"),
            )
            .await;
            (source, matches!(probe, Ok(Ok(_))), started.elapsed())
        });
    }

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((source, true, elapsed)) => {
                info!(platform = source, elapsed_ms = elapsed.as_millis() as u64, "startup probe ok");
            }
            Ok((source, false, elapsed)) => {
                warn!(platform = source, elapsed_ms = elapsed.as_millis() as u64, "startup probe failed");
            }
            Err(err) => warn!(error = %err, "startup probe task panicked"),
        }
    }
}

// ---------------------------------------------------------------------------
// 参数校验
// ---------------------------------------------------------------------------

fn normalize_source(raw: Option<&str>) -> Result<&'static str, AppError> {
    let value = raw.map(str::trim).filter(|v| !v.is_empty()).unwrap_or(DEFAULT_SOURCE);
    SUPPORTED_SOURCES
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(value))
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "`source` must be one of {:?}, got `{value}`", SUPPORTED_SOURCES
            ))
        })
}

fn validate_music_id(raw: Option<&str>) -> Result<&str, AppError> {
    let id = raw.unwrap_or("").trim();
    if id.is_empty() {
        return Err(AppError::BadRequest("`id` query parameter is required".to_string()));
    }
    if id.len() > 128 {
        return Err(AppError::BadRequest("`id` is longer than 128 bytes".to_string()));
    }
    // 只允许字母数字和少量分隔符：既防注入，也避免缓存 key 里出现奇怪字符
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
    {
        return Err(AppError::BadRequest(
            "`id` may only contain [A-Za-z0-9._:-]".to_string(),
        ));
    }
    Ok(id)
}

// ---------------------------------------------------------------------------
// 上游适配器装配：环境变量 → 适配器注册表
// ---------------------------------------------------------------------------

/// 从环境变量装配上游适配器。
///
/// | 环境变量 | 作用 |
/// | --- | --- |
/// | `UPSTREAM_A_URL` / `UPSTREAM_B_URL` | 解析节点地址（不配就跳过这一路） |
/// | `UPSTREAM_A_PATH` / `UPSTREAM_B_PATH` | 节点路径，默认 `/api/url`，支持 `{source}` `{id}` `{quality}` 模板 |
/// | `UPSTREAM_A_TOKEN` / `UPSTREAM_B_TOKEN` | 节点鉴权：`Authorization: Bearer <token>` |
/// | `UPSTREAM_A_COOKIE` / `UPSTREAM_B_COOKIE` | 节点会话 cookie（很多平台的解析节点靠它鉴权） |
/// | `ENABLE_NETEASE=1` | 打开网易云直连适配器（真实音源，源 `wy`） |
/// | `NETEASE_COOKIE` | 网易云登录 cookie，带上才能解锁 320k / flac |
/// | `NETEASE_BASE` | 网易云接口反代地址，默认 `https://music.163.com` |
/// | `ENABLE_QQ=1` 或直接给 `TX_COOKIE` | 打开 QQ 音乐直连适配器（源 `tx`） |
/// | `TX_COOKIE` | QQ 音乐整段 cookie（`qm_keyst=...; uin=o0<QQ号>`），没登录只有 128k |
/// | `TX_UIN` | QQ 号（登录态必须，缺了会被当游客） |
/// | `ENABLE_KUWO`（默认开） | 酷我直连（源 `kw`），`=0` 关闭；零凭据可到 320k |
/// | `KW_COOKIE` | 酷我会员 cookie，带上才可能拿到 flac |
/// | `ENABLE_KUGOU`（默认开） | 酷狗 v5 签名直连（源 `kg`），`=0` 关闭；零凭据可到 flac |
/// | `KG_COOKIE` | 酷狗 cookie（v5 主链路不需要，留给以后解锁更高权益） |
/// | `ENABLE_MIGU`（默认开） | 咪咕试听地址直连（源 `mg`），`=0` 关闭；零凭据 128k |
/// | `MG_COOKIE` / `MG_USER_ID` | 咪咕会员 cookie / 登录 userId，带上才有 HQ/SQ |
///
/// 一个真实上游都没配 → 自动起两个进程内 mock，保证 `cargo run` 就能跑通全链路。
fn build_upstreams() -> Arc<Vec<Arc<dyn Upstream>>> {
    let mut upstreams: Vec<Arc<dyn Upstream>> = Vec::new();

    for (url_var, path_var, token_var, cookie_var, name) in [
        ("UPSTREAM_A_URL", "UPSTREAM_A_PATH", "UPSTREAM_A_TOKEN", "UPSTREAM_A_COOKIE", "node-a"),
        ("UPSTREAM_B_URL", "UPSTREAM_B_PATH", "UPSTREAM_B_TOKEN", "UPSTREAM_B_COOKIE", "node-b"),
    ] {
        let Ok(base) = std::env::var(url_var) else { continue };
        if base.trim().is_empty() {
            continue;
        }

        let mut node = NodeUpstream::new(name, base, UPSTREAM_TIMEOUT);
        if let Ok(path) = std::env::var(path_var) {
            if !path.trim().is_empty() {
                node = node.with_path(path);
            }
        }
        if let Ok(token) = std::env::var(token_var) {
            node = node.with_token(Some(token));
        }
        if let Ok(cookie) = std::env::var(cookie_var) {
            node = node.with_cookie(Some(cookie));
        }
        info!(upstream = name, "解析节点适配器已注册");
        upstreams.push(Arc::new(node));
    }

    let netease_enabled = std::env::var("ENABLE_NETEASE")
        .map(|value| !value.is_empty() && value != "0")
        .unwrap_or(false);
    if netease_enabled {
        let cookie = std::env::var("NETEASE_COOKIE").ok();
        let mut netease = NeteaseUpstream::new(UPSTREAM_TIMEOUT).with_cookie(cookie);
        if let Ok(base) = std::env::var("NETEASE_BASE") {
            if !base.trim().is_empty() {
                netease = netease.with_base(base);
            }
        }
        upstreams.push(Arc::new(netease));
        info!("网易云直连适配器已注册（源 wy）");
    }

    let qq_enabled = std::env::var("ENABLE_QQ")
        .map(|value| !value.is_empty() && value != "0")
        .unwrap_or(false)
        || std::env::var("TX_COOKIE")
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
    if qq_enabled {
        let cookie = std::env::var("TX_COOKIE").ok();
        let uin = std::env::var("TX_UIN").ok();
        upstreams.push(Arc::new(
            QqUpstream::new(UPSTREAM_TIMEOUT).with_credentials(cookie, uin),
        ));
        info!("QQ 音乐直连适配器已注册（源 tx）");
    }

    // 下面三个平台**零凭据**就能直连（酷我 / 酷狗 / 咪咕），默认打开；
    // 想关掉哪个就设 ENABLE_KUWO=0 / ENABLE_KUGOU=0 / ENABLE_MIGU=0。
    let enabled_by_default = |flag: &str| {
        std::env::var(flag)
            .map(|value| !value.is_empty() && value != "0")
            .unwrap_or(true)
    };

    if enabled_by_default("ENABLE_KUWO") {
        let cookie = std::env::var("KW_COOKIE").ok();
        upstreams.push(Arc::new(KwUpstream::new(UPSTREAM_TIMEOUT).with_cookie(cookie)));
        info!("酷我直连适配器已注册（源 kw，mobi 签名直链，零凭据 320k）");
    }

    if enabled_by_default("ENABLE_KUGOU") {
        let cookie = std::env::var("KG_COOKIE").ok();
        upstreams.push(Arc::new(KgUpstream::new(UPSTREAM_TIMEOUT).with_cookie(cookie)));
        info!("酷狗直连适配器已注册（源 kg，v5 签名直链，零凭据可到 flac）");
    }

    if enabled_by_default("ENABLE_MIGU") {
        let cookie = std::env::var("MG_COOKIE").ok();
        let user_id = std::env::var("MG_USER_ID").ok();
        upstreams.push(Arc::new(
            MiguUpstream::new(UPSTREAM_TIMEOUT).with_credentials(cookie, user_id),
        ));
        info!("咪咕直连适配器已注册（源 mg，302 试听地址，零凭据 128k）");
    }

    if upstreams.is_empty() {
        warn!("所有真实上游都被关掉了，回退到两个进程内 mock（删掉 ENABLE_*=0 或配 UPSTREAM_A_URL / ENABLE_NETEASE=1 即可接真实音源）");
        upstreams.push(Arc::new(MockUpstream::new("upstream-a", 80)));
        upstreams.push(Arc::new(MockUpstream::new("upstream-b", 170)));
    }

    Arc::new(upstreams)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // 极客日志：自定义 FormatEvent。只有 target = "access" 的事件会渲染成一行
    // （08:53:38 | [200] tx 002RsKKv2Rd7Ih        flac      | 780ms），
    // 其它 INFO/DEBUG（启动信息、连接池、缓存命中）全部吞掉，WARN/ERROR 留精简版。
    tracing_subscriber::fmt()
        .event_format(logfmt::AccessFormat)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("error,access=info,audio_parse_gateway=info")),
        )
        .init();

    // 预热全局连接池（配置错在启动期就炸，而不是等第一个请求）
    http::warm_up();
    info!(
        max_idle_per_host = http::POOL_MAX_IDLE_PER_HOST,
        ttl_secs = L1_TTL.as_secs(),
        "global http pool & L1 cache warmed up"
    );

    // 上游适配器注册表：配了真实音源就用真的，一个都没配就起两个 mock
    let upstreams = build_upstreams();
    info!(
        adapters = ?upstreams.iter().map(|upstream| upstream.name()).collect::<Vec<_>>(),
        "upstream adapters registered"
    );

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());

    // 启动探测：没装 Redis 就在这里把 L2 关掉，之后每个请求零等待（不再等 500ms 超时）
    let l2 = Arc::new(L2Cache::new(redis_url));
    let _ = l2.probe().await; // 探测结果不影响启动：连不上就是 L1-only

    let state = AppState {
        upstreams,
        l2,
        flight: Arc::new(SingleFlight::default()),
        limiter: Arc::new(Semaphore::new(UPSTREAM_MAX_INFLIGHT)),
        stats: Arc::new(Stats::default()),
        proxy: Arc::new(proxy::ProxyStore::new(String::new())),
    };

    // 启动自检：tx/kg/kw/wy/mg 五平台的适配器矩阵（STARTUP_PROBE=1 时再并发真打一发）
    startup_health_check(&state).await;

    // 预热：每个适配器后台打一发，把 DNS + TLS 提前握好（不阻塞启动，失败也无所谓）
    for upstream in state.upstreams.iter() {
        let upstream = Arc::clone(upstream);
        tokio::spawn(async move { upstream.warm_up().await });
    }

    let app = Router::new()
        .route("/api/parse", get(parse_handler))
        .route("/url/{source}/{music_id}", get(lx_url_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        // 防盗链代拉：/stream/{token} 会补齐 Referer 再把音频流吐回去
        .route("/stream/{token}", get(proxy::stream_handler))
        .with_state(state);

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let addr: SocketAddr = bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!("LX parse gateway listening on http://{addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("gateway stopped cleanly");
    Ok(())
}

/// 优雅退出：收到 Ctrl-C / SIGTERM 后停止接收新连接，等在途请求跑完
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, draining in-flight requests…");
}

// ---------------------------------------------------------------------------
// 出口：响应组装（多平台混音 / 网易云回退都从这里出去）
// ---------------------------------------------------------------------------

async fn finish(
    state: &AppState,
    client_base: String,
    resolved: Resolved,
    music_id: &str,
    quality: &'static str,
    started: Instant,
) -> Result<Json<OkBody>, AppError> {
    let downgraded = resolved.downgraded();
    if downgraded {
        state.stats.downgraded.fetch_add(1, Ordering::Relaxed);
    }

    // 直链是不是 mock 造的假地址（给客户端一个明确信号，别让它傻试）
    let mock = state
        .upstreams
        .iter()
        .any(|upstream| upstream.is_mock() && upstream.name() == resolved.url.provider.as_str());

    // ---- 出口改写：缓存里是上游原链，这里才决定给客户端原链还是 /stream 代拉 ----
    let upstream_url = resolved.url.url.clone();
    let proxied = state.should_proxy(resolved.source);
    let play_url = if proxied {
        let referer = upstream::referer_for(resolved.source).to_string();
        state
            .proxy
            .link_with_base(client_base.as_str(), upstream_url.clone(), referer)
    } else {
        upstream_url.clone()
    };

    access_log(
        200,
        resolved.source,
        music_id,
        &resolved.url.quality,
        started,
    );
    debug!(proxied, upstream = %upstream_url, play = %play_url, "resolved for the client");

    Ok(Json(OkBody {
        code: 0,
        msg: "success",
        data: TrackData {
            url: play_url,
            source: resolved.source.to_string(),
            quality: resolved.url.quality.clone(),
            requested_quality: quality.to_string(),
            downgraded,
            mock,
            provider: resolved.url.provider,
            cache: resolved.layer,
            coalesced: resolved.coalesced,
            elapsed_ms: started.elapsed().as_millis() as u64,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// 测试里模拟「请求来自 http://gw.test」：出口改写会用它拼代拉短链
    const TEST_BASE: &str = "http://gw.test:8080";

    /// 可编程的假适配器：能配延迟、强制失败、只支持某个音源、最高只给到某档音质。
    #[derive(Debug)]
    struct FakeUpstream {
        name: &'static str,
        delay: Duration,
        fails: bool,
        only: Option<&'static str>,
        ceiling: &'static str,
        /// 高于这一档的音质直接拒绝（模拟「无损没版权/没会员」）
        fail_above: Option<&'static str>,
        /// 关键词搜索能返回的 id（跨平台回退用）
        search_result: Option<&'static str>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeUpstream {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                delay: Duration::ZERO,
                fails: false,
                only: None,
                ceiling: quality::TOP,
                fail_above: None,
                search_result: None,
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }

        fn failing(mut self) -> Self {
            self.fails = true;
            self
        }

        fn only_for(mut self, source: &'static str) -> Self {
            self.only = Some(source);
            self
        }

        /// 模拟「这路上游最高只能给到这一档」，比如没 cookie 的网易云
        fn with_ceiling(mut self, ceiling: &'static str) -> Self {
            self.ceiling = ceiling;
            self
        }

        /// 模拟「高于这一档就拒绝」：请求 320k 可以，flac 直接 NoUrl
        fn failing_above(mut self, ceiling: &'static str) -> Self {
            self.fail_above = Some(ceiling);
            self
        }

        /// 模拟一个支持关键词搜索的适配器（跨平台回退用）
        fn can_search(mut self, id: &'static str) -> Self {
            self.search_result = Some(id);
            self
        }
    }

    #[async_trait::async_trait]
    impl Upstream for FakeUpstream {
        fn name(&self) -> &str {
            self.name
        }

        fn supports(&self, source: &str) -> bool {
            self.only.is_none_or(|only| only == source)
        }

        fn max_quality(&self) -> &'static str {
            self.ceiling
        }

        async fn search_keyword(&self, keyword: &str) -> Result<String, UpstreamError> {
            match self.search_result {
                Some(id) => Ok(id.to_string()),
                None => Err(UpstreamError::Unsupported {
                    upstream: self.name.to_string(),
                    source_key: format!("search:{keyword}"),
                }),
            }
        }

        async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(limit) = self.fail_above {
                if !quality::within(req.quality, limit) {
                    return Err(UpstreamError::NoUrl {
                        upstream: self.name.to_string(),
                        detail: format!("{} not available", req.quality),
                    });
                }
            }
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if self.fails {
                return Err(UpstreamError::Status {
                    upstream: self.name.to_string(),
                    status: StatusCode::BAD_GATEWAY,
                });
            }
            Ok(SourceUrl {
                url: format!(
                    "https://cdn.example/{}/{}/{}.mp3",
                    self.name, req.source, req.music_id
                ),
                provider: self.name.to_string(),
                quality: req.quality.to_string(),
                expires_at: now_unix() + 600,
            })
        }
    }

    fn state_with(upstreams: Vec<Arc<dyn Upstream>>) -> AppState {
        state_with_l2(upstreams, L2Cache::disabled())
    }

    fn state_with_l2(upstreams: Vec<Arc<dyn Upstream>>, l2: L2Cache) -> AppState {
        AppState {
            upstreams: Arc::new(upstreams),
            l2: Arc::new(l2),
            flight: Arc::new(SingleFlight::default()),
            limiter: Arc::new(Semaphore::new(64)),
            stats: Arc::new(Stats::default()),
            proxy: Arc::new(proxy::ProxyStore::new("http://127.0.0.1:8080".to_string())),
        }
    }

    #[tokio::test]
    async fn race_keeps_the_fastest_winner() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(FakeUpstream::new("slow").with_delay(Duration::from_millis(300))),
            Arc::new(FakeUpstream::new("fast").with_delay(Duration::from_millis(5))),
        ];
        let resolved = state_with(upstreams)
            .resolve("kw", "race-fast-1", "320k")
            .await
            .expect("最快的那一路应该赢");
        assert_eq!(resolved.url.provider, "fast");
        assert_eq!(resolved.layer, "upstream");
    }

    #[tokio::test]
    async fn race_fails_over_to_a_healthy_upstream() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(FakeUpstream::new("broken").failing()),
            Arc::new(FakeUpstream::new("healthy").with_delay(Duration::from_millis(10))),
        ];
        let resolved = state_with(upstreams)
            .resolve("kw", "race-failover-1", "320k")
            .await
            .expect("一路失败不应影响整体");
        assert_eq!(resolved.url.provider, "healthy");
    }

    #[tokio::test]
    async fn all_upstreams_failing_maps_to_502() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("a").failing())];
        let err = state_with(upstreams)
            .resolve("kw", "race-allfail-1", "320k")
            .await
            .unwrap_err();
        assert_eq!(err.into_response().status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn source_without_candidate_is_rejected() {
        let upstreams: Vec<Arc<dyn Upstream>> =
            vec![Arc::new(FakeUpstream::new("wy-only").only_for("wy"))];
        let err = state_with(upstreams)
            .resolve("kw", "no-candidate-1", "320k")
            .await
            .unwrap_err();
        assert_eq!(err.into_response().status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn upstream_errors_map_to_http_status() {
        let timeout = AppError::from(UpstreamError::Timeout {
            upstream: "node-a".to_string(),
            after: UPSTREAM_TIMEOUT,
        });
        assert_eq!(timeout.into_response().status(), StatusCode::GATEWAY_TIMEOUT);

        let no_url = AppError::from(UpstreamError::NoUrl {
            upstream: "netease".to_string(),
            detail: "no url".to_string(),
        });
        assert_eq!(no_url.into_response().status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn second_request_is_served_from_l1() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("cache-a"))];
        let state = state_with(upstreams);

        let first = state.resolve("kw", "cache-hit-1", "320k").await.expect("首查");
        assert_eq!(first.layer, "upstream");

        let second = state.resolve("kw", "cache-hit-1", "320k").await.expect("二次命中");
        assert_eq!(second.layer, "l1");
        assert_eq!(second.url.url, first.url.url);
    }

    #[tokio::test]
    async fn expired_signed_url_is_not_served() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("exp-a"))];
        let state = state_with(upstreams);

        L1_CACHE
            .insert(
                "kw:expired-1:320k".to_string(),
                Arc::new(SourceUrl {
                    url: "https://dead.example/x.mp3".to_string(),
                    provider: "dead".to_string(),
                    quality: "320k".to_string(),
                    expires_at: now_unix().saturating_sub(60),
                }),
            )
            .await;

        let resolved = state
            .resolve("kw", "expired-1", "320k")
            .await
            .expect("过期条目应该重新解析");
        assert_eq!(resolved.layer, "upstream");
        assert_ne!(resolved.url.url, "https://dead.example/x.mp3");
    }

    #[tokio::test]
    async fn singleflight_coalesces_concurrent_requests() {
        let fake = FakeUpstream::new("slow").with_delay(Duration::from_millis(150));
        let calls = Arc::clone(&fake.calls);
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(fake)];
        let state = Arc::new(state_with(upstreams));

        let mut handles = Vec::new();
        for _ in 0..6 {
            let state = Arc::clone(&state);
            handles.push(tokio::spawn(async move { state.resolve("kw", "flight-1", "320k").await }));
        }

        let mut coalesced = 0;
        for handle in handles {
            let resolved = handle.await.expect("task 不应 panic").expect("并发请求都应成功");
            if resolved.coalesced {
                coalesced += 1;
            }
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1, "6 个并发请求只应打一次上游");
        assert!(coalesced >= 1, "至少应有请求被 singleflight 合并");
    }

    #[tokio::test]
    async fn overload_sheds_load_with_503() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("limit-a"))];
        let state = state_with(upstreams);

        // 把 64 个上游并发令牌全部占满，下一个请求必须直接 503 而不是排队
        let permits = Arc::clone(&state.limiter)
            .acquire_many_owned(64)
            .await
            .expect("应该能拿到令牌");

        let err = state.resolve("kw", "limit-1", "320k").await.unwrap_err();
        assert_eq!(err.into_response().status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(permits);
    }

    #[tokio::test]
    async fn metrics_exposes_prometheus_text() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("m"))];
        let state = state_with(upstreams);
        // 必须走 handler（respond）才会累加 requests_total；直接 resolve 只动命中/穿透计数
        let payload = respond(&state, TEST_BASE.to_string(), "kw", "metrics-1", "320k", &ParseOptions::default())
            .await
            .expect("先打一发");
        assert_eq!(payload.0.code, 0);
        assert!(payload.0.data.url.ends_with(".mp3"));

        let response = metrics_handler(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(body.to_vec()).expect("utf8");

        assert!(text.contains("lx_gateway_requests_total 1"), "实际输出:\n{text}");
        assert!(text.contains(r#"lx_gateway_cache_hits_total{layer="l1"} 0"#));
        assert!(text.contains("lx_gateway_upstream_requests_total 1"));
    }

    #[test]
    fn normalises_source_and_quality() {
        assert_eq!(normalize_source(Some("KW")).expect("大小写不敏感"), "kw");
        assert_eq!(normalize_source(None).expect("默认源"), DEFAULT_SOURCE);
        assert!(normalize_source(Some("bogus")).is_err());

        assert_eq!(normalize_quality(Some(" FLAC ")).expect("去空格"), "flac");
        assert_eq!(normalize_quality(Some("")).expect("空串走默认"), DEFAULT_QUALITY);
        assert!(normalize_quality(Some("999k")).is_err());
    }

    #[test]
    fn validates_music_id() {
        assert_eq!(validate_music_id(Some(" abc123._:- ")).expect("合法 id"), "abc123._:-");
        assert!(validate_music_id(None).is_err());
        assert!(validate_music_id(Some("   ")).is_err());
        assert!(validate_music_id(Some("a/b")).is_err(), "路径分隔符必须被拒绝");
        assert!(validate_music_id(Some(&"x".repeat(129))).is_err());
    }

    /// L2 只在有 Redis 时才跑：CI 用 `services: redis` 提供 `REDIS_URL`，本地没起 redis 就自动跳过。
    #[tokio::test]
    async fn l2_cache_is_used_when_redis_is_available() {
        let Ok(redis_url) = std::env::var("REDIS_URL") else {
            eprintln!("跳过 L2 用例：未设置 REDIS_URL（本地没起 redis）");
            return;
        };

        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("l2-a"))];
        let state = state_with_l2(upstreams, L2Cache::new(redis_url));

        // 手动塞一条「别的实例写好」的 L2 记录，验证多实例共享缓存这条路径
        let key = "kw:l2-roundtrip-1:320k";
        let seeded = SourceUrl {
            url: "https://cdn-l2.example/seeded.mp3".to_string(),
            provider: "seed".to_string(),
            quality: "320k".to_string(),
            expires_at: now_unix() + 600,
        };
        state.l2.set(key, &seeded).await;

        let resolved = state
            .resolve("kw", "l2-roundtrip-1", "320k")
            .await
            .expect("L2 命中应该直接返回");
        assert_eq!(resolved.layer, "l2", "应该是从 Redis 拿到的");
        assert_eq!(resolved.url.url, "https://cdn-l2.example/seeded.mp3");
    }

    #[tokio::test]
    async fn quality_is_downgraded_when_no_adapter_reaches_it() {
        // 最高只到 320k 的适配器 + 请求 flac：应该直接按 320k 打过去，
        // 而不是先拿 flac 白打一次、再靠失败来降档。
        let upstreams: Vec<Arc<dyn Upstream>> =
            vec![Arc::new(FakeUpstream::new("lossy-only").with_ceiling("320k"))];
        let resolved = state_with(upstreams)
            .resolve("wy", "quality-downgrade-1", "flac")
            .await
            .expect("协商降档之后应该能成功");

        assert_eq!(resolved.url.quality, "320k", "实际档位要如实上报");
        assert!(resolved.downgraded(), "flac → 320k 必须标记成降档");
    }

    #[tokio::test]
    async fn full_quality_passes_through_when_adapter_supports_it() {
        // 默认 ceiling 是 flac24bit（无损全开），请求什么就该拿到什么
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("lossless"))];
        let resolved = state_with(upstreams)
            .resolve("wy", "quality-full-1", "flac24bit")
            .await
            .expect("无损适配器应该原样返回");

        assert_eq!(resolved.url.quality, "flac24bit");
        assert!(!resolved.downgraded());
    }

    #[tokio::test]
    async fn response_reports_requested_and_actual_quality() {
        let upstreams: Vec<Arc<dyn Upstream>> =
            vec![Arc::new(FakeUpstream::new("lossy").with_ceiling("320k"))];
        let state = state_with(upstreams);

        let payload = respond(&state, TEST_BASE.to_string(), "wy", "quality-report-1", "flac", &ParseOptions::default())
            .await
            .expect("降档之后仍然是成功响应");

        assert_eq!(payload.0.code, 0);
        assert_eq!(payload.0.data.requested_quality, "flac");
        assert_eq!(payload.0.data.quality, "320k");
        assert!(payload.0.data.downgraded);
    }

    #[tokio::test]
    async fn metrics_counts_quality_downgrades() {
        let upstreams: Vec<Arc<dyn Upstream>> =
            vec![Arc::new(FakeUpstream::new("lossy-m").with_ceiling("320k"))];
        let state = state_with(upstreams);
        let payload = respond(&state, TEST_BASE.to_string(), "wy", "quality-metric-1", "flac", &ParseOptions::default())
            .await
            .expect("降档请求");
        assert!(payload.0.data.downgraded, "这一发本来就该降档");

        let response = metrics_handler(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(body.to_vec()).expect("utf8");

        assert!(
            text.contains("lx_gateway_quality_downgrades_total 1"),
            "实际输出:\n{text}"
        );
    }

    #[tokio::test]
    async fn l2_disables_itself_after_one_failed_connect() {
        // 指向一个必然拒绝连接的端口：探测失败后必须永久关闭，之后请求路径零等待
        let l2 = L2Cache::new("redis://127.0.0.1:9/".to_string());
        assert!(!l2.probe().await, "连不上就算探测失败");
        assert!(!l2.is_enabled(), "探测失败后必须全局关闭 L2");

        let started = Instant::now();
        assert!(l2.get("l2-fast-path-1").await.is_none());
        l2.set(
            "l2-fast-path-1",
            &SourceUrl {
                url: "https://cdn.example/x.mp3".to_string(),
                provider: "none".to_string(),
                quality: "320k".to_string(),
                expires_at: 0,
            },
        )
        .await;
        assert!(
            started.elapsed() < Duration::from_millis(20),
            "关掉 L2 之后读路径必须零等待，实际 {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn quality_chain_falls_back_until_something_plays() {
        // 只有 320k 有版权：请求 flac24bit 应该一路降档到 320k，而不是直接 502
        let upstreams: Vec<Arc<dyn Upstream>> =
            vec![Arc::new(FakeUpstream::new("lossy-chain").failing_above("320k"))];

        let resolved = state_with(upstreams)
            .resolve("wy", "chain-1", "flac24bit")
            .await
            .expect("降级链应该救回来");

        assert_eq!(resolved.url.quality, "320k");
        assert!(resolved.downgraded(), "flac24bit → 320k 要标记降档");
    }

    #[tokio::test]
    async fn cross_source_fallback_searches_netease() {
        // 主源 tx 没有任何适配器；网易云适配器能搜到 id=999
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(FakeUpstream::new("netease").only_for("wy").can_search("999")),
        ];
        let state = state_with(upstreams);

        let options = ParseOptions {
            fallback: true,
            multi: false,
            keyword: Some("海阔天空 Beyond".to_string()),
        };
        let payload = respond(&state, TEST_BASE.to_string(), "tx", "002RsKKv2Rd7Ih", "320k", &options)
            .await
            .expect("跨平台回退应该成功");

        assert_eq!(payload.0.code, 0);
        assert_eq!(payload.0.data.source, "wy", "回退后应报实际的源");
        assert!(payload.0.data.url.contains("999"), "应走搜索到的 id");
    }

    #[tokio::test]
    async fn multi_source_ladder_rescues_a_numeric_id() {
        // tx 挂了（没 cookie），但同一个纯数字 id 在 kg 上有货 → multi=1 应该救回来
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(FakeUpstream::new("qq").only_for("tx").failing()),
            Arc::new(FakeUpstream::new("kugou").only_for("kg")),
        ];
        let state = state_with(upstreams);

        let options = ParseOptions {
            fallback: false,
            multi: true,
            keyword: None,
        };
        let payload = respond(&state, TEST_BASE.to_string(), "tx", "1234567", "128k", &options)
            .await
            .expect("多平台混音应该顶上");
        assert_eq!(payload.0.data.source, "kg", "应该报实际出直链的平台");
    }

    #[tokio::test]
    async fn multi_source_skips_ids_that_belong_to_another_platform() {
        // tx 的 songmid 不是纯数字，拿去问酷我/酷狗只会拿到错歌 → 直接不试
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(FakeUpstream::new("qq").only_for("tx").failing()),
            Arc::new(FakeUpstream::new("kugou").only_for("kg")),
        ];
        let state = state_with(upstreams);
        assert!(
            state.multi_source("002lzYCa14usQ5", "128k").await.is_none(),
            "非纯数字 id 不应该进混音链"
        );
    }

    #[tokio::test]
    async fn cross_source_is_skipped_without_fallback_flag() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(FakeUpstream::new("netease").only_for("wy").can_search("999")),
        ];
        let state = state_with(upstreams);

        // 没给 fallback=1：就算有关键词也不该去搜
        let options = ParseOptions {
            fallback: false,
            multi: false,
            keyword: Some("海阔天空".to_string()),
        };
        let err = respond(&state, TEST_BASE.to_string(), "tx", "002RsKKv2Rd7Ih", "320k", &options)
            .await
            .unwrap_err();
        assert_eq!(err.into_response().status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn health_matrix_covers_all_five_platforms() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(FakeUpstream::new("qq").only_for("tx")),
            Arc::new(FakeUpstream::new("any")),
        ];

        let matrix = platform_matrix(&upstreams);
        for source in ["tx", "kg", "kw", "wy", "mg"] {
            let ok = matrix
                .get(source)
                .and_then(|entry| entry.get("ok"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            assert!(ok, "{source} 应该至少有适配器在服务");
        }
        assert_eq!(matrix.get("tx").and_then(|entry| entry.get("adapters")).map(|v| v.as_array().map(|a| a.len()).unwrap_or(0)), Some(2));
    }

    #[tokio::test]
    async fn response_marks_mock_upstreams() {
        let upstreams: Vec<Arc<dyn Upstream>> =
            vec![Arc::new(MockUpstream::new("upstream-a", 0).without_failures())];
        let state = state_with(upstreams);

        let payload = respond(&state, TEST_BASE.to_string(), "kw", "mock-flag-1", "320k", &ParseOptions::default())
            .await
            .expect("mock 也要能正常返回");

        assert!(payload.0.data.mock, "mock 直链必须在响应里标出来");
        assert!(payload.0.data.url.contains("lx-mock.example"));
    }

    #[tokio::test]
    async fn real_upstream_is_not_marked_as_mock() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![Arc::new(FakeUpstream::new("qq").only_for("tx"))];
        let state = state_with(upstreams);

        let payload = respond(&state, TEST_BASE.to_string(), "tx", "mock-flag-2", "320k", &ParseOptions::default())
            .await
            .expect("真实适配器");

        assert!(!payload.0.data.mock, "真实适配器不能被标成 mock");
    }

    #[tokio::test]
    async fn health_counts_mock_upstreams() {
        let upstreams: Vec<Arc<dyn Upstream>> = vec![
            Arc::new(MockUpstream::new("upstream-a", 0).without_failures()),
            Arc::new(FakeUpstream::new("qq").only_for("tx")),
        ];
        let state = state_with(upstreams);

        let response = health_handler(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(body.to_vec()).expect("utf8");

        assert!(text.contains("\"mock_upstreams\":1"), "实际: {text}");
    }
}
