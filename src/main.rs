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
//! 接真实音源：`UPSTREAM_A_URL=https://node.example.com cargo run --release`，或 `ENABLE_NETEASE=1` 直连网易云；`REDIS_URL` 可选
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
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod cache;
mod http;
mod metrics;
mod upstream;

use upstream::{MockUpstream, NeteaseUpstream, NodeUpstream, TrackRequest, Upstream, UpstreamError};

pub(crate) use cache::{L2Cache, SingleFlight, L1_CACHE};
pub(crate) use metrics::{metrics_handler, Stats};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

/// 单个上游请求的硬超时（需求 3：所有上游请求 800ms）
const UPSTREAM_TIMEOUT: Duration = Duration::from_millis(800);
/// 竞速整体的硬上限（兜底，比单请求超时略宽，避免误杀“刚好在 800ms 返回”的赢家）
const RACE_DEADLINE: Duration = Duration::from_millis(850);
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
/// 连不上 Redis 后的熔断窗口，避免每个请求都去重连
const L2_RETRY_BACKOFF: Duration = Duration::from_secs(5);

const DEFAULT_SOURCE: &str = "kw";
const DEFAULT_QUALITY: &str = "320k";
/// LX 支持的源 key
const SUPPORTED_SOURCES: [&str; 6] = ["kw", "kg", "tx", "wy", "mg", "local"];
/// LX 支持的音质
const SUPPORTED_QUALITIES: [&str; 4] = ["128k", "320k", "flac", "flac24bit"];

// ---------------------------------------------------------------------------
// 领域模型
// ---------------------------------------------------------------------------

/// 上游解析出来的播放地址（会被缓存，因此必须 `Serialize + Deserialize`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceUrl {
    /// 最终播放地址（带签名的直链）
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

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Http(_) | AppError::Upstream(_) => StatusCode::BAD_GATEWAY,
            AppError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            AppError::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
        };

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
    quality: String,
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
}

/// 一次解析的结果 + 命中信息
#[derive(Debug)]
struct Resolved {
    url: SourceUrl,
    /// l1 / l2 / upstream
    layer: &'static str,
    /// 是否等待了别人的在途请求
    coalesced: bool,
}

impl AppState {
    /// 核心链路：L1 → L2 → singleflight → 竞速上游 → 回填缓存
    async fn resolve(
        &self,
        source: &'static str,
        music_id: &str,
        quality: &'static str,
    ) -> Result<Resolved, AppError> {
        // 缓存 key 与适配层共用同一套拼法，避免两处漂移
        let key = TrackRequest::new(source, music_id, quality).cache_key();

        // ---- 1) L1 / L2：绝大多数请求在这里结束，纯内存、无锁读 ----
        if let Some(hit) = self.cache_lookup(&key).await {
            return Ok(hit);
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);

        // ---- 2) 打不中 → 抢 singleflight 门闩（表满时 fail-open，直连上游） ----
        let gate = self.flight.gate(&key);

        let result = self
            .resolve_locked(&key, source, music_id, quality, gate.as_ref())
            .await;

        // 无论成功失败都要摘门闩，否则表项会泄漏
        if let Some(gate) = gate.as_ref() {
            self.flight.release(&key, gate);
        }
        result
    }

    /// 持锁之后的逻辑（锁在函数内部获取，保证任何返回路径都会释放）
    async fn resolve_locked(
        &self,
        key: &str,
        source: &'static str,
        music_id: &str,
        quality: &'static str,
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
        if let Some(mut hit) = self.cache_lookup(key).await {
            hit.coalesced = coalesced;
            return Ok(hit);
        }

        // ---- 3) 上游竞速（唯一的穿透点） ----
        // 过载保护：拿不到令牌就快速失败，而不是把请求堆在队列里拖垮自己
        let permit = self.limiter.try_acquire().map_err(|_| AppError::Overloaded)?;

        let request = TrackRequest::new(source, music_id, quality);
        let url = match upstream::race(&self.upstreams, &request, RACE_DEADLINE).await {
            Ok(url) => url,
            Err(err) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                warn!(upstream = err.upstream(), error = %err, "upstream race failed");
                return Err(AppError::from(err));
            }
        };
        drop(permit);

        self.stats.upstream_hits.fetch_add(1, Ordering::Relaxed);
        info!(
            %key,
            provider = %url.provider,
            "upstream resolved"
        );

        // ---- 4) 回填缓存：L1 同步写（极快），L2 丢后台 ----
        L1_CACHE.insert(key.to_owned(), Arc::new(url.clone())).await;

        {
            let l2 = Arc::clone(&self.l2);
            let key = key.to_owned();
            let payload = url.clone();
            tokio::spawn(async move { l2.set(&key, &payload).await });
        }

        Ok(Resolved { url, layer: "upstream", coalesced })
    }

    /// L1 → L2 的读路径（带直链有效期校验）
    async fn cache_lookup(&self, key: &str) -> Option<Resolved> {
        // L1
        if let Some(hit) = L1_CACHE.get(key).await {
            if hit.is_fresh() {
                self.stats.l1_hits.fetch_add(1, Ordering::Relaxed);
                return Some(Resolved { url: (*hit).clone(), layer: "l1", coalesced: false });
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
            return Some(Resolved { url: hit, layer: "l2", coalesced: false });
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

/// `GET /api/parse?id=<musicId>&source=kw&quality=320k`
async fn parse_handler(
    State(state): State<AppState>,
    Query(query): Query<ParseQuery>,
) -> Result<Json<OkBody>, AppError> {
    let source = normalize_source(query.source.as_deref())?;
    let quality = normalize_quality(query.quality.as_deref())?;
    let music_id = validate_music_id(query.id.as_deref())?;
    respond(&state, source, music_id, quality).await
}

/// `GET /url/{source}/{music_id}?quality=320k`（LX 风格的短链接口，方便脚本直连）
async fn lx_url_handler(
    State(state): State<AppState>,
    Path((source, music_id)): Path<(String, String)>,
    Query(query): Query<QualityQuery>,
) -> Result<Json<OkBody>, AppError> {
    let source = normalize_source(Some(source.as_str()))?;
    let quality = normalize_quality(query.quality.as_deref())?;
    let music_id = validate_music_id(Some(music_id.as_str()))?;
    respond(&state, source, music_id, quality).await
}

/// 两个入口共用的响应组装：解析 → LX 响应包
async fn respond(
    state: &AppState,
    source: &'static str,
    music_id: &str,
    quality: &'static str,
) -> Result<Json<OkBody>, AppError> {
    let started = Instant::now();
    state.stats.total.fetch_add(1, Ordering::Relaxed);

    let resolved = state.resolve(source, music_id, quality).await?;

    Ok(Json(OkBody {
        code: 0,
        msg: "success",
        data: TrackData {
            url: resolved.url.url,
            source: source.to_string(),
            quality: quality.to_string(),
            provider: resolved.url.provider,
            cache: resolved.layer,
            coalesced: resolved.coalesced,
            elapsed_ms: started.elapsed().as_millis() as u64,
        },
    }))
}

async fn health_handler() -> &'static str {
    "ok"
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

fn normalize_quality(raw: Option<&str>) -> Result<&'static str, AppError> {
    let value = raw.map(str::trim).filter(|v| !v.is_empty()).unwrap_or(DEFAULT_QUALITY);
    SUPPORTED_QUALITIES
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(value))
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "`quality` must be one of {:?}, got `{value}`", SUPPORTED_QUALITIES
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

    if upstreams.is_empty() {
        warn!("未配置真实上游，回退到两个进程内 mock（配 UPSTREAM_A_URL 或 ENABLE_NETEASE=1 即可接真实音源）");
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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,audio_parse_gateway=debug")),
        )
        .with_target(false)
        .compact()
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

    let state = AppState {
        upstreams,
        l2: Arc::new(L2Cache::new(redis_url)),
        flight: Arc::new(SingleFlight::default()),
        limiter: Arc::new(Semaphore::new(UPSTREAM_MAX_INFLIGHT)),
        stats: Arc::new(Stats::default()),
    };

    let app = Router::new()
        .route("/api/parse", get(parse_handler))
        .route("/url/{source}/{music_id}", get(lx_url_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
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
// 单元测试：适配层错误语义 + 竞速 + singleflight + 缓存 + 参数校验
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// 可编程的假适配器：能配延迟、强制失败、只支持某个音源。
    #[derive(Debug)]
    struct FakeUpstream {
        name: &'static str,
        delay: Duration,
        fails: bool,
        only: Option<&'static str>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeUpstream {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                delay: Duration::ZERO,
                fails: false,
                only: None,
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
    }

    #[async_trait::async_trait]
    impl Upstream for FakeUpstream {
        fn name(&self) -> &str {
            self.name
        }

        fn supports(&self, source: &str) -> bool {
            self.only.is_none_or(|only| only == source)
        }

        async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
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
        let payload = respond(&state, "kw", "metrics-1", "320k")
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
}
