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
//!   还有 `GET /metrics`（Prometheus 文本格式）与 `GET /health`。
//!
//! 启动：`cargo run --release`（默认监听 0.0.0.0:8080，自动起两个 mock 上游；REDIS_URL 可选）
//! 自测：`curl 'http://127.0.0.1:8080/api/parse?id=123456&source=kw&quality=320k'`

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
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
use dashmap::DashMap;
use moka::future::Cache;
use once_cell::sync::Lazy;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

/// 单个上游请求的硬超时（需求 3：所有上游请求 800ms）
const UPSTREAM_TIMEOUT: Duration = Duration::from_millis(800);
/// 竞速整体的硬上限（兜底，比单请求超时略宽，避免误杀“刚好在 800ms 返回”的赢家）
const RACE_DEADLINE: Duration = Duration::from_millis(850);
/// 建连超时：连不上的上游要尽快失败，把额度让给别的上游
const CONNECT_TIMEOUT: Duration = Duration::from_millis(300);

/// 每个 host 最多保留的空闲长连接数（需求 1）
const POOL_MAX_IDLE_PER_HOST: usize = 100;
/// 空闲连接回收时间，略小于常见 LB / NAT 的空闲踢断时间
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// TCP keep-alive 探测间隔
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

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
// 1. 全局 HTTP 连接池（once_cell 单例，进程内唯一）
// ---------------------------------------------------------------------------

/// 全局唯一的上游 HTTP 客户端。
///
/// * `pool_max_idle_per_host(100)`：每个上游 host 最多缓存 100 条空闲长连接，
///   高并发时直接复用 TCP/TLS，省掉握手 RTT（这是「低延迟」的最大来源）。
/// * `tcp_keepalive(60s)`：内核定期发探测包，及时发现被中间设备静默掐断的死连接，
///   避免把请求发进「看起来还活着」的黑洞里。
/// * `tcp_nodelay(true)`：关闭 Nagle，杜绝小包攒批带来的额外延迟。
/// * `connect_timeout(300ms)`：建连慢的上游早点死，把 800ms 预算留给能干活的那路。
///
/// 注意：这里的 `Lazy` 是**进程级单例**，所有请求共享同一个连接池；
/// 千万不要在 handler 里 `Client::new()`，那会把连接池弄废（每次新握手）。
static HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .tcp_keepalive(TCP_KEEPALIVE)
        .tcp_nodelay(true)
        .connect_timeout(CONNECT_TIMEOUT)
        // 兜底超时：真正生效的 SLO 是下面 tokio::time::timeout(800ms)
        .timeout(Duration::from_millis(1_500))
        .user_agent(concat!("lx-parse-gateway/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("failed to build the global reqwest client")
});

// ---------------------------------------------------------------------------
// 2.1 L1 缓存：moka 无锁并发内存缓存
// ---------------------------------------------------------------------------

/// L1：进程内热点缓存。
///
/// moka 的分段 + 无锁读设计，使得 N 个请求并发 `get` 同一 key 时没有全局锁竞争；
/// 值用 `Arc<SourceUrl>` 包裹，命中时只做一次引用计数自增。
static L1_CACHE: Lazy<Cache<String, Arc<SourceUrl>>> = Lazy::new(|| {
    Cache::builder()
        .max_capacity(L1_CAPACITY)      // 容量上限 10_000 条
        .time_to_live(L1_TTL)           // 30 分钟后自动过期
        .build()
});

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
    fn is_fresh(&self) -> bool {
        self.expires_at == 0 || self.expires_at > now_unix()
    }
}

fn now_unix() -> u64 {
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

#[derive(Debug, Serialize)]
struct StatsSnapshot {
    total_requests: u64,
    l1_hits: u64,
    l2_hits: u64,
    cache_misses: u64,
    upstream_hits: u64,
    coalesced_requests: u64,
    errors: u64,
}

// ---------------------------------------------------------------------------
// 指标（Atomic 计数，热路径零分配）
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Stats {
    total: AtomicU64,
    misses: AtomicU64,
    l1_hits: AtomicU64,
    l2_hits: AtomicU64,
    upstream_hits: AtomicU64,
    coalesced: AtomicU64,
    errors: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            total_requests: self.total.load(Ordering::Relaxed),
            l1_hits: self.l1_hits.load(Ordering::Relaxed),
            l2_hits: self.l2_hits.load(Ordering::Relaxed),
            cache_misses: self.misses.load(Ordering::Relaxed),
            upstream_hits: self.upstream_hits.load(Ordering::Relaxed),
            coalesced_requests: self.coalesced.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// 2.2 L2 缓存：redis-rs ConnectionManager（异步连接池 + 自动重连）
// ---------------------------------------------------------------------------

struct L2State {
    conn: Option<redis::aio::ConnectionManager>,
    /// 熔断：在这个时间点之前不再尝试重连
    next_retry_at: Instant,
}

/// L2：Redis 共享缓存。
///
/// `ConnectionManager` 内部持有 **一条多路复用的连接**（multiplexed），可以 `clone` 后
/// 并发使用；断线时它自己在后台重连并把失败的命令重放给新连接。
/// 这里再包一层「熔断窗口」：Redis 彻底挂掉时每 5s 才尝试一次重连，
/// 其余请求直接判阴性 → 自动降级为 L1-only，绝不阻塞主链路。
struct L2Cache {
    url: String,
    state: Mutex<L2State>,
}

impl L2Cache {
    fn new(url: String) -> Self {
        info!(%url, "L2 cache (redis) configured");
        Self {
            url,
            state: Mutex::new(L2State { conn: None, next_retry_at: Instant::now() }),
        }
    }

    /// 取一个可克隆的连接句柄；拿不到就返回 `None`（= 优雅降级）。
    async fn connection(&self) -> Option<redis::aio::ConnectionManager> {
        let mut state = self.state.lock().await;

        if let Some(conn) = state.conn.as_ref() {
            return Some(conn.clone());
        }
        if Instant::now() < state.next_retry_at {
            return None; // 熔断窗口内，直接跳过
        }

        let client = match redis::Client::open(self.url.as_str()) {
            Ok(client) => client,
            Err(err) => {
                warn!(error = %err, "invalid REDIS_URL, L2 disabled");
                state.next_retry_at = Instant::now() + L2_RETRY_BACKOFF;
                return None;
            }
        };

        // 连接也要有超时：Redis 不可达时不能让请求卡在 SYN 上
        match tokio::time::timeout(
            L2_CONNECT_TIMEOUT,
            redis::aio::ConnectionManager::new(client),
        )
        .await
        {
            Ok(Ok(conn)) => {
                info!("L2 cache (redis) connected");
                state.conn = Some(conn.clone());
                Some(conn)
            }
            Ok(Err(err)) => {
                warn!(error = %err, "redis connect failed, degraded to L1-only");
                state.next_retry_at = Instant::now() + L2_RETRY_BACKOFF;
                None
            }
            Err(_) => {
                warn!("redis connect timed out, degraded to L1-only");
                state.next_retry_at = Instant::now() + L2_RETRY_BACKOFF;
                None
            }
        }
    }

    /// 读 L2。任何错误都只在日志里出现，对外表现永远是「未命中」。
    async fn get(&self, key: &str) -> Option<SourceUrl> {
        let mut conn = self.connection().await?;
        let redis_key = format!("{}{}", L2_KEY_PREFIX, key);

        // 注意：拿到的 conn 是 clone 出来的句柄，可以随便并发用
        let lookup = async {
            let raw: Option<String> = conn.get(redis_key.as_str()).await.ok()?;
            serde_json::from_str::<SourceUrl>(raw.as_deref()?).ok()
        };

        match tokio::time::timeout(L2_OP_TIMEOUT, lookup).await {
            Ok(Some(url)) => {
                debug!(%key, "L2 hit");
                Some(url)
            }
            Ok(None) => None,
            Err(_) => {
                warn!(%key, "L2 GET timed out");
                None
            }
        }
    }

    /// 回写 L2（带 TTL）。调用方通常 `tokio::spawn` 到后台，不阻塞响应。
    async fn set(&self, key: &str, url: &SourceUrl) {
        let Some(mut conn) = self.connection().await else { return };
        let redis_key = format!("{}{}", L2_KEY_PREFIX, key);
        let payload = match serde_json::to_string(url) {
            Ok(payload) => payload,
            Err(err) => {
                warn!(error = %err, %key, "failed to serialize L2 payload");
                return;
            }
        };

        let write = async {
            // SETEX 返回 +OK，用 Value 接住即可（不做 String 解析，省一次分支）
            let _reply: redis::Value = conn
                .set_ex(redis_key.as_str(), payload.as_str(), L2_TTL_SECS)
                .await?;
            Ok::<(), redis::RedisError>(())
        };

        match tokio::time::timeout(L2_OP_TIMEOUT, write).await {
            Ok(Ok(())) => debug!(%key, "L2 write done"),
            Ok(Err(err)) => warn!(error = %err, %key, "L2 write failed"),
            Err(_) => warn!(%key, "L2 write timed out"),
        }
    }
}

// ---------------------------------------------------------------------------
// 5. 防缓存击穿：singleflight
// ---------------------------------------------------------------------------

/// 同 key 只允许一个请求穿透到上游，其余请求 await 同一个门闩。
///
/// 实现要点：
/// * `DashMap` 分片锁 → 取门闩是无锁（分片级）的，不会成为全局瓶颈；
/// * 门闩本身是 `tokio::sync::Mutex<()>`，等待是挂起任务而不是自旋；
/// * 用完即摘除表项，避免 key 无限膨胀（要求 `Arc::strong_count == 2`：map + 自己）。
#[derive(Default)]
struct SingleFlight {
    gates: DashMap<String, Arc<Mutex<()>>>,
}

impl SingleFlight {
    /// 拿到 key 对应的门闩（没有就原子插入一个）。
    ///
    /// 表项数有硬上限：超过 `MAX_FLIGHT_KEYS` 时返回 `None`，调用方放弃合并、直连上游。
    /// 宁可多打几次上游，也不让一张哈希表在大基数突发（比如被扫 ID）下把内存吃穿
    /// —— 这就是「内存稳如老狗」的那道保险。
    fn gate(&self, key: &str) -> Option<Arc<Mutex<()>>> {
        // 已经有人在飞这个 key：直接复用它，别重复插表
        if let Some(existing) = self.gates.get(key) {
            return Some(Arc::clone(&existing));
        }
        // 表满：fail-open，让调用方直连上游，绝不无界增长
        if self.gates.len() >= MAX_FLIGHT_KEYS {
            return None;
        }
        let gate = self
            .gates
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())));
        Some(Arc::clone(&gate))
    }

    /// 收尾：只有当自己是最后一个持有者时才摘除表项。
    fn release(&self, key: &str, gate: &Arc<Mutex<()>>) {
        if Arc::strong_count(gate) == 2 {
            // remove_if 是原子的：只会删掉“确实还是同一个门闩”的表项
            let _ = self.gates.remove_if(key, |_, current| Arc::ptr_eq(current, gate));
        }
    }

    fn len(&self) -> usize {
        self.gates.len()
    }
}

// ---------------------------------------------------------------------------
// 应用状态
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    /// 上游节点池（两个 mock，可通过环境变量换成真实音源节点）
    upstreams: Arc<Vec<String>>,
    l2: Arc<L2Cache>,
    flight: Arc<SingleFlight>,
    /// 上游并发闸门：过载时直接 503，保护自己也不压垮上游
    limiter: Arc<Semaphore>,
    stats: Arc<Stats>,
}

/// 一次解析的结果 + 命中信息
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
        let key = format!("{source}:{music_id}:{quality}");

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

        let url = match race_upstreams(&self.upstreams, source, music_id, quality).await {
            Ok(url) => url,
            Err(err) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(err);
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
// ---------------------------------------------------------------------------

/// 同时请求所有上游节点，**谁先成功用谁**。
///
/// * 用 `tokio::task::JoinSet` 把每路上游变成独立 task（各自跑在自己的 waker 上，
///   互不干扰，也不会因为某一路 panic 拖垮整条请求）；
/// * 用 `tokio::select!` 等「第一个完成的任务」；拿到赢家后 `JoinSet` 被 drop，
///   其余任务立刻 `abort()` —— 这就是「另一个直接取消」，省掉多余的带宽与上游配额；
/// * 另有一路 `sleep(RACE_DEADLINE)` 作为整体硬上限，防止某个任务自身的超时失效。
async fn race_upstreams(
    upstreams: &[String],
    source: &'static str,
    music_id: &str,
    quality: &'static str,
) -> Result<SourceUrl, AppError> {
    if upstreams.is_empty() {
        return Err(AppError::Upstream("no upstream configured".to_string()));
    }

    let mut tasks = tokio::task::JoinSet::new();
    for base in upstreams {
        let base = base.clone();
        let music_id = music_id.to_string();
        tasks.spawn(async move { fetch_upstream(&base, source, &music_id, quality).await });
    }

    let mut deadline = Box::pin(tokio::time::sleep(RACE_DEADLINE));
    let mut last_error: Option<AppError> = None;

    loop {
        let joined = tokio::select! {
            // 先完成的那一路（成功或失败）
            biased;
            joined = tasks.join_next() => joined,
            // 整体兜底：即使每个 task 的超时都没生效，也不会超过 850ms
            _ = &mut deadline => {
                warn!(source, music_id, "upstream race hit the overall deadline");
                break;
            }
        };

        let Some(joined) = joined else {
            break; // 所有上游都已结束且都失败了
        };

        match joined {
            // 赢家：函数返回 → tasks(JoinSet) 被 drop → 其余上游 task 全部 abort
            Ok(Ok(url)) => return Ok(url),
            Ok(Err(err)) => {
                warn!(error = %err, "one upstream failed, still waiting for the other");
                last_error = Some(err);
            }
            Err(join_error) => {
                warn!(error = %join_error, "upstream task panicked or was aborted");
                last_error = Some(AppError::Upstream(format!("task failure: {join_error}")));
            }
        }
    }

    Err(last_error.unwrap_or(AppError::Timeout(RACE_DEADLINE)))
}

/// 单路上游请求：**所有上游请求强制 800ms 超时**（需求 3）。
async fn fetch_upstream(
    base: &str,
    source: &'static str,
    music_id: &str,
    quality: &'static str,
) -> Result<SourceUrl, AppError> {
    let started = Instant::now();
    let path = format!("{base}/api/url");

    // 注意：超时包住「发送 + 读 body」，而不是只包住 send()，
    // 否则慢 body 会绕过 800ms 限制（典型的线上事故）。
    let request = async {
        let response = HTTP_CLIENT
            .get(path.as_str())
            .query(&[("source", source), ("id", music_id), ("quality", quality)])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(AppError::Upstream(format!("{path} returned HTTP {status}")));
        }

        let url: SourceUrl = response.json().await?;
        Ok::<SourceUrl, AppError>(url)
    };

    let url = tokio::time::timeout(UPSTREAM_TIMEOUT, request)
        .await
        .map_err(|_| AppError::Timeout(UPSTREAM_TIMEOUT))??;

    debug!(
        %path,
        provider = %url.provider,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "upstream answered"
    );
    Ok(url)
}

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

/// `GET /metrics` —— Prometheus 文本格式（`text/plain; version=0.0.4`），
/// Prometheus / VictoriaMetrics / Grafana Agent 直接抓就能用。
///
/// 关键指标：
/// * `lx_gateway_cache_hits_total{layer=...}` / `lx_gateway_cache_misses_total`：命中率
/// * `lx_gateway_upstream_requests_total`：真正打穿到上游的次数（≈ 上游配额消耗）
/// * `lx_gateway_coalesced_total`：被 singleflight 合并掉的次数（防击穿生效的证据）
/// * `lx_gateway_inflight_upstream` / `lx_gateway_singleflight_keys`：两个 gauge，
///   内存异常上涨时先看这两个有没有失控。
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.stats.snapshot();
    let inflight_upstream =
        UPSTREAM_MAX_INFLIGHT.saturating_sub(state.limiter.available_permits());
    let body = format!(
        r#"# HELP lx_gateway_requests_total 收到的解析请求总数
# TYPE lx_gateway_requests_total counter
lx_gateway_requests_total {total}
# HELP lx_gateway_cache_hits_total 缓存命中次数（按层级）
# TYPE lx_gateway_cache_hits_total counter
lx_gateway_cache_hits_total{{layer="l1"}} {l1}
lx_gateway_cache_hits_total{{layer="l2"}} {l2}
# HELP lx_gateway_cache_misses_total 首查未命中的请求数
# TYPE lx_gateway_cache_misses_total counter
lx_gateway_cache_misses_total {misses}
# HELP lx_gateway_upstream_requests_total 真正穿透到上游并成功的请求数
# TYPE lx_gateway_upstream_requests_total counter
lx_gateway_upstream_requests_total {upstream}
# HELP lx_gateway_coalesced_total 被 singleflight 合并掉的请求数
# TYPE lx_gateway_coalesced_total counter
lx_gateway_coalesced_total {coalesced}
# HELP lx_gateway_errors_total 上游失败/超时的请求数
# TYPE lx_gateway_errors_total counter
lx_gateway_errors_total {errors}
# HELP lx_gateway_inflight_upstream 当前在飞的上游请求数
# TYPE lx_gateway_inflight_upstream gauge
lx_gateway_inflight_upstream {inflight}
# HELP lx_gateway_singleflight_keys 当前 singleflight 门闩表大小
# TYPE lx_gateway_singleflight_keys gauge
lx_gateway_singleflight_keys {keys}
"#,
        total = stats.total_requests,
        l1 = stats.l1_hits,
        l2 = stats.l2_hits,
        misses = stats.cache_misses,
        upstream = stats.upstream_hits,
        coalesced = stats.coalesced_requests,
        errors = stats.errors,
        inflight = inflight_upstream,
        keys = state.flight.len(),
    );
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
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
// Mock 上游音源节点（演示用；真实环境把 UPSTREAM_A_URL / UPSTREAM_B_URL 指过去即可）
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockUpstream {
    provider: &'static str,
    base_latency_ms: u64,
}

#[derive(Debug, Deserialize)]
struct MockResolveQuery {
    id: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    quality: String,
}

async fn mock_resolve_handler(
    State(cfg): State<MockUpstream>,
    Query(query): Query<MockResolveQuery>,
) -> Response {
    // 模拟网络抖动：基础延迟 + 0..120ms 随机抖动
    let jitter = pseudo_random() % 120;
    tokio::time::sleep(Duration::from_millis(cfg.base_latency_ms + jitter)).await;

    // 3% 概率故意失败，用来验证「竞速 + 失败切换」逻辑是否生效
    if pseudo_random() % 100 < 3 {
        return (StatusCode::INTERNAL_SERVER_ERROR, "mock upstream exploded").into_response();
    }

    let body = SourceUrl {
        url: format!(
            "https://cdn-{}.lx-mock.example/{}/{}/{}.flac",
            cfg.provider, query.source, query.quality, query.id
        ),
        provider: cfg.provider.to_string(),
        quality: query.quality,
        // 直链 10 分钟后过期，比缓存 TTL 短 → 用来演示 is_fresh() 的二次校验
        expires_at: now_unix() + 600,
    };
    Json(body).into_response()
}

/// 起一个进程内的 mock 上游，返回 base url（监听 127.0.0.1 的随机端口）
async fn spawn_mock_upstream(provider: &'static str, base_latency_ms: u64) -> Result<String, BoxError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let app = Router::new()
        .route("/api/url", get(mock_resolve_handler))
        .with_state(MockUpstream { provider, base_latency_ms });

    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            warn!(error = %err, provider, "mock upstream stopped");
        }
    });

    info!(%addr, provider, base_latency_ms, "mock upstream started");
    Ok(format!("http://{addr}"))
}

/// 轻量无锁随机数（xorshift64），只用于 mock 抖动，不需要密码学强度。
fn pseudo_random() -> u64 {
    static SEED: AtomicU64 = AtomicU64::new(0x2545_F491_4F6C_DD1D);
    loop {
        let current = SEED.load(Ordering::Relaxed);
        let mut next = current;
        next ^= next << 13;
        next ^= next >> 7;
        next ^= next << 17;
        if SEED
            .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
    }
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

    // 预热全局连接池（同时让任何配置错误在启动期就 panic，而不是首个请求时）
    Lazy::force(&HTTP_CLIENT);
    info!(
        max_idle_per_host = POOL_MAX_IDLE_PER_HOST,
        ttl_secs = L1_TTL.as_secs(),
        "global http pool & L1 cache warmed up"
    );

    // 上游节点：默认起两个 mock；生产环境用环境变量指向真实音源
    let upstream_a = match std::env::var("UPSTREAM_A_URL") {
        Ok(url) => url,
        Err(_) => spawn_mock_upstream("upstream-a", 80).await?,
    };
    let upstream_b = match std::env::var("UPSTREAM_B_URL") {
        Ok(url) => url,
        Err(_) => spawn_mock_upstream("upstream-b", 170).await?,
    };

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());

    let state = AppState {
        upstreams: Arc::new(vec![upstream_a.clone(), upstream_b.clone()]),
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

    info!(%upstream_a, %upstream_b, "upstreams ready");
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
