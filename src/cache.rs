//! 缓存层：L1（moka 内存）+ L2（Redis）+ singleflight 防击穿。

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use dashmap::DashMap;
use moka::future::Cache;
use once_cell::sync::Lazy;
use redis::AsyncCommands;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{
    SourceUrl, L1_CAPACITY, L1_TTL, L2_CONNECT_TIMEOUT, L2_KEY_PREFIX, L2_OP_TIMEOUT, L2_TTL_SECS,
    MAX_FLIGHT_KEYS,
};

// ---------------------------------------------------------------------------
// 2.1 L1 缓存：moka 无锁并发内存缓存
// ---------------------------------------------------------------------------

/// L1：进程内热点缓存。
///
/// moka 的分段 + 无锁读设计，使得 N 个请求并发 `get` 同一 key 时没有全局锁竞争；
/// 值用 `Arc<SourceUrl>` 包裹，命中时只做一次引用计数自增。
pub(crate) static L1_CACHE: Lazy<Cache<String, Arc<SourceUrl>>> = Lazy::new(|| {
    Cache::builder()
        .max_capacity(L1_CAPACITY)      // 容量上限 10_000 条
        .time_to_live(L1_TTL)           // 30 分钟后自动过期
        .build()
});

// ---------------------------------------------------------------------------
// 2.2 L2 缓存：redis-rs ConnectionManager（启动探测，连不上就永久关掉）
// ---------------------------------------------------------------------------

struct L2State {
    /// 首次连接成功后的多路复用句柄（可以 clone 后并发用）
    conn: Option<redis::aio::ConnectionManager>,
}

/// L2：Redis 共享缓存。
///
/// 这里最要紧的不是「怎么连」，而是**没装 Redis 的机器上别拖慢主链路**：
///
/// * 启动时 [`L2Cache::probe`] 只试连一次；失败就把 `enabled` 关掉，
///   之后所有 `get` / `set` 都是纯内存判断，**零等待**；
/// * 运行期最多连一次（`attempted` 兜底），彻底干掉「每 5 秒重试一次、每次卡 500ms」这种抖法；
/// * 读写各自有硬超时：L2 只是加速层，永远不允许它决定主链路的延迟。
pub(crate) struct L2Cache {
    url: String,
    /// false = 不连 Redis（没配置 / 探测失败 / 测试里显式关闭）
    enabled: AtomicBool,
    /// 连接只尝试一次，避免并发请求一起去建连
    attempted: AtomicBool,
    state: Mutex<L2State>,
}

impl L2Cache {
    pub(crate) fn new(url: String) -> Self {
        let configured = !url.trim().is_empty();
        Self {
            url,
            enabled: AtomicBool::new(configured),
            attempted: AtomicBool::new(false),
            state: Mutex::new(L2State { conn: None }),
        }
    }

    /// 显式关闭 L2：测试里不想连 redis 时用，读路径直接当未命中。
    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self {
            url: String::new(),
            enabled: AtomicBool::new(false),
            attempted: AtomicBool::new(true),
            state: Mutex::new(L2State { conn: None }),
        }
    }

    /// 当前是否还在用 L2（关掉之后请求路径完全零开销）
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// 启动时跑一次：连得上就保留 L2，连不上直接全局关闭。
    pub(crate) async fn probe(&self) -> bool {
        if !self.is_enabled() {
            return false;
        }
        if self.connection().await.is_some() {
            info!(url = %self.url, "L2 cache (redis) connected");
            true
        } else {
            warn!("redis unreachable at startup: L2 disabled, running L1-only");
            false
        }
    }

    /// 取连接句柄：**每个进程只试一次**，失败即永久关闭。
    async fn connection(&self) -> Option<redis::aio::ConnectionManager> {
        if !self.is_enabled() {
            return None;
        }
        {
            let state = self.state.lock().await;
            if let Some(conn) = state.conn.as_ref() {
                return Some(conn.clone());
            }
        }

        // swap 成功的那一个请求负责建连；其余请求不排队、不等待，直接当未命中
        if self.attempted.swap(true, Ordering::SeqCst) {
            return None;
        }

        let client = match redis::Client::open(self.url.as_str()) {
            Ok(client) => client,
            Err(err) => {
                warn!(error = %err, "invalid REDIS_URL, L2 disabled");
                self.enabled.store(false, Ordering::Relaxed);
                return None;
            }
        };

        // 注意：建连期间不持锁，否则并发请求会一起堵在这把锁上
        match tokio::time::timeout(
            L2_CONNECT_TIMEOUT,
            redis::aio::ConnectionManager::new(client),
        )
        .await
        {
            Ok(Ok(conn)) => {
                let mut state = self.state.lock().await;
                state.conn = Some(conn.clone());
                Some(conn)
            }
            Ok(Err(err)) => {
                warn!(error = %err, "redis connect failed, L2 disabled (L1-only)");
                self.enabled.store(false, Ordering::Relaxed);
                None
            }
            Err(_) => {
                warn!("redis connect timed out, L2 disabled (L1-only)");
                self.enabled.store(false, Ordering::Relaxed);
                None
            }
        }
    }

    /// 读 L2。任何错误都只在日志里出现，对外表现永远是「未命中」。
    pub(crate) async fn get(&self, key: &str) -> Option<SourceUrl> {
        let mut conn = self.connection().await?;
        let redis_key = format!("{}{}", L2_KEY_PREFIX, key);

        // 注意：拿到的 conn 是 clone 出来的句柄，可以随便并发用
        let lookup = async {
            let raw: Option<String> = conn.get(redis_key.as_str()).await.ok()?;
            serde_json::from_str::<SourceUrl>(raw.as_deref()?).ok()
        };

        tokio::time::timeout(L2_OP_TIMEOUT, lookup).await.ok().flatten()
    }

    /// 回写 L2（带 TTL）。调用方通常 `tokio::spawn` 到后台，不阻塞响应。
    pub(crate) async fn set(&self, key: &str, url: &SourceUrl) {
        if !self.is_enabled() {
            return;
        }
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
            Ok(Ok(())) => {}
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
pub(crate) struct SingleFlight {
    gates: DashMap<String, Arc<Mutex<()>>>,
}

impl SingleFlight {
    /// 拿到 key 对应的门闩（没有就原子插入一个）。
    ///
    /// 表项数有硬上限：超过 `MAX_FLIGHT_KEYS` 时返回 `None`，调用方放弃合并、直连上游。
    /// 宁可多打几次上游，也不让一张哈希表在大基数突发（比如被扫 ID）下把内存吃穿
    /// —— 这就是「内存稳如老狗」的那道保险。
    pub(crate) fn gate(&self, key: &str) -> Option<Arc<Mutex<()>>> {
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
    pub(crate) fn release(&self, key: &str, gate: &Arc<Mutex<()>>) {
        if Arc::strong_count(gate) == 2 {
            // remove_if 是原子的：只会删掉“确实还是同一个门闩”的表项
            let _ = self.gates.remove_if(key, |_, current| Arc::ptr_eq(current, gate));
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.gates.len()
    }
}
