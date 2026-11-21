//! 缓存层：L1（moka 内存）+ L2（Redis）+ singleflight 防击穿。

use std::{sync::Arc, time::Instant};

use dashmap::DashMap;
use moka::future::Cache;
use once_cell::sync::Lazy;
use redis::AsyncCommands;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::{
    SourceUrl, L1_CAPACITY, L1_TTL, L2_CONNECT_TIMEOUT, L2_KEY_PREFIX, L2_OP_TIMEOUT, L2_RETRY_BACKOFF,
    L2_TTL_SECS, MAX_FLIGHT_KEYS,
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
pub(crate) struct L2Cache {
    url: String,
    state: Mutex<L2State>,
    /// false = 显式关闭（单元测试 / 纯 L1 部署），连 redis 都不尝试
    enabled: bool,
}

impl L2Cache {
    pub(crate) fn new(url: String) -> Self {
        info!(%url, "L2 cache (redis) configured");
        Self {
            url,
            state: Mutex::new(L2State { conn: None, next_retry_at: Instant::now() }),
            enabled: true,
        }
    }

    /// 显式关闭 L2：测试里不想连 redis 时用，读路径直接当未命中。
    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self {
            url: String::new(),
            state: Mutex::new(L2State { conn: None, next_retry_at: Instant::now() }),
            enabled: false,
        }
    }

    /// 取一个可克隆的连接句柄；拿不到就返回 `None`（= 优雅降级）。
    async fn connection(&self) -> Option<redis::aio::ConnectionManager> {
        if !self.enabled {
            return None;
        }
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
    pub(crate) async fn get(&self, key: &str) -> Option<SourceUrl> {
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
    pub(crate) async fn set(&self, key: &str, url: &SourceUrl) {
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
