//! 指标：Atomic 计数（热路径零分配）+ Prometheus 文本输出。

use std::sync::atomic::{AtomicU64, Ordering};

use axum::{extract::State, response::IntoResponse};
use serde::Serialize;

use crate::{AppState, UPSTREAM_MAX_INFLIGHT};

#[derive(Debug, Serialize)]
struct StatsSnapshot {
    total_requests: u64,
    l1_hits: u64,
    l2_hits: u64,
    cache_misses: u64,
    upstream_hits: u64,
    coalesced_requests: u64,
    /// 因上游能力上限 / 接口静默降级而降低音质的请求数
    downgraded_requests: u64,
    errors: u64,
}

// ---------------------------------------------------------------------------
// 指标（Atomic 计数，热路径零分配）
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct Stats {
    pub(crate) total: AtomicU64,
    pub(crate) misses: AtomicU64,
    pub(crate) l1_hits: AtomicU64,
    pub(crate) l2_hits: AtomicU64,
    pub(crate) upstream_hits: AtomicU64,
    pub(crate) coalesced: AtomicU64,
    pub(crate) downgraded: AtomicU64,
    pub(crate) errors: AtomicU64,
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
            downgraded_requests: self.downgraded.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
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
pub(crate) async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
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
# HELP lx_gateway_quality_downgrades_total 实际音质低于请求档位的请求数
# TYPE lx_gateway_quality_downgrades_total counter
lx_gateway_quality_downgrades_total {downgraded}
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
        downgraded = stats.downgraded_requests,
        errors = stats.errors,
        inflight = inflight_upstream,
        keys = state.flight.len(),
    );
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}
