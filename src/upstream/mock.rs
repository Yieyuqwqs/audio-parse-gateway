//! 进程内假上游：本地跑通全链路、跑单元测试时用，不碰任何外网。
//!
//! 它故意保留了一点「网络感」：
//! * 基础延迟 + 0~120ms 抖动 —— 让竞速真的能分出快慢；
//! * 3% 概率返回 500 —— 让「失败切换」这条分支在本地也能被走到。
//!
//! 这两种行为都可以关掉（[`MockUpstream::without_failures`]），测试里用得到。

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::{
    upstream::{TrackRequest, Upstream, UpstreamError},
    SourceUrl,
};

/// 本地假上游。
#[derive(Debug, Clone)]
pub struct MockUpstream {
    name: String,
    base_latency_ms: u64,
    failure_rate_pct: u64,
}

impl MockUpstream {
    pub fn new(name: impl Into<String>, base_latency_ms: u64) -> Self {
        Self { name: name.into(), base_latency_ms, failure_rate_pct: 3 }
    }

    /// 关掉随机失败（测试用；生产演示保持 3% 才能看到竞速接管）
    pub fn without_failures(mut self) -> Self {
        self.failure_rate_pct = 0;
        self
    }

    /// 设成 100 就是「永远失败的上游」，用来测全挂时的错误路径。
    pub fn with_failure_rate(mut self, pct: u64) -> Self {
        self.failure_rate_pct = pct.min(100);
        self
    }
}

#[async_trait::async_trait]
impl Upstream for MockUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports(&self, _source: &str) -> bool {
        true
    }

    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
        let jitter = pseudo_random() % 120;
        tokio::time::sleep(Duration::from_millis(self.base_latency_ms + jitter)).await;

        if pseudo_random() % 100 < self.failure_rate_pct {
            return Err(UpstreamError::Status {
                upstream: self.name.clone(),
                status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            });
        }

        Ok(SourceUrl {
            url: format!(
                "https://cdn-{}.lx-mock.example/{}/{}/{}.flac",
                self.name, req.source, req.quality, req.music_id
            ),
            provider: self.name.clone(),
            quality: req.quality.to_string(),
            // 直链 10 分钟过期，比缓存 TTL 短 → 顺便演示 is_fresh() 的二次校验
            expires_at: crate::now_unix() + 600,
        })
    }
}

/// 轻量无锁随机数（xorshift64）。只用于 mock 抖动，不需要密码学强度。
pub(crate) fn pseudo_random() -> u64 {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builds_a_playable_looking_url() {
        let upstream = MockUpstream::new("mock-a", 0).without_failures();
        let resolved = upstream
            .resolve(&TrackRequest::new("kw", "123456", "320k"))
            .await
            .expect("mock upstream never fails when failures are disabled");

        assert_eq!(resolved.provider, "mock-a");
        assert!(resolved.url.contains("/kw/320k/123456.flac"), "实际: {}", resolved.url);
        assert!(resolved.is_fresh());
        assert!(upstream.supports("kg"));
    }

    #[tokio::test]
    async fn can_be_forced_to_fail() {
        let upstream = MockUpstream::new("broken", 0).with_failure_rate(100);
        let err = upstream
            .resolve(&TrackRequest::new("kw", "1", "320k"))
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::Status { .. }));
        assert!(err.is_retryable());
    }
}
