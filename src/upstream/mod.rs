//! 上游适配层。
//!
//! 网关只负责「编排」：缓存、合并、超时、竞速、指标。
//! **怎么从某个音源拿到一条可播放的直链**是适配器的事，统一收敛到 [`Upstream`] trait：
//!
//! ```text
//!  AppState::resolve ──竞速──▶ [ NodeUpstream   ] ──HTTP──▶ 自建解析节点
//!                         └──▶ [ NeteaseUpstream] ──HTTP──▶ 网易云官方接口
//!                         └──▶ [ MockUpstream   ] ──进程内──▶ 假数据（本地/测试）
//! ```
//!
//! 新增一个音源只要实现 [`Upstream`] 再塞进注册表，主链路一行都不用改。

use std::{fmt, sync::Arc, time::Duration};

use crate::SourceUrl;

pub mod mock;
pub mod netease;
pub mod node;
pub mod qq;

pub use mock::MockUpstream;
pub use netease::NeteaseUpstream;
pub use node::NodeUpstream;
pub use qq::QqUpstream;

/// 平台直链的防盗链 `Referer`（代拉与探活用）。
pub(crate) fn referer_for(source: &str) -> &'static str {
    match source {
        "tx" => "https://y.qq.com/",
        "wy" => "https://music.163.com/",
        "kw" => "https://www.kuwo.cn/",
        "kg" => "https://www.kugou.com/",
        "mg" => "https://music.migu.cn/",
        _ => "",
    }
}

/// 一次解析请求。
///
/// `music_id` 用 owned `String` 是为了让请求能被 `tokio::spawn` 搬进独立 task
/// （竞速时每路上游一个 task，需要 `'static`）。
#[derive(Debug, Clone)]
pub struct TrackRequest {
    /// 已校验的音源 key：kw / kg / tx / wy / mg / local
    pub source: &'static str,
    /// 音源内的曲目 ID（kw 的 rid、tx 的 songmid、kg 的 hash …）
    pub music_id: String,
    /// 已校验的音质：128k / 320k / flac / flac24bit
    pub quality: &'static str,
}

impl TrackRequest {
    pub fn new(source: &'static str, music_id: impl Into<String>, quality: &'static str) -> Self {
        Self { source, music_id: music_id.into(), quality }
    }

    /// 缓存与日志用的稳定 key
    pub fn cache_key(&self) -> String {
        format!("{}:{}:{}", self.source, self.music_id, self.quality)
    }
}

/// 上游失败的分类。
///
/// 分类不只是为了打日志：竞速时「换一路还有救」的错误值得继续等其他任务，
/// 「这个源本身就不支持」的错误则应该更早放弃。
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("upstream `{upstream}` timed out after {after:?}")]
    Timeout {
        upstream: String,
        after: Duration,
    },
    #[error("upstream `{upstream}` transport error: {source}")]
    Transport {
        upstream: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("upstream `{upstream}` returned HTTP {status}")]
    Status {
        upstream: String,
        status: reqwest::StatusCode,
    },
    #[error("upstream `{upstream}` returned an undecodable payload: {detail}")]
    Decode { upstream: String, detail: String },
    #[error("upstream `{upstream}` has no playable url: {detail}")]
    NoUrl { upstream: String, detail: String },
    #[error("upstream `{upstream}` does not support source `{source_key}`")]
    Unsupported {
        upstream: String,
        /// 注意：字段不能叫 `source`，thiserror 会把它当成 error source（要求实现 StdError）
        source_key: String,
    },
    #[error("no registered upstream supports source `{source_key}`")]
    NoCandidate { source_key: String },
}

impl UpstreamError {
    /// 换一路上游还可能有救（超时 / 网络 / 5xx / 脏数据）
    pub fn is_retryable(&self) -> bool {
        !matches!(self, UpstreamError::Unsupported { .. } | UpstreamError::NoCandidate { .. })
    }

    /// 出错的上游名（日志与指标用）
    pub fn upstream(&self) -> &str {
        match self {
            UpstreamError::Timeout { upstream, .. }
            | UpstreamError::Transport { upstream, .. }
            | UpstreamError::Status { upstream, .. }
            | UpstreamError::Decode { upstream, .. }
            | UpstreamError::NoUrl { upstream, .. }
            | UpstreamError::Unsupported { upstream, .. } => upstream,
            UpstreamError::NoCandidate { .. } => "none",
        }
    }
}

/// 音源适配器：一个实现 = 一种「拿到直链」的方式。
///
/// 现成三类：
/// * [`node::NodeUpstream`]：自建 / 第三方解析节点（HTTP JSON，最通用，可带 token）；
/// * [`netease::NeteaseUpstream`]：网易云官方接口直连（真实源，可带 cookie 解锁更高音质）；
/// * [`mock::MockUpstream`]：进程内假上游，本地跑通全链路 + 单元测试用。
#[async_trait::async_trait]
pub trait Upstream: Send + Sync + fmt::Debug {
    /// 唯一名字：进日志，也会写进响应里的 `provider` 字段
    fn name(&self) -> &str;

    /// 是否支持这个音源 key（竞速前先过滤，避免拿 kg 的 id 去打 kw 的接口）
    fn supports(&self, source: &str) -> bool;

    /// 这路上游最高能给到哪一档，默认按「无损全开」算。
    ///
    /// 会实事求是地往下调：比如网易云没带登录 cookie 时，实测 `br=999000`
    /// 只会被静默降级成 320k mp3，那它就该老实返回 `320k`，
    /// 让网关提前降档，别浪费一次请求去够一个够不着的档位。
    fn max_quality(&self) -> &'static str {
        crate::quality::TOP
    }

    /// 能不能服务这个 `(source, quality)` 组合 —— 竞速前用它筛候选。
    fn serves(&self, source: &str, quality: &str) -> bool {
        self.supports(source) && crate::quality::within(quality, self.max_quality())
    }

    /// 解析直链。实现方**必须**给自己的请求套上硬超时。
    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError>;

    /// 关键词搜索：跨平台回退（tx 全挂了去 wy 再找一次）用。
    ///
    /// 默认不支持 —— 目前只有网易云实现了真实搜索；kw/kg 的搜索接口都有反爬会话要求，见 README。
    async fn search_keyword(&self, keyword: &str) -> Result<String, UpstreamError> {
        Err(UpstreamError::Unsupported {
            upstream: self.name().to_string(),
            source_key: format!("search:{keyword}"),
        })
    }
}

/// 竞速：同时打所有支持该音源的上游，谁先成功用谁，其余立刻取消。
///
/// * 每路上游一个独立 task（`JoinSet`），互不干扰，某一路 panic 也不会拖垮整条请求；
/// * `select!` 拿第一个完成的任务；函数返回时 `JoinSet` 被 drop，输家任务全部 abort；
/// * 另有一路 `sleep(deadline)` 兜底，防止某个适配器的超时失效。
pub async fn race(
    upstreams: &[Arc<dyn Upstream>],
    req: &TrackRequest,
    deadline: Duration,
) -> Result<SourceUrl, UpstreamError> {
    let candidates: Vec<&Arc<dyn Upstream>> = upstreams
        .iter()
        .filter(|upstream| upstream.serves(req.source, req.quality))
        .collect();
    if candidates.is_empty() {
        return Err(UpstreamError::NoCandidate { source_key: req.source.to_string() });
    }

    let mut tasks = tokio::task::JoinSet::new();
    for upstream in candidates {
        let upstream = Arc::clone(upstream);
        let req = req.clone();
        tasks.spawn(async move { upstream.resolve(&req).await });
    }

    let mut sleep_until = Box::pin(tokio::time::sleep(deadline));
    let mut last_error: Option<UpstreamError> = None;

    loop {
        let joined = tokio::select! {
            biased;
            joined = tasks.join_next() => joined,
            _ = &mut sleep_until => {
                tracing::warn!(
                    source = req.source,
                    music_id = %req.music_id,
                    "upstream race hit the overall deadline"
                );
                break;
            }
        };

        let Some(joined) = joined else { break };

        match joined {
            // 赢家：直接返回，JoinSet 被 drop → 其余上游任务立刻 abort
            Ok(Ok(url)) => return Ok(url),
            Ok(Err(err)) => {
                tracing::warn!(
                    error = %err,
                    retryable = err.is_retryable(),
                    "one upstream failed, still waiting for the others"
                );
                last_error = Some(err);
            }
            Err(join_error) => {
                tracing::warn!(error = %join_error, "upstream task panicked or was cancelled");
                last_error = Some(UpstreamError::Decode {
                    upstream: "task".to_string(),
                    detail: join_error.to_string(),
                });
            }
        }
    }

    Err(last_error.unwrap_or(UpstreamError::Timeout {
        upstream: "race".to_string(),
        after: deadline,
    }))
}
