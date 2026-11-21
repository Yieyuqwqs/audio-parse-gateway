//! 网易云音乐（`wy`）直连适配器 —— 一个真正能用的「真实上游」。
//!
//! 打的是官方播放地址接口 `GET /api/song/enhance/player/url`：
//!
//! ```text
//! GET https://music.163.com/api/song/enhance/player/url?ids=[347230]&br=320000
//!     Referer: https://music.163.com/
//!     Cookie:  MUSIC_U=...（可选；带上才能解锁 VIP 音质）
//!
//! {"data":[{"id":347230,"url":"http://m801.music.126.net/...mp3?vuutv=...","code":200,"expi":1200,"fee":1}]}
//! ```
//!
//! 要点：
//! * `code == 200` 且 `url` 非空才算成功；付费/版权下架会返回 `url: null`（`code` 可能是 -110/404），
//!   这时抛 `NoUrl` 让竞速层继续等别的上游，而不是把死链接返回给播放器；
//! * `expi` 就是这条签名直链的有效期（秒），写进 `expires_at`，缓存命中时还会二次校验；
//! * 高音质（flac / flac24bit）需要登录 cookie，没 cookie 时网易云直接给 null —— 行为可预期，不会假装成功。

use std::time::Duration;

use crate::{
    upstream::{TrackRequest, Upstream, UpstreamError},
    SourceUrl,
};

/// 官方接口路径
const API_PATH: &str = "/api/song/enhance/player/url";
/// 官方默认有效期（接口没给 `expi` 时兜底）
const DEFAULT_EXPI_SECS: u64 = 1_200;

/// 网易云直连适配器。
#[derive(Debug, Clone)]
pub struct NeteaseUpstream {
    name: String,
    base: String,
    cookie: Option<String>,
    timeout: Duration,
}

impl NeteaseUpstream {
    pub fn new(timeout: Duration) -> Self {
        Self {
            name: "netease".to_string(),
            base: "https://music.163.com".to_string(),
            cookie: None,
            timeout,
        }
    }

    /// 登录态 cookie（`MUSIC_U=...`），有它才能拿 320k / flac。
    pub fn with_cookie(mut self, cookie: Option<String>) -> Self {
        self.cookie = cookie.filter(|value| !value.trim().is_empty());
        self
    }

    /// 换 base（自建反代 / 测试用）。
    pub fn with_base(mut self, base: impl Into<String>) -> Self {
        self.base = base.into();
        self
    }

    fn endpoint(&self) -> String {
        format!("{}{}", self.base.trim_end_matches('/'), API_PATH)
    }

    /// 音质 → 网易云码率（br）。
    fn bitrate_for(quality: &str) -> Option<u32> {
        match quality {
            "128k" => Some(128_000),
            "320k" => Some(320_000),
            // 无损：999000 表示「最高可用」，具体给 flac 还是 24bit 由账号等级决定
            "flac" | "flac24bit" => Some(999_000),
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl Upstream for NeteaseUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports(&self, source: &str) -> bool {
        source == "wy"
    }

    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
        let bitrate = Self::bitrate_for(req.quality).ok_or_else(|| UpstreamError::Unsupported {
            upstream: self.name.clone(),
            source: format!("{}@{}", req.source, req.quality),
        })?;

        // 网易云的 id 必须是纯数字；不是就说明调用方传错了（比如把 kw 的 rid 丢给 wy）
        let track_id: u64 = req.music_id.parse().map_err(|_| UpstreamError::NoUrl {
            upstream: self.name.clone(),
            detail: format!("`{}` is not a numeric netease song id", req.music_id),
        })?;

        let endpoint = self.endpoint();
        let ids = format!("[{track_id}]");
        let br = bitrate.to_string();

        let attempt = async {
            let mut builder = crate::http::client()
                .get(endpoint.as_str())
                .header(reqwest::header::REFERER, "https://music.163.com/")
                .query(&[("ids", ids.as_str()), ("br", br.as_str())]);
            if let Some(cookie) = self.cookie.as_deref() {
                builder = builder.header(reqwest::header::COOKIE, cookie);
            }

            let response = builder.send().await.map_err(|source| UpstreamError::Transport {
                upstream: self.name.clone(),
                source,
            })?;

            let status = response.status();
            if !status.is_success() {
                return Err(UpstreamError::Status { upstream: self.name.clone(), status });
            }

            let body = response.text().await.map_err(|source| UpstreamError::Transport {
                upstream: self.name.clone(),
                source,
            })?;
            parse_netease_body(&self.name, req.quality, &body)
        };

        match tokio::time::timeout(self.timeout, attempt).await {
            Ok(result) => result,
            Err(_) => Err(UpstreamError::Timeout {
                upstream: self.name.clone(),
                after: self.timeout,
            }),
        }
    }
}

/// 解析网易云播放地址响应。
pub(crate) fn parse_netease_body(
    upstream: &str,
    quality: &str,
    body: &str,
) -> Result<SourceUrl, UpstreamError> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|err| UpstreamError::Decode {
            upstream: upstream.to_string(),
            detail: err.to_string(),
        })?;

    let entry = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.first())
        .ok_or_else(|| UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: "netease payload has no data[] entry".to_string(),
        })?;

    let code = entry.get("code").and_then(serde_json::Value::as_i64).unwrap_or(200);
    let url = entry
        .get("url")
        .and_then(serde_json::Value::as_str)
        .filter(|url| !url.trim().is_empty());

    let Some(url) = url else {
        let fee = entry.get("fee").and_then(serde_json::Value::as_i64).unwrap_or(-1);
        return Err(UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: format!(
                "netease 没有返回直链（code {code}, fee {fee}）：多半是版权/付费限制，{quality} 需要带 cookie 的会员账号"
            ),
        });
    };

    let expi = entry
        .get("expi")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(DEFAULT_EXPI_SECS);

    Ok(SourceUrl {
        url: url.to_string(),
        provider: upstream.to_string(),
        quality: quality.to_string(),
        expires_at: crate::now_unix() + expi,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_quality_to_bitrate() {
        assert_eq!(NeteaseUpstream::bitrate_for("128k"), Some(128_000));
        assert_eq!(NeteaseUpstream::bitrate_for("320k"), Some(320_000));
        assert_eq!(NeteaseUpstream::bitrate_for("flac"), Some(999_000));
        assert_eq!(NeteaseUpstream::bitrate_for("flac24bit"), Some(999_000));
        assert_eq!(NeteaseUpstream::bitrate_for("48k"), None);
    }

    #[test]
    fn only_supports_wy() {
        let upstream = NeteaseUpstream::new(Duration::from_millis(800));
        assert!(upstream.supports("wy"));
        assert!(!upstream.supports("kw"));
        assert!(!upstream.supports("tx"));
    }

    #[test]
    fn endpoint_is_stable() {
        let upstream = NeteaseUpstream::new(Duration::from_millis(800));
        assert_eq!(upstream.endpoint(), "https://music.163.com/api/song/enhance/player/url");

        let mirror = upstream.with_base("http://127.0.0.1:3000/");
        assert_eq!(mirror.endpoint(), "http://127.0.0.1:3000/api/song/enhance/player/url");
    }

    #[test]
    fn parses_successful_payload() {
        let body = r#"{"data":[{"id":347230,"url":"http://m801.music.126.net/x.mp3?vuutv=abc","br":320000,"code":200,"expi":1200,"fee":0}],"code":200}"#;
        let before = crate::now_unix();
        let parsed = parse_netease_body("netease", "320k", body).expect("should parse");
        assert!(parsed.url.starts_with("http://m801.music.126.net/x.mp3"));
        assert_eq!(parsed.provider, "netease");
        assert_eq!(parsed.quality, "320k");
        assert!(
            parsed.expires_at >= before + 1_200 && parsed.expires_at <= before + 1_210,
            "expires_at 应该约等于 now + expi，实际 {}",
            parsed.expires_at
        );
        assert!(parsed.is_fresh());
    }

    #[test]
    fn reports_no_url_instead_of_returning_a_dead_link() {
        let body = r#"{"data":[{"id":347230,"url":null,"br":0,"code":-110,"expi":1200,"fee":1}],"code":200}"#;
        let err = parse_netease_body("netease", "flac", body).unwrap_err();
        match err {
            UpstreamError::NoUrl { detail, .. } => {
                assert!(detail.contains("-110"));
                assert!(detail.contains("cookie"));
            }
            other => panic!("expected NoUrl, got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_data() {
        let err = parse_netease_body("netease", "320k", r#"{"code":200,"data":[]}"#).unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));
    }

    #[test]
    fn rejects_garbage() {
        let err = parse_netease_body("netease", "320k", "not json").unwrap_err();
        assert!(matches!(err, UpstreamError::Decode { .. }));
    }

    #[tokio::test]
    async fn non_numeric_id_fails_before_any_network_call() {
        let upstream = NeteaseUpstream::new(Duration::from_millis(800));
        // kw 的 hash 之类非数字 id 丢给网易云，必须在发请求之前就拒绝
        let err = upstream
            .resolve(&TrackRequest::new("wy", "kw-hash-abc", "320k"))
            .await
            .unwrap_err();
        match err {
            UpstreamError::NoUrl { detail, .. } => assert!(detail.contains("numeric netease song id")),
            other => panic!("expected NoUrl, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unsupported_quality_is_rejected_locally() {
        let upstream = NeteaseUpstream::new(Duration::from_millis(800));
        let err = upstream
            .resolve(&TrackRequest::new("wy", "347230", "48k"))
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::Unsupported { .. }));
        assert!(!err.is_retryable());
    }
}
