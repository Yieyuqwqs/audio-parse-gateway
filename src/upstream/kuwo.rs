//! 酷我音乐（`kw`）直连适配器。
//!
//! 主路径走 `mobi.kuwo.cn/mobi.s` 的 `type=convert_url_with_sign`：给 rid + 目标码率，
//! 拿回一条**带签名的直链**（`data.url`）和它**实际**的 `format` / `bitrate`。
//!
//! 2026-11-22 实测（无 cookie、手机网络）：
//!
//! | 请求 `br` | 返回 `format` | 返回 `bitrate` | 直链实测 |
//! | --- | --- | --- | --- |
//! | `320k` | `mp3` | `320` | `206` + `audio/mpeg`，5.9MB/3.5min |
//! | `flac` | `mp3`（没会员就降档，服务端说了算） | `320` 或 `1` | 同上 |
//! | `128k` | `mp3` | `1`（酷我的低码率哨兵值） | `206` + `audio/mpeg` |
//!
//! 结论：`br` 只是「希望」，实际档位以响应字段为准 —— 所以这里按
//! `format` + `bitrate` 反推 [`crate::quality`] 档位并如实上报，绝不假装拿到了 flac。
//!
//! 主接口抽风时用老接口 `antiserver.kuwo.cn/anti.s` 兜底（免费曲仍给真实 mp3）。
//! 另外记一笔：`www.kuwo.cn/api/v1/www/music/playUrl` 需要 `Hm_Iuvt_*` 反爬会话，
//! 实测一律 `{"success":false,"message":"The request is illegal!"}`，别再往那条路上找。

use std::time::Duration;

use serde_json::Value;

use crate::{
    upstream::{prefer_https, TrackRequest, Upstream, UpstreamError},
    SourceUrl,
};

/// `mobi.s` 入口（HTTPS 可用，别再走明文 HTTP）
const ENDPOINT: &str = "https://mobi.kuwo.cn/mobi.s";
/// 客户端标识：服务端只用来挑一套返回格式，固定值足够
const SOURCE: &str = "kwplayer_ar_9.3.0.1_B_jiagu";
/// 防盗链用的 Referer
const REFERER: &str = "https://www.kuwo.cn/";
/// 签名直链保守有效期（秒）：比真实签名短，宁可多解一次也别把死链塞进缓存
const URL_TTL_SECS: u64 = 900;

/// 酷我直连适配器。
#[derive(Debug, Clone)]
pub struct KwUpstream {
    name: String,
    /// 会员 cookie（可选）：带上才有可能拿到 flac
    cookie: Option<String>,
    /// 入口地址；测试里可以指到本地回声服务
    endpoint: String,
    timeout: Duration,
}

impl KwUpstream {
    pub fn new(timeout: Duration) -> Self {
        Self {
            name: "kuwo".to_string(),
            cookie: None,
            endpoint: ENDPOINT.to_string(),
            timeout,
        }
    }

    /// 会员 cookie（`kw_token=...` 之类，整段塞进 `Cookie` 头）
    pub fn with_cookie(mut self, cookie: Option<String>) -> Self {
        self.cookie = cookie.filter(|value| !value.trim().is_empty());
        self
    }

    /// 换入口（测试用：指到本地回声服务）
    #[cfg(test)]
    pub fn with_base(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// 请求档位 → 酷我的 `br` 参数
    fn br_for(quality: &str) -> &'static str {
        match quality {
            "flac24bit" | "flac" => "flac",
            "320k" => "320k",
            _ => "128k",
        }
    }

    fn request_url(&self, rid: &str, quality: &str) -> String {
        format!(
            "{}?f=web&source={}&type=convert_url_with_sign&br={}&rid={}",
            self.endpoint,
            SOURCE,
            Self::br_for(quality),
            rid
        )
    }

    /// 老接口兜底：`anti.s` 直接回一行 URL（免费曲仍给真实 mp3）
    async fn legacy(&self, rid: &str) -> Result<SourceUrl, UpstreamError> {
        let url = format!(
            "https://antiserver.kuwo.cn/anti.s?type=convert_url&format=mp3&response=url&rid=MUSIC_{rid}"
        );
        let response = crate::http::client()
            .get(url.as_str())
            .header(reqwest::header::REFERER, REFERER)
            .send()
            .await
            .map_err(|source| UpstreamError::Transport {
                upstream: format!("{}+anti.s", self.name),
                source,
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(UpstreamError::Status {
                upstream: format!("{}+anti.s", self.name),
                status,
            });
        }
        let body = response.text().await.map_err(|source| UpstreamError::Transport {
            upstream: format!("{}+anti.s", self.name),
            source,
        })?;
        parse_kuwo_legacy_body(&self.name, &body)
    }
}

#[async_trait::async_trait]
impl Upstream for KwUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports(&self, source: &str) -> bool {
        source == "kw"
    }

    /// 不带会员 cookie 时实测最高 320k（flac 要会员）；有 cookie 就按无损上限报。
    fn max_quality(&self) -> &'static str {
        if self.cookie.is_some() {
            crate::quality::TOP
        } else {
            crate::quality::LADDER[1]
        }
    }

    /// 关键词搜索：主源拿不到时按歌名换源用
    async fn search_keyword(&self, keyword: &str) -> Result<String, UpstreamError> {
        crate::upstream::search::kuwo(crate::http::client(), keyword, self.timeout).await
    }

    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
        let rid = req.music_id.trim();
        // 酷我的 rid 是纯数字；拿 kg 的 hash 或 tx 的 songmid 打过来要早点挡掉
        if rid.is_empty() || rid.len() > 24 || !rid.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(UpstreamError::NoUrl {
                upstream: self.name.clone(),
                detail: format!("`{rid}` 不是酷我的数字 rid"),
            });
        }

        let primary_url = self.request_url(rid, req.quality);
        let primary = async {
            let mut builder = crate::http::client()
                .get(primary_url.as_str())
                .header(reqwest::header::REFERER, REFERER);
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
            parse_kuwo_body(&self.name, &body)
        };

        let primary = tokio::time::timeout(self.timeout, primary).await;
        match primary {
            Ok(Ok(url)) => return Ok(url),
            Ok(Err(err)) => {
                tracing::debug!(error = %err, "kuwo primary failed, trying anti.s");
            }
            Err(_) => {
                tracing::debug!("kuwo primary timed out, trying anti.s");
            }
        }

        match tokio::time::timeout(self.timeout, self.legacy(rid)).await {
            Ok(result) => result,
            Err(_) => Err(UpstreamError::Timeout {
                upstream: self.name.clone(),
                after: self.timeout,
            }),
        }
    }
}

/// 解析 `convert_url_with_sign` 的响应：`code == 200` + 非空 `data.url` 才算成功。
pub(crate) fn parse_kuwo_body(upstream: &str, body: &str) -> Result<SourceUrl, UpstreamError> {
    let value: Value = serde_json::from_str(body).map_err(|err| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: err.to_string(),
    })?;

    let code = value.get("code").and_then(Value::as_i64).unwrap_or(-1);
    if code != 200 {
        return Err(UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: format!("kuwo answered code {code}"),
        });
    }

    let data = value.get("data").ok_or_else(|| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: "payload has no `data`".to_string(),
    })?;

    let url = data
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if url.is_empty() {
        return Err(UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: "kuwo 没有下发直链（版权/会员限制）".to_string(),
        });
    }

    let format = data.get("format").and_then(Value::as_str).unwrap_or_default();
    let bitrate = data.get("bitrate").and_then(Value::as_u64).unwrap_or(0);

    Ok(SourceUrl {
        url: prefer_https(&url),
        provider: upstream.to_string(),
        quality: detect_quality(format, bitrate).to_string(),
        expires_at: crate::now_unix() + URL_TTL_SECS,
    })
}

/// 从 `format` + `bitrate` 反推实际档位。
///
/// 酷我的 `bitrate` 单位是 kbps，低码率曲目会给 `1` 这种哨兵值（按 128k 处理）；
/// 也有接口回 bps（`320000`），所以先归一化到 kbps 再比。
pub(crate) fn detect_quality(format: &str, bitrate: u64) -> &'static str {
    let format = format.to_ascii_lowercase();
    let kbps = if bitrate >= 1_000 { bitrate / 1_000 } else { bitrate };

    if format.contains("flac") {
        return if kbps >= 1_000 {
            crate::quality::LADDER[3]
        } else {
            crate::quality::LADDER[2]
        };
    }
    if kbps >= 256 {
        crate::quality::LADDER[1]
    } else {
        crate::quality::LADDER[0]
    }
}

/// 老接口 `anti.s` 的响应就是一行 URL（或 `format=mp3 bitrate=320 url=http://...`）。
///
/// 拿它当主接口的兜底：免费曲仍然给真实 mp3；会员曲会退化成同一个兜底文件，
/// 所以路径里认不出真实资源名就直接报错，别把兜底文件当歌返回。
pub(crate) fn parse_kuwo_legacy_body(
    upstream: &str,
    body: &str,
) -> Result<SourceUrl, UpstreamError> {
    let body = body.trim().trim_matches('"');
    let url = if body.starts_with("http") {
        body.to_string()
    } else {
        body.split_whitespace()
            .find_map(|part| part.strip_prefix("url="))
            .map(str::to_string)
            .unwrap_or_default()
    };

    if url.is_empty() {
        return Err(UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: "kuwo anti.s 没有给出直链".to_string(),
        });
    }

    // 兜底接口一律 mp3，不知道码率就按 128k 保守上报，绝不假装无损
    Ok(SourceUrl {
        url: prefer_https(&url),
        provider: upstream.to_string(),
        quality: crate::quality::LADDER[0].to_string(),
        expires_at: crate::now_unix() + URL_TTL_SECS,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RID: &str = "154706365";

    fn body(format: &str, bitrate: u64, url: &str) -> String {
        serde_json::json!({
            "code": 200,
            "data": { "url": url, "format": format, "bitrate": bitrate, "duration": 149, "rid": RID }
        })
        .to_string()
    }

    #[test]
    fn maps_quality_to_kuwo_br() {
        assert_eq!(KwUpstream::br_for("128k"), "128k");
        assert_eq!(KwUpstream::br_for("320k"), "320k");
        assert_eq!(KwUpstream::br_for("flac"), "flac");
        assert_eq!(KwUpstream::br_for("flac24bit"), "flac");
    }

    #[test]
    fn only_supports_kw() {
        let kw = KwUpstream::new(Duration::from_millis(800));
        assert!(kw.supports("kw"));
        assert!(!kw.supports("kg"));
    }

    #[test]
    fn ceiling_is_320k_without_a_member_cookie() {
        let guest = KwUpstream::new(Duration::from_millis(800));
        assert_eq!(guest.max_quality(), "320k");
        let member =
            KwUpstream::new(Duration::from_millis(800)).with_cookie(Some("kw_token=abc".to_string()));
        assert_eq!(member.max_quality(), crate::quality::TOP);
    }

    #[test]
    fn parses_signed_url_and_reports_actual_quality() {
        let parsed = parse_kuwo_body("kuwo", &body("mp3", 320, "https://kw-bj.kuwo.cn/x.mp3?sign=1"))
            .expect("320k 应该解析成功");
        assert_eq!(parsed.quality, "320k");
        assert_eq!(parsed.url, "https://kw-bj.kuwo.cn/x.mp3?sign=1");
        assert!(parsed.is_fresh());
    }

    #[test]
    fn sentinel_low_bitrate_is_128k_not_flac() {
        let parsed = parse_kuwo_body("kuwo", &body("mp3", 1, "https://kw-bj.kuwo.cn/low.mp3"))
            .expect("低码率也要能播");
        assert_eq!(parsed.quality, "128k");

        let flac = parse_kuwo_body("kuwo", &body("flac", 930, "https://kw-bj.kuwo.cn/x.flac"))
            .expect("flac 解析");
        assert_eq!(flac.quality, "flac");
    }

    #[test]
    fn reports_no_url_instead_of_a_dead_link() {
        let err =
            parse_kuwo_body("kuwo", r#"{"code":200,"data":{"url":"","format":"mp3"}}"#).unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));

        let err = parse_kuwo_body("kuwo", r#"{"code":304,"msg":"会员歌曲"}"#).unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));

        let err = parse_kuwo_body("kuwo", "not json").unwrap_err();
        assert!(matches!(err, UpstreamError::Decode { .. }));
    }

    #[test]
    fn legacy_anti_s_plain_text_is_understood() {
        let parsed = parse_kuwo_legacy_body(
            "kuwo",
            "http://kw-er.kuwo.cn/a895cac7/resource/30106/trackmedia/M5000004Gmy54cGDqK.mp3?bitrate$1",
        )
        .expect("纯文本直链也要认");
        assert!(parsed.url.starts_with("https://kw-er.kuwo.cn/"), "实际：{}", parsed.url);
        assert_eq!(parsed.quality, "128k", "兜底接口不知道码率就按 128k 报");

        let kv = parse_kuwo_legacy_body(
            "kuwo",
            "format=mp3 bitrate=1 url=http://kw-er.kuwo.cn/a/M500x.mp3",
        )
        .expect("key=value 也要认");
        assert!(kv.url.ends_with("M500x.mp3"));

        assert!(parse_kuwo_legacy_body("kuwo", "").is_err());
    }

    #[tokio::test]
    async fn invalid_rid_fails_before_any_network_call() {
        let kw = KwUpstream::new(Duration::from_millis(800));
        let err = kw
            .resolve(&TrackRequest::new("kw", "B3A52A7A958BF0AED0EBFBA2E9A818B7", "320k"))
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));
    }

    /// 真发一次请求：验证问句拼对了（br / rid / type）并且响应能解析
    #[tokio::test]
    async fn sends_the_expected_query_to_the_service() {
        use axum::{extract::Query, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
        use std::collections::HashMap;

        async fn handler(Query(params): Query<HashMap<String, String>>) -> axum::response::Response {
            let ok = params.get("type").map(String::as_str) == Some("convert_url_with_sign")
                && params.get("br").map(String::as_str) == Some("320k")
                && params.get("rid").map(String::as_str) == Some(RID);
            if !ok {
                return (StatusCode::BAD_REQUEST, format!("bad query: {params:?}")).into_response();
            }
            Json(serde_json::json!({
                "code": 200,
                "data": { "url": "https://kw-bj.kuwo.cn/echo.mp3?sig=ok", "format": "mp3", "bitrate": 320 }
            }))
            .into_response()
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/mobi.s", get(handler));
            let _ = axum::serve(listener, app).await;
        });

        let kw = KwUpstream::new(Duration::from_millis(800)).with_base(format!("http://{addr}/mobi.s"));

        let parsed = kw
            .resolve(&TrackRequest::new("kw", RID, "320k"))
            .await
            .expect("query 拼对了才会成功");
        assert_eq!(parsed.quality, "320k");
        assert_eq!(parsed.url, "https://kw-bj.kuwo.cn/echo.mp3?sig=ok");
    }
}
