//! 酷狗音乐（`kg`）直连适配器。
//!
//! 走 `trackercdn.kugou.com/v5/url`：请求参数**按 key 排序**后拼接、
//! `signature = md5(SALT + 拼接串 + SALT)`、`key = md5(hash + 固定后缀)`。
//!
//! 2026-11-22 实测（无 cookie、手机网络）：
//!
//! | `quality` | 返回 `bitRate` | 返回 `extName` | 直链实测 |
//! | --- | --- | --- | --- |
//! | `128` | 128000 | mp3 | `206` + `audio/mpeg` |
//! | `320` | 320000 | mp3 | `206` + `audio/mpeg` |
//! | `flac` | 930000 | flac | `206` + flac 字节 |
//!
//! 换到钱上：**免费曲也能拿 320k / 无损**，比 `i/v2` 那条老路（永远 128k、
//! 会员曲直接 `status:2`）强得多，所以这里只用 v5。
//!
//! 注意 `status`：`1` 才是成功；`0` = 没下发（版权/下架），`2` = 要付费/会员。
//! 另外同一个 hash 未必每个档位都有货，服务端会**静默降档**，所以档位以回包里
//! 的 `extName` + `bitRate` 为准，如实上报。

use std::time::Duration;

use serde_json::Value;

use crate::{
    upstream::{prefer_https, TrackRequest, Upstream, UpstreamError},
    SourceUrl,
};

/// v5 签名入口
const ENDPOINT: &str = "https://trackercdn.kugou.com/v5/url";
/// 签名盐（服务端硬编码，安卓端固定值）
const SIGN_SALT: &str = "OIlwieks28dk2k092lksi2UIkp";
/// `key` 参数的后缀：`md5(hash + KEY_SUFFIX)`
const KEY_SUFFIX: &str = "57ae12eb6890223e355ccfcb74edf70d1005-0";
/// v5 接口认的 Play 端 UA，换成浏览器 UA 会直接 400
const MOBILE_UA: &str = "Android712-AndroidPhone-12227-130-0-play-0";
/// 签名直链保守有效期（秒）
const URL_TTL_SECS: u64 = 900;

/// 酷狗直连适配器。
#[derive(Debug, Clone)]
pub struct KgUpstream {
    name: String,
    /// cookie（可选）：v5 主链路不需要，留给以后解锁更高权益
    cookie: Option<String>,
    /// 入口地址；测试里可以指到本地回声服务
    endpoint: String,
    timeout: Duration,
}

impl KgUpstream {
    pub fn new(timeout: Duration) -> Self {
        Self {
            name: "kugou".to_string(),
            cookie: None,
            endpoint: ENDPOINT.to_string(),
            timeout,
        }
    }

    pub fn with_cookie(mut self, cookie: Option<String>) -> Self {
        self.cookie = cookie.filter(|value| !value.trim().is_empty());
        self
    }

    #[cfg(test)]
    pub fn with_base(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// 请求档位 → v5 的 `quality` 参数
    fn quality_for(quality: &str) -> &'static str {
        match quality {
            "flac24bit" | "flac" => "flac",
            "320k" => "320",
            _ => "128",
        }
    }

    /// 拼出带 `signature` 的完整问句（参数按 key 字典序，参与签名）
    fn signed_query(hash: &str, quality: &str, clienttime: u64) -> String {
        let key = md5_hex(&format!("{hash}{KEY_SUFFIX}"));
        let mut params: Vec<(&str, String)> = vec![
            ("userid", "0".to_string()),
            ("area_code", "1".to_string()),
            ("module", String::new()),
            ("hash", hash.to_string()),
            ("appid", "1005".to_string()),
            ("ssa_flag", "is_fromtrack".to_string()),
            ("version", "12029".to_string()),
            ("vipType", "6".to_string()),
            ("ptype", "0".to_string()),
            ("token", String::new()),
            ("mtype", "3".to_string()),
            ("quality", Self::quality_for(quality).to_string()),
            ("album_audio_id", "0".to_string()),
            ("behavior", "play".to_string()),
            ("pid", "2".to_string()),
            ("clienttime", clienttime.to_string()),
            ("cmd", "26".to_string()),
            ("uuid", "-".to_string()),
            ("mid", "-".to_string()),
            ("dfid", "-".to_string()),
            ("clientver", "12029".to_string()),
            ("pidversion", "3001".to_string()),
            ("key", key),
        ];
        params.sort_by(|left, right| left.0.cmp(right.0));

        // 签名串是「裸拼接」，不带 & 也不做 URL 编码
        let mut joined = String::new();
        for (name, value) in &params {
            joined.push_str(name);
            joined.push('=');
            joined.push_str(value);
        }
        let signature = md5_hex(&format!("{SIGN_SALT}{joined}{SIGN_SALT}"));

        let mut query = params
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        query.push_str("&signature=");
        query.push_str(&signature);
        query
    }
}

/// md5 十六进制小写（酷狗签名用）
fn md5_hex(input: &str) -> String {
    format!("{:x}", md5::compute(input.as_bytes()))
}

#[async_trait::async_trait]
impl Upstream for KgUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports(&self, source: &str) -> bool {
        source == "kg"
    }

    /// 免费曲实测能到 flac（930kbps），但到不了 24bit —— 老实报 `flac`。
    fn max_quality(&self) -> &'static str {
        crate::quality::LADDER[2]
    }

    /// 关键词搜索：主源拿不到时按歌名换源用
    async fn search_keyword(&self, keyword: &str) -> Result<String, UpstreamError> {
        crate::upstream::search::kugou(crate::http::client(), keyword, self.timeout).await
    }

    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
        let hash = req.music_id.trim().to_ascii_uppercase();
        // 酷狗的 hash 是 32 位十六进制
        if hash.len() != 32 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(UpstreamError::NoUrl {
                upstream: self.name.clone(),
                detail: format!("`{hash}` 不是酷狗的 32 位 hash"),
            });
        }

        let query = Self::signed_query(&hash, req.quality, crate::now_unix());
        let url = format!("{}?{query}", self.endpoint);
        let attempt = async {
            let mut builder = crate::http::client()
                .get(url.as_str())
                .header(reqwest::header::USER_AGENT, MOBILE_UA)
                .header("Kg-Thash", "5760185")
                .header("Kg-Rec", "1")
                .header("Kg-Rc", "1");
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
            parse_kugou_body(&self.name, &body)
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

/// 解析 v5 响应：`status == 1` 且有 `url` 才算成功。
pub(crate) fn parse_kugou_body(upstream: &str, body: &str) -> Result<SourceUrl, UpstreamError> {
    let value: Value = serde_json::from_str(body).map_err(|err| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: err.to_string(),
    })?;

    let status = value.get("status").and_then(Value::as_i64).unwrap_or(-1);
    let url = match value.get("url") {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(Value::Array(items)) => items
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string(),
        _ => String::new(),
    };

    if status != 1 || url.is_empty() {
        let hint = match status {
            2 => "酷狗要付费/会员（status=2）",
            0 => "酷狗没下发直链（status=0，版权或下架）",
            _ => "酷狗返回异常",
        };
        return Err(UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: format!("{hint}，status={status}"),
        });
    }

    let ext = value.get("extName").and_then(Value::as_str).unwrap_or_default();
    let bitrate = value.get("bitRate").and_then(Value::as_u64).unwrap_or(0);

    Ok(SourceUrl {
        url: prefer_https(&url),
        provider: upstream.to_string(),
        quality: detect_quality(ext, bitrate).to_string(),
        expires_at: crate::now_unix() + URL_TTL_SECS,
    })
}

/// 从 `extName` + `bitRate`（bps）反推实际档位。
pub(crate) fn detect_quality(ext: &str, bitrate: u64) -> &'static str {
    let ext = ext.to_ascii_lowercase();
    let kbps = if bitrate >= 1_000 { bitrate / 1_000 } else { bitrate };

    if ext.contains("flac") {
        return if kbps >= 1_500 {
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

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "1616C0383A01C80738065697D8548193";

    #[test]
    fn maps_quality_to_kugou_quality_parameter() {
        assert_eq!(KgUpstream::quality_for("128k"), "128");
        assert_eq!(KgUpstream::quality_for("320k"), "320");
        assert_eq!(KgUpstream::quality_for("flac"), "flac");
        assert_eq!(KgUpstream::quality_for("flac24bit"), "flac");
    }

    #[test]
    fn only_supports_kg() {
        let kg = KgUpstream::new(Duration::from_millis(800));
        assert!(kg.supports("kg"));
        assert!(!kg.supports("kw"));
        assert_eq!(kg.max_quality(), "flac");
    }

    /// 黄金用例：签名算法（参数排序 + 盐 + 裸拼接）一旦改坏，酷狗会直接 400
    #[test]
    fn signature_matches_the_reference_implementation() {
        // 注意传的是网关内部的档位名（320k），映射成酷狗的 quality=320 是 quality_for 的事
        let query = KgUpstream::signed_query(HASH, "320k", 1_795_339_200);
        assert!(query.contains("quality=320&"), "quality 没映射成 320：{query}");
        assert!(
            query.contains("key=0cbb4cc72dea54761190cb20872e433b"),
            "key 应该是 md5(hash + 后缀)：{query}"
        );
        assert!(
            query.contains("signature=98a40b66c96e0fac44be6ae56adcf44f"),
            "signature 不对：{query}"
        );
        // 参数必须按 key 字典序进 URL（签名与问句同源）
        let names: Vec<&str> = query
            .split('&')
            .filter_map(|pair| pair.split('=').next())
            .filter(|name| *name != "signature")
            .collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "参数没按字典序: {names:?}");
    }

    #[test]
    fn parses_flac_and_reports_it_honestly() {
        let body = serde_json::json!({
            "status": 1,
            "bitRate": 930_000,
            "extName": "flac",
            "fileSize": 28_000_000,
            "url": "http://fsandroid.kugou.com/x.flac"
        })
        .to_string();
        let parsed = parse_kugou_body("kugou", &body).expect("flac 应该解析成功");
        assert_eq!(parsed.quality, "flac");
        assert_eq!(parsed.url, "https://fsandroid.kugou.com/x.flac", "http 要升到 https");
        assert!(parsed.is_fresh());
    }

    #[test]
    fn accepts_url_array_and_reports_downgrade() {
        // 同一首曲子只有 128k 时，酷狗会静默降档：档位要按返回字段算
        let body = serde_json::json!({
            "status": 1,
            "bitRate": 128_000,
            "extName": "mp3",
            "url": ["http://fsandroid.kugou.com/a.mp3", "http://fsandroid.kugou.com/b.mp3"]
        })
        .to_string();
        let parsed = parse_kugou_body("kugou", &body).expect("数组形态也要认");
        assert_eq!(parsed.quality, "128k");
        assert_eq!(parsed.url, "https://fsandroid.kugou.com/a.mp3");
    }

    #[test]
    fn distinguishes_paid_from_delisted() {
        let paid = parse_kugou_body("kugou", r#"{"status":2,"trans_param":{}}"#).unwrap_err();
        match paid {
            UpstreamError::NoUrl { detail, .. } => assert!(detail.contains("付费"), "实际: {detail}"),
            other => panic!("expected NoUrl, got {other:?}"),
        }

        let delisted = parse_kugou_body("kugou", r#"{"status":0,"error":"no url"}"#).unwrap_err();
        assert!(matches!(delisted, UpstreamError::NoUrl { .. }));

        let garbage = parse_kugou_body("kugou", "not json").unwrap_err();
        assert!(matches!(garbage, UpstreamError::Decode { .. }));
    }

    #[tokio::test]
    async fn invalid_hash_fails_before_any_network_call() {
        let kg = KgUpstream::new(Duration::from_millis(800));
        let err = kg
            .resolve(&TrackRequest::new("kg", "154706365", "320k"))
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));
    }

    /// 真发一次请求：验证签名、Play 端请求头和 UA 都到位了
    #[tokio::test]
    async fn sends_signature_and_play_headers_to_the_service() {
        use axum::{extract::Request, http::StatusCode, response::IntoResponse, routing::get, Json, Router};

        async fn handler(request: Request) -> axum::response::Response {
            let headers = request.headers();
            let ua = headers
                .get(reqwest::header::USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let thash = headers
                .get("Kg-Thash")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let query = request.uri().query().unwrap_or_default().to_string();

            if ua != MOBILE_UA || thash.is_empty() || !query.contains("signature=") {
                return (StatusCode::BAD_REQUEST, format!("bad headers/query: {ua} {thash} {query}"))
                    .into_response();
            }
            if !query.contains("quality=320") || !query.contains(&format!("hash={HASH}")) {
                return (StatusCode::BAD_REQUEST, "hash/quality missing".to_string()).into_response();
            }

            Json(serde_json::json!({
                "status": 1,
                "bitRate": 320_000,
                "extName": "mp3",
                "url": "http://fsandroid.kugou.com/echo.mp3"
            }))
            .into_response()
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/v5/url", get(handler));
            let _ = axum::serve(listener, app).await;
        });

        let kg = KgUpstream::new(Duration::from_millis(800)).with_base(format!("http://{addr}/v5/url"));

        let parsed = kg
            .resolve(&TrackRequest::new("kg", HASH, "320k"))
            .await
            .expect("签名 + 请求头都对才会成功");
        assert_eq!(parsed.quality, "320k");
        assert_eq!(parsed.url, "https://fsandroid.kugou.com/echo.mp3");
    }
}
