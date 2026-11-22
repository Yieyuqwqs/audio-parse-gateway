//! 咪咕音乐（`mg`）直连适配器。
//!
//! 走 `app.pd.nf.migu.cn/MIGUM2.0/.../sub/listenSong.do`：**302 重定向**到音频直链，
//! 拿不到就给 `200 + {"code":"200000","info":"暂不提供试听地址"}`。
//! 所以这里必须用「不跟随重定向」的客户端，把 `Location` 原样当作直链返回。
//!
//! 2026-11-22 实测（无 cookie、手机网络）：
//!
//! * 匿名拿到的永远是 `MP3_128_16_Stero/...mp3` —— `toneFlag`（PQ/HQ/SQ）会被忽略，
//!   所以**没 cookie 时老实报 128k**，配置了会员 cookie 才敢往上报无损；
//! * 返回的直链能正常 `206` 取字节（`audio/mpeg`）；
//! * `music.migu.cn/v3/api/music/audioPlayer/getPlayInfo` 已经下线（301 → `/v5/`），
//!   别再按老配方找。
//!
//! 曲目 id 是**两段**：`copyrightId`（版权方 id）+ `contentId`（内容 id），少一个就会
//! `{"code":"299999","info":"参数校验失败 contentId:must not be blank"}`。所以约定
//! `music_id` 写成 `版权id_内容id`（也接受 `:` 分隔）。

use std::time::Duration;

use serde_json::Value;

use crate::{
    upstream::{prefer_https, TrackRequest, Upstream, UpstreamError},
    SourceUrl,
};

/// 试听地址入口（302 → 直链）
const ENDPOINT: &str = "https://app.pd.nf.migu.cn/MIGUM2.0/v1.0/content/sub/listenSong.do";
/// 咪咕客户端渠道号；缺了会被判非法请求
const CHANNEL: &str = "014000D";
/// 匿名也能用的占位 userId
const DEFAULT_USER_ID: &str = "15512345678";
/// 移动端 UA
const MOBILE_UA: &str = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 \
                         (KHTML, like Gecko) Chrome/120 Mobile Safari/537.36";
/// 直链有效期（秒）：试听地址通常几分钟到几十分钟，宁短勿长
const URL_TTL_SECS: u64 = 600;

/// 咪咕直连适配器。
#[derive(Debug, Clone)]
pub struct MiguUpstream {
    name: String,
    /// 会员 cookie（可选）：带上才有机会拿到 HQ/SQ
    cookie: Option<String>,
    /// 登录 userId（可选）
    user_id: Option<String>,
    /// 入口地址；测试里可以指到本地回声服务
    endpoint: String,
    timeout: Duration,
}

impl MiguUpstream {
    pub fn new(timeout: Duration) -> Self {
        Self {
            name: "migu".to_string(),
            cookie: None,
            user_id: None,
            endpoint: ENDPOINT.to_string(),
            timeout,
        }
    }

    pub fn with_credentials(mut self, cookie: Option<String>, user_id: Option<String>) -> Self {
        self.cookie = cookie.filter(|value| !value.trim().is_empty());
        self.user_id = user_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        self
    }

    #[cfg(test)]
    pub fn with_base(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// 请求档位 → 咪咕的 `toneFlag`
    fn tone_for(quality: &str) -> &'static str {
        match quality {
            "flac24bit" | "flac" => "SQ",
            "320k" => "HQ",
            _ => "PQ",
        }
    }

    fn user_id(&self) -> &str {
        self.user_id.as_deref().unwrap_or(DEFAULT_USER_ID)
    }

    fn request_url(&self, copyright_id: &str, content_id: &str, quality: &str) -> String {
        format!(
            "{}?channel={}&copyrightId={}&contentId={}&toneFlag={}&userId={}&netType=00",
            self.endpoint,
            CHANNEL,
            copyright_id,
            content_id,
            Self::tone_for(quality),
            self.user_id()
        )
    }
}

/// 把 `版权id_内容id` 拆成两段。
fn split_ids(upstream: &str, raw: &str) -> Result<(String, String), UpstreamError> {
    let raw = raw.trim();
    let (copyright_id, content_id) = match raw.split_once(['_', ':']) {
        Some((left, right)) => (left.trim(), right.trim()),
        None => (raw, ""),
    };

    let valid = |part: &str| {
        !part.is_empty()
            && part.len() <= 32
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    };

    if !valid(copyright_id) || !valid(content_id) {
        return Err(UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: format!(
                "`{raw}` 不是咪咕的 id；要写成 `copyrightId_contentId`（两段都必需）"
            ),
        });
    }
    Ok((copyright_id.to_string(), content_id.to_string()))
}

#[async_trait::async_trait]
impl Upstream for MiguUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports(&self, source: &str) -> bool {
        source == "mg"
    }

    /// 匿名实测只有 128k（`toneFlag` 被忽略）；带会员 cookie 才按无损上报。
    fn max_quality(&self) -> &'static str {
        if self.cookie.is_some() {
            crate::quality::LADDER[2]
        } else {
            crate::quality::LADDER[0]
        }
    }

    /// 关键词搜索：主源拿不到时按歌名换源用
    async fn search_keyword(&self, keyword: &str) -> Result<String, UpstreamError> {
        crate::upstream::search::migu(crate::http::client(), keyword, self.timeout).await
    }

    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
        let (copyright_id, content_id) = split_ids(&self.name, &req.music_id)?;
        let url = self.request_url(&copyright_id, &content_id, req.quality);

        let attempt = async {
            let mut builder = crate::http::no_redirect_client()
                .get(url.as_str())
                .header(reqwest::header::USER_AGENT, MOBILE_UA)
                .header("channel", CHANNEL);
            if let Some(cookie) = self.cookie.as_deref() {
                builder = builder.header(reqwest::header::COOKIE, cookie);
            }

            let response = builder.send().await.map_err(|source| UpstreamError::Transport {
                upstream: self.name.clone(),
                source,
            })?;

            let status = response.status();

            // 302 → Location 就是直链
            if status.is_redirection() {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .map(str::trim)
                    .unwrap_or_default()
                    .to_string();
                if location.is_empty() {
                    return Err(UpstreamError::NoUrl {
                        upstream: self.name.clone(),
                        detail: format!("咪咕回了 {status} 却没有 Location"),
                    });
                }
                return Ok(SourceUrl {
                    url: prefer_https(&absolutize(&location)),
                    provider: self.name.clone(),
                    quality: detect_quality(&location).to_string(),
                    expires_at: crate::now_unix() + URL_TTL_SECS,
                });
            }

            if !status.is_success() {
                return Err(UpstreamError::Status { upstream: self.name.clone(), status });
            }

            // 200 说明没给试听地址：把官方话术原样透出来，别伪造成功
            let body = response.text().await.map_err(|source| UpstreamError::Transport {
                upstream: self.name.clone(),
                source,
            })?;
            parse_migu_error(&self.name, &body)
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

/// 相对 Location 补成绝对地址（正常都是绝对的，防御一下）
fn absolutize(location: &str) -> String {
    if location.starts_with("http://") || location.starts_with("https://") {
        location.to_string()
    } else if let Some(rest) = location.strip_prefix('/') {
        format!("https://app.pd.nf.migu.cn/{rest}")
    } else {
        format!("https://app.pd.nf.migu.cn/{location}")
    }
}

/// 200 响应体 → `NoUrl`（把咪咕的 `info` 带出来，方便判断是版权还是参数问题）
pub(crate) fn parse_migu_error(upstream: &str, body: &str) -> Result<SourceUrl, UpstreamError> {
    let hint = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("info")
                .or_else(|| value.get("message"))
                .or_else(|| value.get("msg"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.trim().chars().take(120).collect());

    Err(UpstreamError::NoUrl {
        upstream: upstream.to_string(),
        detail: format!("咪咕没有给出试听地址：{hint}"),
    })
}

/// 从直链文件名反推档位（`MP3_128_16_Stero` / `MP3_320_...` / `*.flac`）。
pub(crate) fn detect_quality(url: &str) -> &'static str {
    let lower = url.to_ascii_lowercase();
    if lower.contains("flac") {
        return crate::quality::LADDER[2];
    }
    if lower.contains("_320") || lower.contains("320_") {
        return crate::quality::LADDER[1];
    }
    // 咪咕匿名只会给 MP3_128；文件名没写码率时也按 128k 保守上报，
    // 绝不用「请求了什么」来假装拿到了更高档位。
    crate::quality::LADDER[0]
}

#[cfg(test)]
mod tests {
    use super::*;

    const CID: &str = "64046801861";
    const CONTENT: &str = "600908000004734752";

    #[test]
    fn maps_quality_to_tone_flag() {
        assert_eq!(MiguUpstream::tone_for("128k"), "PQ");
        assert_eq!(MiguUpstream::tone_for("320k"), "HQ");
        assert_eq!(MiguUpstream::tone_for("flac"), "SQ");
        assert_eq!(MiguUpstream::tone_for("flac24bit"), "SQ");
    }

    #[test]
    fn only_supports_mg() {
        let mg = MiguUpstream::new(Duration::from_millis(800));
        assert!(mg.supports("mg"));
        assert!(!mg.supports("kw"));
    }

    #[test]
    fn ceiling_is_128k_for_anonymous() {
        let guest = MiguUpstream::new(Duration::from_millis(800));
        assert_eq!(guest.max_quality(), "128k");

        let member = MiguUpstream::new(Duration::from_millis(800))
            .with_credentials(Some("migu_token=abc".to_string()), Some("15500000000".to_string()));
        assert_eq!(member.max_quality(), "flac");
    }

    #[test]
    fn splits_the_two_part_id() {
        let (cid, content) = split_ids("migu", &format!("{CID}_{CONTENT}")).expect("两段 id");
        assert_eq!(cid, CID);
        assert_eq!(content, CONTENT);

        let (cid, content) = split_ids("migu", &format!("{CID}:{CONTENT}")).expect("冒号也认");
        assert_eq!(cid, CID);
        assert_eq!(content, CONTENT);

        // 只给一段：必须早点报错，不然接口回「参数校验失败」看不出是谁的问题
        let err = split_ids("migu", CID).unwrap_err();
        match err {
            UpstreamError::NoUrl { detail, .. } => assert!(detail.contains("copyrightId_contentId")),
            other => panic!("expected NoUrl, got {other:?}"),
        }

        assert!(split_ids("migu", "abc def").is_err());
    }

    #[test]
    fn detects_quality_from_the_signed_file_name() {
        assert_eq!(
            detect_quality("https://freetyst.nf.migu.cn/x/MP3_128_16_Stero/64046801861024701.mp3"),
            "128k",
            "文件名写 128 就别报 320k"
        );
        assert_eq!(
            detect_quality("https://freetyst.nf.migu.cn/x/MP3_320_16_Stero/a.mp3"),
            "320k"
        );
        assert_eq!(detect_quality("https://freetyst.nf.migu.cn/x/a.flac"), "flac");
    }

    #[test]
    fn surfaces_the_platform_refusal_instead_of_faking_success() {
        let body = r#"{"code":"200000","info":"暂不提供试听地址"}"#;
        let err = parse_migu_error("migu", body).unwrap_err();
        match err {
            UpstreamError::NoUrl { detail, .. } => assert!(detail.contains("暂不提供试听地址")),
            other => panic!("expected NoUrl, got {other:?}"),
        }
    }

    /// 真发一次请求：本地回声服务回 302，验证 Location 被当成直链
    #[tokio::test]
    async fn follows_the_redirect_manually_and_returns_the_location() {
        use axum::{
            extract::Query,
            http::StatusCode,
            response::{IntoResponse, Redirect},
            routing::get,
            Router,
        };
        use std::collections::HashMap;

        async fn handler(Query(params): Query<HashMap<String, String>>) -> axum::response::Response {
            let ok = params.get("copyrightId").map(String::as_str) == Some(CID)
                && params.get("contentId").map(String::as_str) == Some(CONTENT)
                && params.get("toneFlag").map(String::as_str) == Some("HQ");
            if !ok {
                return (StatusCode::BAD_REQUEST, format!("bad query: {params:?}")).into_response();
            }
            Redirect::temporary(
                "https://freetyst.nf.migu.cn/x/MP3_320_16_Stero/64046801861024701.mp3",
            )
            .into_response()
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/listenSong.do", get(handler));
            let _ = axum::serve(listener, app).await;
        });

        let mg = MiguUpstream::new(Duration::from_millis(800))
            .with_base(format!("http://{addr}/listenSong.do"));

        let parsed = mg
            .resolve(&TrackRequest::new("mg", format!("{CID}_{CONTENT}"), "320k"))
            .await
            .expect("302 应该被解析成直链");
        assert_eq!(parsed.url, "https://freetyst.nf.migu.cn/x/MP3_320_16_Stero/64046801861024701.mp3");
        assert_eq!(parsed.quality, "320k");
    }
}
