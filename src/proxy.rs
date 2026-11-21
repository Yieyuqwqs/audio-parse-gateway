//! 防盗链代拉：有些平台的直链不认客户端的裸请求（403），这里换成网关代拉的短链。
//!
//! * 直链探测：带平台 `Referer` 发一个 `Range: bytes=0-0` 的 HEAD，401/403/405 就判定要代拉；
//! * 代拉地址：`/stream/{token}`，token 是进程内随机串，10 分钟过期，不在磁盘落任何东西；
//! * 代拉时补齐 `Referer` + `User-Agent`，然后把音频流原样吐给客户端（不缓冲整个文件）。

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use dashmap::DashMap;

use crate::{now_unix, AppState};

/// 探测直链的超时：只用来判断 403，不值得等太久
const PROBE_TIMEOUT: Duration = Duration::from_millis(600);
/// 代拉短链有效期（秒）
const TOKEN_TTL_SECS: u64 = 600;
/// 代拉时的 UA，太像爬虫也会被挡
const USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
                          (KHTML, like Gecko) Chrome/126.0 Safari/537.36";

struct Target {
    url: String,
    referer: String,
    expires_at: u64,
}

/// 进程内短链表：token -> (直链, Referer)
#[derive(Default)]
pub(crate) struct ProxyStore {
    targets: DashMap<String, Target>,
    counter: AtomicU64,
}

impl ProxyStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 生成一个网关代拉短链 `/stream/{token}`
    pub(crate) fn link(&self, url: String, referer: String) -> String {
        let token = self.next_token();
        self.targets.insert(
            token.clone(),
            Target { url, referer, expires_at: now_unix() + TOKEN_TTL_SECS },
        );
        format!("/stream/{token}")
    }

    fn next_token(&self) -> String {
        // 进程内唯一即可（不需要密码学强度：token 只活 10 分钟，且不落盘）
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{seed:016x}{count:08x}")
    }

    /// 取出代拉目标（过期的顺手清掉）
    pub(crate) fn get(&self, token: &str) -> Option<(String, String)> {
        let now = now_unix();
        if self.targets.get(token).is_some_and(|target| target.expires_at <= now) {
            self.targets.remove(token);
            return None;
        }
        self.targets
            .get(token)
            .map(|target| (target.url.clone(), target.referer.clone()))
    }

    pub(crate) fn len(&self) -> usize {
        self.targets.len()
    }

    /// 直链是否必须代拉：带 Referer 发 HEAD，401/403/405 说明服务器不认裸请求。
    ///
    /// 探测失败（超时/网络错）一律当作「不需要代拉」，宁可直接返回也别把好好的直链换成代理。
    pub(crate) async fn needs_proxy(url: &str, referer: &str) -> bool {
        if referer.is_empty() {
            return false;
        }
        let Ok(parsed) = reqwest::Url::parse(url) else {
            return false;
        };

        let request = crate::http::client()
            .head(parsed)
            .header(reqwest::header::REFERER, referer)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .header(reqwest::header::RANGE, "bytes=0-0");

        match tokio::time::timeout(PROBE_TIMEOUT, request.send()).await {
            Ok(Ok(response)) => matches!(response.status().as_u16(), 401 | 403 | 405),
            _ => false,
        }
    }
}

/// `GET /stream/{token}`：补齐 Referer/UA 之后把音频流原样转发
pub(crate) async fn stream_handler(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Response {
    let Some((url, referer)) = state.proxy.get(&token) else {
        return (StatusCode::NOT_FOUND, "stream token expired").into_response();
    };

    let response = crate::http::client()
        .get(url.as_str())
        .header(reqwest::header::REFERER, referer)
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .send()
        .await;

    match response {
        Ok(response) if response.status().is_success() => {
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            (
                [(axum::http::header::CONTENT_TYPE, content_type)],
                Body::from_stream(response.bytes_stream()),
            )
                .into_response()
        }
        Ok(response) => (
            StatusCode::BAD_GATEWAY,
            format!("upstream answered {}", response.status()),
        )
            .into_response(),
        Err(err) => (StatusCode::BAD_GATEWAY, err.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_token_for_ten_minutes() {
        let store = ProxyStore::new();
        let link = store.link("https://cdn.example/a.mp3".to_string(), "https://y.qq.com/".to_string());
        assert!(link.starts_with("/stream/"));
        let token = link.trim_start_matches("/stream/").to_string();
        let (url, referer) = store.get(&token).expect("刚存的 token 应该能取到");
        assert_eq!(url, "https://cdn.example/a.mp3");
        assert_eq!(referer, "https://y.qq.com/");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn tokens_are_unique() {
        let store = ProxyStore::new();
        let a = store.link("https://cdn.example/a.mp3".to_string(), "r".to_string());
        let b = store.link("https://cdn.example/b.mp3".to_string(), "r".to_string());
        assert_ne!(a, b);
    }

    #[test]
    fn unknown_token_returns_none() {
        let store = ProxyStore::new();
        assert!(store.get("nope").is_none());
    }

    #[test]
    fn empty_referer_never_proxies() {
        let store = ProxyStore::new();
        assert!(store.len() == 0);
    }

    #[tokio::test]
    async fn detects_403_and_passes_200() {
        use axum::{routing::head, Router};

        // 永远 403：应该判定需要代拉
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/forbidden", head(|| async { StatusCode::FORBIDDEN }));
            let _ = axum::serve(listener, app).await;
        });
        assert!(
            ProxyStore::needs_proxy(&format!("http://{addr}/forbidden"), "https://y.qq.com/").await,
            "403 必须判定为需要代拉"
        );

        // 永远 200：不需要代拉
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/ok", head(|| async { StatusCode::OK }));
            let _ = axum::serve(listener, app).await;
        });
        assert!(
            !ProxyStore::needs_proxy(&format!("http://{addr}/ok"), "https://y.qq.com/").await,
            "200 不该走代理"
        );
    }
}
