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
pub(crate) struct ProxyStore {
    targets: DashMap<String, Target>,
    counter: AtomicU64,
    /// 对外可访问的 base（例如 http://127.0.0.1:8080），用来拼绝对 URL
    base_url: String,
}

impl ProxyStore {
    pub(crate) fn new(base_url: String) -> Self {
        Self {
            targets: DashMap::new(),
            counter: AtomicU64::new(0),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// 生成一个网关代拉短链 `/stream/{token}`
    pub(crate) fn link(&self, url: String, referer: String) -> String {
        let token = self.next_token();
        self.targets.insert(
            token.clone(),
            Target { url, referer, expires_at: now_unix() + TOKEN_TTL_SECS },
        );
        format!("{}/stream/{}", self.base_url, token)
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

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.targets.len()
    }

    /// 直链是否需要网关代拉。
    ///
    /// 探测姿势必须是「**不带 Referer**、像播放器那样真拉 2 字节」：
    ///
    /// * 腾讯 CDN 的坑：裸请求会返回 **200 但 body 为空**（不是 403），只看状态码会把「放不了」
    ///   误判成「能播」——之前就是这么踩的；
    /// * 任何 4xx/5xx、空 body、超时、网络错，都认为裸拉不可用 → 交给网关代拉（代拉会补 Referer）。
    pub(crate) async fn needs_proxy(url: &str, referer: &str) -> bool {
        if referer.is_empty() {
            return false;
        }
        let Ok(parsed) = reqwest::Url::parse(url) else {
            return false;
        };

        let request = crate::http::client()
            .get(parsed)
            .header(reqwest::header::RANGE, "bytes=0-1")
            .header(reqwest::header::USER_AGENT, USER_AGENT);

        let Ok(Ok(response)) = tokio::time::timeout(PROBE_TIMEOUT, request.send()).await else {
            return true; // 连探测都失败：直接代拉，别赌
        };

        let status = response.status();
        if !status.is_success() {
            return true;
        }

        // 200 也可能是个空壳（防盗链静默拒绝）
        match tokio::time::timeout(PROBE_TIMEOUT, response.bytes()).await {
            Ok(Ok(bytes)) => bytes.is_empty(),
            _ => true,
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
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        let link = store.link("https://cdn.example/a.mp3".to_string(), "https://y.qq.com/".to_string());
        assert_eq!(link, "http://127.0.0.1:8080/stream/".to_string() + link.rsplit('/').next().unwrap());
        let token = link.rsplit('/').next().unwrap().to_string();
        let (url, referer) = store.get(&token).expect("刚存的 token 应该能取到");
        assert_eq!(url, "https://cdn.example/a.mp3");
        assert_eq!(referer, "https://y.qq.com/");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn tokens_are_unique() {
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        let a = store.link("https://cdn.example/a.mp3".to_string(), "r".to_string());
        let b = store.link("https://cdn.example/b.mp3".to_string(), "r".to_string());
        assert_ne!(a, b);
    }

    #[test]
    fn unknown_token_returns_none() {
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        assert!(store.get("nope").is_none());
    }

    #[test]
    fn empty_referer_never_proxies() {
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        assert!(store.len() == 0);
    }

    #[tokio::test]
    async fn detects_403_and_passes_200() {
        use axum::{routing::get, Router};

        // 永远 403：应该判定需要代拉
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/forbidden", get(|| async { StatusCode::FORBIDDEN }));
            let _ = axum::serve(listener, app).await;
        });
        assert!(
            ProxyStore::needs_proxy(&format!("http://{addr}/forbidden"), "https://y.qq.com/").await,
            "403 必须判定为需要代拉"
        );

        // 200 且真有字节：不需要代拉
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/ok", get(|| async { vec![0u8, 1u8] }));
            let _ = axum::serve(listener, app).await;
        });
        assert!(
            !ProxyStore::needs_proxy(&format!("http://{addr}/ok"), "https://y.qq.com/").await,
            "200 且有字节不该走代理"
        );

        // 200 但空 body（腾讯 CDN 的防盗链姿势）：必须判定需要代拉
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let app = Router::new().route("/empty", get(|| async { "" }));
            let _ = axum::serve(listener, app).await;
        });
        assert!(
            ProxyStore::needs_proxy(&format!("http://{addr}/empty"), "https://y.qq.com/").await,
            "200 空 body 必须判定为需要代拉"
        );
    }
}
