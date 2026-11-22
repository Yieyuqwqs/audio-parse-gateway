//! 防盗链代拉：有些平台的直链不认客户端的裸请求，这里换成网关代拉的短链。
//!
//! * 代拉地址：`/stream/{token}`，token = `hash(url|referer)`，**幂等**（同一首歌永远是同一个短链），
//!   30 分钟过期，不在磁盘落任何东西；
//! * **只在出口改写**：缓存里存的是上游原链，代拉短链绝不进 L1/L2 —— 否则 token 过期后
//!   命中缓存会返回死链（播放器秒退、没声音），这是上一版最大的坑；
//! * 代拉时补齐 `Referer` + 浏览器 UA + 透传客户端 `Range`，把 206/`Content-Length` 原样吐回去。

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use dashmap::DashMap;

use crate::{now_unix, AppState};

/// 客户端没带 `Range` 时发给上游的 Range：只要一段，用来确认「CDN 真有这个文件」
pub(crate) const PROBE_RANGE: &str = "bytes=0-0";

/// 探测直链的超时：只用来判断 403，不值得等太久
const PROBE_TIMEOUT: Duration = Duration::from_millis(600);
/// 代拉短链有效期（秒）：必须 ≥ 缓存 TTL（30min），否则命中缓存时会拿到过期 token
const TOKEN_TTL_SECS: u64 = 1_800;
/// 上游首字节/idle 超时：CDN 卡住时别让播放器无限转圈
const UPSTREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
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

    /// 生成一个网关代拉短链 `/stream/{token}`。
    ///
    /// token 是 `(url, referer)` 的哈希 —— **同一个目标永远同一个 token**：
    /// 每次解析都换 token 的话，客户端重试/预加载会拿到互相不认识的短链，
    /// 10 分钟后过期还会连累缓存（见模块文档）。
    pub(crate) fn link(&self, url: String, referer: String) -> String {
        let token = self.token_for(&url, &referer);
        self.targets.insert(
            token.clone(),
            Target { url, referer, expires_at: now_unix() + TOKEN_TTL_SECS },
        );
        self.link_for(&token)
    }

    /// 生成短链时用的**对外地址**：`PUBLIC_BASE_URL` 优先，否则用请求自己的 Host。
    ///
    /// 不再用 `127.0.0.1` 拼：客户端不在网关那台机器上时，那个地址指向的是客户端自己。
    pub(crate) fn link_with_base(&self, base: &str, url: String, referer: String) -> String {
        let token = self.token_for(&url, &referer);
        self.targets.insert(
            token.clone(),
            Target { url, referer, expires_at: now_unix() + TOKEN_TTL_SECS },
        );
        if base.trim().is_empty() {
            self.link_for(&token)
        } else {
            format!("{}/stream/{}", base.trim_end_matches('/'), token)
        }
    }

    fn link_for(&self, token: &str) -> String {
        format!("{}/stream/{token}", self.base_url)
    }

    /// 目标 → 稳定 token（64 位 FNV-1a × 两轮混合，够用且不引依赖；只用于「找得到自己存的目标」）
    fn token_for(&self, url: &str, referer: &str) -> String {
        let seed = self.counter.fetch_add(1, Ordering::Relaxed) as u64;
        let mut h1: u64 = 0xcbf2_9ce4_8422_2325 ^ seed;
        let mut h2: u64 = 0x8422_2325_cbf2_9ce4;
        for byte in url.bytes().chain(b'|'.to_string().bytes()).chain(referer.bytes()) {
            h1 = (h1 ^ byte as u64).wrapping_mul(0x0000_0100_0000_01b3);
            h2 = (h2 ^ (byte as u64).rotate_left(3)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        }
        format!("{h1:016x}{h2:016x}")
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
}

/// `GET /stream/{token}`：补齐 Referer/UA + 透传 `Range`，把音频流原样转发。
///
/// 播放器会先发 `Range: bytes=0-` 探路，然后按需拖拽要 `bytes=x-y`；
/// 这里必须把 Range 透给 CDN，并把 206 / `Content-Range` / `Content-Length` 原样回给客户端，
/// 否则进度条变 Infinity、拖不动，部分播放器（AVPlayer 等）直接判「流不可播」→ 没声音。
pub(crate) async fn stream_handler(
    State(state): State<AppState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some((url, referer)) = state.proxy.get(&token) else {
        return json_error(StatusCode::NOT_FOUND, "stream token expired, 请让客户端重新解析");
    };

    // 客户端没带 Range 就只要 1 个字节：足够判断「CDN 真有这个文件」，又不会白拉 10MB
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(|| PROBE_RANGE.to_string());

    let if_range = headers
        .get(header::IF_RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let mut builder = crate::http::streaming_client()
        .get(url.as_str())
        .header(reqwest::header::REFERER, referer.as_str())
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .header(reqwest::header::RANGE, range.as_str());
    if let Some(if_range) = if_range {
        builder = builder.header(reqwest::header::IF_RANGE, if_range);
    }

    let started = std::time::Instant::now();
    let response = match tokio::time::timeout(UPSTREAM_IDLE_TIMEOUT, builder.send()).await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            log_stream(token.as_str(), 502, err.to_string().as_str(), started);
            return json_error(StatusCode::BAD_GATEWAY, &format!("上游取流失败：{err}"));
        }
        Err(_) => {
            log_stream(token.as_str(), 504, "upstream idle timeout", started);
            return json_error(StatusCode::GATEWAY_TIMEOUT, "上游取流超时");
        }
    };

    let status = response.status();
    let upstream_status = status.as_u16();
    if !status.is_success() {
        // 404 说明 CDN 根本没有这个文件（vkey 发了 purl 也没用），交给客户端重解析降档
        log_stream(token.as_str(), 502, "upstream error", started);
        return json_error(
            StatusCode::BAD_GATEWAY,
            &format!("上游返回 HTTP {upstream_status}，这条直链不可播（请重新解析）"),
        );
    }

    // 原样搬回播放器要用的头
    let mut headers_out = HeaderMap::new();
    for name in [
        reqwest::header::CONTENT_TYPE,
        reqwest::header::CONTENT_LENGTH,
        reqwest::header::CONTENT_RANGE,
        reqwest::header::ACCEPT_RANGES,
        reqwest::header::LAST_MODIFIED,
        reqwest::header::ETAG,
    ] {
        if let Some(value) = response.headers().get(&name) {
            if let (Ok(name), Ok(value)) = (
                axum::http::HeaderName::from_bytes(name.as_str().as_bytes()),
                axum::http::HeaderValue::from_bytes(value.as_bytes()),
            ) {
                headers_out.insert(name, value);
            }
        }
    }
    headers_out.insert(reqwest::header::ACCEPT_RANGES, "bytes".parse().expect("static header"));
    if !headers_out.contains_key(reqwest::header::CONTENT_TYPE) {
        let guessed = guess_content_type(url.as_str());
        headers_out.insert(
            reqwest::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static(guessed),
        );
    }

    log_stream(token.as_str(), upstream_status, "streamed", started);
    (headers_out, Body::from_stream(response.bytes_stream())).into_response()
}

/// 上游没给 Content-Type 时按扩展名兜一个（播放器按 MIME 选 demuxer，错了会静音）
pub(crate) fn guess_content_type(url: &str) -> &'static str {
    let path = url.split('?').next().unwrap_or(url).to_ascii_lowercase();
    if path.ends_with(".flac") {
        "audio/flac"
    } else if path.ends_with(".m4a") || path.ends_with(".mp4") || path.ends_with(".aac") {
        "audio/mp4"
    } else if path.ends_with(".ogg") || path.ends_with(".opus") {
        "audio/ogg"
    } else if path.ends_with(".wav") {
        "audio/wav"
    } else {
        "audio/mpeg"
    }
}

/// /stream 也要有访问日志：只显示状态和 token 前缀，不打完整 CDN 地址
fn log_stream(token: &str, status: u16, note: &str, started: std::time::Instant) {
    tracing::info!(
        target: crate::logfmt::ACCESS_TARGET,
        status = status,
        source = "stream",
        id = %&token[..token.len().min(8)],
        quality = note,
        elapsed_ms = started.elapsed().as_millis() as u64,
    );
}

/// 错误一律回 JSON：播放器不会把纯文本当音频去解码（以前回纯文本会「叮」一声或者干脆静音）
fn json_error(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({ "code": 1, "msg": message }).to_string();
    (status, [(reqwest::header::CONTENT_TYPE, "application/json")], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_is_stable_for_the_same_target() {
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        let first = store.link("https://cdn.example/a.mp3".to_string(), "https://y.qq.com/".to_string());
        let second = store.link("https://cdn.example/a.mp3".to_string(), "https://y.qq.com/".to_string());
        assert_eq!(first, second, "同一个目标必须给同一个短链");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn different_targets_get_different_tokens() {
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        let a = store.link("https://cdn.example/a.mp3".to_string(), "r".to_string());
        let b = store.link("https://cdn.example/b.mp3".to_string(), "r".to_string());
        assert_ne!(a, b);
    }

    #[test]
    fn link_uses_the_base_from_the_incoming_request() {
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        let link = store.link_with_base(
            "http://192.168.1.9:8080",
            "https://cdn.example/a.mp3".to_string(),
            "https://y.qq.com/".to_string(),
        );
        assert!(link.starts_with("http://192.168.1.9:8080/stream/"), "实际：{link}");
        let token = link.rsplit('/').next().expect("token");
        assert!(store.get(token).is_some(), "按请求 Host 拼的短链也要能取到目标");
    }

    #[test]
    fn token_lives_at_least_as_long_as_the_cache() {
        // 缓存 30min，token 不能更短，否则命中缓存时会拿到过期短链
        assert!(TOKEN_TTL_SECS >= 1_800, "TOKEN_TTL_SECS 必须 >= L1_TTL");
    }

    #[test]
    fn unknown_token_returns_none() {
        let store = ProxyStore::new("http://127.0.0.1:8080".to_string());
        assert!(store.get("nope").is_none());
    }

    #[test]
    fn guesses_the_content_type_from_the_extension() {
        assert_eq!(guess_content_type("https://a/x.flac?vkey=1"), "audio/flac");
        assert_eq!(guess_content_type("https://a/x.mp3"), "audio/mpeg");
        assert_eq!(guess_content_type("https://a/x.m4a"), "audio/mp4");
    }
}
