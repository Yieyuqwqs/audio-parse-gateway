//! LXSP v2 协议壳（落雪音乐统一音源协议）
//!
//! 网关原有的对外接口是 LX 自定义源那套 `{code,msg,data}`（`/api/parse`、`/url/...`）。
//! 这里再加一层 **LXSP v2**：信封 `{ok,data|error}`、封闭错误码、manifest 声明能力，
//! 于是同一套解析设施既能喂老脚本，也能喂新客户端 —— 协议是薄壳，缓存/竞速/单飞一行没动。
//!
//! 路由（见 `main` 里的 `Router`）：
//! - `GET /manifest`                能力声明（含 `endpoints` 覆盖，把 `/url` 指到 `/lxsp/url`）
//! - `GET /lxsp/url/{source}/{id}`  取直链（LXSP 信封）
//! - `GET /healthz`                 LXSP 信封版健康检查
//!
//! 规范见 App 仓 `docs/LXSP-v2.md` §5。

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;

use crate::quality;
use crate::{normalize_source, validate_music_id, AppError, AppState};

/// 提供方标识（同一提供方的多个镜像必须一致）
const PROVIDER_ID: &str = "yieyu-gateway";

/// 音源键 → 展示名（顺序即 manifest 里的展示顺序）
const SOURCE_NAMES: [(&str, &str); 5] = [
    ("kw", "酷我音乐"),
    ("kg", "酷狗音乐"),
    ("tx", "QQ音乐"),
    ("wy", "网易云音乐"),
    ("mg", "咪咕音乐"),
];

/// `/lxsp/url` 的查询参数。`name`/`singer` 目前只做透传占位（上游纠偏在适配层里做）。
#[derive(Debug, Deserialize)]
pub(crate) struct UrlQuery {
    quality: Option<String>,
    #[allow(dead_code)]
    name: Option<String>,
    #[allow(dead_code)]
    singer: Option<String>,
    #[allow(dead_code)]
    fallback: Option<u8>,
}

/// 把客户端音质归一到本网关的阶梯（`128k < 320k < flac < flac24bit`）。
///
/// LXSP 的词汇更大（master / atmos / wav / 192k …），出口映射一次即可，别让每个调用点各写一套。
fn map_quality(raw: Option<&str>) -> Result<&'static str, AppError> {
    let value = raw.map(str::trim).filter(|v| !v.is_empty()).unwrap_or("auto");
    // 先落一个 String，否则 match 会借用临时值（E0716）
    let lowered = value.to_ascii_lowercase();
    let canonical: &str = match lowered.as_str() {
        "auto" => return quality::normalize(None),
        "master" | "atmos" | "atmos_plus" | "hires" => "flac24bit",
        "wav" | "ape" => "flac",
        "192k" => "320k",
        other => other,
    };
    quality::normalize(Some(canonical))
}

/// 网关错误 → LXSP 错误码 + 是否可重试（客户端行为只由这两个字段驱动）
fn error_code(error: &AppError) -> (&'static str, bool) {
    match error {
        AppError::BadRequest(_) => ("bad_request", false),
        AppError::Timeout(_) => ("timeout", true),
        AppError::Overloaded => ("rate_limited", true),
        AppError::Http(_) => ("network", true),
        AppError::Upstream(_) => ("upstream_error", true),
    }
}

/// LXSP 失败响应：业务失败一律 HTTP 200 + `ok:false`
fn error_response(error: &AppError) -> Response {
    let (code, retryable) = error_code(error);
    (
        StatusCode::OK,
        Json(json!({
            "ok": false,
            "error": { "code": code, "message": error.to_string(), "retryable": retryable },
        })),
    )
        .into_response()
}

/// 某音源的上限音质：取所有支持它的适配器里最高的那一档
fn ceiling_for(state: &AppState, source: &str) -> Option<&'static str> {
    let mut ceiling: Option<&'static str> = None;
    for upstream in state.upstreams.iter() {
        if !upstream.supports(source) {
            continue;
        }
        let max = upstream.max_quality();
        ceiling = Some(match ceiling {
            None => max,
            Some(current) if quality::rank(max) > quality::rank(current) => max,
            Some(current) => current,
        });
    }
    ceiling
}

/// `GET /manifest`
pub(crate) async fn manifest(State(state): State<AppState>) -> impl IntoResponse {
    let mut sources = Vec::new();
    for (key, name) in SOURCE_NAMES {
        let Some(ceiling) = ceiling_for(&state, key) else { continue };
        let top = quality::rank(ceiling).unwrap_or(0);
        let qualities: Vec<&str> = quality::LADDER.iter().copied().take(top + 1).collect();
        sources.push(json!({
            "key": key,
            "name": name,
            "qualities": qualities,
            "capabilities": ["url"],
        }));
    }

    Json(json!({
        "ok": true,
        "data": {
            "lxsp": "2.0",
            "provider": {
                "id": PROVIDER_ID,
                "name": "Yieyu 解析网关",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "auth": { "required": false },
            "capabilities": ["url"],
            "features": { "crossSource": true, "multiSource": true, "streamProxy": true },
            "limits": { "timeoutMs": 30_000, "urlTtlS": 600 },
            "endpoints": { "url": "/lxsp/url", "health": "/healthz" },
            "sources": sources,
        }
    }))
}

/// `GET /lxsp/url/{source}/{id}`
pub(crate) async fn url(
    State(state): State<AppState>,
    Path((source, music_id)): Path<(String, String)>,
    Query(query): Query<UrlQuery>,
) -> Response {
    let requested = match map_quality(query.quality.as_deref()) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };
    let source = match normalize_source(Some(source.as_str())) {
        Ok(value) => value,
        Err(error) => return error_response(&error),
    };
    let music_id = match validate_music_id(Some(music_id.as_str())) {
        Ok(value) => value.to_string(),
        Err(error) => return error_response(&error),
    };

    match state.resolve(source, &music_id, requested).await {
        Ok(resolved) => {
            let expires_at = if resolved.url.expires_at == 0 {
                json!(null)
            } else {
                json!(resolved.url.expires_at)
            };
            Json(json!({
                "ok": true,
                "data": {
                    "url": resolved.url.url,
                    "source": resolved.source,
                    "id": music_id,
                    "quality": resolved.url.quality,
                    "requestedQuality": resolved.requested,
                    "downgraded": resolved.downgraded(),
                    "expiresAt": expires_at,
                    "via": {
                        "upstream": resolved.url.provider,
                        "cache": resolved.layer,
                        "coalesced": resolved.coalesced,
                    },
                }
            }))
            .into_response()
        }
        Err(error) => error_response(&error),
    }
}

/// `GET /healthz`
pub(crate) async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let mut sources = serde_json::Map::new();
    for (key, _) in SOURCE_NAMES {
        let healthy = ceiling_for(&state, key).is_some();
        sources.insert(key.to_string(), json!(if healthy { "ok" } else { "down" }));
    }
    Json(json!({
        "ok": true,
        "data": {
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "sources": sources,
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_lxsp_qualities_onto_the_gateway_ladder() {
        assert_eq!(map_quality(Some("auto")).expect("auto"), crate::DEFAULT_QUALITY);
        assert_eq!(map_quality(None).expect("none"), crate::DEFAULT_QUALITY);
        assert_eq!(map_quality(Some("flac")).expect("flac"), "flac");
        assert_eq!(map_quality(Some("master")).expect("master"), "flac24bit");
        assert_eq!(map_quality(Some("hires")).expect("hires"), "flac24bit");
        assert_eq!(map_quality(Some("wav")).expect("wav"), "flac");
        assert_eq!(map_quality(Some("192k")).expect("192k"), "320k");
        assert!(map_quality(Some("nonsense")).is_err());
    }

    #[test]
    fn maps_errors_onto_lxsp_codes() {
        assert_eq!(error_code(&AppError::BadRequest("x".into())).0, "bad_request");
        assert_eq!(error_code(&AppError::Timeout(std::time::Duration::from_secs(1))).0, "timeout");
        assert_eq!(error_code(&AppError::Overloaded).0, "rate_limited");
        assert_eq!(error_code(&AppError::Upstream("x".into())).0, "upstream_error");
        assert!(error_code(&AppError::Timeout(std::time::Duration::from_secs(1))).1, "超时可重试");
        assert!(!error_code(&AppError::BadRequest("x".into())).1, "参数错不重试");
    }

    #[test]
    fn ladder_slice_covers_every_supported_ceiling() {
        let all: Vec<&str> = quality::LADDER.iter().copied().collect();
        assert_eq!(all, vec!["128k", "320k", "flac", "flac24bit"]);
        assert_eq!(&all[..2], &["128k", "320k"]);
    }
}
