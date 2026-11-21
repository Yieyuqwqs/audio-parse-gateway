//! 通用「解析节点」适配器。
//!
//! 自建节点、第三方 lx-api 之类的服务都长这样：给它 `source + id + quality`，
//! 它返回一条直链。协议细节各家不同，所以这里做了两件兼容：
//!
//! 1. **请求侧**：路径支持模板 `{source}` / `{id}` / `{quality}`
//!    （不写模板就走 `?source=&id=&quality=` 查询参数，和上一版契约完全兼容）；
//! 2. **响应侧**：`{url,...}` / `{code:0,data:{url}}` / `{data:{url}}` /
//!    `{data:[{url}]}` / `{data:{url,expires_at|expi}}` 统统认。

use std::time::Duration;

use crate::{
    upstream::{TrackRequest, Upstream, UpstreamError},
    SourceUrl,
};

/// 一个 HTTP 解析节点。
#[derive(Debug, Clone)]
pub struct NodeUpstream {
    name: String,
    base: String,
    path: String,
    token: Option<String>,
    timeout: Duration,
}

impl NodeUpstream {
    /// `base` 形如 `https://node-a.example.com`（末尾斜杠可有可无）。
    pub fn new(name: impl Into<String>, base: impl Into<String>, timeout: Duration) -> Self {
        Self {
            name: name.into(),
            base: base.into(),
            path: "/api/url".to_string(),
            token: None,
            timeout,
        }
    }

    /// 换路径模板，例如 `/url/{source}/{id}`（`UPSTREAM_A_PATH`）。
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// 节点要求鉴权时带上：`Authorization: Bearer <token>`。
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token.filter(|value| !value.trim().is_empty());
        self
    }

    /// 拼出真正要打的 URL，以及是否还需要补查询参数。
    fn endpoint(&self, req: &TrackRequest) -> (String, bool) {
        let use_query = !self.path.contains('{');
        let path = if use_query {
            self.path.clone()
        } else {
            self.path
                .replace("{source}", req.source)
                .replace("{id}", &req.music_id)
                .replace("{quality}", req.quality)
        };
        let path = if path.starts_with('/') { path } else { format!("/{path}") };
        let base = self.base.trim_end_matches('/');
        (format!("{base}{path}"), use_query)
    }

    /// 供测试直接构造（避免每个用例都写一遍完整配置）
    #[cfg(test)]
    fn for_test(path: &str) -> Self {
        Self::new("node-test", "https://node.example.com/", Duration::from_millis(800))
            .with_path(path)
    }
}

#[async_trait::async_trait]
impl Upstream for NodeUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports(&self, _source: &str) -> bool {
        // 节点自己决定支持哪些源，网关不替它猜
        true
    }

    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
        let (endpoint, use_query) = self.endpoint(req);

        // 超时要包住「发送 + 读 body」：只包 send() 的话，慢 body 能绕过 SLO
        let attempt = async {
            let mut builder = crate::http::client().get(endpoint.as_str());
            if use_query {
                builder = builder.query(&[
                    ("source", req.source),
                    ("id", req.music_id.as_str()),
                    ("quality", req.quality),
                ]);
            }
            if let Some(token) = self.token.as_deref() {
                builder = builder.bearer_auth(token);
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
            parse_node_body(&self.name, &body)
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

/// 把节点的响应体解析成统一的 [`SourceUrl`]。
///
/// 兼容多种历史/第三方返回形状，任何不认识的结构都归到 `Decode` / `NoUrl`，
/// 让竞速层继续等其它上游。
pub(crate) fn parse_node_body(upstream: &str, body: &str) -> Result<SourceUrl, UpstreamError> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|err| UpstreamError::Decode {
            upstream: upstream.to_string(),
            detail: err.to_string(),
        })?;

    // LX 风格：`{code: 0, msg: "success", data: ...}`，code != 0 直接判失败
    if value.get("url").and_then(serde_json::Value::as_str).is_none() {
        if let Some(code) = value.get("code").and_then(serde_json::Value::as_i64) {
            if code != 0 {
                let msg = value
                    .get("msg")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("no message");
                return Err(UpstreamError::NoUrl {
                    upstream: upstream.to_string(),
                    detail: format!("node answered code {code}: {msg}"),
                });
            }
        }
    }

    // 定位真正装 url 的那个对象：本体 / data / data[0]
    let node = if value.get("url").is_some() {
        Some(&value)
    } else {
        match value.get("data") {
            Some(serde_json::Value::Array(items)) => items.first(),
            Some(data @ serde_json::Value::Object(_)) => Some(data),
            _ => None,
        }
    };
    let node = node.ok_or_else(|| UpstreamError::NoUrl {
        upstream: upstream.to_string(),
        detail: "payload has neither `url` nor `data.url`".to_string(),
    })?;

    let url = node
        .get("url")
        .and_then(serde_json::Value::as_str)
        .filter(|url| !url.trim().is_empty())
        .ok_or_else(|| UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: "`url` is null or empty (节点没拿到版权/直链)".to_string(),
        })?;

    let provider = node
        .get("provider")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(upstream);

    let quality = node
        .get("quality")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");

    // 直链有效期：自家契约叫 expires_at，多数第三方节点叫 expi / expires
    let expires_at = node
        .get("expires_at")
        .or_else(|| node.get("expi"))
        .or_else(|| node.get("expires"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    Ok(SourceUrl {
        url: url.to_string(),
        provider: provider.to_string(),
        quality: quality.to_string(),
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> TrackRequest {
        TrackRequest::new("kw", "123456", "320k")
    }

    #[test]
    fn endpoint_defaults_to_query_params() {
        let node = NodeUpstream::for_test("/api/url");
        let (url, use_query) = node.endpoint(&req());
        assert_eq!(url, "https://node.example.com/api/url");
        assert!(use_query, "没有 {placeholders} 时必须走查询参数");
    }

    #[test]
    fn endpoint_expands_path_template() {
        let node = NodeUpstream::for_test("/url/{source}/{id}");
        let (url, use_query) = node.endpoint(&req());
        assert_eq!(url, "https://node.example.com/url/kw/123456");
        assert!(!use_query, "用了模板就不再附加查询参数");
    }

    #[test]
    fn endpoint_normalises_slashes() {
        let node = NodeUpstream::new("n", "https://node.example.com///", Duration::from_millis(10))
            .with_path("api/url");
        let (url, _) = node.endpoint(&req());
        assert_eq!(url, "https://node.example.com/api/url");
    }

    #[test]
    fn parses_flat_contract() {
        let body = r#"{"url":"https://cdn/a.flac","provider":"node-a","quality":"320k","expires_at":1999999999}"#;
        let parsed = parse_node_body("node-a", body).expect("flat payload should parse");
        assert_eq!(parsed.url, "https://cdn/a.flac");
        assert_eq!(parsed.provider, "node-a");
        assert_eq!(parsed.quality, "320k");
        assert_eq!(parsed.expires_at, 1_999_999_999);
    }

    #[test]
    fn parses_lx_wrapped_contract() {
        let body = r#"{"code":0,"msg":"success","data":{"url":"https://cdn/b.mp3","expires_at":42}}"#;
        let parsed = parse_node_body("node-b", body).expect("lx wrapped payload should parse");
        assert_eq!(parsed.url, "https://cdn/b.mp3");
        // 没写 provider 时退回上游名，方便排查是哪一路赢的
        assert_eq!(parsed.provider, "node-b");
        assert_eq!(parsed.expires_at, 42);
    }

    #[test]
    fn parses_nested_and_array_contracts() {
        let nested = r#"{"data":{"url":"https://cdn/c.mp3","quality":"flac"}}"#;
        let parsed = parse_node_body("n", nested).expect("nested payload should parse");
        assert_eq!(parsed.quality, "flac");

        let array = r#"{"data":[{"url":"https://cdn/d.mp3","expi":77}]}"#;
        let parsed = parse_node_body("n", array).expect("array payload should parse");
        assert_eq!(parsed.url, "https://cdn/d.mp3");
        assert_eq!(parsed.expires_at, 77);
    }

    #[test]
    fn parses_bare_url_contract() {
        let parsed = parse_node_body("n", r#"{"url":"https://cdn/e.mp3"}"#).expect("bare url should parse");
        assert_eq!(parsed.url, "https://cdn/e.mp3");
        assert_eq!(parsed.expires_at, 0, "没有有效期字段时按不过期处理");
    }

    #[test]
    fn rejects_non_zero_code() {
        let err = parse_node_body("n", r#"{"code":7,"msg":"quota exceeded"}"#).unwrap_err();
        match err {
            UpstreamError::NoUrl { detail, .. } => assert!(detail.contains("quota exceeded")),
            other => panic!("expected NoUrl, got {other:?}"),
        }
    }

    #[test]
    fn rejects_null_url() {
        let err = parse_node_body("n", r#"{"code":0,"data":{"url":null}}"#).unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn rejects_garbage_body() {
        let err = parse_node_body("n", "<html>502 Bad Gateway</html>").unwrap_err();
        assert!(matches!(err, UpstreamError::Decode { .. }));
    }
}
