//! QQ 音乐（`tx`）直连适配器。
//!
//! 走 `u.y.qq.com/cgi-bin/musicu.fcg` 的 `vkey.GetVkeyServer / CgiGetVkey`：
//! 传 songmid + 一串目标文件名，拿回 `purl`（带 vkey 的相对路径）和 `sip`（CDN 前缀），拼起来就是直链。
//!
//! 一次请求把**从请求档位到 128k 的所有文件名**都带上，取能拿到的最好一档 —— 这样账号没会员时
//! 自动落到 128k（响应里 `downgraded=true`），而不是整个请求 502。
//!
//! 2026-11-21 实测（账号已登录但**非绿钻**）：
//!
//! | 文件 | result | 含义 |
//! | --- | --- | --- |
//! | `M500{mid}.mp3`（128k） | `0` + purl | 免费曲可播，实测 `audio/mpeg` 3.0MB |
//! | `M800{mid}.mp3`（320k） | `101404` | 需要绿钻 |
//! | `F000{mid}.flac`（无损） | `101404` | 需要豪华绿钻 |
//! | `RS01{mid}.flac`（Hi-Res） | `101404` | 需要豪华绿钻 |
//!
//! 另外：`qqmusic_key` 这枚老 key 已经不被认；现在有效的登录 cookie 是 **`qm_keyst` + `uin`**，
//! 两者缺一，接口就当游客处理（320k/无损一律 `101404`）。

use std::time::Duration;

use crate::{
    upstream::{TrackRequest, Upstream, UpstreamError},
    SourceUrl,
};

/// 统一的 vkey 入口
const ENDPOINT: &str = "https://u.y.qq.com/cgi-bin/musicu.fcg";
/// vkey 保守有效期（秒）：比真实 vkey 短，宁可多换一次，也别把死链塞给播放器
const VKEY_TTL_SECS: u64 = 1_800;
/// 音质 → (前缀, 扩展名)。QQ 的文件名直接把档位写死在里面。
const FILE_PREFIXES: [(&str, &str, &str); 4] = [
    ("flac24bit", "RS01", "flac"),
    ("flac", "F000", "flac"),
    ("320k", "M800", "mp3"),
    ("128k", "M500", "mp3"),
];

/// QQ 音乐直连适配器。
#[derive(Debug, Clone)]
pub struct QqUpstream {
    name: String,
    cookie: Option<String>,
    uin: Option<String>,
    guid: String,
    timeout: Duration,
}

impl QqUpstream {
    pub fn new(timeout: Duration) -> Self {
        Self {
            name: "qq".to_string(),
            cookie: None,
            uin: None,
            // 设备标识，QQ 只用来区分 vkey 会话，固定值即可
            guid: "10000".to_string(),
            timeout,
        }
    }

    /// 登录态：`TX_COOKIE`（整段 cookie，含 `qm_keyst=...`）+ `TX_UIN`（QQ 号）。
    pub fn with_credentials(mut self, cookie: Option<String>, uin: Option<String>) -> Self {
        self.cookie = cookie.filter(|value| !value.trim().is_empty());
        self.uin = uin
            .map(|value| value.trim().trim_start_matches("o0").to_string())
            .filter(|value| !value.is_empty());
        self
    }

    fn filename_for(quality: &str, mid: &str) -> Option<String> {
        FILE_PREFIXES
            .iter()
            .find(|(candidate, _, _)| *candidate == quality)
            .map(|(_, prefix, ext)| format!("{prefix}{mid}.{ext}"))
    }

    /// 从请求档位往下的所有候选文件名（好 → 差），一次请求全带上。
    ///
    /// 用阶梯下标往回走，而不是拿着调用方传进来的 `&str` 游标 ——
    /// 后者活不过这次调用，返回 `Vec<(&'static str, String)>` 会报生命周期错误。
    fn candidates(quality: &str, mid: &str) -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        let Some(mut index) = crate::quality::rank(quality) else {
            return out;
        };
        loop {
            let candidate = crate::quality::LADDER[index];
            if let Some(filename) = Self::filename_for(candidate, mid) {
                out.push((candidate, filename));
            }
            if index == 0 {
                break;
            }
            index -= 1;
        }
        out
    }

    fn credentials_complete(&self) -> bool {
        self.cookie.is_some() && self.uin.is_some()
    }
}

#[async_trait::async_trait]
impl Upstream for QqUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    fn supports(&self, source: &str) -> bool {
        source == "tx"
    }

    /// 没登录只敢报 128k；登录了就按会员上限报（实际能不能拿由接口决定，拿到什么就上报什么）。
    fn max_quality(&self) -> &'static str {
        if self.credentials_complete() {
            crate::quality::TOP
        } else {
            crate::quality::LADDER[0]
        }
    }

    async fn resolve(&self, req: &TrackRequest) -> Result<SourceUrl, UpstreamError> {
        let mid = req.music_id.trim();
        // songmid 是十几位大小写字母数字；kw 的纯数字 rid 打过来会被这里拦下
        if mid.is_empty() || mid.len() > 64 || !mid.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(UpstreamError::NoUrl {
                upstream: self.name.clone(),
                detail: format!("`{mid}` is not a valid qq songmid"),
            });
        }

        let candidates = Self::candidates(req.quality, mid);
        if candidates.is_empty() {
            return Err(UpstreamError::Unsupported {
                upstream: self.name.clone(),
                source_key: format!("{}@{}", req.source, req.quality),
            });
        }

        let uin = self.uin.clone().unwrap_or_else(|| "0".to_string());
        let filenames: Vec<&str> = candidates.iter().map(|(_, filename)| filename.as_str()).collect();
        let payload = serde_json::json!({
            "req_0": {
                "module": "vkey.GetVkeyServer",
                "method": "CgiGetVkey",
                "param": {
                    "guid": self.guid,
                    "songmid": [mid],
                    "songtype": [0],
                    "uin": uin,
                    "loginflag": 1,
                    "platform": "20",
                    "filename": filenames,
                }
            },
            "comm": { "uin": uin, "format": "json", "ct": 24, "cv": 0 }
        })
        .to_string();

        let attempt = async {
            let mut builder = crate::http::client()
                .post(ENDPOINT)
                .header(reqwest::header::REFERER, "https://y.qq.com/")
                .header(reqwest::header::ORIGIN, "https://y.qq.com")
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(payload.clone());
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
            parse_vkey_body(&self.name, &candidates, &body)
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

/// 解析 vkey 响应：在候选里挑「有 purl 的最高一档」，`sip + purl` 就是直链。
pub(crate) fn parse_vkey_body(
    upstream: &str,
    candidates: &[(&'static str, String)],
    body: &str,
) -> Result<SourceUrl, UpstreamError> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|err| UpstreamError::Decode {
            upstream: upstream.to_string(),
            detail: err.to_string(),
        })?;

    let request = value.get("req_0").ok_or_else(|| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: "payload has no `req_0`".to_string(),
    })?;

    let code = request.get("code").and_then(serde_json::Value::as_i64).unwrap_or(0);
    if code != 0 {
        return Err(UpstreamError::NoUrl {
            upstream: upstream.to_string(),
            detail: format!("qq module answered code {code}"),
        });
    }

    let data = request.get("data").ok_or_else(|| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: "payload has no `req_0.data`".to_string(),
    })?;

    let sip = data
        .get("sip")
        .and_then(serde_json::Value::as_array)
        .and_then(|hosts| hosts.first())
        .and_then(serde_json::Value::as_str)
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| UpstreamError::Decode {
            upstream: upstream.to_string(),
            detail: "payload has no `data.sip`".to_string(),
        })?;

    let infos: &[serde_json::Value] = data
        .get("midurlinfo")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    // 候选按「好 → 差」排好序，第一个拿到 purl 的就是实际档位
    for (quality, filename) in candidates {
        let Some(info) = infos.iter().find(|info| {
            info.get("filename").and_then(serde_json::Value::as_str) == Some(filename.as_str())
        }) else {
            continue;
        };
        let purl = info
            .get("purl")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if purl.is_empty() {
            continue;
        }
        return Ok(SourceUrl {
            url: format!("{sip}{purl}"),
            provider: upstream.to_string(),
            quality: (*quality).to_string(),
            expires_at: crate::now_unix() + VKEY_TTL_SECS,
        });
    }

    // 一个都没拿到：把各档的 result code 汇总出来，方便判断是「没会员」还是「歌没了」
    let codes = candidates
        .iter()
        .map(|(quality, filename)| {
            let result = infos
                .iter()
                .find(|info| info.get("filename").and_then(serde_json::Value::as_str) == Some(filename.as_str()))
                .and_then(|info| info.get("result"))
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(-1);
            format!("{quality}={result}")
        })
        .collect::<Vec<_>>()
        .join(" ");

    let hint = if codes.contains("101404") {
        "当前账号无权拿这些档位（320k 要绿钻，flac 要豪华绿钻）"
    } else if codes.contains("104003") {
        "付费曲目，需要购买/会员账号"
    } else {
        "接口没有下发 vkey"
    };

    Err(UpstreamError::NoUrl {
        upstream: upstream.to_string(),
        detail: format!("qq 没有返回直链（{codes}）：{hint}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MID: &str = "002lzYCa14usQ5";

    #[test]
    fn maps_quality_to_vkey_filename() {
        assert_eq!(QqUpstream::filename_for("128k", MID), Some(format!("M500{MID}.mp3")));
        assert_eq!(QqUpstream::filename_for("320k", MID), Some(format!("M800{MID}.mp3")));
        assert_eq!(QqUpstream::filename_for("flac", MID), Some(format!("F000{MID}.flac")));
        assert_eq!(QqUpstream::filename_for("flac24bit", MID), Some(format!("RS01{MID}.flac")));
        assert_eq!(QqUpstream::filename_for("48k", MID), None);
    }

    #[test]
    fn candidates_go_from_best_to_worst() {
        let names: Vec<String> = QqUpstream::candidates("flac24bit", MID)
            .into_iter()
            .map(|(_, filename)| filename)
            .collect();
        assert_eq!(
            names,
            vec![
                format!("RS01{MID}.flac"),
                format!("F000{MID}.flac"),
                format!("M800{MID}.mp3"),
                format!("M500{MID}.mp3"),
            ]
        );

        let names: Vec<String> = QqUpstream::candidates("320k", MID)
            .into_iter()
            .map(|(_, filename)| filename)
            .collect();
        assert_eq!(names, vec![format!("M800{MID}.mp3"), format!("M500{MID}.mp3")]);
    }

    #[test]
    fn only_supports_tx() {
        let qq = QqUpstream::new(Duration::from_millis(800));
        assert!(qq.supports("tx"));
        assert!(!qq.supports("wy"));
        assert!(!qq.supports("kw"));
    }

    #[test]
    fn ceiling_depends_on_complete_credentials() {
        let guest = QqUpstream::new(Duration::from_millis(800));
        assert_eq!(guest.max_quality(), "128k", "游客只有 128k");

        let key_only = QqUpstream::new(Duration::from_millis(800))
            .with_credentials(Some("qm_keyst=abc".to_string()), None);
        assert_eq!(key_only.max_quality(), "128k", "光有 key 不算登录");

        let logged_in = QqUpstream::new(Duration::from_millis(800)).with_credentials(
            Some("qm_keyst=abc".to_string()),
            Some("o02310476390".to_string()),
        );
        assert_eq!(logged_in.max_quality(), crate::quality::TOP);
    }

    /// 伪造一个 vkey 响应；entries 是 (文件名, result, purl)
    fn payload(entries: Vec<(String, i64, String)>) -> String {
        let midurlinfo = entries
            .into_iter()
            .map(|(filename, result, purl)| {
                serde_json::json!({ "filename": filename, "result": result, "purl": purl })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "req_0": {
                "code": 0,
                "data": { "sip": ["http://aqqmusic.tc.qq.com/"], "midurlinfo": midurlinfo }
            }
        })
        .to_string()
    }

    #[test]
    fn picks_the_best_available_quality() {
        // 非绿钻账号：只有 128k 有 purl
        let body = payload(vec![
            (format!("RS01{MID}.flac"), 101_404, String::new()),
            (format!("F000{MID}.flac"), 101_404, String::new()),
            (format!("M800{MID}.mp3"), 101_404, String::new()),
            (format!("M500{MID}.mp3"), 0, "M500x.mp3?vkey=AAA".to_string()),
        ]);
        let candidates = QqUpstream::candidates("flac24bit", MID);
        let parsed = parse_vkey_body("qq", &candidates, &body).expect("至少 128k 能播");
        assert_eq!(parsed.quality, "128k", "拿不到无损就要如实降档");
        assert_eq!(parsed.url, "http://aqqmusic.tc.qq.com/M500x.mp3?vkey=AAA");
        assert!(parsed.is_fresh());
    }

    #[test]
    fn prefers_lossless_when_the_account_can_get_it() {
        let body = payload(vec![
            (format!("F000{MID}.flac"), 0, "F000x.flac?vkey=BBB".to_string()),
            (format!("M500{MID}.mp3"), 0, "M500x.mp3?vkey=CCC".to_string()),
        ]);
        let candidates = QqUpstream::candidates("flac", MID);
        let parsed = parse_vkey_body("qq", &candidates, &body).expect("无损应该优先");
        assert_eq!(parsed.quality, "flac");
        assert_eq!(parsed.url, "http://aqqmusic.tc.qq.com/F000x.flac?vkey=BBB");
    }

    #[test]
    fn reports_codes_when_nothing_is_playable() {
        let body = payload(vec![
            (format!("F000{MID}.flac"), 101_404, String::new()),
            (format!("M500{MID}.mp3"), 104_003, String::new()),
        ]);
        let candidates = QqUpstream::candidates("flac", MID);
        let err = parse_vkey_body("qq", &candidates, &body).unwrap_err();
        match err {
            UpstreamError::NoUrl { detail, .. } => {
                assert!(detail.contains("flac=101404"), "实际: {detail}");
                assert!(detail.contains("绿钻"));
            }
            other => panic!("expected NoUrl, got {other:?}"),
        }
    }

    #[test]
    fn refuses_module_errors_and_garbage() {
        let candidates = QqUpstream::candidates("128k", MID);
        let err = parse_vkey_body("qq", &candidates, r#"{"req_0":{"code":1000}}"#).unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));

        let err = parse_vkey_body("qq", &candidates, "not json").unwrap_err();
        assert!(matches!(err, UpstreamError::Decode { .. }));
    }

    #[tokio::test]
    async fn invalid_songmid_fails_before_any_network_call() {
        let qq = QqUpstream::new(Duration::from_millis(800));
        let err = qq
            .resolve(&TrackRequest::new("tx", "0039MnYb0qxYhV/../x", "128k"))
            .await
            .unwrap_err();
        assert!(matches!(err, UpstreamError::NoUrl { .. }));
    }
}
