//! 关键词搜索：零 CK 换源用（酷狗 / 酷我 / 咪咕）。
//!
//! 「VIP 曲拿不到原版」是常态，能出声的办法是拿歌名去**免费库**再找一首能播的。
//! 这里只负责「歌名 → 平台内部 id」，解析仍然走各自的 [`crate::upstream::Upstream`]。
//!
//! 2026-11-22 实测：
//! * 酷狗 `song_search_v2`：回 `FileHash` + `Privilege`（0 = 免费），只挑免费的；
//! * 酷我 `search.kuwo.cn/r.s`：**回单引号 JSON**（要先换引号），`MUSICRID` 形如 `MUSIC_154706365`；
//! * 咪咕 `search_all.do`：回 `copyrightId` + `contentId` 两段，拼成 `cid_content` 才是我们要的 id。

use std::time::Duration;

use serde_json::Value;

use crate::upstream::UpstreamError;

/// 移动端 UA（三家搜索接口都认）
const UA_WEB: &str = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 \
                      (KHTML, like Gecko) Chrome/120 Mobile Safari/537.36";

/// 最小可用的 percent-encode（不引依赖）：只保留 RFC3986 unreserved
pub(crate) fn urlencode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 3);
    for &byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

async fn fetch_json(
    upstream: &str,
    client: &reqwest::Client,
    url: &str,
    referer: Option<&str>,
    timeout: Duration,
) -> Result<String, UpstreamError> {
    let attempt = async {
        let mut builder = client.get(url).header(reqwest::header::USER_AGENT, UA_WEB);
        if let Some(referer) = referer {
            builder = builder.header(reqwest::header::REFERER, referer);
        }
        let response = builder
            .send()
            .await
            .map_err(|source| UpstreamError::Transport { upstream: upstream.to_string(), source })?;
        let status = response.status();
        if !status.is_success() {
            return Err(UpstreamError::Status { upstream: upstream.to_string(), status });
        }
        response
            .text()
            .await
            .map_err(|source| UpstreamError::Transport { upstream: upstream.to_string(), source })
    };

    match tokio::time::timeout(timeout, attempt).await {
        Ok(result) => result,
        Err(_) => Err(UpstreamError::Timeout { upstream: upstream.to_string(), after: timeout }),
    }
}

/// 酷狗：挑第一条免费曲的 hash
pub(crate) async fn kugou(
    client: &reqwest::Client,
    keyword: &str,
    timeout: Duration,
) -> Result<String, UpstreamError> {
    let url = format!(
        "https://songsearch.kugou.com/song_search_v2?keyword={}&page=1&pagesize=20&platform=WebFilter&tag=em&filter=2&iscorrection=1&privilege_filter=0",
        urlencode(keyword)
    );
    let body = fetch_json("kugou-search", client, &url, Some("https://www.kugou.com/"), timeout).await?;
    parse_kugou("kugou-search", &body)
}

/// 酷我：取第一条 `MUSIC_xxx` 的 rid
pub(crate) async fn kuwo(
    client: &reqwest::Client,
    keyword: &str,
    timeout: Duration,
) -> Result<String, UpstreamError> {
    let url = format!(
        "http://search.kuwo.cn/r.s?client=kt&all={}&pn=0&rn=8&uid=794762570&ver=kwplayer_ar_9.2.2.1&vipver=1&show_copyright_off=0&newver=1&ft=music&cluster=0&strategy=2012&encoding=utf8&rformat=json&mobi=1",
        urlencode(keyword)
    );
    let body = fetch_json("kuwo-search", client, &url, Some("https://www.kuwo.cn/"), timeout).await?;
    parse_kuwo("kuwo-search", &body)
}

/// 咪咕：拼 `copyrightId_contentId`
pub(crate) async fn migu(
    client: &reqwest::Client,
    keyword: &str,
    timeout: Duration,
) -> Result<String, UpstreamError> {
    let switch = "%7B%22song%22%3A1%2C%22singer%22%3A0%2C%22album%22%3A0%2C%22playlist%22%3A0%2C%22mv%22%3A0%7D";
    let url = format!(
        "https://app.pd.nf.migu.cn/MIGUM2.0/v1.0/content/search_all.do?ua=Android_migu&version=5.0&text={}&pageNo=1&pageSize=10&searchSwitch={}",
        urlencode(keyword),
        switch
    );
    let body = fetch_json("migu-search", client, &url, Some("https://music.migu.cn/"), timeout).await?;
    parse_migu("migu-search", &body)
}

pub(crate) fn parse_kugou(upstream: &str, body: &str) -> Result<String, UpstreamError> {
    let value: Value = serde_json::from_str(body).map_err(|err| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: err.to_string(),
    })?;
    let lists = value
        .get("data")
        .and_then(|data| data.get("lists"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for item in lists {
        let free = item.get("Privilege").and_then(Value::as_i64).unwrap_or(1) == 0;
        let hash = item.get("FileHash").and_then(Value::as_str).unwrap_or_default();
        if free && hash.len() == 32 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(hash.to_string());
        }
    }
    Err(UpstreamError::NoUrl {
        upstream: upstream.to_string(),
        detail: "酷狗没有免费曲".to_string(),
    })
}

pub(crate) fn parse_kuwo(upstream: &str, body: &str) -> Result<String, UpstreamError> {
    // 这个接口回的是单引号 JSON，先换成双引号再解析
    let normalized = body.replace('\'', "\"");
    let value: Value = serde_json::from_str(&normalized).map_err(|err| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: err.to_string(),
    })?;
    let list = value
        .get("abslist")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for item in list {
        let rid = item.get("MUSICRID").and_then(Value::as_str).unwrap_or_default();
        let digits = rid.trim_start_matches("MUSIC_");
        if !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Ok(digits.to_string());
        }
    }
    Err(UpstreamError::NoUrl {
        upstream: upstream.to_string(),
        detail: "酷我搜索没有结果".to_string(),
    })
}

pub(crate) fn parse_migu(upstream: &str, body: &str) -> Result<String, UpstreamError> {
    let value: Value = serde_json::from_str(body).map_err(|err| UpstreamError::Decode {
        upstream: upstream.to_string(),
        detail: err.to_string(),
    })?;
    let list = value
        .get("songResultData")
        .and_then(|data| data.get("result"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for item in list {
        let copyright = item.get("copyrightId").and_then(Value::as_str).unwrap_or_default();
        let content = item.get("contentId").and_then(Value::as_str).unwrap_or_default();
        if !copyright.is_empty() && !content.is_empty() {
            return Ok(format!("{copyright}_{content}"));
        }
    }
    Err(UpstreamError::NoUrl {
        upstream: upstream.to_string(),
        detail: "咪咕搜索没有结果".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_encodes_chinese() {
        assert_eq!(urlencode("迷路 王菲"), "%E8%BF%B7%E8%B7%AF%20%E7%8E%8B%E8%8F%B2");
        assert_eq!(urlencode("abc-_.~"), "abc-_.~");
    }

    #[test]
    fn picks_a_free_kugou_track() {
        let body = r#"{"data":{"lists":[
            {"FileHash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","Privilege":10},
            {"FileHash":"B3A52A7A958BF0AED0EBFBA2E9A818B7","Privilege":0}
        ]}}"#;
        assert_eq!(parse_kugou("kugou-search", body).unwrap(), "B3A52A7A958BF0AED0EBFBA2E9A818B7");

        let paid = r#"{"data":{"lists":[{"FileHash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","Privilege":10}]}}"#;
        assert!(parse_kugou("kugou-search", paid).is_err());
    }

    #[test]
    fn parses_the_single_quoted_kuwo_payload() {
        let body = "{'abslist':[{'MUSICRID':'MUSIC_154706365','SONGNAME':'x'}]}";
        assert_eq!(parse_kuwo("kuwo-search", body).unwrap(), "154706365");
        assert!(parse_kuwo("kuwo-search", "{}").is_err());
    }

    #[test]
    fn joins_the_two_migu_ids() {
        let body = r#"{"songResultData":{"result":[{"copyrightId":"64046801861","contentId":"600908000004734752"}]}}"#;
        assert_eq!(parse_migu("migu-search", body).unwrap(), "64046801861_600908000004734752");
        assert!(parse_migu("migu-search", r#"{"songResultData":{"result":[]}}"#).is_err());
    }
}
