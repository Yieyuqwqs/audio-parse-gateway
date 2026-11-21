//! 音质档位与降级阶梯：`128k < 320k < flac < flac24bit`。
//!
//! 「高音质」要能回答两个问题，光把 flac 写进路由是不够的：
//!
//! 1. **这路上游最高能给到哪一档** —— [`crate::upstream::Upstream::max_quality`]，
//!    比如网易云不带登录 cookie 时实测最高只到 320k（`br=999000` 会被静默降成 mp3）；
//! 2. **请求的档位够不着时降到哪一档** —— [`downgrade`] 沿阶梯往下走，而不是直接 502。
//!
//! 真实档位由上游返回（`level` / `br` / `type`）决定，网关只负责把「请求档位」和
//! 「实际档位」都说清楚，不假装拿到了 flac。

use crate::AppError;

/// 从低到高排列的音质阶梯，下标即档位。
pub(crate) const LADDER: [&str; 4] = ["128k", "320k", "flac", "flac24bit"];

/// 最高档（无损 24bit）
pub(crate) const TOP: &str = LADDER[3];

/// 档位（越大越好）；不认识的音质返回 `None`。
pub(crate) fn rank(quality: &str) -> Option<usize> {
    LADDER
        .iter()
        .position(|candidate| candidate.eq_ignore_ascii_case(quality))
}

/// 请求档位是否在上限之内（`quality <= ceiling`）。
pub(crate) fn within(quality: &str, ceiling: &str) -> bool {
    match (rank(quality), rank(ceiling)) {
        (Some(requested), Some(limit)) => requested <= limit,
        _ => false,
    }
}

/// 降一档；已经是最低档时返回 `None`。
pub(crate) fn downgrade(quality: &str) -> Option<&'static str> {
    let rank = rank(quality)?;
    if rank == 0 {
        None
    } else {
        Some(LADDER[rank - 1])
    }
}

/// 归一化 `quality` 查询参数：大小写不敏感、空串走默认 `320k`。
pub(crate) fn normalize(raw: Option<&str>) -> Result<&'static str, AppError> {
    let value = raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(crate::DEFAULT_QUALITY);

    LADDER
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(value))
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "`quality` must be one of {LADDER:?}, got `{value}`"
            ))
        })
}

/// 从网易云/节点返回的 `br` 猜实际档位（`level` 字段缺失时的兜底）。
pub(crate) fn from_bitrate(br: u64) -> &'static str {
    match br {
        0 => LADDER[1],
        1_000_000.. => LADDER[3],
        900_000..=999_999 => LADDER[2],
        300_000..=899_999 => LADDER[1],
        _ => LADDER[0],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_is_ordered_and_complete() {
        assert_eq!(LADDER, ["128k", "320k", "flac", "flac24bit"]);
        assert_eq!(rank("128k"), Some(0));
        assert_eq!(rank("FLAC"), Some(2));
        assert_eq!(rank("48k"), None);
    }

    #[test]
    fn downgrade_walks_down_and_stops() {
        assert_eq!(downgrade("flac24bit"), Some("flac"));
        assert_eq!(downgrade("flac"), Some("320k"));
        assert_eq!(downgrade("320k"), Some("128k"));
        assert_eq!(downgrade("128k"), None);
    }

    #[test]
    fn ceiling_check_uses_ranking_not_strings() {
        assert!(within("320k", "flac"));
        assert!(within("flac", "flac"));
        assert!(!within("flac24bit", "flac"));
        assert!(!within("flac", "320k"));
        assert!(!within("flac", "bogus"));
    }

    #[test]
    fn normalises_quality_input() {
        assert_eq!(normalize(Some(" FLAC ")).expect("flac"), "flac");
        assert_eq!(normalize(Some("")).expect("默认"), crate::DEFAULT_QUALITY);
        assert!(normalize(Some("999k")).is_err());
    }

    #[test]
    fn maps_bitrate_to_quality() {
        assert_eq!(from_bitrate(128_000), "128k");
        assert_eq!(from_bitrate(320_000), "320k");
        assert_eq!(from_bitrate(999_000), "flac");
        assert_eq!(from_bitrate(1_999_000), "flac24bit");
        // 0 是网易云「没给码率」的占位，按中间档处理，别误判成 flac
        assert_eq!(from_bitrate(0), "320k");
    }
}
