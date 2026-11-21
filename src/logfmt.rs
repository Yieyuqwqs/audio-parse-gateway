//! 极客风访问日志：一行一条、状态码上色，其它 INFO/DEBUG 噪音全部吞掉。
//!
//! 输出长这样（时间只到秒，列固定对齐）：
//!
//! ```text
//! 08:53:38 | [200] tx 002RsKKv2Rd7Ih        flac      | 780ms
//! 08:53:39 | [502] wy 347230                320k      | 512ms
//! ```
//!
//! 规则：
//! * 只有 `target: "access"` 的事件会被渲染成上面这种格式；
//! * 其余 INFO / DEBUG（启动信息、连接池、缓存命中之类）**一律不输出**；
//! * WARN / ERROR 保留一行精简版，方便排障；
//! * 2xx 绿色（`\x1b[32m`），4xx/5xx 红色（`\x1b[31m`）。

use std::fmt;
use std::fmt::Write as _;

use chrono::Local;
use tracing::{
    field::{Field, Visit},
    Event, Level, Subscriber,
};
use tracing_subscriber::{
    fmt::{format::Writer, FmtContext, FormatEvent, FormatFields},
    registry::LookupSpan,
};

/// 访问日志专用 target：只有这个 target 的事件会渲染成一行
pub(crate) const ACCESS_TARGET: &str = "access";

const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

/// 一条访问日志需要的字段
#[derive(Debug, Default)]
struct AccessFields {
    status: Option<u16>,
    source: Option<String>,
    id: Option<String>,
    quality: Option<String>,
    elapsed_ms: Option<u64>,
}

impl AccessFields {
    fn numeric(&mut self, field: &Field, value: u64) {
        match field.name() {
            "status" => self.status = Some(value as u16),
            "elapsed_ms" => self.elapsed_ms = Some(value),
            _ => {}
        }
    }

    fn text(&mut self, field: &Field, value: &str) {
        match field.name() {
            "source" => self.source = Some(value.to_owned()),
            "id" => self.id = Some(value.to_owned()),
            "quality" => self.quality = Some(value.to_owned()),
            _ => {}
        }
    }
}

impl Visit for AccessFields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.numeric(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if value >= 0 {
            self.numeric(field, value as u64);
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if value >= 0.0 {
            self.numeric(field, value as u64);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.text(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // 用 `%` 传进来的字符串会走这里：转成文本再按字段名归类
        let rendered = format!("{value:?}");
        self.text(field, rendered.trim_matches('"'));
    }
}

/// 状态码 → ANSI 颜色：2xx 绿，其余（4xx/5xx）红，没有状态码时暗色
fn color_for(status: Option<u16>) -> &'static str {
    match status {
        Some(code) if (200..300).contains(&code) => GREEN,
        Some(_) => RED,
        None => DIM,
    }
}

/// 把一条访问记录渲染成固定列宽的一行（单独抽出来，方便测试对齐）
pub(crate) fn render_line(
    time: &str,
    status: Option<u16>,
    source: &str,
    id: &str,
    quality: &str,
    elapsed_ms: u64,
) -> String {
    let color = color_for(status);
    let code = status.map(|value| value.to_string()).unwrap_or_else(|| "-".to_string());
    format!(
        "{time} | {color}[{code:>3}]{RESET} {source:<2} {id:<22}{quality:<10}| {elapsed_ms}ms"
    )
}

/// 自定义事件格式化器
pub(crate) struct AccessFormat;

impl<S, N> FormatEvent<S, N> for AccessFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let time = Local::now().format("%H:%M:%S").to_string();

        // 只认访问日志；其它 INFO/DEBUG 全部隐藏，WARN/ERROR 留一行精简版
        if event.metadata().target() != ACCESS_TARGET {
            if *event.metadata().level() <= Level::WARN {
                let mut visitor = MessageOnly::default();
                event.record(&mut visitor);
                return writeln!(
                    writer,
                    "{time} {DIM}{level}{RESET} {message}",
                    level = event.metadata().level(),
                    message = visitor.0
                );
            }
            return Ok(());
        }

        let mut fields = AccessFields::default();
        event.record(&mut fields);

        let line = render_line(
            &time,
            fields.status,
            fields.source.as_deref().unwrap_or("-"),
            fields.id.as_deref().unwrap_or("-"),
            fields.quality.as_deref().unwrap_or("-"),
            fields.elapsed_ms.unwrap_or(0),
        );
        writeln!(writer, "{line}")
    }
}

/// 只取 `message` 字段（WARN/ERROR 精简版用）
#[derive(Default)]
struct MessageOnly(String);

impl Visit for MessageOnly {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_owned();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_aligned_geoek_line() {
        let line = render_line("08:53:38", Some(200), "tx", "002RsKKv2Rd7Ih", "flac", 780);
        assert_eq!(
            line,
            format!("08:53:38 | {GREEN}[200]{RESET} tx 002RsKKv2Rd7Ih        flac      | 780ms")
        );
        // 掐掉 ANSI 之后应该和需求里给的样例完全一致
        let plain = line.replace(GREEN, "").replace(RESET, "");
        assert_eq!(plain, "08:53:38 | [200] tx 002RsKKv2Rd7Ih        flac      | 780ms");
    }

    #[test]
    fn colors_by_status_class() {
        assert!(render_line("00:00:00", Some(200), "tx", "id", "flac", 1).contains(GREEN));
        assert!(render_line("00:00:00", Some(206), "tx", "id", "flac", 1).contains(GREEN));
        assert!(render_line("00:00:00", Some(404), "tx", "id", "flac", 1).contains(RED));
        assert!(render_line("00:00:00", Some(502), "tx", "id", "flac", 1).contains(RED));
    }

    #[test]
    fn explains_color_and_fields() {
        assert_eq!(color_for(Some(299)), GREEN);
        assert_eq!(color_for(Some(300)), RED);
        assert_eq!(color_for(None), DIM);

        let mut fields = AccessFields::default();
        fields.status = Some(200);
        assert_eq!(fields.status, Some(200));
    }
}
