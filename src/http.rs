//! 全局 HTTP 连接池。
//!
//! 整个进程只有**一个** `reqwest::Client`：所有上游适配器共享同一份连接池，
//! 高并发时直接复用已经握过手的 TCP/TLS 连接（这是「低延迟」最大的来源）。
//!
//! 关键参数都是按「第二次请求必须零握手」来配的：
//!
//! * `pool_max_idle_per_host(200)`：每个上游 host 最多留 200 条空闲长连接；
//! * `pool_idle_timeout(300s)`：空闲连接留久一点，别让一次闲下来就得重新握手；
//! * `tcp_keepalive(30s)`：内核定期探活，及时发现被中间设备静默掐断的死连接；
//! * `tcp_nodelay(true)`：关掉 Nagle，杜绝小包攒批带来的额外 RTT；
//! * `connect_timeout(200ms)`：连不上的上游尽早出局，把 500ms 预算让给能干活的那路；
//! * `http2`（ALPN 协商 + keep-alive ping）：上游支持 h2 就多路复用，省掉并发时的排队。

use std::time::Duration;

use once_cell::sync::Lazy;

/// 每个 host 最多保留的空闲长连接数
pub(crate) const POOL_MAX_IDLE_PER_HOST: usize = 200;
/// 空闲连接回收时间：留长一点，跨请求复用
pub(crate) const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// TCP keep-alive 探测间隔
pub(crate) const TCP_KEEPALIVE: Duration = Duration::from_secs(30);
/// 建连超时：**包含 TCP + TLS 握手**。
///
/// 实测手机网络到 `u.y.qq.com`：TCP ~65ms、TLS 累计 ~190~220ms，原来设 200ms 会卡在边界上
/// 随机 "error sending request"。给到 450ms，剩下 50ms 留给真正的接口响应；
/// 稳态靠连接池复用（这里只在冷启动/连接被回收时才走握手）。
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_millis(450);
/// reqwest 自身的兜底超时。真正的 SLO 由各适配器的 `tokio::time::timeout` 保证，
/// 这里只是防止某条路径漏包超时后把连接永远占住。
const BODY_DEADLINE: Duration = Duration::from_millis(800);

/// 进程级唯一的 HTTP 客户端。
static CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .tcp_keepalive(TCP_KEEPALIVE)
        .tcp_nodelay(true)
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(BODY_DEADLINE)
        // HTTP/2：服务端支持就 ALPN 协商到 h2，多路复用 + 空闲探活
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(true)
        .user_agent(concat!("lx-parse-gateway/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("failed to build the global reqwest client")
});

/// 取全局客户端。**不要在请求路径里 `Client::new()`**，那会把连接池弄废。
pub(crate) fn client() -> &'static reqwest::Client {
    &CLIENT
}

/// 流式转发专用客户端：**没有总超时**。
///
/// 普通客户端的 `BODY_DEADLINE` 是 800ms，对「代拉一个 3~10MB 的音频流」是致命的
/// （传到一半就被砍）。这里只保留建连超时，读 body 交给客户端自己的节奏。
static STREAM_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .tcp_keepalive(TCP_KEEPALIVE)
        .tcp_nodelay(true)
        .connect_timeout(CONNECT_TIMEOUT)
        .user_agent(concat!("lx-parse-gateway/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("failed to build the streaming client")
});

/// 取流式客户端（给 `/stream/{token}` 用）
pub(crate) fn streaming_client() -> &'static reqwest::Client {
    &STREAM_CLIENT
}

/// 启动期预热：让配置错误在启动时炸出来，而不是等第一个请求。
pub(crate) fn warm_up() {
    Lazy::force(&CLIENT);
}
