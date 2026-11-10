//! AFS 所有 gRPC 连接共用的 HTTP/2 与超时配置。
//!
//! 学习提示：这个文件不负责“建立连接”或“启动 Server”。它只把一组统一参数
//! 应用到 Tonic 提供的 builder 上，应用完以后仍由调用方执行 `connect()` 或
//! `serve()`。因此它是配置对象，而不是一层新的通信框架。

// `Duration` 是 Rust 标准库的时间长度类型，避免用裸整数表达秒或毫秒。
use std::time::Duration;

// `Endpoint` 是 Tonic 客户端连接的 builder；`Server` 是服务端 builder。
// builder 的方法通常消费 `self` 并返回修改后的新值，所以后面会连续链式调用。
use tonic::transport::{Endpoint, Server, server::TcpIncoming};

/// Client→Node、Node→Node、Node→Meta 共用的 gRPC 默认参数。
///
/// `Clone` 允许不同进程组件复制这份小配置；`Debug` 便于日志打印；
/// `Eq/PartialEq` 让测试可以直接比较两份配置。derive 由编译器生成这些实现。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrpcConfig {
    /// TCP/UDS 连接建立阶段最多等待多久。
    pub connect_timeout: Duration,
    /// 单次 RPC 从发送到收到响应的默认上限。
    pub request_timeout: Duration,
    /// 操作系统 TCP keepalive 的探测周期；UDS 不使用这个字段。
    pub tcp_keepalive: Duration,
    /// HTTP/2 PING 的发送周期，用来发现连接已经失活。
    pub http2_keepalive_interval: Duration,
    /// 发出 HTTP/2 PING 后等待 ACK 的最长时间。
    pub http2_keepalive_timeout: Duration,
    /// 即使连接暂时没有 RPC，也继续发送 HTTP/2 keepalive。
    pub keepalive_while_idle: bool,
    /// 整条 HTTP/2 connection 可接收但尚未确认的字节窗口。
    pub initial_connection_window_size: u32,
    /// 单条 HTTP/2 stream 的流控窗口；一个 RPC 通常对应一条 stream。
    pub initial_stream_window_size: u32,
    /// 对端允许同时存在的最大 HTTP/2 stream 数量。
    pub max_concurrent_streams: u32,
    /// Tonic 在一条连接上接受的并发请求上限，提供进程内背压。
    pub concurrency_limit_per_connection: usize,
    /// 单条 protobuf 消息最大编码字节数；显式有界，避免大对象路径变成无限内存承诺。
    pub max_encoding_message_bytes: usize,
    /// 单条 protobuf 消息最大解码字节数；与编码上限一致覆盖双端收发。
    pub max_decoding_message_bytes: usize,
}

impl Default for GrpcConfig {
    /// `Default` 是 Rust 的标准构造约定，调用方可写 `GrpcConfig::default()`。
    fn default() -> Self {
        // `Self` 在这里等价于 `GrpcConfig`，可减少重复类型名。
        Self {
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            tcp_keepalive: Duration::from_secs(30),
            http2_keepalive_interval: Duration::from_secs(10),
            http2_keepalive_timeout: Duration::from_secs(3),
            keepalive_while_idle: true,
            initial_connection_window_size: 2 * 1024 * 1024,
            initial_stream_window_size: 2 * 1024 * 1024,
            max_concurrent_streams: 1024,
            concurrency_limit_per_connection: 1024,
            max_encoding_message_bytes: 16 * 1024 * 1024,
            max_decoding_message_bytes: 16 * 1024 * 1024,
        }
    }
}

impl GrpcConfig {
    /// 配置调用方已绑定 listener 的接受路径。
    ///
    /// `serve_with_incoming` 不应用 Server builder 的 TCP socket 选项；必须让
    /// incoming 在每次 accept 后设置。复用 Tonic 的实现，不再自写 accept 循环。
    /// UDS 没有 TCP_NODELAY，继续使用自己的 incoming。
    pub fn configure_tcp_incoming(&self, incoming: TcpIncoming) -> TcpIncoming {
        incoming
            .with_nodelay(Some(true))
            .with_keepalive(Some(self.tcp_keepalive))
    }

    /// 把 AFS 默认参数应用到一个具体的 Tonic 客户端 Endpoint。
    ///
    /// The caller still owns endpoint parsing, UDS/TCP selection and creation
    /// of the generated protobuf client.
    pub fn configure_client(&self, endpoint: Endpoint) -> Endpoint {
        self.configure_client_base(endpoint)
            .timeout(self.request_timeout)
    }

    /// Configure a client for semantic long-running RPCs that are cancelled by
    /// an explicit protocol request. The returned endpoint keeps connect and
    /// HTTP/2 keepalive bounds but deliberately has no default per-request
    /// timeout.
    pub fn configure_long_wait_client(&self, endpoint: Endpoint) -> Endpoint {
        self.configure_client_base(endpoint)
    }

    fn configure_client_base(&self, endpoint: Endpoint) -> Endpoint {
        // 每个 builder 方法都取得前一个 Endpoint 的所有权并返回新的 Endpoint；
        // 最终返回值仍未连接，调用者还需要 `.connect().await`。
        endpoint
            .connect_timeout(self.connect_timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(Some(self.tcp_keepalive))
            .http2_keep_alive_interval(self.http2_keepalive_interval)
            .keep_alive_timeout(self.http2_keepalive_timeout)
            .keep_alive_while_idle(self.keepalive_while_idle)
            .initial_connection_window_size(Some(self.initial_connection_window_size))
            .initial_stream_window_size(Some(self.initial_stream_window_size))
    }

    /// 把 AFS 默认参数应用到进程提供的 Tonic Server builder。
    ///
    /// The process remains responsible for registering its generated services
    /// and choosing TCP or UDS listeners.
    pub fn configure_server(&self, server: Server) -> Server {
        // 和客户端一样这里只配置 builder，不绑定端口，也不注册业务 Service。
        server
            .tcp_nodelay(true)
            .tcp_keepalive(Some(self.tcp_keepalive))
            .http2_keepalive_interval(Some(self.http2_keepalive_interval))
            .http2_keepalive_timeout(Some(self.http2_keepalive_timeout))
            .initial_connection_window_size(Some(self.initial_connection_window_size))
            .initial_stream_window_size(Some(self.initial_stream_window_size))
            .max_concurrent_streams(Some(self.max_concurrent_streams))
            .concurrency_limit_per_connection(self.concurrency_limit_per_connection)
    }
}
