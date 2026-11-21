//! 观测存储（业务大盘 ①）：只把"跨副本去重"这一件事落库。
//!
//! 业务大盘的其余指标全部在进程内直埋：首用时延/首输出/任务结果/在途
//!（`chat.send` 只落一个 ingress 副本，跨副本求和精确）、请求族与全流族。
//! Prometheus 给不出的是"跨副本去重的人数"——窗口去重不可加，per-user 标签又
//! 会打爆基数——所以只有活跃/会话/DAU 需要一张表，去重由主键保证。
//!
//! 存储是软依赖：`Error::Unavailable` 时调用方只损失活跃族与质量族，不阻塞业务。
use serde::Deserialize;
use std::fmt;
use std::time::Duration;

/// 单条 PG IO 的超时（连接/语句/池等待），与 accounts 一致。
pub(crate) const IO_TIMEOUT: Duration = Duration::from_secs(10);

pub mod active;
pub use active::ActivitySink;

/// PostgreSQL 连接配置（与 `accounts::AccountConfig` 的数据库三件套同构）。
#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    /// 数据库 URL 的环境变量名（如 `ADX_OBSERVATION_DATABASE_URL`）。
    pub database_url_env: String,
    #[serde(default)]
    pub database_ca_file: Option<String>,
    #[serde(default)]
    pub database_allow_plaintext: bool,
}

/// `ADX_OBSERVABILITY_CONFIG` 的完整配置（一段，不再是四段）。
///
/// 文件存在即启用；不配置时活跃族、质量族和 PG 写入都不存在，其余直埋照常。
#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    pub postgres: PostgresConfig,
    /// HMAC 密钥的环境变量名；密钥必须长期稳定（轮换等于换一批用户）。
    pub hmac_key_env: String,
    /// 落库周期（秒）。
    #[serde(default = "default_flush_interval_seconds")]
    pub flush_interval_seconds: u64,
    /// 保留天数；到期由 sink 自己清理。
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    /// 每 `(分钟, 用户)` 接受的会话数上限——`session_id` 由客户端自报，必须设界。
    #[serde(default = "default_max_sessions_per_user_minute")]
    pub max_sessions_per_user_minute: u16,
    /// 进程内待写入元组上限；超出丢弃并计数（不阻塞热路径）。
    #[serde(default = "default_capacity")]
    pub capacity: usize,
}

impl ObservabilityConfig {
    /// 零值会让采集空转或永不落库，配置错误拒启动。
    pub fn validate(&self) -> Result<(), Error> {
        if self.flush_interval_seconds == 0
            || self.retention_days == 0
            || self.max_sessions_per_user_minute == 0
            || self.capacity == 0
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

fn default_flush_interval_seconds() -> u64 {
    15
}

fn default_retention_days() -> u32 {
    30
}

fn default_max_sessions_per_user_minute() -> u16 {
    20
}

fn default_capacity() -> usize {
    200_000
}

/// 配置或存储错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// URL/TLS/密钥配置错误，或 schema 版本与代码期望不符（拒启动）。
    Configuration,
    /// PG 不可用或语句失败（软依赖：调用方降级，不阻塞业务）。
    Unavailable,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration => "observation storage misconfigured".fmt(formatter),
            Self::Unavailable => "observation storage unavailable".fmt(formatter),
        }
    }
}

impl std::error::Error for Error {}
