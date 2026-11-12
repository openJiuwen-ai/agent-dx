//! 观测存储（M2-3，详设 §6.9）：`adx_ops_observations` schema 的存储层。
//!
//! 业务事件链路的落地端：事件写入（`(producer_id, event_id)` 去重 + 终态吸
//! 收）、聚合读取（occurred_at 游标）、分钟桶替换、持久化 Counter、保留期清
//! 理。接收端点（M2-2）与单写者聚合器（M2-5）构建在本存储之上。
//!
//! 三条铁律（§5.5.5，由 SQL 语义实现）：
//!
//! 1. **终态吸收**——`task_state` 的终态列只被首个终态事件写入；第二个不同
//!    结局保留首个并落 `ingest_reject{reason_code=terminal_conflict}`；
//! 2. **迟到只修原桶**——rollup `ON CONFLICT` **替换**计数值并递增 `revision`，
//!    不累加；
//! 3. **未决不伪装失败**——无终态的体验停留在 `accepted_at`/`first_output_at`
//!    阶段，终态列为空，不进成功率分母。
//!
//! 存储是软依赖：`Error::Unavailable` 时调用方必须降级（503/水位陈旧），
//! 不得阻塞业务请求。
use serde::Deserialize;
use std::fmt;
use std::time::Duration;

/// 观测存储连接配置（与 `accounts::AccountConfig` 的数据库三件套同构）。
#[derive(Debug, Clone, Deserialize)]
pub struct ObservationConfig {
    /// 数据库 URL 的环境变量名（如 `ADX_OBSERVATION_DATABASE_URL`）。
    pub database_url_env: String,
    #[serde(default)]
    pub database_ca_file: Option<String>,
    #[serde(default)]
    pub database_allow_plaintext: bool,
}

/// 存储错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// URL/TLS 配置错误，或 schema 版本与代码期望不符（拒启动，§6.9）。
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

/// 单条 PG IO 的超时（连接/语句/池等待），与 accounts 一致。
pub(crate) const IO_TIMEOUT: Duration = Duration::from_secs(10);

pub mod aggregator;
pub mod emitter;
pub mod ingest;
mod store;
pub use aggregator::{Aggregator, AggregatorConfig};
pub use emitter::{EmitConfig, ObservationSink};
pub use ingest::{IngestConfig, IngestError, IngestState, ProducerBinding};
pub use store::ObservationStore;

/// `ADX_OBSERVABILITY_CONFIG` 的完整配置（M2：存储 + 接收端点 + 聚合器 + 发射）。
///
/// 四段相互独立：只做发射（指向远端接收端点）不需要本地存储；只做接收/聚合
/// 必须配 `store`（校验在 `IngressService` 启动时执行）。
#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default)]
    pub store: Option<ObservationConfig>,
    #[serde(default)]
    pub ingest: Option<IngestConfig>,
    /// 聚合器（§6.10）；缺省不启动。
    #[serde(default)]
    pub aggregator: Option<AggregatorConfig>,
    /// 事件发射（§6.4 M2-4b）；缺省不发射。
    #[serde(default)]
    pub emit: Option<EmitConfig>,
}

/// 测试基建：PG 集成测试共享一个测试库，schema 重建必须全局串行。
#[cfg(test)]
pub(crate) mod test_support {
    use super::store::ObservationStore;
    use std::sync::LazyLock;
    use tokio::sync::{Mutex, MutexGuard};

    const SCHEMA_SQL: &str = include_str!("schema.sql");
    static PG_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    pub(crate) fn test_url() -> String {
        std::env::var("ADX_OBSERVATION_TEST_URL").unwrap_or_else(|_| {
            "postgresql://postgres:adx-test@127.0.0.1:5432/adx_observability_test".to_owned()
        })
    }

    /// 每个测试一套全新 schema；本地 PG 不可用时跳过（不挡 CI）。
    /// 返回的守卫持有至测试结束——整套 PG 测试串行执行。
    pub(crate) async fn fresh_store() -> Option<(ObservationStore, MutexGuard<'static, ()>)> {
        let guard = PG_LOCK.lock().await;
        let url = test_url();
        let (client, connection) = match tokio_postgres::connect(&url, tokio_postgres::NoTls).await
        {
            Ok(value) => value,
            Err(error) => {
                eprintln!("skipped: local PostgreSQL unavailable ({error})");
                return None;
            }
        };
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let ready = client
            .simple_query("DROP SCHEMA IF EXISTS adx_ops_observations CASCADE")
            .await
            .is_ok()
            && client.simple_query(SCHEMA_SQL).await.is_ok();
        drop(client);
        driver.abort();
        if !ready {
            eprintln!("skipped: observation schema reset failed");
            return None;
        }
        match ObservationStore::from_url(&url, true, None).await {
            Ok(store) => Some((store, guard)),
            Err(error) => {
                eprintln!("skipped: store connect failed ({error:?})");
                None
            }
        }
    }
}
