//! 活跃去重（业务大盘 ①）：进程内按分钟收集去重元组，批量落
//! `adx_ops_observations.active_minute`，由 Grafana 对窗口做 `count(DISTINCT)`。
//!
//! 为什么只有这一件事需要落库：任务结果、任务时长、动作数都是直埋 counter 与
//! histogram（一条 `chat.send` 只落一个 ingress 副本，跨副本求和精确）；只有
//! "跨副本去重的人数"Prometheus 给不出——窗口去重不可加，per-user 标签又会打爆
//! 基数。去重由表主键保证，不依赖游标、重算或终态吸收。
//!
//! 存储是软依赖：连接失败只让活跃族缺数并抬高 `dropped`，不阻塞业务请求，也不
//! 影响其余直埋族。
use super::{Error, ObservabilityConfig, IO_TIMEOUT};
use adx_observability::metrics::Text;
use deadpool_postgres::{Manager, Pool, Runtime};
use ring::hmac;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use tokio::sync::watch;
use tokio_postgres::config::SslMode;

/// 代码期望的 schema 版本；不匹配拒启动（不自动迁移）。
const EXPECTED_SCHEMA_VERSION: i32 = 2;
/// 单次 flush 的主键条数上限：`unnest` 参数不宜过大。
const FLUSH_BATCH: usize = 20_000;
/// 保留期清理的最短间隔。
const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);

const INSERT_SQL: &str = "INSERT INTO adx_ops_observations.active_minute \
     (occurred_minute, tenant_ref, user_ref, session_ref, template_release) \
     SELECT * FROM unnest($1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::text[]) \
     ON CONFLICT DO NOTHING";
const PRUNE_SQL: &str = "DELETE FROM adx_ops_observations.active_minute WHERE occurred_minute < $1";

/// 一个活跃元组。身份列全是 HMAC 引用；`session` 为空串表示没有会话维度。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ActiveTuple {
    tenant: String,
    user: String,
    session: String,
    template: String,
}

/// 进程内收集器：`分钟 → 去重元组`。
///
/// 锁顺序固定为 `minutes → sessions`；`render` 只读计数，不跨 await。
struct Collector {
    minutes: Mutex<BTreeMap<i64, BTreeSet<ActiveTuple>>>,
    /// 每 `(分钟, 租户+用户)` 已接受的会话数，用于会话上限护栏。
    /// `take` 之后只保留当前分钟，已结束分钟的计数立即回收。
    sessions: Mutex<HashMap<(i64, String), u16>>,
    entries: AtomicU64,
    dropped: AtomicU64,
    session_overflow: AtomicU64,
    capacity: usize,
    max_sessions: u16,
}

impl Collector {
    fn new(capacity: usize, max_sessions: u16) -> Self {
        Self {
            minutes: Mutex::new(BTreeMap::new()),
            sessions: Mutex::new(HashMap::new()),
            entries: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            session_overflow: AtomicU64::new(0),
            capacity,
            max_sessions,
        }
    }

    /// 收一个元组；返回是否为新样本（覆盖率分子只认这个口径）。
    fn insert(&self, minute: i64, tuple: ActiveTuple) -> bool {
        let mut minutes = lock(&self.minutes);
        if minutes.get(&minute).is_some_and(|set| set.contains(&tuple)) {
            return false;
        }
        if self.entries.load(Ordering::Relaxed) >= self.capacity as u64 {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let mut tuple = tuple;
        if !tuple.session.is_empty() {
            let key = (minute, format!("{}\u{0}{}", tuple.tenant, tuple.user));
            let mut sessions = lock(&self.sessions);
            let accepted = sessions.entry(key).or_insert(0);
            if *accepted >= self.max_sessions {
                // 客户端自报的 `session_id` 不可信：超限后仍记活跃，但丢弃会话
                // 维度，避免单个用户刷出无界行数。
                tuple.session.clear();
                self.session_overflow.fetch_add(1, Ordering::Relaxed);
            } else {
                *accepted += 1;
            }
        }
        if minutes.entry(minute).or_default().insert(tuple) {
            self.entries.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// 取走全部待写入元组（同一分钟可能被多次取走——落库按主键幂等）。
    fn take(&self) -> Vec<(i64, ActiveTuple)> {
        let mut minutes = lock(&self.minutes);
        let drained = std::mem::take(&mut *minutes);
        self.entries.store(0, Ordering::Relaxed);
        // 已结束分钟的会话计数不会再被使用；只保留当前分钟，避免 HashMap 随运行
        // 时间无界增长。保持锁顺序 `minutes → sessions`。
        let current = minute_of(now_unix());
        lock(&self.sessions).retain(|(minute, _), _| *minute >= current);
        drained
            .into_iter()
            .flat_map(|(minute, set)| set.into_iter().map(move |tuple| (minute, tuple)))
            .collect()
    }

    /// 写库失败的回灌：重新并入（主键幂等，重复并入不会多出行）。
    fn restore(&self, batch: Vec<(i64, ActiveTuple)>) {
        let mut minutes = lock(&self.minutes);
        for (minute, tuple) in batch {
            if minutes.entry(minute).or_default().insert(tuple) {
                self.entries.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn pending(&self) -> u64 {
        self.entries.load(Ordering::Relaxed)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

fn minute_of(unix_seconds: u64) -> i64 {
    (unix_seconds / 60 * 60) as i64
}

fn hmac_ref(key: &[u8], tenant: &str, value: &str) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let mut input = Vec::with_capacity(tenant.len() + 1 + value.len());
    input.extend_from_slice(tenant.as_bytes());
    input.push(0);
    input.extend_from_slice(value.as_bytes());
    hex::encode(hmac::sign(&key, &input).as_ref())
}

/// 活跃采集 + 落库。`record` 是非阻塞内存操作；`run` 是唯一写库任务。
pub struct ActivitySink {
    pool: Pool,
    hmac_key: Vec<u8>,
    collector: Collector,
    flush_interval: Duration,
    retention_days: i64,
    last_success: AtomicU64,
}

impl ActivitySink {
    /// 从配置读 URL（`postgres.database_url_env`）与 HMAC 密钥并连接。
    /// 配置缺失或 schema 版本不符返回 [`Error::Configuration`]（拒启动）。
    pub async fn connect(config: &ObservabilityConfig) -> Result<Self, Error> {
        let url =
            std::env::var(&config.postgres.database_url_env).map_err(|_| Error::Configuration)?;
        let key = std::env::var(&config.hmac_key_env).map_err(|_| Error::Configuration)?;
        if key.is_empty() {
            return Err(Error::Configuration);
        }
        Self::from_url(
            &url,
            config.postgres.database_allow_plaintext,
            config.postgres.database_ca_file.as_deref(),
            key.into_bytes(),
            config,
        )
        .await
    }

    /// 连接并校验 schema 版本；供配置装配与组件测试共用。
    pub(crate) async fn from_url(
        url: &str,
        allow_plaintext: bool,
        ca_file: Option<&str>,
        hmac_key: Vec<u8>,
        config: &ObservabilityConfig,
    ) -> Result<Self, Error> {
        let mut pg: tokio_postgres::Config = url.parse().map_err(|_| Error::Configuration)?;
        pg.connect_timeout(IO_TIMEOUT);
        pg.options("-c statement_timeout=10000 -c lock_timeout=5000");
        let manager = if allow_plaintext {
            pg.ssl_mode(SslMode::Disable);
            Manager::new(pg, tokio_postgres::NoTls)
        } else {
            pg.ssl_mode(SslMode::Require);
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            if let Some(path) = ca_file {
                let pem = std::fs::read(path).map_err(|_| Error::Configuration)?;
                for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                    roots
                        .add(cert.map_err(|_| Error::Configuration)?)
                        .map_err(|_| Error::Configuration)?;
                }
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::Configuration)?
            .with_root_certificates(roots)
            .with_no_client_auth();
            Manager::new(pg, tokio_postgres_rustls::MakeRustlsConnect::new(tls))
        };
        let pool = Pool::builder(manager)
            .max_size(4)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(IO_TIMEOUT))
            .create_timeout(Some(IO_TIMEOUT))
            .recycle_timeout(Some(IO_TIMEOUT))
            .build()
            .map_err(|_| Error::Configuration)?;
        let sink = Self {
            pool,
            hmac_key,
            collector: Collector::new(config.capacity, config.max_sessions_per_user_minute),
            flush_interval: Duration::from_secs(config.flush_interval_seconds),
            retention_days: i64::from(config.retention_days),
            last_success: AtomicU64::new(0),
        };
        let client = sink.pool.get().await.map_err(|_| Error::Unavailable)?;
        let rows = client
            .query(
                "SELECT version FROM adx_ops_observations.schema_version",
                &[],
            )
            .await
            .map_err(|_| Error::Configuration)?;
        if rows.len() != 1 || rows[0].get::<_, i32>(0) != EXPECTED_SCHEMA_VERSION {
            return Err(Error::Configuration);
        }
        Ok(sink)
    }

    /// 脱敏引用：HMAC(tenant ‖ 0 ‖ value)。租户并入输入，跨租户不会误合并；
    /// 同一密钥下稳定，活跃去重依赖这一点——密钥轮换等于换了一批新用户。
    pub fn hmac_ref(&self, tenant: &str, value: &str) -> String {
        hmac_ref(&self.hmac_key, tenant, value)
    }

    /// 记一次业务活跃（H3 受理 / 文件上传）。热路径只做内存插入。
    /// 返回是否为新样本。
    pub fn record(
        &self,
        tenant: &str,
        user_ref: &str,
        session_ref: Option<&str>,
        template: &str,
    ) -> bool {
        self.collector.insert(
            minute_of(now_unix()),
            ActiveTuple {
                tenant: tenant.to_owned(),
                user: user_ref.to_owned(),
                session: session_ref.unwrap_or_default().to_owned(),
                template: template.to_owned(),
            },
        )
    }

    /// 周期落库 + 每日清理。`shutdown` 置位时做最后一次 flush 再退出。
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_prune = std::time::Instant::now();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.flush_once().await;
                        break;
                    }
                }
                _ = ticker.tick() => {
                    self.flush_once().await;
                    if last_prune.elapsed() >= PRUNE_INTERVAL {
                        self.prune_once().await;
                        last_prune = std::time::Instant::now();
                    }
                }
            }
        }
    }

    async fn flush_once(&self) {
        let batch = self.collector.take();
        if batch.is_empty() {
            return;
        }
        match self.write(&batch).await {
            Ok(()) => {
                self.last_success.store(now_unix(), Ordering::Relaxed);
            }
            Err(error) => {
                self.collector.restore(batch);
                tracing::warn!(%error, "activity flush failed; batch requeued");
            }
        }
    }

    async fn write(&self, batch: &[(i64, ActiveTuple)]) -> Result<(), Error> {
        for chunk in batch.chunks(FLUSH_BATCH) {
            let mut minutes = Vec::with_capacity(chunk.len());
            let mut tenants = Vec::with_capacity(chunk.len());
            let mut users = Vec::with_capacity(chunk.len());
            let mut sessions = Vec::with_capacity(chunk.len());
            let mut templates = Vec::with_capacity(chunk.len());
            for (minute, tuple) in chunk {
                minutes.push(
                    OffsetDateTime::from_unix_timestamp(*minute)
                        .map_err(|_| Error::Configuration)?,
                );
                tenants.push(tuple.tenant.clone());
                users.push(tuple.user.clone());
                sessions.push(tuple.session.clone());
                templates.push(tuple.template.clone());
            }
            let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
            client
                .execute(
                    INSERT_SQL,
                    &[&minutes, &tenants, &users, &sessions, &templates],
                )
                .await
                .map_err(|_| Error::Unavailable)?;
        }
        Ok(())
    }

    async fn prune_once(&self) {
        let Some(cutoff) =
            OffsetDateTime::now_utc().checked_sub(time::Duration::days(self.retention_days))
        else {
            return;
        };
        let Ok(client) = self.pool.get().await else {
            return;
        };
        if let Err(error) = client.execute(PRUNE_SQL, &[&cutoff]).await {
            tracing::warn!(%error, "activity retention prune failed");
        }
    }

    /// 采集链自身的质量族；未启用时整段不出现（与其余直埋族共用 /metrics）。
    pub fn render(&self, out: &mut Text) {
        out.gauge(
            "adx_observation_active_pending_tuples",
            &[],
            self.collector.pending(),
        );
        out.counter(
            "adx_observation_active_dropped_total",
            "Activity tuples rejected before the database",
            &[("reason", "capacity".to_owned())],
            self.collector.dropped.load(Ordering::Relaxed),
        );
        out.counter(
            "adx_observation_active_dropped_total",
            "Activity tuples rejected before the database",
            &[("reason", "session_overflow".to_owned())],
            self.collector.session_overflow.load(Ordering::Relaxed),
        );
        let last_success = self.last_success.load(Ordering::Relaxed);
        if last_success > 0 {
            out.gauge(
                "adx_observation_active_last_success_timestamp_seconds",
                &[],
                last_success,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(session: &str) -> ActiveTuple {
        ActiveTuple {
            tenant: "tenant-1".to_owned(),
            user: "user-1".to_owned(),
            session: session.to_owned(),
            template: "tpl@1".to_owned(),
        }
    }

    #[test]
    fn duplicate_tuple_in_the_same_minute_is_absorbed() {
        let collector = Collector::new(100, 20);
        assert!(collector.insert(60, tuple("s1")));
        assert!(!collector.insert(60, tuple("s1")));
        assert_eq!(collector.pending(), 1);
        assert_eq!(collector.take().len(), 1);
        assert_eq!(collector.pending(), 0);
    }

    #[test]
    fn different_minutes_produce_separate_rows() {
        let collector = Collector::new(100, 20);
        assert!(collector.insert(60, tuple("s1")));
        assert!(collector.insert(120, tuple("s1")));
        assert_eq!(collector.take().len(), 2);
    }

    #[test]
    fn session_flood_is_capped_per_user_minute() {
        let collector = Collector::new(100, 2);
        assert!(collector.insert(60, tuple("s1")));
        assert!(collector.insert(60, tuple("s2")));
        // 第三个会话超限：仍计活跃，但丢弃会话维度（空串合并为一行）。
        assert!(collector.insert(60, tuple("s3")));
        assert_eq!(collector.session_overflow.load(Ordering::Relaxed), 1);
        let batch = collector.take();
        assert_eq!(batch.len(), 3);
        assert_eq!(
            batch.iter().filter(|(_, t)| t.session.is_empty()).count(),
            1
        );
    }

    #[test]
    fn take_prunes_finished_minute_session_counters() {
        let collector = Collector::new(100, 1);
        let current = minute_of(now_unix());
        collector.insert(current - 1, tuple("s1"));
        collector.insert(current, tuple("s1"));
        let _ = collector.take();
        // 旧分钟回收、当前分钟保留：同一分钟再来一个会话仍触发上限。
        collector.insert(current, tuple("s2"));
        assert_eq!(lock(&collector.sessions).len(), 1);
        assert_eq!(collector.session_overflow.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn capacity_drops_instead_of_growing() {
        let collector = Collector::new(1, 20);
        assert!(collector.insert(60, tuple("s1")));
        assert!(!collector.insert(60, tuple("s2")));
        assert_eq!(collector.dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn failed_flush_requeues_without_duplicates() {
        let collector = Collector::new(100, 20);
        collector.insert(60, tuple("s1"));
        let batch = collector.take();
        collector.restore(batch);
        assert_eq!(collector.pending(), 1);
        collector.insert(60, tuple("s1"));
        assert_eq!(collector.pending(), 1);
    }

    #[test]
    fn hmac_reference_is_tenant_scoped_and_stable() {
        let key = b"test-key-material";
        let first = hmac_ref(key, "tenant-1", "user-1");
        assert_eq!(first, hmac_ref(key, "tenant-1", "user-1"));
        assert_ne!(first, hmac_ref(key, "tenant-2", "user-1"));
        assert_ne!(first, hmac_ref(b"other-key", "tenant-1", "user-1"));
        assert_eq!(first.len(), 64);
        assert!(!first.contains("user-1"));
    }

    /// 真实 PostgreSQL：同一元组重复写入只落一行，flush 幂等。
    /// 本地没有测试库时跳过（不挡 CI）。
    #[tokio::test]
    async fn flush_is_idempotent_against_postgres() {
        let url = std::env::var("ADX_OBSERVATION_TEST_URL").unwrap_or_else(|_| {
            "postgresql://postgres:adx-test@127.0.0.1:5432/adx_observability_test".to_owned()
        });
        let Ok((client, connection)) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await
        else {
            eprintln!("skipped: local PostgreSQL unavailable");
            return;
        };
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let reset = client
            .simple_query("DROP SCHEMA IF EXISTS adx_ops_observations CASCADE")
            .await
            .is_ok()
            && client
                .simple_query(include_str!("schema.sql"))
                .await
                .is_ok();
        if !reset {
            eprintln!("skipped: observation schema reset failed");
            driver.abort();
            return;
        }
        let sink = ActivitySink::from_url(
            &url,
            true,
            None,
            b"test-key-material".to_vec(),
            &test_config(),
        )
        .await
        .expect("activity sink");
        assert!(sink.record("tenant-1", "user-ref-1", Some("session-ref-1"), "tpl@1"));
        assert!(sink.record("tenant-1", "user-ref-2", None, "tpl@1"));
        // 同一分钟同一元组：收集器直接吸收。
        assert!(!sink.record("tenant-1", "user-ref-1", Some("session-ref-1"), "tpl@1"));
        sink.flush_once().await;
        // 再记一次同样的元组并 flush：主键冲突，不新增行。
        sink.record("tenant-1", "user-ref-1", Some("session-ref-1"), "tpl@1");
        sink.flush_once().await;
        let rows = client
            .query(
                "SELECT count(*) FROM adx_ops_observations.active_minute",
                &[],
            )
            .await
            .expect("count rows");
        assert_eq!(rows[0].get::<_, i64>(0), 2);
        let distinct = client
            .query(
                "SELECT count(DISTINCT user_ref) FROM adx_ops_observations.active_minute",
                &[],
            )
            .await
            .expect("count users");
        assert_eq!(distinct[0].get::<_, i64>(0), 2);
        driver.abort();
    }

    fn test_config() -> ObservabilityConfig {
        ObservabilityConfig {
            postgres: super::super::PostgresConfig {
                database_url_env: "ADX_OBSERVATION_TEST_URL".to_owned(),
                database_ca_file: None,
                database_allow_plaintext: true,
            },
            hmac_key_env: "ADX_TEST_ACTIVE_KEY".to_owned(),
            flush_interval_seconds: 15,
            retention_days: 30,
            max_sessions_per_user_minute: 20,
            capacity: 100,
        }
    }
}
