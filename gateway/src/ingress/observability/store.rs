//! `adx_ops_observations` 存储实现（M2-3）。连接与 schema 校验沿用
//! [accounts/store.rs](super::super::accounts::store) 的 deadpool 模式；
//! 写入语义按三条铁律（§5.5.5）实现，集成测试需本地 PostgreSQL。
use super::{Error, ObservationConfig, IO_TIMEOUT};
use adx_observability::business::{BusinessObservation, EventType, ReceivedObservation};
use deadpool_postgres::{Manager, Pool, Runtime};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio_postgres::{config::SslMode, Transaction};

/// 代码期望的 schema 版本；不匹配即拒启动（§6.9，不自动迁移）。
const EXPECTED_SCHEMA_VERSION: i32 = 1;

/// 批内单条事件的写入结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventInsert {
    /// 新事件：明细与 task_state 均已落库。
    Accepted,
    /// `(producer_id, event_id)` 已存在：幂等跳过，返回 202（§5.5.5）。
    Duplicate,
}

/// 聚合读取的一行事件游标：只取聚合器真正用到的四列。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventCursor {
    pub producer_id: String,
    pub event_id: String,
    pub occurred_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
}

/// 分钟桶重算结果：一个（环境 × 模板 × 结果 × arrival × start_mode）的终态计数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MinuteTaskCount {
    pub environment_id: String,
    pub template_release: String,
    pub outcome: String,
    pub arrival: String,
    pub start_mode: String,
    pub task_count: i64,
}

/// 终态计数汇总行（标签维度与 `adx_business_task_results_total` 一致）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskResultTotal {
    pub template_release: String,
    pub outcome: String,
    pub arrival: String,
    pub start_mode: String,
    pub task_count: i64,
}

/// 时长桶汇总行（`task_duration_seconds` 的直方图原料）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DurationTotal {
    pub template_release: String,
    pub outcome: String,
    pub bucket_upper_seconds: f64,
    pub task_count: i64,
    pub duration_sum_seconds: f64,
}

/// 分钟桶重算输入：一条终态时长样本。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MinuteDuration {
    pub environment_id: String,
    pub template_release: String,
    pub outcome: String,
    pub duration_seconds: f64,
}

/// 单批内一个体验的状态变更（同批多事件按到达顺序合并后的结果）。
///
/// `None` 表示"本批没有可填的值"；合并规则与库侧 `COALESCE(state, EXCLUDED)`
/// 一致——首个非空值胜出，已有值不被改写。
#[derive(Default)]
struct TaskDelta<'a> {
    tenant_ref: Option<&'a str>,
    user_ref: Option<&'a str>,
    session_ref: Option<&'a str>,
    environment_id: Option<&'a str>,
    template_release: Option<&'a str>,
    start_mode: Option<&'a str>,
    accepted_at: Option<OffsetDateTime>,
    first_output_at: Option<OffsetDateTime>,
    terminal: Option<TerminalStamp<'a>>,
    /// 批内与首个终态结局冲突的后续事件。
    conflicts: Vec<ConflictStamp<'a>>,
}

/// 批内首个终态事件的判定字段。
struct TerminalStamp<'a> {
    outcome: &'a str,
    reason_class: &'a str,
    duration: Option<f64>,
    occurred_at: OffsetDateTime,
    event_id: &'a str,
    received_at: OffsetDateTime,
}

/// 一条终态冲突诊断（原因码固定 `terminal_conflict`）。
struct ConflictStamp<'a> {
    event_id: &'a str,
    received_at: OffsetDateTime,
}

impl<'a> TaskDelta<'a> {
    /// 按到达顺序合并一条事件（只填空，不覆盖）。
    fn fill(&mut self, observation: &'a BusinessObservation, event: &'a ReceivedObservation) {
        if self.tenant_ref.is_none() {
            self.tenant_ref = Some(observation.tenant_ref.as_str());
        }
        if self.user_ref.is_none() {
            self.user_ref = observation.user_ref.as_deref();
        }
        if self.session_ref.is_none() {
            self.session_ref = observation.session_ref.as_deref();
        }
        if self.environment_id.is_none() {
            self.environment_id = observation.environment_id.as_deref();
        }
        if self.template_release.is_none() {
            self.template_release = observation.template_release.as_deref();
        }
        if self.start_mode.is_none() {
            self.start_mode = observation.start_mode.map(|mode| mode.as_str());
        }
        match observation.event_type {
            EventType::ExperienceAccepted => {
                self.accepted_at.get_or_insert(observation.occurred_at);
            }
            EventType::FirstOutput => {
                self.first_output_at.get_or_insert(observation.occurred_at);
            }
            EventType::TaskTerminal => {
                // 校验保证终态带 outcome/reasonClass；缺失时按空串落库（不 panic）。
                let outcome = observation
                    .outcome
                    .map(|value| value.as_str())
                    .unwrap_or("");
                let reason_class = observation
                    .reason_class
                    .map(|value| value.as_str())
                    .unwrap_or("");
                match &self.terminal {
                    None => {
                        self.terminal = Some(TerminalStamp {
                            outcome,
                            reason_class,
                            duration: observation.duration_seconds,
                            occurred_at: observation.occurred_at,
                            event_id: observation.event_id.as_str(),
                            received_at: event.received_at(),
                        });
                    }
                    Some(first) => {
                        if first.outcome != outcome {
                            self.conflicts.push(ConflictStamp {
                                event_id: observation.event_id.as_str(),
                                received_at: event.received_at(),
                            });
                        }
                    }
                }
            }
            EventType::Action => {}
        }
    }
}

/// 观测存储：进程内单实例，软依赖（PG 不可用时返回 [`Error::Unavailable`]）。
#[derive(Clone)]
pub struct ObservationStore {
    pool: Pool,
}

impl ObservationStore {
    /// 从配置的环境变量读 URL 并连接。
    pub async fn connect(config: &ObservationConfig) -> Result<Self, Error> {
        let url = std::env::var(&config.database_url_env).map_err(|_| Error::Configuration)?;
        Self::from_url(
            &url,
            config.database_allow_plaintext,
            config.database_ca_file.as_deref(),
        )
        .await
    }

    /// 连接并校验 schema 版本；不匹配返回 [`Error::Configuration`]（拒启动）。
    pub async fn from_url(
        url: &str,
        allow_plaintext: bool,
        ca_file: Option<&str>,
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
            .max_size(8)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(IO_TIMEOUT))
            .create_timeout(Some(IO_TIMEOUT))
            .recycle_timeout(Some(IO_TIMEOUT))
            .build()
            .map_err(|_| Error::Configuration)?;
        let store = Self { pool };
        let client = store.pool.get().await.map_err(|_| Error::Unavailable)?;
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
        Ok(store)
    }

    /// 整批原子写入（§6.8：不做部分成功）：事件明细（即去重账本）→
    /// task_state（终态吸收）。任何一条语句失败则整批回滚。
    ///
    /// 批内只用**常数条语句**（明细 / 状态 upsert / 冲突诊断），不是每条事件
    /// 3 条：接收端按批（≤500）投递，逐条往返会把单实例写入上限压到几百
    /// events/s（实测 500 条 ≈ 0.8s）。
    pub async fn insert_batch(
        &self,
        batch: &[ReceivedObservation],
    ) -> Result<Vec<EventInsert>, Error> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let mut client = self.connection().await?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        // ① 事件明细：主键 `(producer_id, event_id)` 冲突即幂等跳过，只回吐
        // 真正新插入的行——已存在的事件不会被重复写入。
        let accepted = Self::insert_events(&tx, batch).await?;
        // ② 批内同一 `(producer, event)` 出现多次时只有第一条计 Accepted。
        let mut results = Vec::with_capacity(batch.len());
        let mut fresh: Vec<&ReceivedObservation> = Vec::new();
        let mut seen: HashSet<(&str, &str)> = HashSet::new();
        for event in batch {
            let key = (event.producer_id(), event.observation().event_id.as_str());
            let is_fresh =
                accepted.contains(&(key.0.to_owned(), key.1.to_owned())) && seen.insert(key);
            results.push(if is_fresh {
                EventInsert::Accepted
            } else {
                EventInsert::Duplicate
            });
            if is_fresh {
                fresh.push(event);
            }
        }
        if fresh.is_empty() {
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(results);
        }
        // ③ task_state：同批同一体验先合并成一次状态变更（终态吸收）。
        Self::apply_task_state_batch(&tx, &fresh).await?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(results)
    }

    /// 一条 `unnest` 语句写入整批事件明细，返回真正插入的 `(producer, event)`。
    async fn insert_events(
        tx: &Transaction<'_>,
        events: &[ReceivedObservation],
    ) -> Result<HashSet<(String, String)>, Error> {
        // 列顺序与下方 `unnest` 一一对应；借用事件字段，不复制字符串。
        let observations: Vec<&BusinessObservation> =
            events.iter().map(|event| event.observation()).collect();
        let column = |f: fn(&BusinessObservation) -> &str| -> Vec<&str> {
            observations.iter().map(|o| f(o)).collect()
        };
        let opt = |f: fn(&BusinessObservation) -> Option<&str>| -> Vec<Option<&str>> {
            observations.iter().map(|o| f(o)).collect()
        };
        let producers: Vec<&str> = events.iter().map(|event| event.producer_id()).collect();
        let event_ids = column(|o| o.event_id.as_str());
        let business_types = column(|o| o.business_type.as_str());
        let event_types = column(|o| o.event_type.as_str());
        let occurred_at: Vec<_> = observations.iter().map(|o| o.occurred_at).collect();
        let received_at: Vec<_> = events.iter().map(|event| event.received_at()).collect();
        let tenant_refs = column(|o| o.tenant_ref.as_str());
        let user_refs = opt(|o| o.user_ref.as_deref());
        let session_refs = opt(|o| o.session_ref.as_deref());
        let experience_ids = column(|o| o.experience_id.as_str());
        let environment_ids = opt(|o| o.environment_id.as_deref());
        let template_releases = opt(|o| o.template_release.as_deref());
        let start_modes = opt(|o| o.start_mode.map(|mode| mode.as_str()));
        let trace_ids = opt(|o| o.trace_id.as_deref());
        let span_ids = opt(|o| o.span_id.as_deref());
        let outcomes = opt(|o| o.outcome.map(|outcome| outcome.as_str()));
        let reason_classes = opt(|o| o.reason_class.map(|class| class.as_str()));
        let durations: Vec<Option<f64>> = observations.iter().map(|o| o.duration_seconds).collect();
        let action_kinds = opt(|o| o.action_kind.map(|kind| kind.as_str()));
        let inserted = tx
            .query(
                "INSERT INTO adx_ops_observations.observation_event(\
                 producer_id,event_id,business_type,event_type,occurred_at,received_at,\
                 tenant_ref,user_ref,session_ref,experience_id,environment_id,\
                 template_release,start_mode,trace_id,span_id,outcome,reason_class,\
                 duration_seconds,action_kind)\
                 SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],\
                 $5::timestamptz[],$6::timestamptz[],$7::text[],$8::text[],$9::text[],\
                 $10::text[],$11::text[],$12::text[],$13::text[],$14::text[],$15::text[],\
                 $16::text[],$17::text[],$18::float8[],$19::text[])\
                 ON CONFLICT (producer_id,event_id) DO NOTHING \
                 RETURNING producer_id,event_id",
                &[
                    &producers,
                    &event_ids,
                    &business_types,
                    &event_types,
                    &occurred_at,
                    &received_at,
                    &tenant_refs,
                    &user_refs,
                    &session_refs,
                    &experience_ids,
                    &environment_ids,
                    &template_releases,
                    &start_modes,
                    &trace_ids,
                    &span_ids,
                    &outcomes,
                    &reason_classes,
                    &durations,
                    &action_kinds,
                ],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(inserted
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect())
    }

    /// task_state 批维护（铁律 1：终态吸收；非终态只填空）。
    ///
    /// 同批同一体验的多个事件先按到达顺序合并（首个非空值胜出），再统一
    /// upsert；`COALESCE(state, EXCLUDED)` 保证库内已有值不被改写。终态冲突
    /// （批内或与库内首个结局不同）逐条落 `ingest_reject`。
    async fn apply_task_state_batch<'a>(
        tx: &Transaction<'_>,
        events: &'a [&'a ReceivedObservation],
    ) -> Result<(), Error> {
        let mut order: Vec<(String, String)> = Vec::new();
        let mut deltas: HashMap<(String, String), TaskDelta<'_>> = HashMap::new();
        for event in events {
            let observation = event.observation();
            // action 不维护任务态（§5.5.2：与体验生命周期无关）。
            if observation.event_type == EventType::Action {
                continue;
            }
            let key = (
                event.producer_id().to_owned(),
                observation.experience_id.clone(),
            );
            if !deltas.contains_key(&key) {
                order.push(key.clone());
            }
            let delta = deltas.entry(key).or_default();
            delta.fill(observation, event);
        }
        if order.is_empty() {
            return Ok(());
        }

        let deltas_of: Vec<&TaskDelta<'_>> = order
            .iter()
            .map(|key| deltas.get(key).ok_or(Error::Unavailable))
            .collect::<Result<_, _>>()?;
        let producers: Vec<&str> = order
            .iter()
            .map(|(producer, _)| producer.as_str())
            .collect();
        let experiences: Vec<&str> = order
            .iter()
            .map(|(_, experience)| experience.as_str())
            .collect();
        let tenants: Vec<&str> = deltas_of
            .iter()
            .map(|delta| delta.tenant_ref.unwrap_or_default())
            .collect();
        let users: Vec<_> = deltas_of.iter().map(|delta| delta.user_ref).collect();
        let sessions: Vec<_> = deltas_of.iter().map(|delta| delta.session_ref).collect();
        let environments: Vec<_> = deltas_of.iter().map(|delta| delta.environment_id).collect();
        let templates: Vec<_> = deltas_of
            .iter()
            .map(|delta| delta.template_release)
            .collect();
        let start_modes: Vec<_> = deltas_of.iter().map(|delta| delta.start_mode).collect();
        let accepted_at: Vec<_> = deltas_of.iter().map(|delta| delta.accepted_at).collect();
        let first_output_at: Vec<_> = deltas_of
            .iter()
            .map(|delta| delta.first_output_at)
            .collect();
        let terminal_at: Vec<_> = deltas_of
            .iter()
            .map(|delta| delta.terminal.as_ref().map(|value| value.occurred_at))
            .collect();
        let terminal_received_at: Vec<_> = deltas_of
            .iter()
            .map(|delta| delta.terminal.as_ref().map(|value| value.received_at))
            .collect();
        let terminal_outcomes: Vec<_> = deltas_of
            .iter()
            .map(|delta| delta.terminal.as_ref().map(|value| value.outcome))
            .collect();
        let terminal_reasons: Vec<_> = deltas_of
            .iter()
            .map(|delta| delta.terminal.as_ref().map(|value| value.reason_class))
            .collect();
        let durations: Vec<_> = deltas_of
            .iter()
            .map(|delta| delta.terminal.as_ref().and_then(|value| value.duration))
            .collect();
        // 首个终态胜出由 `COALESCE(task_state.x, EXCLUDED.x)` 原子完成：并发
        // 写入无需预读加锁，`RETURNING` 直接给出权威结局，用于与本次尝试对账。
        let winning = tx
            .query(
                "INSERT INTO adx_ops_observations.task_state(\
                 producer_id,experience_id,tenant_ref,user_ref,session_ref,environment_id,\
                 template_release,start_mode,accepted_at,first_output_at,terminal_at,\
                 terminal_received_at,terminal_outcome,terminal_reason_class,duration_seconds)\
                 SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],\
                 $6::text[],$7::text[],$8::text[],$9::timestamptz[],$10::timestamptz[],\
                 $11::timestamptz[],$12::timestamptz[],$13::text[],$14::text[],$15::float8[])\
                 ON CONFLICT (producer_id,experience_id) DO UPDATE SET \
                 tenant_ref=COALESCE(task_state.tenant_ref,EXCLUDED.tenant_ref),\
                 user_ref=COALESCE(task_state.user_ref,EXCLUDED.user_ref),\
                 session_ref=COALESCE(task_state.session_ref,EXCLUDED.session_ref),\
                 environment_id=COALESCE(task_state.environment_id,EXCLUDED.environment_id),\
                 template_release=COALESCE(task_state.template_release,EXCLUDED.template_release),\
                 start_mode=COALESCE(task_state.start_mode,EXCLUDED.start_mode),\
                 accepted_at=COALESCE(task_state.accepted_at,EXCLUDED.accepted_at),\
                 first_output_at=COALESCE(task_state.first_output_at,EXCLUDED.first_output_at),\
                 terminal_at=COALESCE(task_state.terminal_at,EXCLUDED.terminal_at),\
                 terminal_received_at=COALESCE(task_state.terminal_received_at,EXCLUDED.terminal_received_at),\
                 terminal_outcome=COALESCE(task_state.terminal_outcome,EXCLUDED.terminal_outcome),\
                 terminal_reason_class=COALESCE(task_state.terminal_reason_class,EXCLUDED.terminal_reason_class),\
                 duration_seconds=COALESCE(task_state.duration_seconds,EXCLUDED.duration_seconds),\
                 revision=task_state.revision+1,updated_at=now() \
                 RETURNING producer_id,experience_id,terminal_outcome",
                &[
                    &producers,
                    &experiences,
                    &tenants,
                    &users,
                    &sessions,
                    &environments,
                    &templates,
                    &start_modes,
                    &accepted_at,
                    &first_output_at,
                    &terminal_at,
                    &terminal_received_at,
                    &terminal_outcomes,
                    &terminal_reasons,
                    &durations,
                ],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        let mut winning: HashMap<(String, String), String> = winning
            .into_iter()
            .filter_map(|row| {
                row.get::<_, Option<String>>(2)
                    .map(|outcome| ((row.get(0), row.get(1)), outcome))
            })
            .collect();

        // 冲突诊断：批内与库内首个结局不同的终态（首个胜出，后续只记账）。
        let mut conflict_received = Vec::new();
        let mut conflict_producers = Vec::new();
        let mut conflict_events = Vec::new();
        for (key, delta) in order.iter().zip(&deltas_of) {
            if let Some(terminal) = delta.terminal.as_ref() {
                if winning
                    .remove(key)
                    .is_some_and(|outcome| outcome != terminal.outcome)
                {
                    conflict_received.push(terminal.received_at);
                    conflict_producers.push(key.0.clone());
                    conflict_events.push(terminal.event_id.to_owned());
                }
            }
            for conflict in &delta.conflicts {
                conflict_received.push(conflict.received_at);
                conflict_producers.push(key.0.clone());
                conflict_events.push(conflict.event_id.to_owned());
            }
        }

        if !conflict_events.is_empty() {
            let reasons: Vec<&str> = conflict_events
                .iter()
                .map(|_| "terminal_conflict")
                .collect();
            tx.execute(
                "INSERT INTO adx_ops_observations.ingest_reject\
                 (received_at,producer_id,event_id,reason_code)\
                 SELECT * FROM unnest($1::timestamptz[],$2::text[],$3::text[],$4::text[])",
                &[
                    &conflict_received,
                    &conflict_producers,
                    &conflict_events,
                    &reasons,
                ],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        }
        Ok(())
    }

    /// 落一条拒收/冲突诊断（只存原因码与元数据，不存正文，M-A11）。
    /// `detail_bytes` 记录被拒请求体的字节数，用于区分空批与超限批。
    pub async fn record_reject(
        &self,
        received_at: OffsetDateTime,
        producer_id: Option<&str>,
        event_id: Option<&str>,
        reason_code: &str,
        field: Option<&str>,
        detail_bytes: Option<i32>,
    ) -> Result<(), Error> {
        let client = self.connection().await?;
        client
            .execute(
                "INSERT INTO adx_ops_observations.ingest_reject\
                 (received_at,producer_id,event_id,reason_code,field,detail_bytes)\
                 VALUES($1,$2,$3,$4,$5,$6)",
                &[
                    &received_at,
                    &producer_id,
                    &event_id,
                    &reason_code,
                    &field,
                    &detail_bytes,
                ],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// 按**接收时间**游标读取事件（聚合器输入）：迟到事件（occurred_at 早于
    /// 水位）也能被扫到，随后由精确重算修正原桶。
    ///
    /// 游标是 `(received_at, producer_id, event_id)` 复合键，不是单纯时间：
    /// 同一批 HTTP 事件共享同一个 `received_at`，只用时间做排他下界时，分页
    /// 一旦截断在该批中间，剩余同时间戳事件会被永久跳过且无法自愈。
    pub(crate) async fn fetch_received_after(
        &self,
        after_received_at: OffsetDateTime,
        after_producer_id: &str,
        after_event_id: &str,
        before: OffsetDateTime,
        limit: i64,
    ) -> Result<Vec<EventCursor>, Error> {
        let client = self.connection().await?;
        let rows = client
            .query(
                "SELECT producer_id,event_id,occurred_at,received_at \
                 FROM adx_ops_observations.observation_event \
                 WHERE (received_at,producer_id,event_id) > ($1,$2,$3) \
                 AND received_at <= $4 \
                 ORDER BY received_at,producer_id,event_id LIMIT $5",
                &[
                    &after_received_at,
                    &after_producer_id,
                    &after_event_id,
                    &before,
                    &limit,
                ],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| EventCursor {
                producer_id: row.get(0),
                event_id: row.get(1),
                occurred_at: row.get(2),
                received_at: row.get(3),
            })
            .collect())
    }

    /// 一个 UTC 分钟内的终态计数，按环境 × 模板 × 结果 × arrival × start_mode
    /// 分组（聚合重算输入）。`arrival`：接收时间与事件时间相差超过 5 分钟记为
    /// `late`（§5.1.1）。
    pub(crate) async fn minute_task_counts(
        &self,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<MinuteTaskCount>, Error> {
        let client = self.connection().await?;
        let rows = client
            .query(
                // 业务结果只从 `task_state`（唯一权威终态）派生：冲突终态被
                // 首个终态吸收，不会在桶里出现两份（C02/D02）。
                "SELECT COALESCE(environment_id,''),COALESCE(template_release,''),\
                 COALESCE(terminal_outcome,''),\
                 CASE WHEN terminal_received_at - terminal_at > interval '5 minutes' \
                      THEN 'late' ELSE 'on_time' END,\
                 COALESCE(start_mode,'unknown'),count(*)::bigint \
                 FROM adx_ops_observations.task_state \
                 WHERE terminal_at >= $1 AND terminal_at < $2 \
                 GROUP BY 1,2,3,4,5",
                &[&from, &to],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| MinuteTaskCount {
                environment_id: row.get(0),
                template_release: row.get(1),
                outcome: row.get(2),
                arrival: row.get(3),
                start_mode: row.get(4),
                task_count: row.get(5),
            })
            .collect())
    }

    /// 一个 UTC 分钟内的终态时长样本（桶化在聚合器 Rust 侧完成）。
    pub(crate) async fn minute_durations(
        &self,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<MinuteDuration>, Error> {
        let client = self.connection().await?;
        let rows = client
            .query(
                "SELECT COALESCE(environment_id,''),COALESCE(template_release,''),\
                 COALESCE(terminal_outcome,''),duration_seconds \
                 FROM adx_ops_observations.task_state \
                 WHERE terminal_at >= $1 AND terminal_at < $2 \
                 AND duration_seconds IS NOT NULL",
                &[&from, &to],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| MinuteDuration {
                environment_id: row.get(0),
                template_release: row.get(1),
                outcome: row.get(2),
                duration_seconds: row.get(3),
            })
            .collect())
    }

    /// 终态计数汇总：从分钟桶重算（幂等，崩溃重跑不双计）。
    pub(crate) async fn task_result_totals(&self) -> Result<Vec<TaskResultTotal>, Error> {
        let client = self.connection().await?;
        let rows = client
            .query(
                "SELECT template_release,outcome,arrival,start_mode,sum(task_count)::bigint \
                 FROM adx_ops_observations.task_minute_rollup \
                 GROUP BY 1,2,3,4",
                &[],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| TaskResultTotal {
                template_release: row.get(0),
                outcome: row.get(1),
                arrival: row.get(2),
                start_mode: row.get(3),
                task_count: row.get(4),
            })
            .collect())
    }

    /// 动作计数汇总（非根任务动作）：模板 × 动作类型 × 结果，从分钟桶求和。
    pub(crate) async fn action_totals(&self) -> Result<Vec<(String, String, String, i64)>, Error> {
        let client = self.connection().await?;
        let rows = client
            .query(
                "SELECT template_release,action_kind,outcome,sum(action_count)::bigint \
                 FROM adx_ops_observations.action_minute_rollup GROUP BY 1,2,3",
                &[],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2),
                    row.get::<_, i64>(3),
                )
            })
            .collect())
    }

    /// 时长桶汇总（含 `duration_sum_seconds`）：从分钟桶重算。
    pub(crate) async fn duration_totals(&self) -> Result<Vec<DurationTotal>, Error> {
        let client = self.connection().await?;
        let rows = client
            .query(
                "SELECT template_release,outcome,bucket_upper_seconds,\
                 sum(task_count)::bigint,sum(duration_sum_seconds) \
                 FROM adx_ops_observations.duration_minute_rollup \
                 GROUP BY 1,2,3",
                &[],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| DurationTotal {
                template_release: row.get(0),
                outcome: row.get(1),
                bucket_upper_seconds: row.get(2),
                task_count: row.get(3),
                duration_sum_seconds: row.get(4),
            })
            .collect())
    }

    /// 时间窗口内的去重活跃用户数（`user_ref` 非空）。窗口按**分钟桶**对齐：
    /// 读取 `active_minute_rollup` 而不是扫描事件明细，24h 窗口不再随事件量
    /// 线性变重。
    pub(crate) async fn distinct_users(&self, from: OffsetDateTime) -> Result<i64, Error> {
        let client = self.connection().await?;
        let row = client
            .query_one(
                "SELECT count(DISTINCT user_ref)::bigint \
                 FROM adx_ops_observations.active_minute_rollup \
                 WHERE occurred_minute >= $1",
                &[&from],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(row.get(0))
    }

    /// 时间窗口内的去重活跃会话数（`session_ref` 非空）。
    pub(crate) async fn distinct_sessions(&self, from: OffsetDateTime) -> Result<i64, Error> {
        let client = self.connection().await?;
        let row = client
            .query_one(
                "SELECT count(DISTINCT session_ref)::bigint \
                 FROM adx_ops_observations.active_minute_rollup \
                 WHERE occurred_minute >= $1 AND session_ref <> ''",
                &[&from],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(row.get(0))
    }

    /// 时间窗口内按模板的去重用户数（降序；标签基数上限由聚合器截断）。
    pub(crate) async fn template_distinct_users(
        &self,
        from: OffsetDateTime,
    ) -> Result<Vec<(String, i64)>, Error> {
        let client = self.connection().await?;
        let rows = client
            .query(
                "SELECT template_release,count(DISTINCT user_ref)::bigint \
                 FROM adx_ops_observations.active_minute_rollup \
                 WHERE occurred_minute >= $1 \
                 GROUP BY 1 ORDER BY 2 DESC, 1",
                &[&from],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect())
    }

    /// 时间窗口内**排除**给定模板后的去重用户数（`other` 桶；去重计数不可加）。
    pub(crate) async fn distinct_users_excluding(
        &self,
        from: OffsetDateTime,
        excluded: &[String],
    ) -> Result<i64, Error> {
        let client = self.connection().await?;
        let row = client
            .query_one(
                "SELECT count(DISTINCT user_ref)::bigint \
                 FROM adx_ops_observations.active_minute_rollup \
                 WHERE occurred_minute >= $1 AND NOT (template_release = ANY($2))",
                &[&from, &excluded.to_vec()],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(row.get(0))
    }

    /// 重算一分钟的动作计数桶（替换语义：先删该分钟再整段重算）。
    pub(crate) async fn recompute_action_minute(
        &self,
        minute: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Result<(), Error> {
        let client = self.connection().await?;
        client
            .execute(
                "DELETE FROM adx_ops_observations.action_minute_rollup \
                 WHERE occurred_minute=$1",
                &[&minute],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        client
            .execute(
                "INSERT INTO adx_ops_observations.action_minute_rollup\
                 (occurred_minute,template_release,action_kind,outcome,action_count) \
                 SELECT $1,COALESCE(template_release,''),COALESCE(action_kind,''),\
                 COALESCE(outcome,''),count(*)::bigint \
                 FROM adx_ops_observations.observation_event \
                 WHERE event_type='action' AND occurred_at>=$1 AND occurred_at<$2 \
                 GROUP BY 2,3,4",
                &[&minute, &end],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// 重算一分钟的活跃维度桶（每分钟的 (用户, 会话, 模板) 去重元组）。
    pub(crate) async fn recompute_active_minute(
        &self,
        minute: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Result<(), Error> {
        let client = self.connection().await?;
        client
            .execute(
                "DELETE FROM adx_ops_observations.active_minute_rollup \
                 WHERE occurred_minute=$1",
                &[&minute],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        client
            .execute(
                "INSERT INTO adx_ops_observations.active_minute_rollup\
                 (occurred_minute,user_ref,session_ref,template_release) \
                 SELECT $1,user_ref,COALESCE(session_ref,''),COALESCE(template_release,'') \
                 FROM adx_ops_observations.observation_event \
                 WHERE occurred_at>=$1 AND occurred_at<$2 AND user_ref IS NOT NULL \
                 GROUP BY 2,3,4",
                &[&minute, &end],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// 检出一条池化连接（单写者 advisory lock 用；调用方持有以维持会话锁）。
    pub(crate) async fn connection(&self) -> Result<deadpool_postgres::Object, Error> {
        self.pool.get().await.map_err(|_| Error::Unavailable)
    }

    /// 尝试会话级 advisory lock；成功后由调用方持有的连接维持，断开即释放。
    pub(crate) async fn try_advisory_lock(
        connection: &deadpool_postgres::Object,
        key: i64,
    ) -> Result<bool, Error> {
        let row = connection
            .query_one("SELECT pg_try_advisory_lock($1)", &[&key])
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(row.get(0))
    }

    /// 释放会话级 advisory lock（优雅关停时调用，避免连接回池后锁滞留）。
    pub(crate) async fn release_advisory_lock(
        connection: &deadpool_postgres::Object,
        key: i64,
    ) -> Result<(), Error> {
        connection
            .execute("SELECT pg_advisory_unlock($1)", &[&key])
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// 测试用：取单值文本查询。
    #[cfg(test)]
    pub(crate) async fn query_scalar(&self, sql: &str) -> String {
        let client = self.pool.get().await.unwrap();
        let row = client.query_one(sql, &[]).await.unwrap();
        row.get::<_, String>(0)
    }

    /// 替换一个任务分钟桶（铁律 2：替换不累加，revision+1）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn replace_task_rollup(
        &self,
        occurred_minute: OffsetDateTime,
        environment_id: &str,
        template_release: &str,
        outcome: &str,
        arrival: &str,
        start_mode: &str,
        task_count: i64,
    ) -> Result<(), Error> {
        let client = self.connection().await?;
        client
            .execute(
                "INSERT INTO adx_ops_observations.task_minute_rollup\
                 (occurred_minute,environment_id,template_release,outcome,arrival,start_mode,task_count)\
                 VALUES($1,$2,$3,$4,$5,$6,$7)\
                 ON CONFLICT (occurred_minute,environment_id,template_release,outcome,arrival,start_mode)\
                 DO UPDATE SET \
                 task_count=EXCLUDED.task_count,revision=task_minute_rollup.revision+1,updated_at=now()",
                &[
                    &occurred_minute,
                    &environment_id,
                    &template_release,
                    &outcome,
                    &arrival,
                    &start_mode,
                    &task_count,
                ],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// 替换一个时长直方图分钟桶（同上：替换不累加，含时长求和）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn replace_duration_rollup(
        &self,
        occurred_minute: OffsetDateTime,
        environment_id: &str,
        template_release: &str,
        outcome: &str,
        bucket_upper_seconds: f64,
        task_count: i64,
        duration_sum_seconds: f64,
    ) -> Result<(), Error> {
        let client = self.connection().await?;
        client
            .execute(
                "INSERT INTO adx_ops_observations.duration_minute_rollup\
                 (occurred_minute,environment_id,template_release,outcome,bucket_upper_seconds,\
                 task_count,duration_sum_seconds)\
                 VALUES($1,$2,$3,$4,$5,$6,$7)\
                 ON CONFLICT (occurred_minute,environment_id,template_release,outcome,bucket_upper_seconds)\
                 DO UPDATE SET \
                 task_count=EXCLUDED.task_count,duration_sum_seconds=EXCLUDED.duration_sum_seconds,\
                 revision=duration_minute_rollup.revision+1,updated_at=now()",
                &[
                    &occurred_minute,
                    &environment_id,
                    &template_release,
                    &outcome,
                    &bucket_upper_seconds,
                    &task_count,
                    &duration_sum_seconds,
                ],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// 读取一条 pipeline 的复合水位 `(received_at, producer_id, event_id)`
    /// （未初始化返回 `None`）。只存时间会在重启后退化为时间戳下界，永久
    /// 漏掉同批次剩余事件。
    pub async fn watermark(
        &self,
        pipeline: &str,
    ) -> Result<Option<(OffsetDateTime, String, String)>, Error> {
        let name = format!("watermark:{pipeline}");
        let client = self.connection().await?;
        let row = client
            .query_opt(
                "SELECT at,COALESCE(cursor_producer_id,''),COALESCE(cursor_event_id,'') \
                 FROM adx_ops_observations.pipeline_state \
                 WHERE name=$1 AND dimensions='' AND at IS NOT NULL",
                &[&name],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(row.map(|row| (row.get(0), row.get(1), row.get(2))))
    }

    /// 推进水位（只进不减由调用方保证；此处仅持久化）。
    pub async fn advance_watermark(
        &self,
        pipeline: &str,
        at: OffsetDateTime,
        producer_id: &str,
        event_id: &str,
    ) -> Result<(), Error> {
        let name = format!("watermark:{pipeline}");
        let client = self.connection().await?;
        client
            .execute(
                "INSERT INTO adx_ops_observations.pipeline_state\
                 (name,dimensions,at,cursor_producer_id,cursor_event_id)\
                 VALUES($1,'',$2,$3,$4)\
                 ON CONFLICT (name,dimensions) DO UPDATE SET \
                 at=EXCLUDED.at,cursor_producer_id=EXCLUDED.cursor_producer_id,\
                 cursor_event_id=EXCLUDED.cursor_event_id,\
                 revision=pipeline_state.revision+1,updated_at=now()",
                &[&name, &at, &producer_id, &event_id],
            )
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// 保留期清理（§6.9）：原始事件/诊断 7 天、task_state 90 天、活跃去重
    /// 维度 7 天。`now` 由调用方传入以便测试；截止时间在 Rust 侧计算（避免
    /// PG 对 `$1 - interval` 的参数类型推断歧义）。
    ///
    /// **分钟桶与动作桶不清理**：`adx_business_*_total` 就是从它们求和得到的，
    /// 清理会让 Counter 回退（C12）。桶的行数由有界维度决定，增量很小。
    pub async fn prune(&self, now: OffsetDateTime) -> Result<(), Error> {
        let mut client = self.connection().await?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        let statements = [
            (
                "DELETE FROM adx_ops_observations.observation_event WHERE occurred_at < $1",
                now - Duration::days(7),
            ),
            (
                "DELETE FROM adx_ops_observations.task_state \
                 WHERE COALESCE(terminal_at,accepted_at,updated_at) < $1",
                now - Duration::days(90),
            ),
            (
                "DELETE FROM adx_ops_observations.ingest_reject WHERE received_at < $1",
                now - Duration::days(7),
            ),
            (
                "DELETE FROM adx_ops_observations.active_minute_rollup \
                 WHERE occurred_minute < $1",
                now - Duration::days(7),
            ),
        ];
        for (statement, cutoff) in statements {
            tx.execute(statement, &[&cutoff])
                .await
                .map_err(|_| Error::Unavailable)?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::observability::test_support::{fresh_store, test_url};
    use adx_observability::business::{
        BusinessType, Outcome, ReasonClass, StartMode, ViolationClass,
    };
    use time::Duration;

    /// PG timestamptz 为微秒精度：截断纳秒，保证往返比较相等。
    fn pg_now() -> OffsetDateTime {
        let now = OffsetDateTime::now_utc();
        now.replace_nanosecond(now.nanosecond() / 1_000 * 1_000)
            .unwrap()
    }

    fn observation(
        event_type: EventType,
        event_id: &str,
        experience_id: &str,
    ) -> BusinessObservation {
        let mut event = BusinessObservation {
            schema_version: "v1".to_owned(),
            event_id: event_id.to_owned(),
            business_type: BusinessType::Jiuwen,
            action_kind: None,
            event_type,
            occurred_at: pg_now() - Duration::seconds(60),
            tenant_ref: "tenant-1".to_owned(),
            user_ref: Some("user-1".to_owned()),
            session_ref: Some("session-1".to_owned()),
            experience_id: experience_id.to_owned(),
            environment_id: None,
            template_release: Some("chat@1.0.0".to_owned()),
            start_mode: Some(StartMode::Unknown),
            trace_id: None,
            span_id: None,
            outcome: None,
            reason_class: None,
            duration_seconds: None,
        };
        if event_type.is_terminal() {
            event.outcome = Some(Outcome::Success);
            event.reason_class = Some(ReasonClass::Unknown);
            event.duration_seconds = Some(10.0);
        }
        event
    }

    fn received(event: BusinessObservation) -> ReceivedObservation {
        ReceivedObservation::new("producer-1", OffsetDateTime::now_utc(), event).unwrap()
    }

    fn received_at(event: BusinessObservation, at: OffsetDateTime) -> ReceivedObservation {
        ReceivedObservation::new("producer-1", at, event).unwrap()
    }

    async fn scalar(store: &ObservationStore, sql: &str) -> String {
        let client = store.pool.get().await.unwrap();
        let row = client.query_one(sql, &[]).await.unwrap();
        row.get::<_, String>(0)
    }

    #[tokio::test]
    async fn insert_batch_dedups_and_reinserts_nothing() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        let batch = vec![
            received(observation(EventType::ExperienceAccepted, "evt-1", "exp-1")),
            received(observation(EventType::TaskTerminal, "evt-2", "exp-1")),
        ];
        assert_eq!(
            store.insert_batch(&batch).await.unwrap(),
            vec![EventInsert::Accepted, EventInsert::Accepted]
        );
        // 同批重发：全部 Duplicate，明细不翻倍。
        assert_eq!(
            store.insert_batch(&batch).await.unwrap(),
            vec![EventInsert::Duplicate, EventInsert::Duplicate]
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*)::text FROM adx_ops_observations.observation_event"
            )
            .await,
            "2"
        );
        // 同批内重复 eventId：只有第一条 Accepted。
        assert_eq!(
            store
                .insert_batch(&[
                    received(observation(EventType::ExperienceAccepted, "evt-9", "exp-9")),
                    received(observation(EventType::ExperienceAccepted, "evt-9", "exp-9")),
                ])
                .await
                .unwrap(),
            vec![EventInsert::Accepted, EventInsert::Duplicate]
        );
    }

    /// 去重命名空间是 `(producer, event)`：不同 producer 的同 eventId 不互相吞掉。
    #[tokio::test]
    async fn same_event_id_is_scoped_by_producer() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        let first = ReceivedObservation::new(
            "producer-1",
            OffsetDateTime::now_utc(),
            observation(EventType::ExperienceAccepted, "evt-x", "exp-1"),
        )
        .unwrap();
        let second = ReceivedObservation::new(
            "producer-2",
            OffsetDateTime::now_utc(),
            observation(EventType::ExperienceAccepted, "evt-x", "exp-2"),
        )
        .unwrap();
        assert_eq!(
            store.insert_batch(&[first, second]).await.unwrap(),
            vec![EventInsert::Accepted, EventInsert::Accepted]
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*)::text FROM adx_ops_observations.observation_event"
            )
            .await,
            "2"
        );
    }

    #[tokio::test]
    async fn task_state_lifecycle_and_fill_semantics() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        let accepted = observation(EventType::ExperienceAccepted, "evt-a", "exp-1");
        let accepted_at = accepted.occurred_at;
        store.insert_batch(&[received(accepted)]).await.unwrap();
        let first_output = observation(EventType::FirstOutput, "evt-f", "exp-1");
        let first_output_at = first_output.occurred_at;
        store.insert_batch(&[received(first_output)]).await.unwrap();
        let terminal = observation(EventType::TaskTerminal, "evt-t", "exp-1");
        let terminal_at = terminal.occurred_at;
        store.insert_batch(&[received(terminal)]).await.unwrap();
        // 迟到的 accepted 带不同 user：已有值不被改写（首个胜出）。
        let mut late = observation(EventType::ExperienceAccepted, "evt-a2", "exp-1");
        late.user_ref = Some("user-2".to_owned());
        store.insert_batch(&[received(late)]).await.unwrap();

        let client = store.pool.get().await.unwrap();
        let row = client
            .query_one(
                "SELECT user_ref,accepted_at,first_output_at,terminal_at,terminal_outcome,\
                 duration_seconds FROM adx_ops_observations.task_state",
                &[],
            )
            .await
            .unwrap();
        drop(client);
        assert_eq!(row.get::<_, Option<String>>(0).as_deref(), Some("user-1"));
        assert_eq!(row.get::<_, Option<OffsetDateTime>>(1), Some(accepted_at));
        assert_eq!(
            row.get::<_, Option<OffsetDateTime>>(2),
            Some(first_output_at)
        );
        assert_eq!(row.get::<_, Option<OffsetDateTime>>(3), Some(terminal_at));
        assert_eq!(row.get::<_, Option<String>>(4).as_deref(), Some("success"));
        assert_eq!(row.get::<_, Option<f64>>(5), Some(10.0));
    }

    #[tokio::test]
    async fn terminal_absorption_conflicts_are_diagnosed() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        store
            .insert_batch(&[received(observation(
                EventType::TaskTerminal,
                "evt-t1",
                "exp-1",
            ))])
            .await
            .unwrap();
        // 第二个终态、不同结局：保留首个，落 terminal_conflict 诊断。
        let mut conflicting = observation(EventType::TaskTerminal, "evt-t2", "exp-1");
        conflicting.outcome = Some(Outcome::Failure);
        conflicting.reason_class = Some(ReasonClass::ModelUpstream);
        store.insert_batch(&[received(conflicting)]).await.unwrap();
        // 第三个终态、同结局：幂等 no-op，不新增诊断。
        store
            .insert_batch(&[received(observation(
                EventType::TaskTerminal,
                "evt-t3",
                "exp-1",
            ))])
            .await
            .unwrap();

        assert_eq!(
            scalar(
                &store,
                "SELECT terminal_outcome FROM adx_ops_observations.task_state"
            )
            .await,
            "success"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*)::text FROM adx_ops_observations.ingest_reject \
                 WHERE reason_code='terminal_conflict'"
            )
            .await,
            "1"
        );
    }

    #[tokio::test]
    async fn unresolved_experiences_stay_open() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        // 只有 accepted + first_output，无终态：终态列保持空（未决不伪装失败）。
        store
            .insert_batch(&[received(observation(
                EventType::ExperienceAccepted,
                "evt-a",
                "exp-1",
            ))])
            .await
            .unwrap();
        store
            .insert_batch(&[received(observation(
                EventType::FirstOutput,
                "evt-f",
                "exp-1",
            ))])
            .await
            .unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*)::text FROM adx_ops_observations.task_state \
                 WHERE terminal_at IS NULL AND accepted_at IS NOT NULL"
            )
            .await,
            "1"
        );
    }

    #[tokio::test]
    async fn rollups_replace_instead_of_accumulate() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        let minute = OffsetDateTime::now_utc();
        store
            .replace_task_rollup(
                minute,
                "env-1",
                "chat@1.0.0",
                "success",
                "on_time",
                "unknown",
                5,
            )
            .await
            .unwrap();
        // 迟到重算：同桶替换为 3，不累加成 8。
        store
            .replace_task_rollup(
                minute,
                "env-1",
                "chat@1.0.0",
                "success",
                "on_time",
                "unknown",
                3,
            )
            .await
            .unwrap();
        store
            .replace_duration_rollup(minute, "env-1", "chat@1.0.0", "success", 10.0, 2, 20.0)
            .await
            .unwrap();
        store
            .replace_duration_rollup(minute, "env-1", "chat@1.0.0", "success", 10.0, 1, 10.0)
            .await
            .unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT task_count::text FROM adx_ops_observations.task_minute_rollup"
            )
            .await,
            "3"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT revision::text FROM adx_ops_observations.task_minute_rollup"
            )
            .await,
            "2"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT task_count::text FROM adx_ops_observations.duration_minute_rollup"
            )
            .await,
            "1"
        );
    }

    #[tokio::test]
    async fn watermark_roundtrip() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        assert_eq!(store.watermark("business_events").await.unwrap(), None);
        let t1 = pg_now();
        // 复合游标：时间 + (producer, event)，重启后不会退化为纯时间下界。
        store
            .advance_watermark("business_events", t1, "producer-1", "evt-9")
            .await
            .unwrap();
        assert_eq!(
            store.watermark("business_events").await.unwrap(),
            Some((t1, "producer-1".to_owned(), "evt-9".to_owned()))
        );
    }

    /// 回归：同一 `received_at` 的事件被分页截断时，复合游标必须一页不漏。
    /// 只用 `received_at` 做排他下界会永久跳过同时间戳的剩余事件。
    #[tokio::test]
    async fn received_cursor_paginates_within_one_timestamp() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        let at = pg_now();
        // 两批事件共享同一个接收时刻（接收端点对整批只取一次时钟）。
        let first: Vec<_> = (0..5)
            .map(|index| {
                received_at(
                    observation(
                        EventType::ExperienceAccepted,
                        &format!("evt-{index}"),
                        &format!("exp-{index}"),
                    ),
                    at,
                )
            })
            .collect();
        let second: Vec<_> = (5..8)
            .map(|index| {
                received_at(
                    observation(
                        EventType::ExperienceAccepted,
                        &format!("evt-{index}"),
                        &format!("exp-{index}"),
                    ),
                    at,
                )
            })
            .collect();
        store.insert_batch(&first).await.unwrap();
        store.insert_batch(&second).await.unwrap();

        let mut seen = Vec::new();
        let (mut cursor_at, mut cursor_producer, mut cursor_event) =
            (at - Duration::seconds(1), String::new(), String::new());
        loop {
            let page = store
                .fetch_received_after(
                    cursor_at,
                    &cursor_producer,
                    &cursor_event,
                    at + Duration::seconds(1),
                    3,
                )
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            for event in &page {
                seen.push(event.event_id.clone());
            }
            let last = page.last().unwrap();
            cursor_at = last.received_at;
            cursor_producer = last.producer_id.clone();
            cursor_event = last.event_id.clone();
        }
        seen.sort();
        assert_eq!(
            seen,
            (0..8)
                .map(|index| format!("evt-{index}"))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn prune_enforces_retention_windows() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        store
            .insert_batch(&[received(observation(
                EventType::TaskTerminal,
                "evt-1",
                "exp-1",
            ))])
            .await
            .unwrap();
        let now = OffsetDateTime::now_utc();
        store
            .replace_task_rollup(
                now - Duration::days(91),
                "",
                "chat@1.0.0",
                "success",
                "on_time",
                "unknown",
                1,
            )
            .await
            .unwrap();
        // 前进 8 天 + 1 小时：事件（7 天）过期，task_state（90 天）保留。
        store
            .prune(now + Duration::days(8) + Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*)::text FROM adx_ops_observations.observation_event"
            )
            .await,
            "0"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*)::text FROM adx_ops_observations.task_state"
            )
            .await,
            "1"
        );
        // 分钟桶不随保留期清理：`adx_business_*_total` 由它求和，清理会让
        // Counter 回退（C12）。90 天前的桶必须仍在。
        store.prune(now).await.unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*)::text FROM adx_ops_observations.task_minute_rollup"
            )
            .await,
            "1"
        );
    }

    #[tokio::test]
    async fn schema_version_mismatch_refuses_to_start() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        let client = store.pool.get().await.unwrap();
        client
            .execute(
                "UPDATE adx_ops_observations.schema_version SET version = 2",
                &[],
            )
            .await
            .unwrap();
        drop(client);
        match ObservationStore::from_url(&test_url(), true, None).await {
            Err(Error::Configuration) => {}
            Err(other) => panic!("expected Configuration, got {other:?}"),
            Ok(_) => panic!("expected Configuration, store connected"),
        }
    }

    #[tokio::test]
    async fn record_reject_stores_diagnostics_without_payload() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        store
            .record_reject(
                OffsetDateTime::now_utc(),
                Some("producer-1"),
                Some("evt-bad"),
                ViolationClass::ClockSkew.as_str(),
                Some("occurredAt"),
                Some(321),
            )
            .await
            .unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT reason_code || '/' || field || '/' || detail_bytes::text \
                 FROM adx_ops_observations.ingest_reject"
            )
            .await,
            "clock_skew/occurredAt/321"
        );
    }

    /// 环境身份与 Trace 必须原样落库（事件是唯一事实层）。
    #[tokio::test]
    async fn environment_and_trace_identity_roundtrip() {
        let Some((store, _schema_lock)) = fresh_store().await else {
            return;
        };
        let mut event = observation(EventType::ExperienceAccepted, "evt-1", "exp-1");
        event.environment_id = Some("env-1".to_owned());
        event.trace_id = Some("0123456789abcdef0123456789abcdef".to_owned());
        event.span_id = Some("0123456789abcdef".to_owned());
        store.insert_batch(&[received(event)]).await.unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT environment_id||'/'||trace_id||'/'||span_id \
                 FROM adx_ops_observations.observation_event"
            )
            .await,
            "env-1/0123456789abcdef0123456789abcdef/0123456789abcdef"
        );
    }
}
