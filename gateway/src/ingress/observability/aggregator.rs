//! 单写者聚合器（M2-5，详设 §6.10）：事件时间的原始事实 → 分钟桶与业务
//! 全局指标；保证重启不重复求和。
//!
//! - **游标**：`pipeline_state` 存**接收时间**水位。每轮按 received_at
//!   增量读取新事件，迟到事件（occurred_at 早于水位）同样会被扫到。
//! - **重算替换**：受影响分钟从事件明细**精确重算**后 `ON CONFLICT` 替换
//!   （revision+1），不累加——迟到只修原桶，重复执行结果一致。
//! - **导出幂等**：快照每轮从分钟桶重算（`sum`），崩溃重跑不双计；重启后
//!   直接从桶恢复，不另存一份 Counter。
//! - **单写者**：PG 会话级 advisory lock；锁连接全程持有，优雅关停时显式
//!   解锁（避免连接回池后锁滞留）。follower 只导出质量族。
//! - **软依赖**：任何 PG 错误只影响本轮；下一轮重试，不阻塞业务请求。
use super::store::ObservationStore;
use adx_core::observation::Outcome;
use adx_observability::business::StartMode as BusinessStartMode;
use adx_observability::metrics::{Text, B18};
use serde::Deserialize;
use std::{
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{watch, Mutex as AsyncMutex};

/// 聚合流水线名（`pipeline_state.name` 的 `watermark:` 后缀）。
pub const PIPELINE: &str = "business_events";
/// PG 会话级 advisory lock 键（业务观测聚合器专用）。
pub const ADVISORY_LOCK_KEY: i64 = 0x0A0D_0B5E_2026_0001;
/// 到达分类（§5.1.1：接收与事件时间相差 >5 分钟为 late）。
const ARRIVALS: [&str; 2] = ["on_time", "late"];
/// 首次启动（无水位）的回溯范围：与事件保留期一致。
const INITIAL_LOOKBACK: TimeDuration = TimeDuration::days(7);

/// 聚合器配置（§6.10；`interval_seconds`/`max_batch`）。
#[derive(Debug, Clone, Deserialize)]
pub struct AggregatorConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_interval")]
    pub interval_seconds: u64,
    #[serde(default = "default_batch")]
    pub max_batch: i64,
    /// 保留期清理间隔（M2-3 的 prune 由聚合器驱动，默认 10 分钟）。
    #[serde(default = "default_prune_interval")]
    pub prune_interval_seconds: u64,
}

fn default_interval() -> u64 {
    30
}

fn default_batch() -> i64 {
    500
}

fn default_prune_interval() -> u64 {
    600
}

/// 去重人数/会话的滚动窗口（§5.1.1：只允许 5m/24h）。
const ACTIVE_WINDOWS: [(&str, TimeDuration); 2] = [
    ("5m", TimeDuration::minutes(5)),
    ("24h", TimeDuration::hours(24)),
];
/// 模板标签基数上限（D4：12 个活跃值 + other）。
const TEMPLATE_LIMIT: usize = 12;

#[derive(Default, Clone)]
struct AggregationExport {
    leader: bool,
    watermark: Option<OffsetDateTime>,
    task_results: Vec<super::store::TaskResultTotal>,
    duration: Vec<super::store::DurationTotal>,
    /// (window, 去重用户数)
    active_users: Vec<(&'static str, i64)>,
    /// (window, 去重会话数)
    active_sessions: Vec<(&'static str, i64)>,
    /// (window, [(template, 去重用户数)])——已按上限截断，`other` 单列
    template_users: Vec<(&'static str, Vec<(String, i64)>)>,
    daily_active_users: i64,
    /// 动作计数：(template, action_kind, outcome, count)
    actions: Vec<(String, String, String, i64)>,
    last_cycle_ok: bool,
    last_success: Option<OffsetDateTime>,
}

/// 单写者聚合器：进程内单实例，嵌入 adx-ingress。
pub struct Aggregator {
    config: AggregatorConfig,
    store: Arc<ObservationStore>,
    export: Mutex<AggregationExport>,
    /// 持有 advisory lock 的池化连接（Some = leader）。
    leader: AsyncMutex<Option<deadpool_postgres::Object>>,
    /// 上次保留期清理时间（驱动 M2-3 的 prune）。
    last_prune: Mutex<Option<Instant>>,
    started: OffsetDateTime,
}

impl Aggregator {
    pub fn new(config: AggregatorConfig, store: Arc<ObservationStore>) -> Self {
        Self {
            config,
            store,
            export: Mutex::new(AggregationExport::default()),
            leader: AsyncMutex::new(None),
            last_prune: Mutex::new(None),
            started: OffsetDateTime::now_utc(),
        }
    }

    /// 是否持有 leader 锁（follower 只导出质量族）。
    pub fn is_leader(&self) -> bool {
        lock_export(&self.export).leader
    }

    /// 负责循环：先恢复持久化 Counter，再按 `interval` 周期聚合；随 shutdown 退出。
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let _ = self.restore().await;
        let mut ticker =
            tokio::time::interval(Duration::from_secs(self.config.interval_seconds.max(1)));
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.release_leadership().await;
                        return;
                    }
                }
                _ = ticker.tick() => {
                    if self.cycle().await.is_err() {
                        lock_export(&self.export).last_cycle_ok = false;
                    }
                }
            }
        }
    }

    /// 重启恢复：水位与分钟桶是唯一权威来源，导出快照直接从桶重算。
    pub async fn restore(&self) -> Result<(), super::Error> {
        let watermark = self.store.watermark(PIPELINE).await?;
        let task_results = self.store.task_result_totals().await?;
        let duration = self.store.duration_totals().await?;
        let actions = self.store.action_totals().await?;
        let mut export = lock_export(&self.export);
        export.watermark = watermark.map(|(at, _, _)| at);
        export.task_results = task_results;
        export.duration = duration;
        export.actions = actions;
        Ok(())
    }

    /// 一轮聚合：选主 → 增量扫事件 → 精确重算受影响分钟桶 → 重算导出快照。
    pub(crate) async fn cycle(&self) -> Result<(), super::Error> {
        if !self.ensure_leader().await? {
            let mut export = lock_export(&self.export);
            export.leader = false;
            export.last_cycle_ok = false;
            return Ok(());
        }
        let now = OffsetDateTime::now_utc();
        // 游标是 `(received_at, producer_id, event_id)`：同一批事件共享一个
        // 接收时刻，只用时间做排他下界会在分页截断时永久跳过同时间戳的剩余
        // 事件（见 `ObservationStore::fetch_received_after`）。
        let mut cursor = match self.store.watermark(PIPELINE).await? {
            Some(cursor) => cursor,
            None => (now - INITIAL_LOOKBACK, String::new(), String::new()),
        };
        loop {
            let events = self
                .store
                .fetch_received_after(cursor.0, &cursor.1, &cursor.2, now, self.config.max_batch)
                .await?;
            if events.is_empty() {
                break;
            }
            let minutes: std::collections::BTreeSet<OffsetDateTime> = events
                .iter()
                .map(|event| minute_of(event.occurred_at))
                .collect();
            for minute in &minutes {
                self.recompute_minute(*minute).await?;
            }
            let Some(last) = events.last() else { break };
            cursor = (
                last.received_at,
                last.producer_id.clone(),
                last.event_id.clone(),
            );
            self.store
                .advance_watermark(PIPELINE, cursor.0, &cursor.1, &cursor.2)
                .await?;
            if (events.len() as i64) < self.config.max_batch {
                break;
            }
        }
        // 导出快照从分钟桶重算（幂等；崩溃重跑不双计）。
        let task_results = self.store.task_result_totals().await?;
        let duration = self.store.duration_totals().await?;
        let actions = self.store.action_totals().await?;
        // 去重活跃族（滚动窗口）与 UTC 日去重。
        let mut active_users = Vec::with_capacity(ACTIVE_WINDOWS.len());
        let mut active_sessions = Vec::with_capacity(ACTIVE_WINDOWS.len());
        let mut template_users = Vec::with_capacity(ACTIVE_WINDOWS.len());
        for (window_name, window) in ACTIVE_WINDOWS {
            let from = now - window;
            active_users.push((window_name, self.store.distinct_users(from).await?));
            active_sessions.push((window_name, self.store.distinct_sessions(from).await?));
            template_users.push((window_name, self.template_users(from).await?));
        }
        let day_start = now - TimeDuration::seconds(now.unix_timestamp().rem_euclid(86_400));
        let daily_active_users = self.store.distinct_users(day_start).await?;
        // 保留期清理（M2-3 的 prune 由聚合器按间隔驱动；失败不影响本轮导出）。
        let prune_due = {
            let mut last = self
                .last_prune
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let interval = Duration::from_secs(self.config.prune_interval_seconds.max(60));
            let due = last.is_none_or(|at| at.elapsed() >= interval);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if prune_due {
            if let Err(error) = self.store.prune(now).await {
                tracing::warn!(%error, "Observation retention prune failed");
            }
        }
        let mut export = lock_export(&self.export);
        export.leader = true;
        export.last_cycle_ok = true;
        export.last_success = Some(now);
        export.watermark = Some(cursor.0);
        export.task_results = task_results;
        export.duration = duration;
        export.actions = actions;
        export.active_users = active_users;
        export.active_sessions = active_sessions;
        export.template_users = template_users;
        export.daily_active_users = daily_active_users;
        Ok(())
    }

    /// 单模板去重人数：前 `TEMPLATE_LIMIT` 个按人数降序，其余归 `other`
    /// （去重计数不可加，`other` 用排除查询单独算——M-A04）。
    async fn template_users(
        &self,
        from: OffsetDateTime,
    ) -> Result<Vec<(String, i64)>, super::Error> {
        let all = self.store.template_distinct_users(from).await?;
        let (top, rest) = all.split_at(all.len().min(TEMPLATE_LIMIT));
        let mut rendered = top.to_vec();
        if !rest.is_empty() {
            let excluded: Vec<String> = rest.iter().map(|(name, _)| name.clone()).collect();
            let other = self.store.distinct_users_excluding(from, &excluded).await?;
            rendered.push(("other".to_owned(), other));
        }
        Ok(rendered)
    }

    /// 精确重算一个 UTC 分钟的全部任务/时长桶（替换语义）。
    async fn recompute_minute(&self, minute: OffsetDateTime) -> Result<(), super::Error> {
        let end = minute + TimeDuration::minutes(1);
        for row in self.store.minute_task_counts(minute, end).await? {
            self.store
                .replace_task_rollup(
                    minute,
                    &row.environment_id,
                    &row.template_release,
                    &row.outcome,
                    &row.arrival,
                    &row.start_mode,
                    row.task_count,
                )
                .await?;
        }
        let mut buckets: std::collections::BTreeMap<(String, String, String, usize), (i64, f64)> =
            std::collections::BTreeMap::new();
        for row in self.store.minute_durations(minute, end).await? {
            let key = (
                row.environment_id,
                row.template_release,
                row.outcome,
                bucket_index(row.duration_seconds),
            );
            let entry = buckets.entry(key).or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += row.duration_seconds;
        }
        for ((environment_id, template_release, outcome, index), (count, sum)) in buckets {
            self.store
                .replace_duration_rollup(
                    minute,
                    &environment_id,
                    &template_release,
                    &outcome,
                    bucket_bound(index),
                    count,
                    sum,
                )
                .await?;
        }
        // 动作与活跃维度同样按分钟整体重算：导出侧不再每轮扫事件明细。
        self.store.recompute_action_minute(minute, end).await?;
        self.store.recompute_active_minute(minute, end).await?;
        Ok(())
    }

    /// 选主：已持锁则探测连接存活；否则尝试取得 advisory lock。
    async fn ensure_leader(&self) -> Result<bool, super::Error> {
        let mut leader = self.leader.lock().await;
        if let Some(connection) = leader.as_ref() {
            if connection.simple_query("SELECT 1").await.is_ok() {
                return Ok(true);
            }
            // 锁连接已死：会话锁随连接释放，下一轮重新竞争。
            *leader = None;
        }
        let connection = self.store.connection().await?;
        if ObservationStore::try_advisory_lock(&connection, ADVISORY_LOCK_KEY).await? {
            *leader = Some(connection);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// 优雅关停：显式解锁后再放回池，避免锁随连接滞留。
    pub async fn release_leadership(&self) {
        let mut leader = self.leader.lock().await;
        if let Some(connection) = leader.take() {
            let _ = ObservationStore::release_advisory_lock(&connection, ADVISORY_LOCK_KEY).await;
        }
    }

    /// 导出口：leader 出业务全局族，follower 只出质量族；两者都出角色族。
    pub fn render(&self, text: &mut Text) {
        let export = lock_export(&self.export);
        let role = if export.leader { "leader" } else { "follower" };
        text.gauge("adx_business_export_role", &[("role", role.to_owned())], 1);
        text.gauge(
            "adx_business_export_started_timestamp_seconds",
            &[],
            self.started.unix_timestamp().max(0) as u64,
        );
        // 质量族：两角色都出（各自本地值）。
        let pipeline_label = [("pipeline", "rollup".to_owned())];
        match export.watermark {
            Some(watermark) => {
                let age = (OffsetDateTime::now_utc() - watermark)
                    .as_seconds_f64()
                    .max(0.0);
                text.gauge_f64(
                    "adx_observation_watermark_age_seconds",
                    "Age of the business-event aggregation watermark",
                    &pipeline_label,
                    age,
                );
                text.gauge_f64(
                    "adx_observation_lag_seconds",
                    "Occurred-to-aggregated lag of the business-event pipeline",
                    &pipeline_label,
                    age,
                );
                text.gauge("adx_observation_watermark_initialized", &[], 1);
            }
            None => text.gauge("adx_observation_watermark_initialized", &[], 0),
        }
        if let Some(last_success) = export.last_success {
            text.gauge(
                "adx_observation_last_success_timestamp_seconds",
                &pipeline_label,
                last_success.unix_timestamp().max(0) as u64,
            );
        }
        if !export.leader {
            return;
        }
        // 业务行：模板截断（12 活跃值 + other，D4），计数稠密。截断按**活跃度**
        // （终态计数降序，同值按名称）取前 12，与 `template_active_users`
        // 的人次口径一致；字典序截断会让稳定但低频的模板挤掉真实热点。
        let mut totals: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
        for row in &export.task_results {
            if row.template_release.is_empty() {
                continue;
            }
            *totals.entry(row.template_release.as_str()).or_insert(0) +=
                row.task_count.max(0) as u64;
        }
        // 动作也计入活跃度：只有动作、没有终态任务的模板同样要出现在导出里。
        for (template, _, _, count) in &export.actions {
            if template.is_empty() {
                continue;
            }
            *totals.entry(template.as_str()).or_insert(0) += (*count).max(0) as u64;
        }
        let mut ranked: Vec<(&str, u64)> = totals.into_iter().collect();
        ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
        let mut templates: Vec<String> = ranked
            .iter()
            .take(TEMPLATE_LIMIT)
            .map(|(name, _)| (*name).to_owned())
            .collect();
        if ranked.len() > TEMPLATE_LIMIT {
            templates.push("other".to_owned());
        }
        for template in &templates {
            for outcome in Outcome::ALL {
                for arrival in ARRIVALS {
                    for start_mode in BusinessStartMode::ALL {
                        text.counter(
                            "adx_business_task_results_total",
                            "Business task terminal results (processed volume, not success rate)",
                            &[
                                ("template_release", template.clone()),
                                ("outcome", outcome.as_str().to_owned()),
                                ("arrival", arrival.to_owned()),
                                ("start_mode", start_mode.as_str().to_owned()),
                            ],
                            count_for(
                                &export.task_results,
                                &templates,
                                template,
                                outcome.as_str(),
                                arrival,
                                start_mode.as_str(),
                            ),
                        );
                    }
                }
            }
        }
        // 体验总时长直方图（B18，按模板截断后的维度聚合）。
        for template in &templates {
            for outcome in Outcome::ALL {
                text.histogram(
                    "adx_business_task_duration_seconds",
                    "Business task terminal duration by event time",
                    &[
                        ("template_release", template.clone()),
                        ("outcome", outcome.as_str().to_owned()),
                    ],
                    &B18,
                    &duration_snapshot(&export.duration, &templates, template, outcome.as_str()),
                );
            }
        }
        // 去重活跃族（滚动窗口；跨窗口/跨模板不可相加）。
        for (window, value) in &export.active_users {
            text.gauge(
                "adx_business_active_users",
                &[("window", (*window).to_owned())],
                (*value).max(0) as u64,
            );
        }
        for (window, value) in &export.active_sessions {
            text.gauge(
                "adx_business_active_sessions",
                &[("window", (*window).to_owned())],
                (*value).max(0) as u64,
            );
        }
        for (window, rows) in &export.template_users {
            for (template, value) in rows {
                text.gauge(
                    "adx_business_template_active_users",
                    &[
                        ("window", (*window).to_owned()),
                        ("template_release", template.clone()),
                    ],
                    (*value).max(0) as u64,
                );
            }
        }
        text.gauge(
            "adx_business_daily_active_users",
            &[],
            export.daily_active_users.max(0) as u64,
        );
        // 动作族：非根任务动作计数（M2-4c），模板沿用 12+other 截断。
        let mut action_kinds: std::collections::BTreeSet<&str> = std::iter::once("file_upload")
            .chain(export.actions.iter().map(|(_, kind, _, _)| kind.as_str()))
            .collect();
        action_kinds.retain(|kind| !kind.is_empty());
        if !export.actions.is_empty() {
            for template in &templates {
                for kind in &action_kinds {
                    for outcome in Outcome::ALL {
                        text.counter(
                            "adx_business_actions_total",
                            "Business actions (non-root tasks) by kind and outcome",
                            &[
                                ("template_release", template.clone()),
                                ("action_kind", (*kind).to_owned()),
                                ("outcome", outcome.as_str().to_owned()),
                            ],
                            action_count(
                                &export.actions,
                                &templates,
                                template,
                                kind,
                                outcome.as_str(),
                            ),
                        );
                    }
                }
            }
        }
    }
}

/// 累计动作计数：`template == "other"` 时聚合不在 `templates` 里的模板。
fn action_count(
    rows: &[(String, String, String, i64)],
    templates: &[String],
    template: &str,
    action_kind: &str,
    outcome: &str,
) -> u64 {
    rows.iter()
        .filter(|(name, kind, result, _)| {
            let template_match = if template == "other" {
                !templates.iter().any(|known| known == name)
            } else {
                name == template
            };
            template_match && kind == action_kind && result == outcome
        })
        .map(|(_, _, _, count)| (*count).max(0) as u64)
        .sum()
}

/// 累计计数：`template == "other"` 时聚合不在 `templates` 里的模板。
fn count_for(
    rows: &[super::store::TaskResultTotal],
    templates: &[String],
    template: &str,
    outcome: &str,
    arrival: &str,
    start_mode: &str,
) -> u64 {
    rows.iter()
        .filter(|row| {
            let template_match = if template == "other" {
                !templates.iter().any(|name| name == &row.template_release)
            } else {
                row.template_release == template
            };
            template_match
                && row.outcome == outcome
                && row.arrival == arrival
                && row.start_mode == start_mode
        })
        .map(|row| row.task_count.max(0) as u64)
        .sum()
}

/// 由一个（模板, 结果）的桶行构造 B18 累积直方图快照（含 `+Inf` = count）。
fn duration_snapshot(
    rows: &[super::store::DurationTotal],
    templates: &[String],
    template: &str,
    outcome: &str,
) -> adx_observability::metrics::HistogramSnapshot {
    let mut buckets = vec![0_u64; B18.bounds.len()];
    let mut count = 0_u64;
    let mut sum_seconds = 0.0_f64;
    for row in rows {
        let template_match = if template == "other" {
            !templates.iter().any(|name| name == &row.template_release)
        } else {
            row.template_release == template
        };
        if !template_match || row.outcome != outcome {
            continue;
        }
        let value = row.task_count.max(0) as u64;
        count += value;
        sum_seconds += row.duration_sum_seconds.max(0.0);
        for (index, bound) in B18.bounds.iter().enumerate() {
            if *bound >= row.bucket_upper_seconds {
                buckets[index] += value;
            }
        }
    }
    adx_observability::metrics::HistogramSnapshot {
        buckets,
        count,
        sum_seconds,
    }
}

fn lock_export(export: &Mutex<AggregationExport>) -> std::sync::MutexGuard<'_, AggregationExport> {
    export.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 把事件时间截断到 UTC 分钟起点（纯减法，不会失败）。
fn minute_of(at: OffsetDateTime) -> OffsetDateTime {
    at - TimeDuration::new(at.second() as i64, at.nanosecond() as i32)
}

/// 时长 → B18 桶下标；超出最大桶落溢出桶（`bounds.len()`）。
fn bucket_index(duration_seconds: f64) -> usize {
    B18.bounds
        .iter()
        .position(|bound| duration_seconds <= *bound)
        .unwrap_or(B18.bounds.len())
}

/// 桶下标 → 上界；溢出桶为 `+Inf`。
fn bucket_bound(index: usize) -> f64 {
    B18.bounds.get(index).copied().unwrap_or(f64::INFINITY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::observability::test_support::fresh_store;
    use adx_observability::business::{
        ActionKind, BusinessObservation, BusinessType, EventType, Outcome, ReasonClass,
        ReceivedObservation, StartMode,
    };
    use time::Duration as TimeDuration;

    fn config() -> AggregatorConfig {
        AggregatorConfig {
            enabled: true,
            interval_seconds: 30,
            max_batch: 500,
            prune_interval_seconds: 600,
        }
    }

    fn terminal(
        event_id: &str,
        experience_id: &str,
        occurred_at: OffsetDateTime,
    ) -> ReceivedObservation {
        terminal_for(event_id, experience_id, occurred_at, "chat@1.0.0")
    }

    fn terminal_for(
        event_id: &str,
        experience_id: &str,
        occurred_at: OffsetDateTime,
        template_release: &str,
    ) -> ReceivedObservation {
        let observation = BusinessObservation {
            schema_version: "v1".to_owned(),
            event_id: event_id.to_owned(),
            business_type: BusinessType::Jiuwen,
            action_kind: None,
            event_type: EventType::TaskTerminal,
            occurred_at,
            tenant_ref: "tenant-1".to_owned(),
            user_ref: Some("user-1".to_owned()),
            session_ref: Some("session-1".to_owned()),
            experience_id: experience_id.to_owned(),
            environment_id: Some("env-1".to_owned()),
            template_release: Some(template_release.to_owned()),
            start_mode: Some(StartMode::Unknown),
            trace_id: None,
            span_id: None,
            outcome: Some(Outcome::Success),
            reason_class: Some(ReasonClass::Unknown),
            duration_seconds: Some(10.0),
        };
        ReceivedObservation::new("producer-1", OffsetDateTime::now_utc(), observation).unwrap()
    }

    fn action_event(
        event_id: &str,
        template_release: &str,
        occurred_at: OffsetDateTime,
    ) -> ReceivedObservation {
        let observation = BusinessObservation {
            schema_version: "v1".to_owned(),
            event_id: event_id.to_owned(),
            business_type: BusinessType::Jiuwen,
            action_kind: Some(ActionKind::FileUpload),
            event_type: EventType::Action,
            occurred_at,
            tenant_ref: "tenant-1".to_owned(),
            user_ref: Some("user-1".to_owned()),
            session_ref: Some("session-1".to_owned()),
            experience_id: "exp-1".to_owned(),
            environment_id: None,
            template_release: Some(template_release.to_owned()),
            start_mode: Some(StartMode::Unknown),
            trace_id: None,
            span_id: None,
            outcome: Some(Outcome::Success),
            reason_class: None,
            duration_seconds: None,
        };
        ReceivedObservation::new("producer-1", OffsetDateTime::now_utc(), observation).unwrap()
    }

    fn aggregator(store: &ObservationStore) -> Arc<Aggregator> {
        Arc::new(Aggregator::new(config(), Arc::new(store.clone())))
    }

    #[test]
    fn bucket_index_maps_to_b18_bounds() {
        assert_eq!(bucket_bound(bucket_index(0.0005)), 0.001);
        assert_eq!(bucket_bound(bucket_index(0.001)), 0.001);
        assert_eq!(bucket_bound(bucket_index(9.5)), 10.0);
        assert_eq!(bucket_bound(bucket_index(3600.0)), 3600.0);
        assert_eq!(bucket_bound(bucket_index(3601.0)), f64::INFINITY);
    }

    #[test]
    fn minute_of_truncates_seconds_and_nanos() {
        let at = OffsetDateTime::from_unix_timestamp(1_800_000_123).unwrap()
            + TimeDuration::milliseconds(456);
        let minute = minute_of(at);
        assert_eq!(minute.unix_timestamp(), 1_800_000_120);
        assert_eq!(minute.second(), 0);
        assert_eq!(minute.nanosecond(), 0);
    }

    #[tokio::test]
    async fn cycle_builds_rollups_and_counters_idempotently() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let minute = minute_of(OffsetDateTime::now_utc());
        store
            .insert_batch(&[
                terminal("e1", "x1", minute + TimeDuration::seconds(5)),
                terminal("e2", "x2", minute + TimeDuration::seconds(20)),
            ])
            .await
            .unwrap();
        let aggregator = aggregator(&store);
        aggregator.cycle().await.unwrap();
        assert_eq!(
            store
                .query_scalar(
                    "SELECT count(*)::text FROM adx_ops_observations.task_minute_rollup \
                     WHERE environment_id='env-1' AND template_release='chat@1.0.0' \
                     AND outcome='success' AND task_count=2"
                )
                .await,
            "1"
        );
        // 模拟"重算后、推水位前崩溃"：把水位回拨再跑一轮 → 同批事件重放，
        // 计数仍为 2（替换不累加），revision 递增。
        store
            .advance_watermark(
                PIPELINE,
                OffsetDateTime::now_utc() - TimeDuration::days(1),
                "",
                "",
            )
            .await
            .unwrap();
        aggregator.cycle().await.unwrap();
        assert_eq!(
            store
                .query_scalar(
                    "SELECT task_count::text FROM adx_ops_observations.task_minute_rollup"
                )
                .await,
            "2"
        );
        assert!(
            store
                .query_scalar("SELECT revision::text FROM adx_ops_observations.task_minute_rollup")
                .await
                .parse::<i64>()
                .unwrap()
                >= 2
        );
    }

    #[tokio::test]
    async fn late_event_recomputes_original_bucket() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let minute = minute_of(OffsetDateTime::now_utc() - TimeDuration::minutes(5));
        store
            .insert_batch(&[terminal("e1", "x1", minute + TimeDuration::seconds(5))])
            .await
            .unwrap();
        let aggregator = aggregator(&store);
        aggregator.cycle().await.unwrap();
        // 迟到事件：occurred_at 仍在同一分钟，received_at 在下一轮之后。
        store
            .insert_batch(&[terminal("e2", "x2", minute + TimeDuration::seconds(40))])
            .await
            .unwrap();
        aggregator.cycle().await.unwrap();
        // 两条事件可能分属 on_time / late 两个 arrival 桶，按分钟求和断言。
        assert_eq!(
            store
                .query_scalar(
                    "SELECT sum(task_count)::text FROM adx_ops_observations.task_minute_rollup"
                )
                .await,
            "2"
        );
    }

    /// C02/D02：同一体验先 success 后 failure，桶里只能有一个终态、一份时长。
    #[tokio::test]
    async fn conflicting_terminal_is_counted_once() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let minute = minute_of(OffsetDateTime::now_utc());
        store
            .insert_batch(&[terminal("e1", "x1", minute + TimeDuration::seconds(5))])
            .await
            .unwrap();
        let mut conflicting = terminal("e2", "x1", minute + TimeDuration::seconds(10))
            .observation()
            .clone();
        conflicting.outcome = Some(Outcome::Failure);
        store
            .insert_batch(&[ReceivedObservation::new(
                "producer-1",
                OffsetDateTime::now_utc(),
                conflicting,
            )
            .unwrap()])
            .await
            .unwrap();
        aggregator(&store).cycle().await.unwrap();
        assert_eq!(
            store
                .query_scalar(
                    "SELECT sum(task_count)::text FROM adx_ops_observations.task_minute_rollup"
                )
                .await,
            "1"
        );
        assert_eq!(
            store
                .query_scalar("SELECT outcome FROM adx_ops_observations.task_minute_rollup")
                .await,
            "success"
        );
    }

    #[tokio::test]
    async fn restart_restores_rollups_without_double_counting() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let minute = minute_of(OffsetDateTime::now_utc());
        store
            .insert_batch(&[terminal("e1", "x1", minute + TimeDuration::seconds(5))])
            .await
            .unwrap();
        aggregator(&store).cycle().await.unwrap();
        // "重启"：全新聚合器实例，从水位恢复后重跑；桶替换语义保证不双计。
        let restarted = aggregator(&store);
        restarted.restore().await.unwrap();
        restarted.cycle().await.unwrap();
        assert_eq!(
            store
                .query_scalar(
                    "SELECT task_count::text FROM adx_ops_observations.task_minute_rollup"
                )
                .await,
            "1"
        );
    }

    #[tokio::test]
    async fn advisory_lock_is_exclusive_between_aggregators() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let first = aggregator(&store);
        first.cycle().await.unwrap();
        assert!(first.is_leader());
        let second = aggregator(&store);
        second.cycle().await.unwrap();
        assert!(!second.is_leader());
        // 优雅释放后第二个才能当选。
        first.release_leadership().await;
        second.cycle().await.unwrap();
        assert!(second.is_leader());
    }

    #[tokio::test]
    async fn render_exports_role_and_terminal_totals() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let minute = minute_of(OffsetDateTime::now_utc());
        store
            .insert_batch(&[terminal("e1", "x1", minute + TimeDuration::seconds(5))])
            .await
            .unwrap();
        let aggregator = aggregator(&store);
        aggregator.cycle().await.unwrap();
        let mut text = Text::default();
        aggregator.render(&mut text);
        let output = text.finish();
        assert!(output.contains("adx_business_export_role{role=\"leader\"} 1"));
        assert!(output.contains(
            "adx_business_task_results_total{template_release=\"chat@1.0.0\",\
             outcome=\"success\",arrival=\"on_time\",start_mode=\"unknown\"} 1"
        ));
        assert!(output.contains(
            "adx_business_task_results_total{template_release=\"chat@1.0.0\",\
             outcome=\"failure\",arrival=\"on_time\",start_mode=\"unknown\"} 0"
        ));
        assert!(output.contains(
            "adx_business_task_duration_seconds_count{template_release=\"chat@1.0.0\",\
             outcome=\"success\"} 1"
        ));
        assert!(output.contains("adx_business_active_users{window=\"5m\"} 1"));
        assert!(output.contains("adx_business_active_sessions{window=\"5m\"} 1"));
        assert!(output.contains("adx_business_daily_active_users 1"));
        assert!(output.contains("adx_observation_watermark_initialized 1"));
    }

    /// 动作计数走分钟桶（不再每轮扫 7 天事件明细），导出口径不变。
    #[tokio::test]
    async fn actions_are_counted_from_minute_rollup() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let minute = minute_of(OffsetDateTime::now_utc());
        store
            .insert_batch(&[
                action_event("a1", "chat@1.0.0", minute + TimeDuration::seconds(5)),
                action_event("a2", "chat@1.0.0", minute + TimeDuration::seconds(6)),
            ])
            .await
            .unwrap();
        let aggregator = aggregator(&store);
        aggregator.cycle().await.unwrap();
        let mut text = Text::default();
        aggregator.render(&mut text);
        let output = text.finish();
        assert!(
            output.contains(
                "adx_business_actions_total{template_release=\"chat@1.0.0\",\
                 action_kind=\"file_upload\",outcome=\"success\"} 2"
            ),
            "{output}"
        );
    }

    /// 12+other 截断按活跃度而非字典序：低频但字典序靠前的模板必须折进
    /// `other`，不能被它挤掉真实热点。
    #[tokio::test]
    async fn template_truncation_prefers_active_templates() {
        let Some((store, _schema)) = fresh_store().await else {
            return;
        };
        let minute = minute_of(OffsetDateTime::now_utc());
        let mut batch = vec![terminal_for(
            "e-low",
            "x-low",
            minute + TimeDuration::seconds(5),
            "aaa@1.0.0",
        )];
        for index in 0..TEMPLATE_LIMIT {
            for copy in 0..2 {
                batch.push(terminal_for(
                    &format!("e-{index}-{copy}"),
                    &format!("x-{index}-{copy}"),
                    minute + TimeDuration::seconds(5),
                    &format!("tpl-{index}@1.0.0"),
                ));
            }
        }
        store.insert_batch(&batch).await.unwrap();
        let aggregator = aggregator(&store);
        aggregator.cycle().await.unwrap();
        let mut text = Text::default();
        aggregator.render(&mut text);
        let output = text.finish();
        // 只看终态族（活跃用户族另有按人次的截断口径，不在本断言范围内）。
        assert!(output.contains("adx_business_task_results_total{template_release=\"tpl-0@1.0.0\""));
        assert!(!output.contains("adx_business_task_results_total{template_release=\"aaa@1.0.0\""));
        assert!(output.contains(
            "adx_business_task_results_total{template_release=\"other\",\
             outcome=\"success\",arrival=\"on_time\",start_mode=\"unknown\"} 1"
        ));
    }
}
