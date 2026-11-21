//! 体验族直埋：Jiuwen 适配器的业务体验指标。
//!
//! 只交付原始需求列出的观测项：首用时延、首输出时延、业务成功率与并发任务。
//! 不采阶段分解：`auth`/`binding`/`model_prepare` 都不在需求条款里，恢复时延/
//! 排队时长由平台侧族交付。
//!
//! 体验 = 一条 `chat.send` 根请求（D12）。[`ExperienceAnchor`] 随 /ws 请求从
//! entrypoint 漂到 driver：首用时延在 backend `connection.ack` 到达时记录，
//! 首输出里程碑在 driver 收到 `session.submit` 后记录。全部指标在本进程直埋，
//! 随既有 health `:18080/metrics` 导出；
//! H3 同时向活跃采集器报一次（`observability::active`），那是唯一落库的业务数据。
#[cfg(feature = "agent-api")]
use crate::ingress::digitalmategateway::observability::ActivitySink;
use adx_core::observation::Outcome;
use adx_observability::metrics::{AtomicHistogram, BucketFamily, HistogramSnapshot, Text, B15};
#[cfg(feature = "agent-api")]
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
#[cfg(feature = "agent-api")]
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
#[cfg(feature = "agent-api")]
use std::time::Duration;
#[cfg(feature = "agent-api")]
use tokio::time::Instant;

/// `template_release` 基数上限：12 个活跃值，超出归 `other`（D4）。
#[cfg(feature = "agent-api")]
const TEMPLATE_LIMIT: usize = 12;
/// `template_release` 编码长度上限（D4，UTF-8 字节）。
#[cfg(feature = "agent-api")]
const TEMPLATE_MAX_BYTES: usize = 96;

/// 业务结果族导出的结果集：`success` 当前恒 0（Jiuwen 无业务终态证据），
/// 保留它以让"成功率"曲线有分母；`cancelled` 不会产生，不导出。
const TASK_OUTCOMES: [Outcome; 3] = [Outcome::Success, Outcome::Failure, Outcome::Unknown];

/// 体验启动模式（详设 §6.4 判定表；值集冻结）。由 Activator 的
/// `ActivationKind` 判定回传：新建/重建为 `cold`，既有绑定从暂停恢复为
/// `resumed`，缓存命中或执行已就绪为 `binding_reuse`；旧 Activator 未提供
/// `activation` 时回落 `unknown`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StartMode {
    Cold,
    Resumed,
    BindingReuse,
    Unknown,
}

impl StartMode {
    const COUNT: usize = 4;
    const ALL: [StartMode; StartMode::COUNT] = [
        StartMode::Cold,
        StartMode::Resumed,
        StartMode::BindingReuse,
        StartMode::Unknown,
    ];
    fn as_str(self) -> &'static str {
        match self {
            StartMode::Cold => "cold",
            StartMode::Resumed => "resumed",
            StartMode::BindingReuse => "binding_reuse",
            StartMode::Unknown => "unknown",
        }
    }
}

/// 首输出里程碑。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum OutputKind {
    FirstEvent,
    FirstText,
}

impl OutputKind {
    const COUNT: usize = 2;
    const ALL: [OutputKind; OutputKind::COUNT] = [OutputKind::FirstEvent, OutputKind::FirstText];
    fn as_str(self) -> &'static str {
        match self {
            OutputKind::FirstEvent => "first_event",
            OutputKind::FirstText => "first_text",
        }
    }
}

/// 体验族指标存储。first_use/first_output 按**已注册
/// template** 稠密导出枚举组合；inflight 无标签、恒导出。
/// 锁均为短临界区、不跨 await。
pub(crate) struct ExperienceMetrics {
    templates: Mutex<BTreeSet<String>>,
    /// 基数溢出发生过至少一次（`other` 参与稠密导出）。
    other_used: AtomicBool,
    /// 首用时延：/ws 认证 t0 → 连接可用（backend connection.ack）。
    first_use: Mutex<BTreeMap<String, AtomicHistogram<15>>>,
    first_output: Mutex<BTreeMap<(String, OutputKind, StartMode), AtomicHistogram<15>>>,
    inflight: AtomicI64,
    /// 模板标签截断到 `other` 的次数（`adx_business_cardinality_overflow_total`）。
    template_overflow: AtomicU64,
    /// 终态结果计数（H5 收到终态响应）：模板 × 结果 × 启动模式。
    task_results: Mutex<BTreeMap<(String, Outcome, StartMode), AtomicU64>>,
}

impl Default for ExperienceMetrics {
    fn default() -> Self {
        Self {
            templates: Mutex::new(BTreeSet::new()),
            other_used: AtomicBool::new(false),
            first_use: Mutex::new(BTreeMap::new()),
            first_output: Mutex::new(BTreeMap::new()),
            inflight: AtomicI64::new(0),
            template_overflow: AtomicU64::new(0),
            task_results: Mutex::new(BTreeMap::new()),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// 记录侧接口：仅 Jiuwen 适配器（agent-api）使用；渲染侧见下方 `render`。
#[cfg(feature = "agent-api")]
impl ExperienceMetrics {
    /// 注册 template，返回生效值：超长度上限、或为新值且超基数上限时归
    /// `other`（D4）。已注册值幂等返回。
    pub(crate) fn effective_template(&self, template: &str) -> String {
        let mut templates = lock(&self.templates);
        if template.len() > TEMPLATE_MAX_BYTES
            || (templates.len() >= TEMPLATE_LIMIT && !templates.contains(template))
        {
            self.other_used.store(true, Ordering::Relaxed);
            self.template_overflow.fetch_add(1, Ordering::Relaxed);
            return "other".to_owned();
        }
        templates.insert(template.to_owned());
        template.to_owned()
    }

    /// 记一个首用时延样本（连接 t0 → 连接可用）。
    fn observe_first_use(&self, template: &str, elapsed: Duration) {
        lock(&self.first_use)
            .entry(template.to_owned())
            .or_default()
            .observe(&B15, elapsed.as_secs_f64());
    }

    /// 记一个首输出里程碑样本。
    fn observe_first_output(
        &self,
        template: &str,
        kind: OutputKind,
        mode: StartMode,
        elapsed: Duration,
    ) {
        lock(&self.first_output)
            .entry((template.to_owned(), kind, mode))
            .or_default()
            .observe(&B15, elapsed.as_secs_f64());
    }

    /// 体验接受（H3）：inflight +1。
    fn task_opened(&self) {
        self.inflight.fetch_add(1, Ordering::Relaxed);
    }

    /// 体验闭合（H5 终态/超时/Drop 清场）：inflight -1。
    fn task_closed(&self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }

    /// H5 终态：结果计数直埋。
    ///
    /// `outcome=unknown` 是 Jiuwen 当前的真实结果（协议层"响应结束"不是业务
    /// 成功证据），必须原样导出，不能用 unknown 冒充 success。
    pub(crate) fn task_terminal(&self, template: &str, outcome: Outcome, mode: StartMode) {
        lock(&self.task_results)
            .entry((template.to_owned(), outcome, mode))
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl ExperienceMetrics {
    /// 渲染体验族四族（追加到既有 /metrics 文本）。
    pub(crate) fn render(&self, out: &mut Text) {
        let mut templates = lock(&self.templates).clone();
        if self.other_used.load(Ordering::Relaxed) {
            templates.insert("other".to_owned());
        }
        {
            let first_use = lock(&self.first_use);
            for template in &templates {
                render_histogram(
                    out,
                    "adx_experience_first_use_seconds",
                    "Connection request start to connection ready (backend \
                     connection.ack); unit is seconds",
                    &[("template_release", template.as_str())],
                    &B15,
                    first_use.get(template).map(AtomicHistogram::snapshot),
                );
            }
        }
        {
            let first_output = lock(&self.first_output);
            for template in &templates {
                for kind in OutputKind::ALL {
                    for mode in StartMode::ALL {
                        render_histogram(
                            out,
                            "adx_business_first_output_seconds",
                            "Chat submit to the first business output \
                             (first_event/first_text)",
                            &[
                                ("template_release", template.as_str()),
                                ("output_kind", kind.as_str()),
                                ("start_mode", mode.as_str()),
                            ],
                            &B15,
                            first_output
                                .get(&(template.clone(), kind, mode))
                                .map(AtomicHistogram::snapshot),
                        );
                    }
                }
            }
        }
        out.gauge(
            "adx_business_tasks_inflight",
            &[],
            self.inflight.load(Ordering::Relaxed).max(0) as u64,
        );
        // 标签基数溢出：两个族共享同一 template_release 注册表（D4）。
        let overflow = self.template_overflow.load(Ordering::Relaxed);
        for metric in [
            "adx_experience_first_use_seconds",
            "adx_business_first_output_seconds",
        ] {
            out.counter(
                "adx_business_cardinality_overflow_total",
                "Label values truncated to \"other\" by cardinality limits",
                &[
                    ("metric", metric.to_owned()),
                    ("label", "template_release".to_owned()),
                ],
                overflow,
            );
        }
        // 终态结果（H5 直埋）：去重由"一条 chat.send 只落一个副本"保证，
        // 跨副本求和即精确，不需要落库或聚合器。
        {
            let results = lock(&self.task_results);
            for template in &templates {
                for outcome in TASK_OUTCOMES {
                    for mode in StartMode::ALL {
                        out.counter(
                            "adx_business_task_results_total",
                            "Terminal business tasks by outcome (unknown = no business \
                             success evidence yet)",
                            &[
                                ("template_release", template.clone()),
                                ("outcome", outcome.as_str().to_owned()),
                                ("start_mode", mode.as_str().to_owned()),
                            ],
                            results
                                .get(&(template.clone(), outcome, mode))
                                .map_or(0, |counter| counter.load(Ordering::Relaxed)),
                        );
                    }
                }
            }
        }
    }
}

/// 渲染一个直方图组合；`None` 快照渲染全零（稠密导出，U4 语义）。
fn render_histogram(
    out: &mut Text,
    name: &str,
    help: &str,
    labels: &[(&str, &str)],
    family: &BucketFamily,
    snapshot: Option<HistogramSnapshot>,
) {
    let zero = HistogramSnapshot {
        buckets: vec![0; family.labels.len()],
        count: 0,
        sum_seconds: 0.0,
    };
    let snapshot = snapshot.as_ref().unwrap_or(&zero);
    let labels: Vec<(&str, String)> = labels
        .iter()
        .map(|(key, value)| (*key, (*value).to_owned()))
        .collect();
    out.histogram(name, help, &labels, family, snapshot);
}

/// 体验锚点：随 /ws 请求从 entrypoint 漂到 driver（详设 §6.4 M2-4a）。
/// `started` = /ws 请求进入认证流程的时刻，仅作为 `first_use`（连接可用）
/// 的 t0；首个 Chat 的首输出时延以 `session.submit` 时刻为 t0，不能把用户
/// 建连后的空闲等待算进业务时延。活跃采集的可选依赖与脱敏引用挂在锚点上，
/// driver 的 H3/H4/H5 无需额外穿参。
#[cfg(feature = "agent-api")]
#[derive(Clone)]
pub struct ExperienceAnchor {
    metrics: Arc<ExperienceMetrics>,
    activity: Option<Arc<ActivitySink>>,
    /// 生效模板（指标标签值，超基数上限归 `other`）。
    template_release: String,
    tenant_ref: String,
    /// `userRef`（HMAC）；无活跃采集器时为 None。
    user_ref: Option<String>,
    /// 启动模式（H2 后由 Activator 判定回填）；未知时保持 unknown。
    start_mode: StartMode,
    started: Instant,
}

#[cfg(feature = "agent-api")]
impl ExperienceAnchor {
    pub(crate) fn new(
        metrics: Arc<ExperienceMetrics>,
        activity: Option<Arc<ActivitySink>>,
        template_release: &str,
        started: Instant,
        tenant_ref: &str,
        user_id: &str,
    ) -> Self {
        let effective = metrics.effective_template(template_release);
        let user_ref = activity
            .as_ref()
            .map(|sink| sink.hmac_ref(tenant_ref, user_id));
        Self {
            metrics,
            activity,
            template_release: effective,
            tenant_ref: tenant_ref.to_owned(),
            user_ref,
            start_mode: StartMode::Unknown,
            started,
        }
    }

    /// H2 之后回填启动模式（Activator 判定；不改变已记录的阶段样本）。
    pub(super) fn with_start_mode(mut self, mode: StartMode) -> Self {
        self.start_mode = mode;
        self
    }

    pub(super) fn start_mode(&self) -> StartMode {
        self.start_mode
    }

    /// 首用时延：t0（/ws 进入认证）→ 连接可用（环境就绪 + 后端建连 + 会话校验）。
    pub(super) fn observe_first_use(&self, elapsed: Duration) {
        self.metrics
            .observe_first_use(&self.template_release, elapsed);
    }

    pub(super) fn started(&self) -> Instant {
        self.started
    }

    /// 记一次活跃（H3 受理）：`session_id` 由客户端自报，这里做 HMAC 再落库。
    pub(super) fn record_activity(&self, session_id: Option<&str>) {
        let (Some(activity), Some(user_ref)) = (&self.activity, &self.user_ref) else {
            return;
        };
        let session_ref = session_id.map(|id| activity.hmac_ref(&self.tenant_ref, id));
        activity.record(
            &self.tenant_ref,
            user_ref,
            session_ref.as_deref(),
            &self.template_release,
        );
    }
}

/// Activator 的判定结果是 H2 的唯一来源，映射在这里收敛一处。
#[cfg(feature = "agent-api")]
impl From<adx_agent_core::activator::ActivationKind> for StartMode {
    fn from(mode: adx_agent_core::activator::ActivationKind) -> Self {
        match mode {
            adx_agent_core::activator::ActivationKind::Cold => Self::Cold,
            adx_agent_core::activator::ActivationKind::Resumed => Self::Resumed,
            adx_agent_core::activator::ActivationKind::BindingReuse => Self::BindingReuse,
            adx_agent_core::activator::ActivationKind::Unknown => Self::Unknown,
        }
    }
}

/// 投影帧的首输出分类（详设 §6.4 分类表；输入是投影后的帧）。
#[cfg(feature = "agent-api")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameClass {
    /// 不计任何里程碑（控制面应答/错误/空帧）。
    NotOutput,
    /// 只计 first_event（reasoning/工具/中间事件/非文本内容）。
    Event,
    /// first_event＋first_text（文本 delta 与 chat.final 兜底）。
    Text,
}

/// 错误帧判定（`chat.error`/`runtime.error`）：可见的失败信号，但不计首输出。
#[cfg(feature = "agent-api")]
fn is_error_frame(frame: &Value) -> bool {
    frame.get("type").and_then(Value::as_str) == Some("event")
        && matches!(
            frame.get("event").and_then(Value::as_str),
            Some("chat.error") | Some("runtime.error")
        )
}

#[cfg(feature = "agent-api")]
pub(super) fn classify_frame(frame: &Value) -> FrameClass {
    if frame.get("type").and_then(Value::as_str) != Some("event") {
        return FrameClass::NotOutput;
    }
    let event = frame.get("event").and_then(Value::as_str).unwrap_or("");
    match event {
        // 连接 ack 与错误不是业务输出；错误可见性走终态 outcome=failure。
        "connection.ack" | "chat.error" | "runtime.error" => FrameClass::NotOutput,
        "chat.delta" => {
            let payload = frame.get("payload");
            let reasoning = payload
                .and_then(|payload| payload.get("source_chunk_type"))
                .and_then(Value::as_str)
                == Some("llm_reasoning");
            match payload.and_then(|payload| payload.get("content")) {
                Some(Value::String(text)) if !text.is_empty() => {
                    if reasoning {
                        FrameClass::Event
                    } else {
                        FrameClass::Text
                    }
                }
                // 空串/缺失/Null：空 delta。
                Some(Value::String(_)) | Some(Value::Null) | None => FrameClass::NotOutput,
                // 非文本内容：可观察输出，但不是文本。
                Some(_) => FrameClass::Event,
            }
        }
        // 兜底：final 到达即视为已给出输出。
        "chat.final" => FrameClass::Text,
        _ => FrameClass::Event,
    }
}

/// driver 本地观察状态：open 体验表＋Drop 清场。
///
/// driver 正常返回或被取消（前端断连/路由撤销时未来被直接丢弃）都会走
/// [`Drop`]：遗留 open 体验只闭合 inflight，不产生结果（未决不伪装失败），
/// 并以 debug 日志留下"有多少体验无终态"的现场。
#[cfg(feature = "agent-api")]
pub(super) struct DriverObservation {
    anchor: Option<ExperienceAnchor>,
    /// 后端 `connection.ack` 到达后结算一次首用时延；ACK 缺失时不产生样本。
    first_use_done: bool,
    open: BTreeMap<String, TaskRecord>,
}

/// 一个 open 体验（Chat 请求接受后到终态前）。
#[cfg(feature = "agent-api")]
struct TaskRecord {
    /// 体验起点：每条 Chat 都用 `session.submit` 时刻；建连等待只属于
    /// `first_use`，不能混入首输出时延。
    experience_started: Instant,
    first_event_at: Option<Instant>,
    first_text_at: Option<Instant>,
    start_mode: StartMode,
    /// 见到 `chat.error`/`runtime.error` → 终态按失败计数。
    failed: bool,
}

/// 闭合原因：只有 `Terminal`（收到终态响应）才计入任务结果；
/// deadline 与 Drop 清场结果未知——未决不伪装失败。
#[cfg(feature = "agent-api")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Close {
    Terminal,
    Unknown,
}

#[cfg(feature = "agent-api")]
impl DriverObservation {
    pub(super) fn new(anchor: Option<ExperienceAnchor>) -> Self {
        Self {
            anchor,
            first_use_done: false,
            open: BTreeMap::new(),
        }
    }

    /// 后端 `connection.ack` 到达、driver 首次接收请求时结算首用时延。
    /// ACK 缺失时 driver 不会进入就绪状态，也就不会产生样本。
    pub(super) fn connection_ready(&mut self, now: Instant) {
        let Some(anchor) = &self.anchor else { return };
        if self.first_use_done {
            return;
        }
        self.first_use_done = true;
        anchor.observe_first_use(now - anchor.started());
    }

    /// H3：Chat 体验接受（`session.submit` Ok；非 Chat 由调用方过滤，submit
    /// 失败不开——未接受）。`backend_id` 是 submit 分配的后端 UUID。
    pub(super) fn task_opened(&mut self, backend_id: &str, session_id: Option<&str>, now: Instant) {
        let Some(anchor) = &self.anchor else { return };
        // 同一 backend 重复开启（重放/补偿路径）不双计 inflight 与接受量；
        // 后到的记录以首次为准。
        if self.open.contains_key(backend_id) {
            return;
        }
        // 首输出从 submit 开始计时；`anchor.started()` 只用于 first_use，
        // 否则用户建连后空闲等待会被错误计入首输出时延。
        let experience_started = now;
        anchor.metrics.task_opened();
        anchor.record_activity(session_id);
        self.open.insert(
            backend_id.to_owned(),
            TaskRecord {
                experience_started,
                first_event_at: None,
                first_text_at: None,
                start_mode: anchor.start_mode(),
                failed: false,
            },
        );
    }

    /// H4：归因一个投影帧（`backend_id` 为空或不对应 open 体验时 no-op）。
    pub(super) fn frame_seen(&mut self, backend_id: Option<&str>, frame: &Value, now: Instant) {
        let Some(anchor) = &self.anchor else { return };
        let Some(id) = backend_id else { return };
        let Some(record) = self.open.get_mut(id) else {
            return;
        };
        // 错误帧是可见信号（计入终态结果），但不是业务输出（不计首输出）。
        if is_error_frame(frame) {
            record.failed = true;
        }
        let class = classify_frame(frame);
        if class != FrameClass::NotOutput && record.first_event_at.is_none() {
            record.first_event_at = Some(now);
            anchor.metrics.observe_first_output(
                &anchor.template_release,
                OutputKind::FirstEvent,
                record.start_mode,
                now - record.experience_started,
            );
        }
        if class == FrameClass::Text && record.first_text_at.is_none() {
            record.first_text_at = Some(now);
            anchor.metrics.observe_first_output(
                &anchor.template_release,
                OutputKind::FirstText,
                record.start_mode,
                now - record.experience_started,
            );
        }
    }

    /// H5：体验闭合。`Terminal`（收到终态响应）计入任务结果；
    /// deadline/Drop 只闭合 inflight，不产生结果（未决不伪装失败）。
    pub(super) fn task_closed(&mut self, backend_id: Option<&str>, close: Close) {
        let Some(anchor) = &self.anchor else { return };
        let Some(id) = backend_id else { return };
        let Some(record) = self.open.remove(id) else {
            return;
        };
        anchor.metrics.task_closed();
        if close != Close::Terminal {
            return;
        }
        // 协议层"响应结束"不等于业务任务成功：没有业务终态证据时落
        // `unknown`，不进入成功率分子；只有明确的错误帧才记 failure。
        let outcome = if record.failed {
            Outcome::Failure
        } else {
            Outcome::Unknown
        };
        anchor
            .metrics
            .task_terminal(&anchor.template_release, outcome, record.start_mode);
    }
}

#[cfg(feature = "agent-api")]
impl Drop for DriverObservation {
    fn drop(&mut self) {
        let Some(anchor) = &self.anchor else { return };
        if !self.open.is_empty() {
            tracing::debug!(
                open = self.open.len(),
                "Jiuwen driver dropped with open experiences; inflight closed without a terminal result"
            );
        }
        for _ in self.open.values() {
            anchor.metrics.task_closed();
        }
    }
}

#[cfg(all(test, feature = "agent-api"))]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_anchor(template: &str) -> (Arc<ExperienceMetrics>, ExperienceAnchor) {
        let metrics = Arc::new(ExperienceMetrics::default());
        let anchor = ExperienceAnchor::new(
            Arc::clone(&metrics),
            None,
            template,
            Instant::now(),
            "tenant-1",
            "user-1",
        );
        (metrics, anchor)
    }

    fn render(metrics: &ExperienceMetrics) -> String {
        let mut text = Text::default();
        metrics.render(&mut text);
        text.finish()
    }

    /// H5 终态直埋：结果计数；`unknown` 必须原样导出（不冒充成功）。
    #[test]
    fn terminal_results_are_direct_metrics() {
        let (metrics, _anchor) = make_anchor("tpl@1");
        metrics.task_terminal("tpl@1", Outcome::Unknown, StartMode::Cold);
        metrics.task_terminal("tpl@1", Outcome::Failure, StartMode::Cold);
        let text = render(&metrics);
        assert_eq!(
            sample_value(
                &text,
                "adx_business_task_results_total{template_release=\"tpl@1\",\
                 outcome=\"unknown\",start_mode=\"cold\"}"
            ),
            Some(1)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_business_task_results_total{template_release=\"tpl@1\",\
                 outcome=\"failure\",start_mode=\"cold\"}"
            ),
            Some(1)
        );
    }

    /// 取样本 `name{labels}` 的值；`None` 表示样本不存在。
    fn sample_value(text: &str, sample: &str) -> Option<u64> {
        text.lines()
            .find(|line| {
                line.starts_with(sample) && line.as_bytes().get(sample.len()) == Some(&b' ')
            })
            .and_then(|line| line.split(' ').nth(1))
            .and_then(|value| value.parse().ok())
    }

    /// 分类表（详设 §6.4 M2-4a）：控制面/错误/空帧不计；reasoning 与非文本
    /// 内容只计 first_event；文本 delta 与 chat.final 计两者。
    #[test]
    fn classify_frame_table() {
        for (frame, expected) in [
            (
                json!({"type":"res","id":"1","ok":true,"payload":{"accepted":true}}),
                FrameClass::NotOutput,
            ),
            (
                json!({"type":"event","event":"connection.ack","payload":{}}),
                FrameClass::NotOutput,
            ),
            (
                json!({"type":"event","event":"chat.error","payload":{"is_complete":true}}),
                FrameClass::NotOutput,
            ),
            (
                json!({"type":"event","event":"chat.delta","payload":{"content":""}}),
                FrameClass::NotOutput,
            ),
            (
                json!({"type":"event","event":"chat.delta","payload":{}}),
                FrameClass::NotOutput,
            ),
            (
                json!({"type":"event","event":"chat.delta","payload":{"content":"思考","source_chunk_type":"llm_reasoning"}}),
                FrameClass::Event,
            ),
            (
                json!({"type":"event","event":"chat.delta","payload":{"content":"hello"}}),
                FrameClass::Text,
            ),
            (
                json!({"type":"event","event":"chat.delta","payload":{"content":{"tool":"state"}}}),
                FrameClass::Event,
            ),
            (
                json!({"type":"event","event":"tool.progress","payload":{}}),
                FrameClass::Event,
            ),
            (
                json!({"type":"event","event":"chat.processing_status","payload":{"is_processing":true}}),
                FrameClass::Event,
            ),
            (
                json!({"type":"event","event":"chat.final","payload":{}}),
                FrameClass::Text,
            ),
            (
                json!({"event":"chat.delta","payload":{"content":"x"}}),
                FrameClass::NotOutput,
            ),
        ] {
            assert_eq!(classify_frame(&frame), expected, "{frame}");
        }
    }

    /// 同一体验内里程碑恰好一次；重复 Text 帧不重复记。
    #[test]
    fn milestones_record_exactly_once() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        let frame = json!({"type":"event","event":"chat.delta","payload":{"content":"hello"}});
        observation.frame_seen(Some("b1"), &frame, Instant::now());
        observation.frame_seen(Some("b1"), &frame, Instant::now());
        observation.frame_seen(
            Some("b1"),
            &json!({"type":"event","event":"chat.final","payload":{}}),
            Instant::now(),
        );
        let text = render(&metrics);
        assert_eq!(
            sample_value(
                &text,
                "adx_business_first_output_seconds_count{template_release=\"tpl@1\",\
                 output_kind=\"first_event\",start_mode=\"unknown\"}"
            ),
            Some(1)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_business_first_output_seconds_count{template_release=\"tpl@1\",\
                 output_kind=\"first_text\",start_mode=\"unknown\"}"
            ),
            Some(1)
        );
    }

    /// 首用时延在 driver 收到后端 ACK 时结算一次；重复调用不重复计数。
    #[test]
    fn first_use_settles_once_when_connection_ready() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.connection_ready(Instant::now());
        observation.connection_ready(Instant::now());
        let text = render(&metrics);
        assert_eq!(
            sample_value(
                &text,
                "adx_experience_first_use_seconds_count{template_release=\"tpl@1\"}"
            ),
            Some(1)
        );
    }

    /// 同一 backend 重复开启不双计 inflight 与接受量（重放/补偿路径）。
    #[test]
    fn duplicate_task_open_does_not_double_count() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        observation.task_opened("b1", Some("s1"), Instant::now());
        let text = render(&metrics);
        assert_eq!(sample_value(&text, "adx_business_tasks_inflight"), Some(1));
        // 只闭合一次就应归零：不存在第二份幽灵记录。
        observation.task_closed(Some("b1"), Close::Terminal);
        let text = render(&metrics);
        assert_eq!(sample_value(&text, "adx_business_tasks_inflight"), Some(0));
    }

    /// 终态闭合：inflight 归零，并记一次 `outcome=unknown` 的任务结果（无错误帧）。
    #[test]
    fn task_closed_drains_inflight() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        observation.task_closed(Some("b1"), Close::Terminal);
        drop(observation);
        let text = render(&metrics);
        assert_eq!(sample_value(&text, "adx_business_tasks_inflight"), Some(0));
    }

    /// Drop 清场：遗留 open 体验只闭合 inflight，不产生结果。
    #[test]
    fn drop_sweep_closes_inflight() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        observation.task_opened("b2", Some("s1"), Instant::now());
        drop(observation);
        let text = render(&metrics);
        assert_eq!(sample_value(&text, "adx_business_tasks_inflight"), Some(0));
        assert_eq!(
            sample_value(
                &text,
                "adx_business_task_results_total{template_release=\"tpl@1\",\
                 outcome=\"unknown\",start_mode=\"unknown\"}"
            ),
            Some(0)
        );
    }

    /// 锚点关闭（观测开关 off）时一切 no-op。
    #[test]
    fn disabled_anchor_is_silent() {
        let mut observation = DriverObservation::new(None);
        observation.task_opened("b1", Some("s1"), Instant::now());
        observation.frame_seen(
            Some("b1"),
            &json!({"type":"event","event":"chat.delta","payload":{"content":"x"}}),
            Instant::now(),
        );
        observation.task_closed(Some("b1"), Close::Terminal);
        drop(observation);
    }

    /// 渲染：HELP/TYPE、已注册 template 稠密导出、基数与长度超限归 `other`、
    /// inflight 无标签恒导出。
    #[test]
    fn render_is_dense_and_caps_templates() {
        let metrics = Arc::new(ExperienceMetrics::default());
        for index in 0..13 {
            let anchor = ExperienceAnchor::new(
                Arc::clone(&metrics),
                None,
                &format!("t{index}@1"),
                Instant::now(),
                "tenant-1",
                "user-1",
            );
            anchor.observe_first_use(Duration::from_millis(5));
        }
        let text = render(&metrics);
        assert!(text.contains("# HELP adx_experience_first_use_seconds"));
        assert!(text.contains("# TYPE adx_experience_first_use_seconds histogram"));
        // 第 13 个 template 归 other，样本落在 other 下。
        assert!(
            text.contains("adx_experience_first_use_seconds_count{template_release=\"other\"} 1")
        );
        // 已注册 template 稠密导出未观测过的（零样本）。
        assert_eq!(
            sample_value(
                &text,
                "adx_experience_first_use_seconds_count{template_release=\"t0@1\"}"
            ),
            Some(1)
        );
        // 长度超限同样归 other。
        let long = "x".repeat(TEMPLATE_MAX_BYTES + 1);
        let anchor = ExperienceAnchor::new(
            Arc::clone(&metrics),
            None,
            &long,
            Instant::now(),
            "tenant-1",
            "user-1",
        );
        anchor.observe_first_use(Duration::from_millis(1));
        let text = render(&metrics);
        assert!(
            text.contains("adx_experience_first_use_seconds_count{template_release=\"other\"} 2")
        );
        // inflight 无标签、恒导出。
        assert!(text.contains("adx_business_tasks_inflight 0"));
        // 基数溢出计数：第 13 个模板 + 超长模板各计一次，两个共享注册表的族都导出。
        for metric in [
            "adx_experience_first_use_seconds",
            "adx_business_first_output_seconds",
        ] {
            assert!(text.contains(&format!(
                "adx_business_cardinality_overflow_total{{metric=\"{metric}\",\
                 label=\"template_release\"}} 2"
            )));
        }
    }
}
