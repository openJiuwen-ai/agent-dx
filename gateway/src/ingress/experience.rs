//! 体验族直埋：Jiuwen 适配器的业务体验指标。
//!
//! 体验 = 一条 `chat.send` 根请求（D12）。[`ExperienceAnchor`] 随 /ws 请求从
//! entrypoint 漂到 driver：连接建立阶段（auth/binding/model_prepare）在
//! entrypoint 记录；首输出里程碑、first_use 与未决清场在 driver 记录。存储为
//! 进程内直埋，随既有 health `:18080/metrics` 导出；事件发射
//! BusinessObservation本模块不涉及。
use adx_core::observation::Outcome;
#[cfg(feature = "agent-api")]
use adx_observability::business::{
    BusinessObservation, BusinessType, EventType as BusinessEventType, ReasonClass,
    StartMode as BusinessStartMode, SCHEMA_VERSION,
};
use adx_observability::metrics::{AtomicHistogram, HistogramSnapshot, Text, B15};
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
use time::OffsetDateTime;
#[cfg(feature = "agent-api")]
use tokio::time::Instant;

/// `template_release` 基数上限：12 个活跃值，超出归 `other`（D4）。
#[cfg(feature = "agent-api")]
const TEMPLATE_LIMIT: usize = 12;
/// `template_release` 编码长度上限（D4，UTF-8 字节）。
#[cfg(feature = "agent-api")]
const TEMPLATE_MAX_BYTES: usize = 96;

/// 体验阶段。v1 只产生 auth/binding/
/// model_prepare/first_use 样本；environment_wait/resume_wait 的真值源在
/// agent_service 内，接入前不出样本（不臆造）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Phase {
    Auth,
    Binding,
    EnvironmentWait,
    ResumeWait,
    ModelPrepare,
    FirstUse,
}

impl Phase {
    const COUNT: usize = 6;
    const ALL: [Phase; Phase::COUNT] = [
        Phase::Auth,
        Phase::Binding,
        Phase::EnvironmentWait,
        Phase::ResumeWait,
        Phase::ModelPrepare,
        Phase::FirstUse,
    ];
    fn as_str(self) -> &'static str {
        match self {
            Phase::Auth => "auth",
            Phase::Binding => "binding",
            Phase::EnvironmentWait => "environment_wait",
            Phase::ResumeWait => "resume_wait",
            Phase::ModelPrepare => "model_prepare",
            Phase::FirstUse => "first_use",
        }
    }
}

/// 体验启动模式（详设 §6.4 判定表；值集冻结）。v1 恒 `unknown`——cold/resumed/
/// binding_reuse 的判定源（Coordinator 目录、ENV_RESUMING 旁证、Activator 缓存
/// 命中回传）均未就绪，不猜。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum StartMode {
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

/// v1 只产生 client_gone/backend_gone；
/// no_terminal/clock_skew 需要跨进程对账，属事件链。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum UnresolvedReason {
    ClientGone,
    BackendGone,
    NoTerminal,
    ClockSkew,
}

impl UnresolvedReason {
    const COUNT: usize = 4;
    const ALL: [UnresolvedReason; UnresolvedReason::COUNT] = [
        UnresolvedReason::ClientGone,
        UnresolvedReason::BackendGone,
        UnresolvedReason::NoTerminal,
        UnresolvedReason::ClockSkew,
    ];
    fn as_str(self) -> &'static str {
        match self {
            UnresolvedReason::ClientGone => "client_gone",
            UnresolvedReason::BackendGone => "backend_gone",
            UnresolvedReason::NoTerminal => "no_terminal",
            UnresolvedReason::ClockSkew => "clock_skew",
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

/// 体验族指标存储。phase/first_output 按**已注册
/// template** 稠密导出枚举组合；inflight/unresolved 无 template 标签、恒稠密。
/// 锁均为短临界区、不跨 await。
pub(crate) struct ExperienceMetrics {
    templates: Mutex<BTreeSet<String>>,
    /// 基数溢出发生过至少一次（`other` 参与稠密导出）。
    other_used: AtomicBool,
    phases: Mutex<BTreeMap<(String, Phase, Outcome), AtomicHistogram<15>>>,
    first_output: Mutex<BTreeMap<(String, OutputKind, StartMode), AtomicHistogram<15>>>,
    inflight: [AtomicI64; StartMode::COUNT],
    unresolved: [AtomicU64; UnresolvedReason::COUNT],
    /// 模板标签截断到 `other` 的次数（`adx_observation_cardinality_overflow_total`）。
    template_overflow: AtomicU64,
    /// 全部 Chat 根体验数（H3，按生效模板）——覆盖率分母。
    root_experiences: Mutex<BTreeMap<String, AtomicU64>>,
    /// 其中已进入事件链（发射 accepted）的体验数——覆盖率分子。
    instrumented_experiences: Mutex<BTreeMap<String, AtomicU64>>,
}

impl Default for ExperienceMetrics {
    fn default() -> Self {
        Self {
            templates: Mutex::new(BTreeSet::new()),
            other_used: AtomicBool::new(false),
            phases: Mutex::new(BTreeMap::new()),
            first_output: Mutex::new(BTreeMap::new()),
            inflight: [const { AtomicI64::new(0) }; StartMode::COUNT],
            unresolved: [const { AtomicU64::new(0) }; UnresolvedReason::COUNT],
            template_overflow: AtomicU64::new(0),
            root_experiences: Mutex::new(BTreeMap::new()),
            instrumented_experiences: Mutex::new(BTreeMap::new()),
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
    fn effective_template(&self, template: &str) -> String {
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

    /// 记一个阶段样本（连接建立阶段＋first_use）。
    fn observe_phase(&self, template: &str, phase: Phase, outcome: Outcome, elapsed: Duration) {
        lock(&self.phases)
            .entry((template.to_owned(), phase, outcome))
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
    fn task_opened(&self, mode: StartMode) {
        self.inflight[mode as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// 覆盖率分母：一个 Chat 根体验（无论是否进入事件链）。
    fn root_opened(&self, template: &str) {
        lock(&self.root_experiences)
            .entry(template.to_owned())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 覆盖率分子：该体验已发射 accepted（进入事件链）。
    fn instrumented_opened(&self, template: &str) {
        lock(&self.instrumented_experiences)
            .entry(template.to_owned())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 体验闭合（H5 终态/超时/Drop 清场）：inflight -1。
    fn task_closed(&self, mode: StartMode) {
        self.inflight[mode as usize].fetch_sub(1, Ordering::Relaxed);
    }

    /// 未决清场：driver 退出或被丢弃时仍有 open 体验。
    fn task_unresolved(&self, reason: UnresolvedReason) {
        self.unresolved[reason as usize].fetch_add(1, Ordering::Relaxed);
    }
}

impl ExperienceMetrics {
    /// 渲染体验族四族（追加到既有 /metrics 文本）。
    pub(super) fn render(&self, out: &mut Text) {
        let mut templates = lock(&self.templates).clone();
        if self.other_used.load(Ordering::Relaxed) {
            templates.insert("other".to_owned());
        }
        {
            let phases = lock(&self.phases);
            for template in &templates {
                for phase in Phase::ALL {
                    for outcome in Outcome::ALL {
                        render_histogram(
                            out,
                            "adx_experience_phase_duration_seconds",
                            "Per-phase wait inside one business experience; unit is \
                             experience_id. Phases may overlap; never sum across phases.",
                            &[
                                ("phase", phase.as_str()),
                                ("outcome", outcome.as_str()),
                                ("template_release", template.as_str()),
                            ],
                            phases
                                .get(&(template.clone(), phase, *outcome))
                                .map(AtomicHistogram::snapshot),
                        );
                    }
                }
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
                            "Experience start to the first business output \
                             (first_event/first_text)",
                            &[
                                ("template_release", template.as_str()),
                                ("output_kind", kind.as_str()),
                                ("start_mode", mode.as_str()),
                            ],
                            first_output
                                .get(&(template.clone(), kind, mode))
                                .map(AtomicHistogram::snapshot),
                        );
                    }
                }
            }
        }
        for mode in StartMode::ALL {
            out.gauge(
                "adx_business_tasks_inflight",
                &[("start_mode", mode.as_str().to_owned())],
                self.inflight[mode as usize].load(Ordering::Relaxed).max(0) as u64,
            );
        }
        for reason in UnresolvedReason::ALL {
            out.gauge(
                "adx_business_tasks_unresolved",
                &[("reason", reason.as_str().to_owned())],
                self.unresolved[reason as usize].load(Ordering::Relaxed),
            );
        }
        // 标签基数溢出：两个族共享同一 template_release 注册表（D4）。
        let overflow = self.template_overflow.load(Ordering::Relaxed);
        for metric in [
            "adx_experience_phase_duration_seconds",
            "adx_business_first_output_seconds",
        ] {
            out.counter(
                "adx_observation_cardinality_overflow_total",
                "Label values truncated to \"other\" by cardinality limits",
                &[
                    ("metric", metric.to_owned()),
                    ("label", "template_release".to_owned()),
                ],
                overflow,
            );
        }
        // 覆盖率（M-A 系列）：已进入事件链的体验 / 全部 Chat 根体验。
        // 分母为 0（该模板无根体验）不导出——No data 语义（§5.4）。
        {
            let roots = lock(&self.root_experiences);
            let instrumented = lock(&self.instrumented_experiences);
            for template in &templates {
                let root = roots
                    .get(template)
                    .map(|counter| counter.load(Ordering::Relaxed))
                    .unwrap_or(0);
                if root == 0 {
                    continue;
                }
                let covered = instrumented
                    .get(template)
                    .map(|counter| counter.load(Ordering::Relaxed))
                    .unwrap_or(0);
                out.gauge_f64(
                    "adx_business_instrumentation_coverage_ratio",
                    "Instrumented experiences divided by all root experiences (per ingress instance)",
                    &[
                        ("business_type", "jiuwen".to_owned()),
                        ("template_release", template.clone()),
                    ],
                    covered as f64 / root as f64,
                );
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
    snapshot: Option<HistogramSnapshot>,
) {
    let zero = HistogramSnapshot {
        buckets: vec![0; B15.labels.len()],
        count: 0,
        sum_seconds: 0.0,
    };
    let snapshot = snapshot.as_ref().unwrap_or(&zero);
    let labels: Vec<(&str, String)> = labels
        .iter()
        .map(|(key, value)| (*key, (*value).to_owned()))
        .collect();
    out.histogram(name, help, &labels, &B15, snapshot);
}

/// 体验锚点：随 /ws 请求从 entrypoint 漂到 driver（详设 §6.4 M2-4a/M2-4b）。
/// `started` = /ws 请求进入认证流程的时刻——连接上首个 Chat 体验的起点，
/// 也是 `first_use` 阶段的 t0。事件发射的可选依赖与脱敏引用也挂在锚点上，
/// driver 的 H3/H4/H5 无需额外穿参。
#[cfg(feature = "agent-api")]
#[derive(Clone)]
pub struct ExperienceAnchor {
    metrics: Arc<ExperienceMetrics>,
    sink: Option<Arc<super::observability::ObservationSink>>,
    /// 生效模板（指标标签值）。`template_release` 是这里的事件模板字段。
    template_release: String,
    /// 事件用模板串：原始值满足 `{template}@{version}` 且不超长才发送。
    event_template_release: Option<String>,
    tenant_ref: String,
    /// `userRef`（HMAC）；无发射器时为 None。
    user_ref: Option<String>,
    /// 启动模式（H2 后由 Activator 判定回填；M2-4）；未知时保持 unknown。
    start_mode: StartMode,
    /// 环境身份（H2 选中服务后回填）；未选中前为 None。
    environment_id: Option<String>,
    /// 请求入口的 W3C trace/span（H1 捕获，供事件跳转既有 Trace）。
    trace_id: Option<String>,
    span_id: Option<String>,
    started: Instant,
}

#[cfg(feature = "agent-api")]
impl ExperienceAnchor {
    pub(crate) fn new(
        metrics: Arc<ExperienceMetrics>,
        sink: Option<Arc<super::observability::ObservationSink>>,
        template_release: &str,
        started: Instant,
        tenant_ref: &str,
        user_id: &str,
    ) -> Self {
        let effective = metrics.effective_template(template_release);
        let event_template_release = (template_release.len() <= TEMPLATE_MAX_BYTES
            && template_release.contains('@')
            && !template_release.trim().is_empty())
        .then(|| template_release.to_owned());
        let user_ref = sink.as_ref().map(|sink| sink.hmac_ref(tenant_ref, user_id));
        // 复用既有 Trace：入口请求的 span 已由 server.rs 建立，事件只需带着它
        // 跳转到既有 Tempo 页面，不另建关联体系。
        let (trace_id, span_id) = adx_observability::trace::log_context();
        Self {
            metrics,
            sink,
            template_release: effective,
            event_template_release,
            tenant_ref: tenant_ref.to_owned(),
            user_ref,
            start_mode: StartMode::Unknown,
            environment_id: None,
            trace_id: (!trace_id.is_empty()).then_some(trace_id),
            span_id: (!span_id.is_empty()).then_some(span_id),
            started,
        }
    }

    /// H2 之后回填启动模式（Activator 判定；不改变已记录的阶段样本）。
    pub(super) fn with_start_mode(mut self, mode: StartMode) -> Self {
        self.start_mode = mode;
        self
    }

    /// H2 之后回填环境身份（`ServiceSelection` 已持有的权威值）。
    pub(super) fn with_environment(mut self, environment_id: &str) -> Self {
        self.environment_id = Some(environment_id.to_owned());
        self
    }

    pub(super) fn start_mode(&self) -> StartMode {
        self.start_mode
    }

    /// H1/H2/model_prepare/first_use：记一个阶段样本。
    pub(super) fn observe_phase(&self, phase: Phase, outcome: Outcome, elapsed: Duration) {
        self.metrics
            .observe_phase(&self.template_release, phase, outcome, elapsed);
    }

    pub(super) fn started(&self) -> Instant {
        self.started
    }

    pub(super) fn sink(&self) -> Option<&Arc<super::observability::ObservationSink>> {
        self.sink.as_ref()
    }

    /// 构造一条业务事件（§5.5 信封；`session_ref` 为 HMAC 引用）。
    fn observation(
        &self,
        event_type: BusinessEventType,
        event_id: String,
        experience_id: &str,
        session_ref: &str,
        terminal: Option<TerminalDetail>,
    ) -> BusinessObservation {
        let outcome = terminal.as_ref().map(|detail| detail.outcome);
        let reason_class = terminal.as_ref().map(|detail| detail.reason_class);
        let duration_seconds = terminal.map(|detail| detail.duration_seconds);
        BusinessObservation {
            schema_version: SCHEMA_VERSION.to_owned(),
            event_id,
            business_type: BusinessType::Jiuwen,
            action_kind: None,
            event_type,
            occurred_at: OffsetDateTime::now_utc(),
            tenant_ref: self.tenant_ref.clone(),
            user_ref: self.user_ref.clone(),
            session_ref: Some(session_ref.to_owned()),
            experience_id: experience_id.to_owned(),
            environment_id: self.environment_id.clone(),
            template_release: self.event_template_release.clone(),
            start_mode: Some(self.start_mode.into()),
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            outcome,
            reason_class,
            duration_seconds,
        }
    }
}

/// 终态事件细节（仅 `task_terminal`）。
#[cfg(feature = "agent-api")]
struct TerminalDetail {
    outcome: Outcome,
    reason_class: ReasonClass,
    duration_seconds: f64,
}

/// 体验族启动模式 → 事件契约启动模式（取值集一致）。
#[cfg(feature = "agent-api")]
impl From<StartMode> for BusinessStartMode {
    fn from(mode: StartMode) -> Self {
        match mode {
            StartMode::Cold => Self::Cold,
            StartMode::Resumed => Self::Resumed,
            StartMode::BindingReuse => Self::BindingReuse,
            StartMode::Unknown => Self::Unknown,
        }
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
        // 连接 ack 与错误不是业务输出；错误可见性走事件链 reason_class。
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

/// driver 本地观察状态（详设 §6.4 M2-4a）：open 体验表＋Drop 清场。
///
/// driver 正常返回或被取消（前端断连/路由撤销时未来被直接丢弃）都会走
/// [`Drop`]：遗留 open 体验按 `exit_reason` 清场——inflight -1＋unresolved
/// 计数（H5"断开且无终态 → Unknown"的直埋表达）。默认 `client_gone`；
/// 后端侧错误路径返回前调用 [`DriverObservation::backend_exit`]。
#[cfg(feature = "agent-api")]
pub(super) struct DriverObservation {
    anchor: Option<ExperienceAnchor>,
    first_task_seen: bool,
    first_use_done: bool,
    open: BTreeMap<String, TaskRecord>,
    exit_reason: UnresolvedReason,
}

/// 一个 open 体验（Chat 请求接受后到终态前）。
#[cfg(feature = "agent-api")]
struct TaskRecord {
    /// 体验起点：连接上首个 Chat 用 anchor t0（含连接建立），后续用 submit 时刻。
    experience_started: Instant,
    /// 事件口径的 `acceptedAt`（submit 时刻，不含连接建立成本）。
    accepted_at: Instant,
    first_event_at: Option<Instant>,
    first_text_at: Option<Instant>,
    start_mode: StartMode,
    /// `sessionRef`（HMAC）；为 None 时不发射事件（契约要求去重引用）。
    session_ref: Option<String>,
    /// 见到 `chat.error`/`runtime.error` → 终态事件按失败落账。
    failed: bool,
}

/// 闭合原因：只有 `Terminal`（收到终态响应）才发射 `task_terminal` 事件；
/// deadline 与 Drop 清场结果未知——未决不伪装失败（§5.5.5）。
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
            first_task_seen: false,
            first_use_done: false,
            open: BTreeMap::new(),
            exit_reason: UnresolvedReason::ClientGone,
        }
    }

    /// H3：Chat 体验接受（`session.submit` Ok；非 Chat 由调用方过滤，submit
    /// 失败不开——未接受）。`backend_id` 是 submit 分配的后端 UUID。
    pub(super) fn task_opened(&mut self, backend_id: &str, session_id: Option<&str>, now: Instant) {
        let Some(anchor) = &self.anchor else { return };
        // 同一 backend 重复开启（重放/补偿路径）不双计 inflight、接受量与
        // 覆盖率分子；后到的记录以首次为准。
        if self.open.contains_key(backend_id) {
            return;
        }
        let first = !self.first_task_seen;
        self.first_task_seen = true;
        let experience_started = if first { anchor.started() } else { now };
        anchor.metrics.task_opened(anchor.start_mode());
        anchor.metrics.root_opened(&anchor.template_release);
        let session_ref = match (anchor.sink(), session_id) {
            (Some(sink), Some(session_id)) => Some(sink.hmac_ref(&anchor.tenant_ref, session_id)),
            _ => None,
        };
        if let (Some(sink), Some(session_ref)) = (anchor.sink(), session_ref.as_deref()) {
            // 覆盖率分子只认"已进入 spool"；入库/可聚合的阶段由 `adx_observation_*`
            // 质量族单独暴露，不用入队冒充全链路覆盖（M-A13）。
            if sink.emit(anchor.observation(
                BusinessEventType::ExperienceAccepted,
                format!("{backend_id}:accepted"),
                backend_id,
                session_ref,
                None,
            )) {
                anchor.metrics.instrumented_opened(&anchor.template_release);
            }
        }
        self.open.insert(
            backend_id.to_owned(),
            TaskRecord {
                experience_started,
                accepted_at: now,
                first_event_at: None,
                first_text_at: None,
                start_mode: anchor.start_mode(),
                session_ref,
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
        let mut first_event = false;
        if class != FrameClass::NotOutput && record.first_event_at.is_none() {
            record.first_event_at = Some(now);
            first_event = true;
            anchor.metrics.observe_first_output(
                &anchor.template_release,
                OutputKind::FirstEvent,
                record.start_mode,
                now - record.experience_started,
            );
            if let (Some(sink), Some(session_ref)) = (anchor.sink(), record.session_ref.as_deref())
            {
                sink.emit(anchor.observation(
                    BusinessEventType::FirstOutput,
                    format!("{id}:first_output"),
                    id,
                    session_ref,
                    None,
                ));
            }
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
        // 连接上首个有效业务输出 → first_use 阶段样本（anchor t0 → now）。
        if first_event && !self.first_use_done {
            self.first_use_done = true;
            anchor.observe_phase(Phase::FirstUse, Outcome::Success, now - anchor.started());
        }
    }

    /// H5：体验闭合。`Terminal`（收到终态响应）发射 `task_terminal` 事件；
    /// deadline/Drop 只闭合 inflight，不发事件（未决不伪装失败）。
    pub(super) fn task_closed(&mut self, backend_id: Option<&str>, close: Close, now: Instant) {
        let Some(anchor) = &self.anchor else { return };
        let Some(id) = backend_id else { return };
        let Some(record) = self.open.remove(id) else {
            return;
        };
        anchor.metrics.task_closed(record.start_mode);
        if close != Close::Terminal {
            return;
        }
        if let (Some(sink), Some(session_ref)) = (anchor.sink(), record.session_ref.as_deref()) {
            // 协议层"响应结束"不等于业务任务成功（详设 D01/C01）：没有业务
            // 终态证据时落 `unknown`，不进入成功率分子；只有明确的错误帧才
            // 记 failure。
            let (outcome, reason_class) = if record.failed {
                (Outcome::Failure, ReasonClass::Business)
            } else {
                (Outcome::Unknown, ReasonClass::Unknown)
            };
            sink.emit(anchor.observation(
                BusinessEventType::TaskTerminal,
                format!("{id}:terminal"),
                id,
                session_ref,
                Some(TerminalDetail {
                    outcome,
                    reason_class,
                    duration_seconds: (now - record.accepted_at).as_secs_f64(),
                }),
            ));
        }
    }

    /// 后端侧退出：driver 即将因 backend 错误返回时调用；前端侧退出与取消
    /// 丢弃沿用默认 `client_gone`。
    pub(super) fn backend_exit(&mut self) {
        self.exit_reason = UnresolvedReason::BackendGone;
    }
}

#[cfg(feature = "agent-api")]
impl Drop for DriverObservation {
    fn drop(&mut self) {
        let Some(anchor) = &self.anchor else { return };
        for record in self.open.values() {
            anchor.metrics.task_closed(record.start_mode);
            anchor.metrics.task_unresolved(self.exit_reason);
        }
    }
}

#[cfg(all(test, feature = "agent-api"))]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_anchor(template: &str) -> (Arc<ExperienceMetrics>, ExperienceAnchor) {
        make_anchor_with_sink(template, None)
    }

    fn make_anchor_with_sink(
        template: &str,
        sink: Option<Arc<crate::ingress::observability::ObservationSink>>,
    ) -> (Arc<ExperienceMetrics>, ExperienceAnchor) {
        let metrics = Arc::new(ExperienceMetrics::default());
        let anchor = ExperienceAnchor::new(
            Arc::clone(&metrics),
            sink,
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

    /// H2 之后回填的环境身份必须进入事件（此前恒 `None`，分钟桶因此全空）。
    #[test]
    fn environment_identity_reaches_the_event() {
        let (_metrics, anchor) = make_anchor("tpl@1");
        let event = anchor.with_environment("env-1").observation(
            BusinessEventType::ExperienceAccepted,
            "b1:accepted".to_owned(),
            "b1",
            "session-ref",
            None,
        );
        assert_eq!(event.environment_id.as_deref(), Some("env-1"));
        assert_eq!(event.template_release.as_deref(), Some("tpl@1"));
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

    /// first_use 只在连接上首个有效业务输出时记一次（无论哪个体验产生）。
    #[test]
    fn first_use_fires_once_on_first_output() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        observation.task_opened("b2", Some("s1"), Instant::now());
        let frame = json!({"type":"event","event":"chat.delta","payload":{"content":"hi"}});
        observation.frame_seen(Some("b2"), &frame, Instant::now());
        observation.frame_seen(Some("b1"), &frame, Instant::now());
        let text = render(&metrics);
        assert_eq!(
            sample_value(
                &text,
                "adx_experience_phase_duration_seconds_count{phase=\"first_use\",\
                 outcome=\"success\",template_release=\"tpl@1\"}"
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
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(1)
        );
        // 只闭合一次就应归零：不存在第二份幽灵记录。
        observation.task_closed(Some("b1"), Close::Terminal, Instant::now());
        let text = render(&metrics);
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(0)
        );
    }

    /// 终态闭合只减 inflight，不进 unresolved。
    #[test]
    fn task_closed_drains_inflight_without_unresolved() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        observation.task_closed(Some("b1"), Close::Terminal, Instant::now());
        drop(observation);
        let text = render(&metrics);
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(0)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_business_tasks_unresolved{reason=\"client_gone\"}"
            ),
            Some(0)
        );
    }

    /// Drop 清场：遗留 open 体验按退出原因计 unresolved、inflight 归零。
    #[test]
    fn drop_sweep_counts_unresolved_by_exit_reason() {
        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        observation.task_opened("b2", Some("s1"), Instant::now());
        observation.backend_exit();
        drop(observation);
        let text = render(&metrics);
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(0)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_business_tasks_unresolved{reason=\"backend_gone\"}"
            ),
            Some(2)
        );

        let (metrics, anchor) = make_anchor("tpl@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        drop(observation);
        assert_eq!(
            sample_value(
                &render(&metrics),
                "adx_business_tasks_unresolved{reason=\"client_gone\"}"
            ),
            Some(1)
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
        observation.task_closed(Some("b1"), Close::Terminal, Instant::now());
        observation.backend_exit();
        drop(observation);
    }

    /// 渲染：HELP/TYPE、已注册 template 稠密导出全组合、基数与长度超限归
    /// `other`、inflight/unresolved 恒稠密。
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
            anchor.observe_phase(Phase::Auth, Outcome::Success, Duration::from_millis(5));
        }
        let text = render(&metrics);
        assert!(text.contains("# HELP adx_experience_phase_duration_seconds"));
        assert!(text.contains("# TYPE adx_experience_phase_duration_seconds histogram"));
        // 第 13 个 template 归 other，样本落在 other 下。
        assert!(text.contains("phase=\"auth\",outcome=\"success\",template_release=\"other\""));
        // 已注册 template 稠密导出未观测过的组合（零样本）。
        assert_eq!(
            sample_value(
                &text,
                "adx_experience_phase_duration_seconds_count{phase=\"auth\",\
                 outcome=\"cancelled\",template_release=\"t0@1\"}"
            ),
            Some(0)
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
        anchor.observe_phase(Phase::Binding, Outcome::Failure, Duration::from_millis(1));
        assert!(render(&metrics)
            .contains("phase=\"binding\",outcome=\"failure\",template_release=\"other\""));
        // inflight/unresolved 无 template 标签、恒稠密 4 值。
        let text = render(&metrics);
        for mode in ["cold", "resumed", "binding_reuse", "unknown"] {
            assert!(text.contains(&format!(
                "adx_business_tasks_inflight{{start_mode=\"{mode}\"}}"
            )));
        }
        for reason in ["client_gone", "backend_gone", "no_terminal", "clock_skew"] {
            assert!(text.contains(&format!(
                "adx_business_tasks_unresolved{{reason=\"{reason}\"}}"
            )));
        }
        // 基数溢出计数：第 13 个模板 + 超长模板各计一次，两个共享注册表的族都导出。
        for metric in [
            "adx_experience_phase_duration_seconds",
            "adx_business_first_output_seconds",
        ] {
            assert!(text.contains(&format!(
                "adx_observation_cardinality_overflow_total{{metric=\"{metric}\",\
                 label=\"template_release\"}} 2"
            )));
        }
    }

    #[tokio::test]
    async fn coverage_ratio_counts_roots_and_instrumented() {
        std::env::set_var("ADX_TEST_COVERAGE_HMAC", "test-key-material");
        let emit = crate::ingress::observability::EmitConfig {
            endpoint: "http://127.0.0.1:1/events".to_owned(),
            token: "t".to_owned(),
            hmac_key_env: "ADX_TEST_COVERAGE_HMAC".to_owned(),
            spool_capacity: 8,
            batch_size: 4,
            flush_interval_millis: 100,
            max_retry_window_seconds: 1,
        };
        let (sink, _receiver) = crate::ingress::observability::ObservationSink::new(&emit).unwrap();
        // 有发射器：根体验全部进入事件链 → 覆盖率 1。
        let (metrics, anchor) = make_anchor_with_sink("cov@1", Some(sink));
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        assert!(render(&metrics).contains(
            "adx_business_instrumentation_coverage_ratio{business_type=\"jiuwen\",\
             template_release=\"cov@1\"} 1"
        ));
        // 无发射器：根仍计数、分子为 0 → 覆盖率 0。
        let (metrics, anchor) = make_anchor("cov2@1");
        let mut observation = DriverObservation::new(Some(anchor));
        observation.task_opened("b1", Some("s1"), Instant::now());
        assert!(render(&metrics).contains(
            "adx_business_instrumentation_coverage_ratio{business_type=\"jiuwen\",\
             template_release=\"cov2@1\"} 0"
        ));
    }
}
