//! `BusinessObservation v1` 契约：业务事件信封、枚举与入口校验（详设 §5.5，M2-1）。
//!
//! - 生产者（Jiuwen 适配器，M2-4）构造 [`BusinessObservation`] 并序列化发送。
//!   线格式声明 `deny_unknown_fields`：客户端自报 `producerId`/`receivedAt`
//!   按未知字段解析失败拒收（D6）；枚举越界与未知字段同属解析失败。
//! - 接收端（M2-2）解析凭据后以 [`ReceivedObservation::new`] 注入
//!   `producerId`/`receivedAt` 并复核一次。
//! - 校验覆盖详设 §5.5.4 规则 1–6；规则 7（限流）属接收服务。幂等、终态
//!   吸收与补报（§5.5.5）需要存储，属 M2-3；本模块只保证同一信封在相同
//!   `receivedAt` 下重复校验结果一致。
//! - 长度按 UTF-8 字节计；必填字段判"非空白"，可选字段出现但为空同样拒收。
use serde::{Deserialize, Serialize};
use std::fmt;
use time::{Duration, OffsetDateTime, UtcOffset};

/// 线格式版本（详设 §5.5.1）；不兼容变更升 `v2` 并新增表列或视图。
pub const SCHEMA_VERSION: &str = "v1";
/// `eventId` 长度上限（UTF-8 字节）。
pub const MAX_EVENT_ID_BYTES: usize = 256;
/// `producerId` 长度上限；由服务凭据注入，客户端不可自报。
pub const MAX_PRODUCER_ID_BYTES: usize = 128;
/// 引用类字段（tenant/user/session/association/environment/node）长度上限。
pub const MAX_REF_BYTES: usize = 128;
/// `experienceId`/`requestRef`/`operationId`/`taskRef.logicalTaskId` 长度上限。
pub const MAX_EXPERIENCE_ID_BYTES: usize = 256;
/// `templateRelease`（`{template}@{version}`）长度上限，与 D4 标签基数上限一致。
pub const MAX_TEMPLATE_RELEASE_BYTES: usize = 96;
/// 补报窗口：`occurredAt` 距 `receivedAt` 超过该值拒收（规则 2）。
pub const REPLAY_WINDOW: Duration = Duration::days(7);
/// 时钟偏移容忍：`occurredAt` 超前 `receivedAt` 超过该值拒收并计 `clock_skew`（规则 3）。
pub const CLOCK_SKEW_TOLERANCE: Duration = Duration::minutes(5);
/// `durationSeconds` 上限：90 天，与 `task_state` 保留期对齐（§6.9）。
pub const MAX_DURATION_SECONDS: f64 = 7_776_000.0;
/// W3C `traceId` 长度（十六进制字节）。
pub const W3C_TRACE_ID_BYTES: usize = 32;
/// W3C `spanId` 长度（十六进制字节）。
pub const W3C_SPAN_ID_BYTES: usize = 16;

/// 业务类型；与 `producerId` 绑定，由服务端校验（§5.5.1）。首个值 `jiuwen`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusinessType {
    Jiuwen,
}

impl BusinessType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Jiuwen => "jiuwen",
        }
    }
}

/// 事件类型（§5.5.1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    /// 体验接受（H3）。
    ExperienceAccepted,
    /// 任务终态（H5）；唯一携带 `outcome`/`reasonClass`/`durationSeconds` 的事件。
    TaskTerminal,
    /// 首输出里程碑（H4）。
    FirstOutput,
    /// 通用用户动作（如文件上传）。
    Action,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExperienceAccepted => "experience_accepted",
            Self::TaskTerminal => "task_terminal",
            Self::FirstOutput => "first_output",
            Self::Action => "action",
        }
    }

    /// 是否终态事件（§5.5.3：`outcome`/`reasonClass`/`durationSeconds` 仅终态允许）。
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::TaskTerminal)
    }
}

/// 业务动作类型（`eventType=action`，§5.5.1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    /// 文件上传（`/file-api/upload`）。
    FileUpload,
}

impl ActionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FileUpload => "file_upload",
        }
    }
}

/// 有界结果分类（详设 §5.3.1）：直接复用平台既有的执行结果枚举，
/// 线格式（`success/failure/cancelled/unknown`）与语义完全一致。
pub use adx_core::observation::Outcome;

/// 失败原因粗分类（详设 §5.3.1/§5.3.3）；细原因保留在既有 `reason` 标签。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonClass {
    Auth,
    Budget,
    Capacity,
    Placement,
    RuntimeStart,
    Readiness,
    Route,
    ModelUpstream,
    Business,
    Transport,
    Cancelled,
    Unknown,
    Other,
}

impl ReasonClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Budget => "budget",
            Self::Capacity => "capacity",
            Self::Placement => "placement",
            Self::RuntimeStart => "runtime_start",
            Self::Readiness => "readiness",
            Self::Route => "route",
            Self::ModelUpstream => "model_upstream",
            Self::Business => "business",
            Self::Transport => "transport",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
            Self::Other => "other",
        }
    }
}

/// 体验启动模式（§5.5.3）；v1 恒 `unknown`（判定源未接，不臆造）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartMode {
    Cold,
    Resumed,
    BindingReuse,
    Unknown,
}

impl StartMode {
    /// 全部取值（稠密导出用）；顺序即导出顺序。
    pub const ALL: [Self; 4] = [Self::Cold, Self::Resumed, Self::BindingReuse, Self::Unknown];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Resumed => "resumed",
            Self::BindingReuse => "binding_reuse",
            Self::Unknown => "unknown",
        }
    }
}

/// 任务引用（§5.5.2）：只有具备真实任务身份的适配器才填；Jiuwen 首期不填（D12）。
/// 业务事件信封（§5.5.1–§5.5.3）。线格式不含 `producerId`/`receivedAt`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BusinessObservation {
    /// 固定 [`SCHEMA_VERSION`]；未知版本拒收（规则 1，整批语义由接收服务执行）。
    pub schema_version: String,
    /// 生产者内唯一；去重键 `(producerId, eventId)`。
    pub event_id: String,
    /// 与 `producerId` 凭据绑定；接收端按 producer 绑定复核。
    pub business_type: BusinessType,
    pub event_type: EventType,
    /// 动作类型（仅 `action` 允许且必填）。
    pub action_kind: Option<ActionKind>,
    /// 事件时间（UTC），决定分钟桶；与 `receivedAt` 的距离受规则 2/3 约束。
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    /// 租户引用（不可直接反查身份）。
    pub tenant_ref: String,
    /// 活跃用户去重引用；experience_accepted/task_terminal/first_output 必填。
    pub user_ref: Option<String>,
    /// 活跃会话去重引用；条件同 `user_ref`。
    pub session_ref: Option<String>,
    /// 体验唯一标识；`task_terminal` 与 `experience_accepted` 必须一致（M2-3 复核）。
    pub experience_id: String,
    /// 平台 Environment 身份（H2 已选中的权威值）。
    pub environment_id: Option<String>,
    /// `{template}@{version}`，由 Gateway 编码（D4）。
    pub template_release: Option<String>,
    pub start_mode: Option<StartMode>,
    /// W3C；跳转 Trace 用，不进指标。
    pub trace_id: Option<String>,
    /// W3C；跳转 Trace 用，不进指标。
    pub span_id: Option<String>,
    /// 仅 `task_terminal` 允许（规则 5）。
    pub outcome: Option<Outcome>,
    /// 仅 `task_terminal` 允许（规则 5）。
    pub reason_class: Option<ReasonClass>,
    /// 仅 `task_terminal`；生产者用 `occurredAt - acceptedAt` 计算，受上限约束。
    pub duration_seconds: Option<f64>,
}

impl BusinessObservation {
    /// 入口校验（§5.5.4 规则 1–6）。`received_at` 为服务端接收时刻（或生产者
    /// 发送前的本地时钟）；同一 `(信封, receivedAt)` 结果确定一致。
    pub fn validate(&self, received_at: OffsetDateTime) -> Result<(), Violation> {
        // 规则 1：未知 schemaVersion。
        if self.schema_version != SCHEMA_VERSION {
            return Err(Violation::new(
                "schemaVersion",
                ViolationClass::UnknownSchemaVersion,
            ));
        }
        // 规则 6：长度（UTF-8 字节）与空白；可选字段出现但为空同样拒收。
        check_required("eventId", &self.event_id, MAX_EVENT_ID_BYTES)?;
        check_required("tenantRef", &self.tenant_ref, MAX_REF_BYTES)?;
        check_required("experienceId", &self.experience_id, MAX_EXPERIENCE_ID_BYTES)?;
        check_optional("userRef", &self.user_ref, MAX_REF_BYTES)?;
        check_optional("sessionRef", &self.session_ref, MAX_REF_BYTES)?;
        check_optional("environmentId", &self.environment_id, MAX_REF_BYTES)?;
        check_optional(
            "templateRelease",
            &self.template_release,
            MAX_TEMPLATE_RELEASE_BYTES,
        )?;
        // occurredAt 必须为 UTC（分钟桶按 UTC 落）。
        if self.occurred_at.offset() != UtcOffset::UTC {
            return Err(Violation::new("occurredAt", ViolationClass::MalformedField));
        }
        // 规则 2：补报窗口。
        if received_at - self.occurred_at > REPLAY_WINDOW {
            return Err(Violation::new(
                "occurredAt",
                ViolationClass::OutsideReplayWindow,
            ));
        }
        // 规则 3：时钟偏移（超前）。
        if self.occurred_at - received_at > CLOCK_SKEW_TOLERANCE {
            return Err(Violation::new("occurredAt", ViolationClass::ClockSkew));
        }
        // 条件必填（§5.5.2 冻结解释）：去重引用对体验事件（accepted/terminal/
        // first_output）必填，action 可选。
        if self.event_type != EventType::Action {
            if self.user_ref.is_none() {
                return Err(Violation::new(
                    "userRef",
                    ViolationClass::ConditionallyRequired,
                ));
            }
            if self.session_ref.is_none() {
                return Err(Violation::new(
                    "sessionRef",
                    ViolationClass::ConditionallyRequired,
                ));
            }
        }
        // 规则 5：结果分类——终态全字段必填；动作事件必填 outcome/actionKind；
        // 其余事件类型不允许这些字段。
        match self.event_type {
            EventType::TaskTerminal => {
                if self.outcome.is_none() {
                    return Err(Violation::new(
                        "outcome",
                        ViolationClass::MissingClassification,
                    ));
                }
                if self.reason_class.is_none() {
                    return Err(Violation::new(
                        "reasonClass",
                        ViolationClass::MissingClassification,
                    ));
                }
                let Some(duration) = self.duration_seconds else {
                    return Err(Violation::new(
                        "durationSeconds",
                        ViolationClass::ConditionallyRequired,
                    ));
                };
                if !duration.is_finite() || !(0.0..=MAX_DURATION_SECONDS).contains(&duration) {
                    return Err(Violation::new(
                        "durationSeconds",
                        ViolationClass::InvalidDuration,
                    ));
                }
                if self.action_kind.is_some() {
                    return Err(Violation::new(
                        "actionKind",
                        ViolationClass::ActionOnlyField,
                    ));
                }
            }
            EventType::Action => {
                if self.action_kind.is_none() {
                    return Err(Violation::new(
                        "actionKind",
                        ViolationClass::ConditionallyRequired,
                    ));
                }
                if self.outcome.is_none() {
                    return Err(Violation::new(
                        "outcome",
                        ViolationClass::MissingClassification,
                    ));
                }
                if self.reason_class.is_some() {
                    return Err(Violation::new(
                        "reasonClass",
                        ViolationClass::TerminalOnlyField,
                    ));
                }
                if self.duration_seconds.is_some() {
                    return Err(Violation::new(
                        "durationSeconds",
                        ViolationClass::TerminalOnlyField,
                    ));
                }
            }
            _ => {
                // 诊断字段必须指向真正越界的那个字段，否则排障会照着错误字段修。
                if self.outcome.is_some() {
                    return Err(Violation::new("outcome", ViolationClass::TerminalOnlyField));
                }
                if self.reason_class.is_some() {
                    return Err(Violation::new(
                        "reasonClass",
                        ViolationClass::TerminalOnlyField,
                    ));
                }
                if self.duration_seconds.is_some() {
                    return Err(Violation::new(
                        "durationSeconds",
                        ViolationClass::TerminalOnlyField,
                    ));
                }
                if self.action_kind.is_some() {
                    return Err(Violation::new(
                        "actionKind",
                        ViolationClass::ActionOnlyField,
                    ));
                }
            }
        }
        // 格式校验：W3C 十六进制、templateRelease 结构。
        if let Some(trace_id) = &self.trace_id {
            if !valid_hex_id(trace_id, W3C_TRACE_ID_BYTES) {
                return Err(Violation::new("traceId", ViolationClass::MalformedField));
            }
        }
        if let Some(span_id) = &self.span_id {
            if !valid_hex_id(span_id, W3C_SPAN_ID_BYTES) {
                return Err(Violation::new("spanId", ViolationClass::MalformedField));
            }
        }
        if let Some(template_release) = &self.template_release {
            let (template, version) = template_release
                .split_once('@')
                .ok_or_else(|| Violation::new("templateRelease", ViolationClass::MalformedField))?;
            if template.trim().is_empty() || version.trim().is_empty() {
                return Err(Violation::new(
                    "templateRelease",
                    ViolationClass::MalformedField,
                ));
            }
        }
        Ok(())
    }
}

/// 服务端附加身份后的事件：`producerId` 来自服务凭据、`receivedAt` 为接收
/// 时刻；构造时对信封复核一次（§5.5.6）。
#[derive(Debug, Clone, PartialEq)]
pub struct ReceivedObservation {
    producer_id: String,
    received_at: OffsetDateTime,
    observation: BusinessObservation,
}

impl ReceivedObservation {
    /// 注入身份并复核；`producer_id` 越限或信封校验失败返回 [`Violation`]。
    pub fn new(
        producer_id: impl Into<String>,
        received_at: OffsetDateTime,
        observation: BusinessObservation,
    ) -> Result<Self, Violation> {
        let producer_id = producer_id.into();
        check_required("producerId", &producer_id, MAX_PRODUCER_ID_BYTES)?;
        observation.validate(received_at)?;
        Ok(Self {
            producer_id,
            received_at,
            observation,
        })
    }

    pub fn producer_id(&self) -> &str {
        &self.producer_id
    }

    pub fn received_at(&self) -> OffsetDateTime {
        self.received_at
    }

    pub fn observation(&self) -> &BusinessObservation {
        &self.observation
    }

    pub fn into_observation(self) -> BusinessObservation {
        self.observation
    }
}

/// 拒收诊断分类；用于日志与响应体，**不是**指标标签值（§5.5.6）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationClass {
    /// 规则 1：未知 `schemaVersion`。
    UnknownSchemaVersion,
    /// 规则 2：`occurredAt` 超出补报窗口。
    OutsideReplayWindow,
    /// 规则 3：`occurredAt` 超前 `receivedAt` 超过容忍。
    ClockSkew,
    /// 规则 5：终态/动作事件缺 `outcome`（终态另缺 `reasonClass`）。
    MissingClassification,
    /// 动作专属字段（`actionKind`）出现在非动作事件。
    ActionOnlyField,
    /// 规则 6：字段超长。
    FieldTooLong,
    /// 规则 6：字段空白（必填判非空白；可选出现但为空）。
    BlankField,
    /// 条件必填缺失（去重引用、终态 `durationSeconds`）。
    ConditionallyRequired,
    /// 终态字段出现在非终态事件。
    TerminalOnlyField,
    /// 格式不符（UTC、W3C hex、`requestRef`/`templateRelease` 结构、attempt）。
    MalformedField,
    /// `durationSeconds` 非有限、为负或超上限。
    InvalidDuration,
}

impl ViolationClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnknownSchemaVersion => "unknown_schema_version",
            Self::OutsideReplayWindow => "outside_replay_window",
            Self::ClockSkew => "clock_skew",
            Self::MissingClassification => "missing_classification",
            Self::ActionOnlyField => "action_only_field",
            Self::FieldTooLong => "field_too_long",
            Self::BlankField => "blank_field",
            Self::ConditionallyRequired => "conditionally_required",
            Self::TerminalOnlyField => "terminal_only_field",
            Self::MalformedField => "malformed_field",
            Self::InvalidDuration => "invalid_duration",
        }
    }
}

/// 一条拒收诊断：字段 + 分类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    field: &'static str,
    class: ViolationClass,
}

impl Violation {
    fn new(field: &'static str, class: ViolationClass) -> Self {
        Self { field, class }
    }

    /// 诊断分类（日志与响应体用；指标侧 `result` 仍只有
    /// accepted/duplicate/rejected/dead_lettered）。
    pub fn class(&self) -> ViolationClass {
        self.class
    }

    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.class.as_str())
    }
}

impl std::error::Error for Violation {}

/// 必填字符串：非空白且不超长。
fn check_required(field: &'static str, value: &str, max_bytes: usize) -> Result<(), Violation> {
    if value.trim().is_empty() {
        Err(Violation::new(field, ViolationClass::BlankField))
    } else if value.len() > max_bytes {
        Err(Violation::new(field, ViolationClass::FieldTooLong))
    } else {
        Ok(())
    }
}

/// 可选字符串：出现时按必填同样规则检查（出现但为空同样拒收）。
fn check_optional(
    field: &'static str,
    value: &Option<String>,
    max_bytes: usize,
) -> Result<(), Violation> {
    match value {
        None => Ok(()),
        Some(value) => check_required(field, value, max_bytes),
    }
}

/// W3C 十六进制标识：规范为小写，此处大小写皆收，避免对合法生产者误拒。
fn valid_hex_id(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::{Date, Month, PrimitiveDateTime};

    fn utc(month: Month, day: u8, hour: u8, minute: u8) -> OffsetDateTime {
        PrimitiveDateTime::new(
            Date::from_calendar_date(2026, month, day).unwrap(),
            time::Time::from_hms(hour, minute, 0).unwrap(),
        )
        .assume_utc()
    }

    fn received() -> OffsetDateTime {
        utc(Month::November, 10, 12, 0)
    }

    fn occurred() -> OffsetDateTime {
        utc(Month::November, 10, 11, 0)
    }

    fn observation(event_type: EventType) -> BusinessObservation {
        BusinessObservation {
            schema_version: SCHEMA_VERSION.to_owned(),
            event_id: "evt-1".to_owned(),
            business_type: BusinessType::Jiuwen,
            event_type,
            action_kind: None,
            occurred_at: occurred(),
            tenant_ref: "tenant-1".to_owned(),
            user_ref: Some("user-1".to_owned()),
            session_ref: Some("session-1".to_owned()),
            experience_id: "exp-1".to_owned(),
            environment_id: None,
            template_release: Some("chat@1.0.0".to_owned()),
            start_mode: Some(StartMode::Unknown),
            trace_id: None,
            span_id: None,
            outcome: None,
            reason_class: None,
            duration_seconds: None,
        }
    }

    fn terminal() -> BusinessObservation {
        let mut event = observation(EventType::TaskTerminal);
        event.outcome = Some(Outcome::Success);
        event.reason_class = Some(ReasonClass::Unknown);
        event.duration_seconds = Some(12.5);
        event
    }

    fn assert_class(event: &BusinessObservation, class: ViolationClass) {
        assert_eq!(event.validate(received()).unwrap_err().class(), class);
    }

    #[test]
    fn valid_events_pass_for_every_type() {
        observation(EventType::ExperienceAccepted)
            .validate(received())
            .unwrap();
        observation(EventType::FirstOutput)
            .validate(received())
            .unwrap();
        terminal().validate(received()).unwrap();
        let mut action = observation(EventType::Action);
        action.user_ref = None;
        action.session_ref = None;
        action.action_kind = Some(ActionKind::FileUpload);
        action.outcome = Some(Outcome::Success);
        action.validate(received()).unwrap();
    }

    #[test]
    fn action_requires_kind_and_outcome_and_rejects_terminal_fields() {
        let mut action = observation(EventType::Action);
        action.outcome = Some(Outcome::Success);
        assert_class(&action, ViolationClass::ConditionallyRequired);

        let mut action = observation(EventType::Action);
        action.action_kind = Some(ActionKind::FileUpload);
        assert_class(&action, ViolationClass::MissingClassification);

        let mut action = observation(EventType::Action);
        action.action_kind = Some(ActionKind::FileUpload);
        action.outcome = Some(Outcome::Failure);
        action.reason_class = Some(ReasonClass::Business);
        assert_class(&action, ViolationClass::TerminalOnlyField);

        // actionKind 出现在非动作事件 → 拒收。
        let mut accepted = observation(EventType::ExperienceAccepted);
        accepted.action_kind = Some(ActionKind::FileUpload);
        assert_class(&accepted, ViolationClass::ActionOnlyField);

        let mut terminal = terminal();
        terminal.action_kind = Some(ActionKind::FileUpload);
        assert_class(&terminal, ViolationClass::ActionOnlyField);
    }

    /// 诊断必须指向真正越界的字段：`durationSeconds` 不能报成 `reasonClass`。
    #[test]
    fn terminal_only_diagnostics_name_the_offending_field() {
        let mut action = observation(EventType::Action);
        action.action_kind = Some(ActionKind::FileUpload);
        action.outcome = Some(Outcome::Success);
        action.duration_seconds = Some(1.5);
        assert_eq!(
            action.validate(received()).unwrap_err().field(),
            "durationSeconds"
        );

        let mut accepted = observation(EventType::ExperienceAccepted);
        accepted.duration_seconds = Some(1.5);
        assert_eq!(
            accepted.validate(received()).unwrap_err().field(),
            "durationSeconds"
        );

        let mut accepted = observation(EventType::ExperienceAccepted);
        accepted.reason_class = Some(ReasonClass::Business);
        assert_eq!(
            accepted.validate(received()).unwrap_err().field(),
            "reasonClass"
        );
    }

    #[test]
    fn rule1_unknown_schema_version_is_rejected() {
        let mut event = observation(EventType::ExperienceAccepted);
        event.schema_version = "v2".to_owned();
        assert_class(&event, ViolationClass::UnknownSchemaVersion);
    }

    #[test]
    fn rule6_length_and_blank_rules() {
        let mut event = observation(EventType::ExperienceAccepted);
        event.event_id = "e".repeat(MAX_EVENT_ID_BYTES + 1);
        assert_class(&event, ViolationClass::FieldTooLong);

        let mut event = observation(EventType::ExperienceAccepted);
        event.tenant_ref = "   ".to_owned();
        assert_class(&event, ViolationClass::BlankField);

        let mut event = observation(EventType::ExperienceAccepted);
        event.template_release = Some("t".repeat(MAX_TEMPLATE_RELEASE_BYTES + 1));
        assert_class(&event, ViolationClass::FieldTooLong);

        let mut event = observation(EventType::ExperienceAccepted);
        event.event_id = "e".repeat(MAX_EVENT_ID_BYTES);
        event.template_release = Some(format!("{}@1", "t".repeat(MAX_TEMPLATE_RELEASE_BYTES - 2)));
        event.validate(received()).unwrap();
    }

    #[test]
    fn rule2_replay_window_bounds() {
        let mut event = observation(EventType::ExperienceAccepted);
        event.occurred_at = received() - REPLAY_WINDOW - Duration::seconds(1);
        assert_class(&event, ViolationClass::OutsideReplayWindow);

        let mut event = observation(EventType::ExperienceAccepted);
        event.occurred_at = received() - REPLAY_WINDOW;
        event.validate(received()).unwrap();
    }

    #[test]
    fn rule3_clock_skew_bounds() {
        let mut event = observation(EventType::ExperienceAccepted);
        event.occurred_at = received() + CLOCK_SKEW_TOLERANCE + Duration::seconds(1);
        assert_class(&event, ViolationClass::ClockSkew);

        let mut event = observation(EventType::ExperienceAccepted);
        event.occurred_at = received() + CLOCK_SKEW_TOLERANCE;
        event.validate(received()).unwrap();
    }

    #[test]
    fn rule5_terminal_classification_required() {
        let mut event = terminal();
        event.outcome = None;
        assert_class(&event, ViolationClass::MissingClassification);

        let mut event = terminal();
        event.reason_class = None;
        assert_class(&event, ViolationClass::MissingClassification);

        let mut event = terminal();
        event.duration_seconds = None;
        assert_class(&event, ViolationClass::ConditionallyRequired);
    }

    #[test]
    fn terminal_only_fields_rejected_on_non_terminal() {
        let mut event = observation(EventType::ExperienceAccepted);
        event.outcome = Some(Outcome::Failure);
        assert_class(&event, ViolationClass::TerminalOnlyField);

        let mut event = observation(EventType::FirstOutput);
        event.reason_class = Some(ReasonClass::Transport);
        assert_class(&event, ViolationClass::TerminalOnlyField);

        // 动作事件同样拒绝终态字段（action 已补齐必填后才检查）。
        let mut event = observation(EventType::Action);
        event.action_kind = Some(ActionKind::FileUpload);
        event.outcome = Some(Outcome::Success);
        event.duration_seconds = Some(1.0);
        assert_class(&event, ViolationClass::TerminalOnlyField);
    }

    #[test]
    fn duration_bounds() {
        let mut event = terminal();
        event.duration_seconds = Some(-0.5);
        assert_class(&event, ViolationClass::InvalidDuration);

        let mut event = terminal();
        event.duration_seconds = Some(f64::NAN);
        assert_class(&event, ViolationClass::InvalidDuration);

        let mut event = terminal();
        event.duration_seconds = Some(MAX_DURATION_SECONDS + 0.5);
        assert_class(&event, ViolationClass::InvalidDuration);

        let mut event = terminal();
        event.duration_seconds = Some(MAX_DURATION_SECONDS);
        event.validate(received()).unwrap();
    }

    #[test]
    fn identity_refs_required_for_experience_events() {
        let mut event = observation(EventType::ExperienceAccepted);
        event.user_ref = None;
        assert_class(&event, ViolationClass::ConditionallyRequired);

        let mut event = terminal();
        event.session_ref = None;
        assert_class(&event, ViolationClass::ConditionallyRequired);
    }

    #[test]
    fn malformed_fields_are_rejected() {
        let mut event = observation(EventType::ExperienceAccepted);
        event.occurred_at = occurred().replace_offset(UtcOffset::from_hms(8, 0, 0).unwrap());
        assert_class(&event, ViolationClass::MalformedField);

        let mut event = observation(EventType::ExperienceAccepted);
        event.trace_id = Some("zz".repeat(W3C_TRACE_ID_BYTES));
        assert_class(&event, ViolationClass::MalformedField);

        let mut event = observation(EventType::ExperienceAccepted);
        event.trace_id = Some("ab".to_owned());
        assert_class(&event, ViolationClass::MalformedField);

        let mut event = observation(EventType::ExperienceAccepted);
        event.span_id = Some("abcd".to_owned());
        assert_class(&event, ViolationClass::MalformedField);

        let mut event = observation(EventType::ExperienceAccepted);
        event.trace_id = Some("0123456789abcdef0123456789ABCDEF".to_owned());
        event.span_id = Some("0123456789abcdef".to_owned());
        event.validate(received()).unwrap();

        let mut event = observation(EventType::ExperienceAccepted);
        event.template_release = Some("no-version-marker".to_owned());
        assert_class(&event, ViolationClass::MalformedField);

        let mut event = observation(EventType::ExperienceAccepted);
        event.template_release = Some("@1.0.0".to_owned());
        assert_class(&event, ViolationClass::MalformedField);
    }

    #[test]
    fn wire_format_is_camel_case_and_closed() {
        let event = terminal();
        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["schemaVersion"], json!("v1"));
        assert_eq!(value["eventType"], json!("task_terminal"));
        assert_eq!(value["templateRelease"], json!("chat@1.0.0"));
        assert!(value.get("schema_version").is_none());
        assert!(value.get("producerId").is_none());
        assert!(value.get("receivedAt").is_none());

        let parsed: BusinessObservation = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(parsed, event);

        let mut self_reported = value.clone();
        self_reported["producerId"] = json!("p-1");
        assert!(serde_json::from_value::<BusinessObservation>(self_reported).is_err());

        let mut self_reported = value;
        self_reported["receivedAt"] = json!("2026-11-10T12:00:00Z");
        assert!(serde_json::from_value::<BusinessObservation>(self_reported).is_err());
    }

    #[test]
    fn wire_rejects_unknown_enum_and_missing_required_field() {
        let mut value = serde_json::to_value(observation(EventType::FirstOutput)).unwrap();
        value["eventType"] = json!("bogus");
        assert!(serde_json::from_value::<BusinessObservation>(value).is_err());

        let mut value = serde_json::to_value(observation(EventType::FirstOutput)).unwrap();
        value.as_object_mut().unwrap().remove("experienceId");
        assert!(serde_json::from_value::<BusinessObservation>(value).is_err());
    }

    #[test]
    fn received_observation_attaches_identity_and_revalidates() {
        let received_observation =
            ReceivedObservation::new("producer-1", received(), terminal()).unwrap();
        assert_eq!(received_observation.producer_id(), "producer-1");
        assert_eq!(received_observation.received_at(), received());
        assert_eq!(
            received_observation.observation().event_type,
            EventType::TaskTerminal
        );
        assert_eq!(received_observation.clone().into_observation(), terminal());

        assert_eq!(
            ReceivedObservation::new(" ", received(), terminal())
                .unwrap_err()
                .class(),
            ViolationClass::BlankField
        );
        assert_eq!(
            ReceivedObservation::new(
                "p".repeat(MAX_PRODUCER_ID_BYTES + 1),
                received(),
                terminal()
            )
            .unwrap_err()
            .class(),
            ViolationClass::FieldTooLong
        );
        let mut stale = terminal();
        stale.occurred_at = received() - REPLAY_WINDOW - Duration::seconds(1);
        assert_eq!(
            ReceivedObservation::new("producer-1", received(), stale)
                .unwrap_err()
                .class(),
            ViolationClass::OutsideReplayWindow
        );
    }
}
