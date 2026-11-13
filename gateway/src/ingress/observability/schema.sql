-- ADX 观测存储（M2-3，详设 §6.9）：业务事件链路的表结构。
-- Apply with a schema-owner role before enabling the observability ingest.
-- Runtime (adx_ops_writer) needs DML only; readers use the _safe views (M2-6).
BEGIN;
CREATE SCHEMA IF NOT EXISTS adx_ops_observations;
CREATE TABLE IF NOT EXISTS adx_ops_observations.schema_version (version integer PRIMARY KEY);
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM adx_ops_observations.schema_version WHERE version <> 1) THEN
        RAISE EXCEPTION 'Unsupported observation schema version';
    END IF;
END $$;
INSERT INTO adx_ops_observations.schema_version VALUES (1) ON CONFLICT DO NOTHING;

-- 一条业务事件（保留 7 天）。主键 `(producer_id, event_id)` 同时充当去重账本：
-- `ON CONFLICT DO NOTHING` 即幂等，不需要单独的 dedup 表。
CREATE TABLE IF NOT EXISTS adx_ops_observations.observation_event (
    producer_id text NOT NULL,
    event_id text NOT NULL,
    business_type text NOT NULL,
    event_type text NOT NULL,
    action_kind text,
    occurred_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    tenant_ref text NOT NULL,
    user_ref text,
    session_ref text,
    experience_id text NOT NULL,
    environment_id text,
    template_release text,
    start_mode text,
    trace_id text,
    span_id text,
    outcome text,
    reason_class text,
    duration_seconds double precision,
    PRIMARY KEY (producer_id, event_id)
);
CREATE INDEX IF NOT EXISTS observation_event_occurred
    ON adx_ops_observations.observation_event(occurred_at);
-- 聚合游标按接收时间推进：迟到事件（occurred_at 早于水位）也能被扫到并修原桶。
CREATE INDEX IF NOT EXISTS observation_event_received
    ON adx_ops_observations.observation_event(received_at);
CREATE INDEX IF NOT EXISTS observation_event_experience
    ON adx_ops_observations.observation_event(producer_id, experience_id);

-- 一个体验的当前状态（保留 90 天）。终态列只被首个终态事件写入（终态吸收，
-- 由 store 的 COALESCE 语义保证）；非终态事件只做"填空"，不改写已有值。
CREATE TABLE IF NOT EXISTS adx_ops_observations.task_state (
    producer_id text NOT NULL,
    experience_id text NOT NULL,
    tenant_ref text NOT NULL,
    user_ref text,
    session_ref text,
    environment_id text,
    template_release text,
    start_mode text,
    accepted_at timestamptz,
    first_output_at timestamptz,
    terminal_at timestamptz,
    -- 首个终态事件的接收时间：只用于判定 arrival=late（接收−事件 > 5m）。
    terminal_received_at timestamptz,
    terminal_outcome text,
    terminal_reason_class text,
    duration_seconds double precision,
    revision integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (producer_id, experience_id)
);
CREATE INDEX IF NOT EXISTS task_state_accepted ON adx_ops_observations.task_state(accepted_at);
CREATE INDEX IF NOT EXISTS task_state_terminal ON adx_ops_observations.task_state(terminal_at);

-- 分钟桶（保留 90 天）。聚合器按事件时间重算整桶后替换（revision+1），不累加
-- ——迟到只修原桶（§5.5.5）。arrival 由 receive−occur 间隔判定（>5m 为 late）。
CREATE TABLE IF NOT EXISTS adx_ops_observations.task_minute_rollup (
    occurred_minute timestamptz NOT NULL,
    environment_id text NOT NULL DEFAULT '',
    template_release text NOT NULL DEFAULT '',
    outcome text NOT NULL,
    arrival text NOT NULL DEFAULT 'on_time',
    start_mode text NOT NULL DEFAULT 'unknown',
    task_count bigint NOT NULL DEFAULT 0,
    revision integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (occurred_minute, environment_id, template_release, outcome, arrival, start_mode)
);

-- 时长直方图分钟桶（保留 90 天）。bucket_upper_seconds 为 B18 桶上界；
-- 溢出桶（> 3600s）用 'Infinity' 上界表示。duration_sum_seconds 供直方图 `_sum`。
CREATE TABLE IF NOT EXISTS adx_ops_observations.duration_minute_rollup (
    occurred_minute timestamptz NOT NULL,
    environment_id text NOT NULL DEFAULT '',
    template_release text NOT NULL DEFAULT '',
    outcome text NOT NULL,
    bucket_upper_seconds double precision NOT NULL,
    task_count bigint NOT NULL DEFAULT 0,
    duration_sum_seconds double precision NOT NULL DEFAULT 0,
    revision integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (occurred_minute, environment_id, template_release, outcome, bucket_upper_seconds)
);

-- 动作计数分钟桶（保留 90 天）。聚合器按分钟整体重算替换；避免每轮对
-- `observation_event` 的 7 天动作事件做全表 GROUP BY。
CREATE TABLE IF NOT EXISTS adx_ops_observations.action_minute_rollup (
    occurred_minute timestamptz NOT NULL,
    template_release text NOT NULL DEFAULT '',
    action_kind text NOT NULL DEFAULT '',
    outcome text NOT NULL DEFAULT '',
    action_count bigint NOT NULL DEFAULT 0,
    revision integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (occurred_minute, template_release, action_kind, outcome)
);

-- 活跃维度分钟桶（保留 7 天）：每分钟出现过的 (用户, 会话, 模板) 去重元组。
-- 5m/24h 活跃族从这里 `count(DISTINCT)`，避免在事件明细上扫大窗口。
CREATE TABLE IF NOT EXISTS adx_ops_observations.active_minute_rollup (
    occurred_minute timestamptz NOT NULL,
    user_ref text NOT NULL,
    session_ref text NOT NULL DEFAULT '',
    template_release text NOT NULL DEFAULT '',
    PRIMARY KEY (occurred_minute, user_ref, session_ref, template_release)
);

-- 聚合管线状态（常驻）：水位与重启恢复用的持久化 Counter 共用一张小表。
-- 行名约定 `watermark:<pipeline>` / `counter:<counter_name>`，`dimensions` 为
-- Counter 的标签维度。
CREATE TABLE IF NOT EXISTS adx_ops_observations.pipeline_state (
    name text NOT NULL,
    dimensions text NOT NULL DEFAULT '',
    at timestamptz,
    -- 聚合游标的完整键：只存时间会在重启后丢掉同时间戳的剩余事件（C04）。
    cursor_producer_id text,
    cursor_event_id text,
    revision bigint NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, dimensions)
);

-- 拒收/冲突诊断（保留 7 天）。只存原因码与元数据，不存原始正文（M-A11）。
CREATE TABLE IF NOT EXISTS adx_ops_observations.ingest_reject (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at timestamptz NOT NULL,
    producer_id text,
    event_id text,
    reason_code text NOT NULL,
    field text,
    detail_bytes integer
);

-- —— 只读视图（M2-6，§5.7）：不含身份列，供 Grafana `adx-business-rollups` 直读 ——
CREATE OR REPLACE VIEW adx_ops_observations.task_results_minute_safe AS
SELECT occurred_minute AS bucket_start_utc,
       environment_id,
       template_release,
       outcome,
       arrival,
       start_mode,
       task_count,
       revision
FROM adx_ops_observations.task_minute_rollup;

CREATE OR REPLACE VIEW adx_ops_observations.task_duration_minute_safe AS
SELECT occurred_minute AS bucket_start_utc,
       environment_id,
       template_release,
       outcome,
       bucket_upper_seconds,
       task_count AS sample_count,
       duration_sum_seconds,
       revision
FROM adx_ops_observations.duration_minute_rollup;

CREATE OR REPLACE VIEW adx_ops_observations.quality_watermark_safe AS
SELECT substr(name, length('watermark:') + 1) AS pipeline,
       at AS watermark,
       revision,
       updated_at
FROM adx_ops_observations.pipeline_state
WHERE name LIKE 'watermark:%' AND at IS NOT NULL;
COMMIT;
