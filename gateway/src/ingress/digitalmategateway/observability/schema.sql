-- ADX 业务大盘 · 活跃去重存储（方案 B）。
-- Apply with a schema-owner role before enabling the observability sink.
-- Runtime (adx_ops_writer) needs DML only; readers use the _safe view.
BEGIN;
CREATE SCHEMA IF NOT EXISTS adx_ops_observations;

-- 结构版本：与 coordinator 的 resource ledger（`ledger_schema_version`）各自
-- 独立，同一 schema 下互不影响。版本不匹配拒绝启动（不自动迁移）。
CREATE TABLE IF NOT EXISTS adx_ops_observations.schema_version (version integer PRIMARY KEY);
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM adx_ops_observations.schema_version WHERE version <> 2) THEN
        RAISE EXCEPTION 'Unsupported observation schema version';
    END IF;
END $$;
INSERT INTO adx_ops_observations.schema_version VALUES (2) ON CONFLICT DO NOTHING;

-- 一个活跃元组：某分钟里出现过的一次（租户, 用户, 会话, 模板）。
--
-- 主键里**没有** producer/实例列：多副本写同一主键，`ON CONFLICT DO NOTHING`
-- 即全局去重——活跃用户、活跃会话、按模板活跃用户、DAU 全部由这张表
-- `count(DISTINCT ...)` 得出，不再需要事件账本、状态表或分钟桶。
-- 身份列均为 HMAC 引用（租户已并入 HMAC 输入），不落原文。
CREATE TABLE IF NOT EXISTS adx_ops_observations.active_minute (
    occurred_minute timestamptz NOT NULL,
    tenant_ref text NOT NULL,
    user_ref text NOT NULL,
    -- 空串 = 动作事件（上传）没有会话维度。
    session_ref text NOT NULL DEFAULT '',
    template_release text NOT NULL DEFAULT '',
    PRIMARY KEY (occurred_minute, tenant_ref, user_ref, session_ref, template_release)
);

-- 只读视图（供 Grafana `adx-business-activity` 数据源直读）：只含 HMAC 引用。
CREATE OR REPLACE VIEW adx_ops_observations.active_minute_safe AS
SELECT occurred_minute AS bucket_start_utc,
       tenant_ref,
       user_ref,
       session_ref,
       template_release
FROM adx_ops_observations.active_minute;
COMMIT;
