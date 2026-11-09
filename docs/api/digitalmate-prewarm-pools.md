# DigitalMate 预热池：配置与内部接口

本文描述当前 ADX 实现的 DigitalMate 预热池：池配置、槽位状态、Activator 内部接口，以及 Gateway 在新用户首次连接 `/digitalmate/ws` 时的领取行为。

这些接口都是内部接口，路径为 `POST /internal/adx/v1/pools/*`，沿用 Activator 的服务认证，不对业务客户端开放。实现位于 `agent/crates/core/src/pool.rs`、`agent/crates/store/src/pools.rs`、`agent/activator/src/pool.rs` 和 `gateway/src/ingress/digitalmategateway/pool.rs`。

## 工作方式

1. 运维用 `pools/put` 建立池。池固定一个 `(tenant, template, version)`，只接受数字版本号，不接受 Alias。
2. Activator 的 Reconciler 按固定间隔巡检池：补足目标库存、按最旧优先淘汰多余槽位、隔离异常槽位、处理过期槽位，并释放已确认清理的 Environment。
3. Gateway 的 PoolSupplier 按 `supplier_interval_seconds` 调用 `pools/reserve` 预留槽位，再通过 `pools/pending_keys` / `pools/key_confirmed` 确认槽位的模型 Key。
4. 槽位由 Platform 创建并启动。Running 且 Ready 的槽位记为 `ready_running`；被 Platform 挂起的槽位记为 `ready_paused`，领取时再恢复。
5. 新用户首次连接 `/digitalmate/ws` 时，Gateway 用请求的模板版本查 `digitalmate.pool.pools_by_version` 映射，命中则调用 `pools/claim`。
6. 领取成功后，Gateway 依次写账户领取记录、把模型 Key 交给用户、`pools/commit`、提交账户记录。Binding 指向池的 Environment 与 `pool-<slot_id>` 工作区。
7. 已有 Binding 的用户不参与领取，继续使用原 Binding。

## 池配置

`pools/put` 的 `config` 字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `pool_id` | string，必填 | 池标识。 |
| `tenant` | string，必填 | 所属租户。 |
| `template` | string，必填 | 模板名。 |
| `version` | string，必填 | 固定模板版本，必须是数字版本号。 |
| `enabled` | bool | 为 `false` 时停止预留与领取，已有槽位按 Reconciler 规则退出。 |
| `min_running` | integer | 目标 Running 槽位数。 |
| `min_ready` | integer | 目标 Ready 槽位数（Running + Paused）。 |
| `max_slots` | integer | 槽位总数上限，必须大于 0。 |
| `max_provisioning` | integer | 同时在创建中的槽位上限，取值 `1..=max_slots`。 |
| `key_confirm_timeout_seconds` | integer | Key 确认期限，`1..=2592000`。 |
| `provision_timeout_seconds` | integer | 槽位创建期限，`1..=2592000`。 |
| `claim_timeout_seconds` | integer | Granted 到提交的期限，`1..=2592000`。超期由 Reconciler 置为 `aborted(claim_timeout)`。 |
| `max_ready_age_seconds` | integer | Ready 槽位的最长保留时间，`1..=2592000`，按 `created_at` 计时。 |

约束为 `min_running <= min_ready <= max_slots`。`pools/put` 使用 `expected_revision` 做 CAS：首次创建传 `null`，更新时传当前 revision；revision 不匹配返回 409，调用方应重新读取后重试。

请求示例：

```json
{
  "config": {
    "pool_id": "digitalmate-v17",
    "tenant": "example-tenant",
    "template": "digitalmate",
    "version": "17",
    "enabled": true,
    "min_running": 2,
    "min_ready": 4,
    "max_slots": 10,
    "max_provisioning": 3,
    "key_confirm_timeout_seconds": 60,
    "provision_timeout_seconds": 300,
    "claim_timeout_seconds": 30,
    "max_ready_age_seconds": 86400
  },
  "expected_revision": null
}
```

`pools/drain` 把 `enabled` 置为 `false`，等价于一次只改该字段的 `pools/put`。

## 槽位与计数

槽位状态：

| 状态 | 含义 |
| --- | --- |
| `key_pending` | 已预留，等待 Key 确认。 |
| `provisioning` | Key 已确认，等待 Platform 创建与启动。 |
| `ready` | 可用，`phase` 为 `running` 或 `paused`。 |
| `claiming` | 已被一次领取占用。 |
| `quarantined` | 异常，等待清理；`quarantine_reason` 记录原因，如 `environment_missing`、`platform_failed`、`version_changed`、`pool_disabled`。 |
| `retiring` | 正常退役，等待清理。 |

`pools/status` 的 `counts` 语义：

- `key_pending + provisioning` 同时计入预期 Running 与预期 Ready 缺口。
- `claiming` 占用 `max_slots`，不占用 `max_provisioning`。
- `quarantined + retiring` 在清理完成前仍占物理槽位上限。
- `claims_granted`、`claims_committed`、`claims_aborted` 是领取终态计数。

槽位 ID 使用完整 UUID。`pool-workspace/<slot_id>` 是跨池、跨租户的持久占位。领取提交删除活动槽位时保留该占位，因此同一槽位的工作区不会被再次预热。

缩容优先淘汰最旧的 Paused 槽位，其次是其余未领取槽位；处于 `claiming` 的槽位不因缩容改变终态，由领取自身的 CAS 决定。

## 内部接口

| 接口 | 请求 | 返回 |
| --- | --- | --- |
| `pools/put` | `{config, expected_revision}` | 200 返回 `{config, revision}`；revision 冲突 409。 |
| `pools/get` | `{tenant, pool_id}` | 200 返回 `{config, revision}`；不存在 404。 |
| `pools/status` | `{tenant, pool_id}` | 200 返回 `{pool_id, version, enabled, counts}`。 |
| `pools/drain` | `{tenant, pool_id}` | 200 返回 `{config, revision}`。 |
| `pools/reserve` | `{tenant, pool_id, model_api_base}` | 有缺口返回 200 与 `ReservedSlot`；无缺口 204。 |
| `pools/pending_keys` | `{tenant, pool_id, limit}` | 200 返回 `{slots: [{slot_id, model_owner, model_api_key, key_digest, deadline_unix_ms}]}`。 |
| `pools/key_confirmed` | `{tenant, pool_id, slot_id, key_digest}` | 200 返回槽位视图；槽位或摘要不匹配返回错误。 |
| `pools/claim` | `{scope, pool_id, claim_id}` | Granted / Committed 返回 200；Aborted 返回 409 与终态；空池、禁用或版本未命中返回 404；池不存在、或 `claim_id` 已属于其他 scope 返回 409。 |
| `pools/claims/get` | `{tenant, claim_id}` | 200 返回 `PoolClaim`；不存在 404。 |
| `pools/commit` | `{tenant, pool_id, claim_id}` | 幂等提交；终态 Aborted 返回 409；不能提交其他池的领取。 |
| `pools/abort` | `{tenant, pool_id, claim_id, reason}` | 返回终态；已 Committed 的领取返回 409。 |
| `pools/retired` | `{tenant, pool_id, limit}` | 200 返回 `{slots: [{preserve_key, slot_id, model_owner, key_digest, model_api_key}]}`。 |
| `pools/retired/ack` | `{tenant, pool_id, slot_id}` | 200，空响应；移除该退休记录。 |

## 领取

领取状态为 `granted`、`committed`、`aborted`。领取的持久身份是 `(tenant, claim_id)`，恢复时不依赖当前 Alias、当前版本映射或池是否仍启用。

Gateway 处理一次 DigitalMate 连接的顺序：

1. Binding 已存在：直接使用，不查询池。
2. 读取账户领取状态。assignment 与 credential 只出现一半，或身份不一致时返回可重试错误。
3. 用 `(tenant, claim_id)` 查询全局领取记录。命中则使用记录中的原 `pool_id`，按原终态处理。
4. 没有记录时，用请求的模板版本查池映射；版本没有映射则走原冷启动。
5. 调用 `pools/claim`。返回 404 且账户没有 assignment 时走原冷启动；读写故障和结果不明不按未命中处理。

`claim_budget_seconds` 与请求剩余预算取较小值，覆盖 Binding 查询、账户快照、领取、账户事务、Key 转移、提交和 assignment 确认。超出预算返回结果不明的可重试错误，不回退冷启动。

账户记录写入后，Gateway 只重试提交，不主动中止。提交时若目标模板版本已变化，`pools/commit` 返回冲突并进入终止处理，不会另开工作区。

账户记录写入后，若领取为 `aborted` 且 `released=false`，Gateway 阻止冷启动；等 Platform 确认删除并置 `released=true` 后，冷启动复用该 assignment 的工作区。

## Key 生命周期

池槽位使用一把独立于用户的模型 Key。Key 的 owner 是 `pool:<slot_id>`，摘要保存在槽位记录中。

- **确认**：Supplier 的 Key 确认只检查该 Key 尚未存在，或仍由池 owner 持有且 `blocked=true`；它不创建 Key。
- **转移**：领取时把 Key 的 owner 改为用户，清除 `blocked`，设置用户 Key 别名；Key 不存在时按相同 Key 原文为领用人创建。转移不修改 spend、预算和限流。
- **补偿**：账户保存的 Key 在远端缺失时，按相同原文为已验证用户重新创建，再走正常 ensure。被外部删除过的 Key，其历史计费数据无法恢复。
- **删除**：退休或隔离槽位清理时写入 retired 记录。`preserve_key` 由该槽位是否产生过 Grant 决定：产生过 Grant 的 Key 一律保留，即使账户最终未写入；未产生过 Grant 的槽位才允许 Supplier 在 owner 仍为池时删除。Key 不存在视为已删除。

未领取的池 Key 不在 LiteLLM 中物化。LiteLLM 按 worker 进程缓存 Key 对象（默认 60 秒），`/key/update` 只清除处理该请求的 worker 的缓存；若池 Key 以 `blocked=true` 预先创建，预热实例领取前的模型探测会把该状态缓存到其他 worker，转移后仍可能在缓存过期前返回 401。因此 Key 只在领取时创建。

## Gateway 配置

```json
{
  "pool": {
    "pools_by_version": {"17": "digitalmate-v17"},
    "claim_budget_seconds": 10,
    "supplier_interval_seconds": 5
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `pools_by_version` | 模板版本到池 ID 的映射，不能为空；版本必须是数字。 |
| `claim_budget_seconds` | 单次领取的总预算，`1..=600`。 |
| `supplier_interval_seconds` | Supplier 与 Reconciler 的巡检间隔，`1..=300`。 |

`pool` 可选。只有目标模板版本存在映射、池已启用且有 Ready 槽位时，新用户才会命中预热实例；其余情况保持原冷启动路径。

## 观测

`pools/status` 是当前的库存与领取观测接口，返回上表的计数字段。当前实现没有按请求统计命中率或首访耗时的指标。
