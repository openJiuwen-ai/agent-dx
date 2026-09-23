# HTTP 与 SDK 放置约束

SDK 的 `node_id` 现在通过既有 `scheduleAffinities` JSON 进入新的 `EnvironmentSpec.scheduling.required_node`。adxlet 可在配置中设置 `labels`；Coordinator 在注册节点时根据经过认证的节点 ID 写入 `NODE_ID`，拒绝用其他值伪造该标签。

```json
{
  "labels": {
    "pool": "compute",
    "disk": "fast"
  }
}
```

当前 HTTP 适配器支持节点的硬性亲和和反亲和：

- 同一 subcondition 的标签表达式为 AND，多条 subcondition 为 OR。
- `In`、`NotIn`、`Exists`、`DoesNotExist` 按原 HTTP 契约转换。原 `NotIn` 要求标签存在，因此会额外加入 `Exists`；不能直接套用新内核允许标签缺失的 `NotIn`。
- 反亲和对整组条件取反，不能把每个标签单独取反后直接 AND。转换使用德摩根规则，生成内核已有的 OR-of-AND 条件。
- 展开最多256个候选条件，每个合取最多512个内部表达式。超过上限返回参数错误，不静默截断。

HTTP 与 SDK 已接通实例亲和／反亲和、权重和顺序偏好。`PlacementGroup` 保存条件组，Go 只做编码和校验；Rust 的 `placement-groups` Filter 与 `placement-group-preference` Score 执行判断，复用本轮不可变快照和查询索引。组进入新协议及 Redis 持久化模型，不增加旧调度进程或旧协议调用。

## 实例标签与条件组

创建接口支持 `labels` 字符串映射，最多256项；键不能为空或包含 `:` / `=`。adxlet 配置里的 `labels` 是节点标签，创建请求里的 `labels` 是实例标签，两者分开匹配。

`scheduleAffinities` 的 `kind=0` 匹配节点标签，`kind=1` 匹配候选 adxlet 上同租户实例的标签。实例条件的所有表达式必须由同一个实例满足；同一条件内的表达式不能由多个实例拼凑出一个匹配。已分配但尚未启动的实例也计入快照。暂停或删除释放分配后退出匹配集合。

| affinity | 含义 |
| --- | --- |
| 0 | 软亲和 |
| 1 | 软反亲和 |
| 2 | 硬亲和：至少满足一个候选条件 |
| 3 | 硬反亲和：没有实例满足任一候选条件 |

实例硬反亲和也约束后来的创建请求，保持反向排斥；只作用于同租户。没有匹配实例时，硬亲和默认无法放置；请求自身标签能满足该组且集群没有匹配同租户实例时，可作为该亲和组的首个实例启动。

实例的本地范围是 adxlet。HTTP 不暴露物理宿主范围；内部兼容字段显式指定物理 NODE scope 时仍返回未实现，避免把多个 adxlet 错当作同一执行范围。

## 权重与顺序

- `weight` 取0–1000，省略或0按1；负数或超限返回参数错误。无序模式累加匹配条件的权重并归一化。
- `preferredPriority=true` 按请求中条件顺序评分，只取第一个满足偏好的条件；不会因为后面的条件匹配更多而在组内加分。此模式忽略权重。
- 同一类条件组不能混用有序与无序模式，拒绝最后一个字段覆盖前面设置。
- 软偏好是评分，最终与资源放置及其他 Score 组合；它不保证抢先选择某节点。需要硬约束时用2或3。`preferredAntiOtherLabels` 与有序资源偏好的既有组合仍编码为硬约束。
- 最多8组，每组256个候选条件；组内每个条件的标签表达式为AND，条件之间为OR。`NotIn` 继续保留“标签存在且值不属于集合”的HTTP语义。

```python
peer = Sandbox(image="my-image", labels={"app": "database"}, connection=connection)
worker = Sandbox(
    image="my-image", labels={"app": "worker"}, connection=connection,
    schedule_affinities=[{
        "kind": 1, "affinity": 2,
        "labelOps": [{"type": 0, "labelKey": "app", "labelValues": ["database"]}],
    }],
)
```

SDK `node_id` 与自定义条件同时使用时，将节点约束AND进每个节点硬亲和候选条件，不追加成一个可绕开的OR分支，也不修改调用者传入的条件列表。

## 本轮验证

Rust调度/协议/Coordinator 61项非ignored检查通过，真实Redis/mTLS RPC 8项与Go HTTPS调用通过，完整Go测试及vet通过，SDK 140项及56个subtests通过；四个相关Rust包的全目标Clippy通过。日志位于 `out/ci/stage-5/groups/`。普通路径同机6场景×7轮复测通过，见 [性能记录](2026-09-16-placement-groups-recheck.md)。package-v17/Lima r21与r22的新放置用例均通过；整套运行均在后续双克隆文件上传失败（10/18），网桥FDB错误端口学习与直连超时已取证，见 [网络记录](2026-09-16-fc-clone-network.md)。GPU/NPU真实设备、混合约束负载和长期公平性仍需单独验收。

验证记录在 `out/ci/stage-5/affinity-green.log`：所有Go包测试、vet和API构建通过，其中真值表覆盖OR、缺失标签的NotIn、反亲和合取。`labels-2.log`：9项Rust协议相关测试与Coordinator/adxlet Clippy通过。`affinity-http.log`：真实HTTPS→Coordinator的指定节点调度集成通过，节点执行后端为fixture。`out/ci/stage-7/fc-r14.log`：最新统一package-v11通过10项真实FC/S3用例，公共SDK显式指定 `node_id=node1` 创建成功；最终对象、文件、实例及资源均清理。
