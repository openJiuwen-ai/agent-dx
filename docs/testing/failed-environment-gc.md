# Failed Environment 自动回收

Coordinator 自动回收终态 `Failed`，默认保留 600 秒。资源释放与元数据回收分开：创建 Failed 立即释放预留资源，保留期只用于保留诊断结果，不继续占用创建配额。

## 配置与范围

在部署 YAML 中的 Coordinator 服务配置设置：

```yaml
services:
  - id: coordinator
    role: coordinator
    config:
      # 与 listen、redis_url、tls 等其他 Coordinator 参数一起设置
      failed_retention_seconds: 600
```

该值必须大于零；省略时使用 600 秒。Coordinator 每 5 秒检查一次，每轮至多处理 16 条 Failed，按游标轮转，不扫描全部运行实例。保留期到达不等于立即消失：积压、节点清理或存储故障会延迟回收。

失败时间随 Failed 结果持久化到 Redis；相同失败的重试和 Coordinator 重启不重置保留期。旧记录没有失败时间时，从首次巡检开始计时。等待自动重启、正在跨节点恢复，或失效归属仍有有效共享恢复点的记录暂不回收；共享恢复点过期且未恢复后可进入回收。

## 清理与隔离契约

- 所属节点健康时，Coordinator 调用 adxlet 删除接口，携带归属 generation、节点会话和期望 Failed revision。adxlet 在该 Environment 的串行控制任务中复核状态；已恢复为 Running、待重启、修订号或会话不匹配时拒绝 GC。
- 节点先清理后端和恢复点，再发布 Deleted。清理或持久化失败保留记录并在后续巡检重试；不会因为保留期到达就忽略仍在线节点的清理失败。
- 节点归属已失效且资源已释放时，Coordinator 可以原子退役 Failed。此时不声称旧节点的物理执行已经停止；旧节点返回后按权威目录清理旧执行，旧 generation 不能再发布 Running。
- 成功后移除 Redis 主目录、Coordinator 调度目录以及在线 adxlet 控制任务。仅保留现有 600 秒最小删除回执，不保留完整 Failed／Deleted 记录。同名创建获得新 generation；迟到提交不能恢复旧归属。

GC 在 mTLS 模式下仅允许 Coordinator 服务身份调用受控删除；普通 API Server DELETE 的用户权限检查保持不变。network 模式遵循既有内部网络信任边界。

## 验证

组件用例使用真实 Redis 和 mTLS，覆盖在线清理、失败重试、同名重建、离线归属退役、迟到 Running 拒绝、失败年龄跨重试与重启保持。adxlet 生命周期用例覆盖 Running／待重启保护、revision fencing 和后端清理失败后重试。

```sh
python3 build/ci/run.py storage --jobs 2 --output out/ci/failed-gc-storage
python3 build/ci/run.py control-rpc --jobs 2 --output out/ci/failed-gc-rpc
```

上述是组件测试；实际 runtime 的 standalone 验证结果另见 [PR78 回归记录](pr78-runtime-regressions.md)。
