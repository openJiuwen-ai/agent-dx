# Agent v2 开发约定

- Rust ADX 层的职责与部署见 `README.md`、`dispatcher/README.md`。
- Agent 负责 Template、Session、实例亲和与编排。Gateway 嵌入 Agent API 并暴露 Sandbox 边界；Dispatcher 独立部署，不直接连接 Master 或平台存储。
- 本次实施不修改 Platform 源码。记录能力缺口，使用已明确说明的预装验证镜像；不能静默丢弃用户配置。
- Redis 仅保存当前状态。本期不实现操作流水、持久任务队列、Session owner 租约或自动弹性控制器。
- SDK、CLI、EventLog 和 Agent invoke 后置，RRT 替代旧 Executor；不恢复 Python/FaaS 依赖。
- 使用 `make agent-test` 运行组件检查。真实 Redis 与平台验收单列，耗时构建/测试按根目录约定委派并保留完整日志。
- Session 确认删除后可复用 ID，内部 generation 隔离旧生命周期请求。Affinity 绑定逻辑 Instance ID，在用户进程/运行时重启后保留，随 Session 删除或逻辑实例结束失效。不提供显式释放 affinity 的接口。Resolve 的 `bypass_cache` 必须读取权威状态，失败时不得回退旧缓存。
- Substrate 负责首次 Ready 之后的健康管理。ADX 只协调创建和删除，不持续探测稳定实例的 Sandbox/RRT/端口，也不因连接失败触发健康查询。启动失败保留到显式删除；传输结果未知时沿用原 ID，达到已存储的创建期限（默认 60 秒）后自动进入删除。创建查询从 500ms 退避至最多 5s，进程替换不能延长期限。仅后端确认删除后才移除 Session 成员。后台只恢复 Creating、删除意图及 Deleting Session；缓存旁路只重读 Repository。
- inline create/get/kill 直接调用 Gateway Sandbox，不创建 ADX Instance、InstanceSource、后台恢复或健康任务，也不访问 ADX Redis。不提供 inline list，创建 trace 回查后置。Sandbox 受理创建/确认删除后才能返回相应成功结果，未知结果交给调用方处理。ADX Instance 仅属于受管 Session。
- 本期不实现 Ensure、预留实例或暖池填充。创建 Session 只存空池，请求触发冷启动；不提供 capacity 或 desired_instances。冷启动通过 CAS 原子认领空池，并发请求复用已有实例。
- 共享 Gateway 转发仅接受真实 Sandbox ID，不探测 ADX UUID 别名、不查询 ADX Redis。受管 Resolve 返回用于管理的 instance_id 和用于转发的 sandbox_id；HTTP/WS Agent 入口先选址，再进入共享转发。
- Session 缓存锁只覆盖内存操作，不跨 Redis/Sandbox I/O 或退避等待。普通缓存刷新使用单独的合并锁，本地 epoch 拒绝过期写入；每实例生命周期锁与 Repository CAS 继续作为并发边界。
- 不提前实现 Draining 或排空转移。删除 Session 时仍清理其 Affinity，generation 隔离不能代替垃圾清理。Dispatcher 重选共用一个绝对请求期限（含发现），不能重置已存储的创建期限。项目无已发布的旧 v2 schema，初始化当前 marker 时不扫描命名空间，不虚构迁移路径。
