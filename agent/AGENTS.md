# Agent v2 开发约定

- 当前范围与部署方式见 agent/README.md；控制入口为无状态多副本 Activator。
- Agent 负责 Template、Environment 元数据与按需激活。Environment 对应稳定的逻辑 Sandbox 身份，不保留 Session/Task 层级、亲和、实例池、自动弹性、选主、成员注册或 Hash 路由。
- Platform 本轮只读，负责 Sandbox 状态、健康、创建超时清理、pause/resume 和执行替换。Activator 不复制平台状态，不运行生命周期恢复或健康扫描。首版按 Running 视为服务就绪，实际就绪保证列为 Platform 缺口，不恢复 ADX 探测；pause/resume 不做 ADX 适配。
- 平台副作用前先提交 Environment 身份；多个副本使用相同 Sandbox ID 与规格。未知结果不能换新身份重建。产品删除意图和 generation 隔离不等同于平台运行状态。
- Gateway 装配 Agent 入口，通过 ActivatorClient 访问独立无状态 Activator；ADX 状态 Redis 仅由 Activator 访问，生命周期操作经 Sandbox HTTP 接口。按真实 Sandbox ID 转发，不添加 Template/Target 本地缓存或旁路参数。inline 管理独立适配旧协议；HTTP/WS/SSH 与 v2 共享访问入口，用 instance/target 选择内部认证与协议分支，旧凭据不能回退到 v2 的 Platform API Key 验证；create/get/list/kill 和 exec/files 独立于 Activator 和 ADX 持久化；运行时连接复用 Gateway 的认证路由和 Relay，平台缺口见 agent/PLATFORM_GAPS.md。
- Execd 管用户进程，用户 Harness 自行定义业务协议。ADX 提供 HTTP/WS/SSH 透明访问，不限制业务并发；审计轨迹能力暂缓，不保留专用执行协议或日志接入。
- 重写时删除旧业务 UT 和夹具，不新增仅断言旧功能不存在的测试。保留有意义的并发、身份、授权、超时和故障用例，不恢复 Python/FaaS 依赖。
- 使用 make agent-test 和聚焦原生检查，长构建/测试按根目录要求委派并保留日志。真实 Redis 与平台证据单列；平台缺口如实记录，不能在 ADX 中补一套控制器。
