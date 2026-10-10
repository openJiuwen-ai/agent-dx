# ADX 当前的平台依赖与缺口

本清单对应当前源码。Platform 仅包含 Execd 启动锁修复；ADX 复用已有能力，缺失部分不以额外状态库、健康轮询或生命周期控制器替代。Inline HTTP 兼容接口已移除。

## 暂缓项

| 能力 | 当前事实与影响 | 当前处理 |
| --- | --- | --- |
| Agent 镜像 runtime 注入及启动继承 | Platform 已有 runtime profile 与 inherit_entrypoint。Agent/Gateway 已实现统一创建映射，业务镜像定义启动命令、工作目录和用户；无需预装 Execd。独立 Activator 现通过 API Server HTTP 创建，runtime 注入由 API Server 部署配置负责 | 创建规格、部署示例及组件测试已通过；修复 Execd 启动锁后，独立 Activator 的真实创建、镜像继承及 Gateway runtime 凭据验证通过；嵌入式路径的栈占用及规格规范化修复后，两条真实创建路径均通过 |
| 持久挂载、workspace 及模板工作目录覆盖 | 平台已有底层 mount 字段，但 Agent 模板未提供挂载及工作目录覆盖；Agent 继承镜像的 WORKDIR/USER | 本轮暂不实施；不静默丢弃挂载或覆盖意图 |
| 自定义 startup/liveness 健康检查 | ADX 将 Platform Running 视为可用；当前 adxlet 创建路径检查 Execd 控制状态并激活节点路由，但不保证用户业务端口及 Gateway 路由传播已完成 | **待完善**；Jiuwen 仅对发送业务字节前的 TCP 拒绝连接，在原期限内等待业务端口监听；这不替代平台健康检查与就绪契约，失败后调用方使用原绑定身份重试 |

## Execd 启动锁竞争

2026-09-23 的 `3141e27` 真实容器验证（当时组件名为 RRT，现名 Execd）中已观察到：业务 HTTP 返回 200，但 Execd `/control/v1/status` 持续超时，Platform 因而保持 Creating。与运行二进制相同 Build ID 的调试符号定位到 Execd 主线程在 `entrypoint::Manager::complete_create()` 等待子进程 Mutex；监视线程在 `start_from_environment()` 的 `match child.lock().try_wait()` 分支中持有同一把锁休眠 10ms，并循环抢锁。同步锁等待会阻塞 Execd 的单线程异步运行时，导致控制接口也无法处理请求。

2026-10-08 经用户授权，仅将 `start_from_environment()` 的子进程锁限制在 try_wait 调用范围内，结果处理及 10ms 休眠在锁外执行。新增进程回归先复现旧逻辑超时，修复后连续四次启动通过；Execd 聚焦测试 95 项、fmt 及全工作区严格 Clippy 通过。独立 Activator 的真实 Sandbox 创建、镜像启动继承、AgentServer 监听及 Gateway runtime 端口／凭据验证通过，已复现的 Execd 锁阻塞解除。原始诊断在 `out/jiuwen-inherit-20261008/`，修复后证据在 `out/jiuwen-inherit-fixed-20261008/`。这不补齐业务健康检查或文件读取一致性。

## 嵌入式 Activator 真实创建栈溢出

此前隔离容器验收中，嵌入式 API Server 因嵌套 RPC Future 的栈占用溢出。用户授权继续修复后，Gateway/API Server 将创建链路的大 Future 放到堆上，完整 TLS Ingress→嵌入式 Activator 回归在 2 MiB 工作线程栈上通过，未扩大线程栈。随后还修复了 Gateway mapper 未规范化默认子消息导致的创建确认 409：复用既有 Platform pb/core 转换统一表示，不修改 Platform。`out/jiuwen-public-canonical-20261008/` 和 `out/jiuwen-public-download-20261008/` 已验证独立及嵌入式两条真实创建、Execd 控制接口及 AgentServer 镜像启动继承；栈溢出与误报冲突均未再出现。业务设计与验证边界见 [Jiuwen 设计](../docs/development/jiuwenswarm-adx-gateway-design.md)。

## 创建期限与结果未知

ADX 从入口传递绝对期限，独立 Activator 用剩余时间限制 API Server HTTP 等待。API Server 现有创建接口不接收这一绝对期限，提交的 `createTimeoutSeconds` 固定为 Activator 配置，以保证同一操作重试的请求体不变化。取消 HTTP 等待不能证明已受理的创建被取消；返回 OutcomeUnknown 后只查询或重试原身份。当前未增加绝对期限字段；API Server 修改已获授权，但不因本次统一创建入口而扩大期限协议。

## 管理入口与文件访问

独立与嵌入式 Activator 均只经 API Server 管理 Sandbox。独立模式用部署者配置的租户 Platform API Key，API Server 从 key 决定租户；现有查询响应不返回租户，配置归属须由部署者保证。

PlatformSandbox 和旧 runtime 查询已删除。Ingress 通用文件能力经 API Server 的既有实例查询或 SandboxService 校验租户归属和 Running，文件正文走 Relay/Execd。共享数据面读取部署注入的 Execd 凭据，Jiuwen 不取得 token。旧 `/api/sandbox/v2/instances` 与 OpenAPI 声明已删除。

API Server runtime profile、节点和数据面必须共用端口及 Secret。节点 env 可覆盖 runtime_profile.env，实例目录不足以证明最终凭据；当前没有跨进程配置一致性自动检查，也不支持任意节点独立凭据或单侧轮换。新链路已通过本地组件测试及独立部署的真实新建 Sandbox、跨副本上传下载验证。详细范围见[文件访问方案](../docs/development/jiuwenswarm-adx-gateway-design.md#通用沙箱文件访问与管理查询)。

## AgentBinding 的中间记录与删除

流量触发激活时，ADX 先写入 AgentBinding 身份，再向平台提交同一 Sandbox ID。进程中断、创建失败或响应丢失时，可能存在“有 AgentBinding 元数据、无已确认可用 Sandbox”的状态；Active 表示产品记录可用于激活，不代表运行健康。

AgentBinding 统一由 HTTP/WS/SSH 访问触发；查询和列表可能看到上述未完成激活的中间记录，重试使用已返回的原 AgentBinding ID。

平台不能为从未观察到的创建提供取消标记/墓碑，查询不存在并不能排除迟到的创建。因此删除可能保留 Deleting 记录并返回 OutcomeUnknown。需要平台提供按稳定身份 fence 迟到创建、并确认最终删除的能力；ADX 不在缺少确认时删除元数据冒充已清理。

## 验证边界

独立及嵌入式 Activator 创建路径已有真实容器验证。2026-10-10 的独立云端部署进一步验证了 API Server HTTP 创建、新规格文件准入、跨副本上传下载及经 LiteLLM 的真实模型调用；合入最新上游后通过本地回归，未再次部署云端。冷启动样本通过不代表全部首请求可靠性或容量压测通过。真实华为授权码登录、鸿蒙实机、动态挂载和自定义健康检查仍待验证或实现。详细证据与限制见 [Jiuwen 设计](../docs/development/jiuwenswarm-adx-gateway-design.md)。
