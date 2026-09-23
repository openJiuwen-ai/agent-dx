# ADX 当前的平台依赖与缺口

本清单对应当前源码。Platform 源码本轮只读；ADX 复用已有能力，缺失部分不以额外状态库、健康轮询或生命周期控制器替代。inline 对照元戎 Frontend `2bef00f` 的 `/api/agent` 协议；历史 invoke 不在 v2 范围内。

## 暂缓项

| 能力 | 当前事实与影响 | 当前处理 |
| --- | --- | --- |
| 自定义镜像动态注入 Execd、启动配置及依赖 | 当前 ADX Sandbox 适配使用镜像中预置的 Execd 和进程配置，执行规格必须匹配部署 profile。不能把任意镜像视为已具备 Execd。独立 Activator 发布模板时验证产品规格，预装 profile 匹配由 Sandbox API 在创建时执行；发布成功不代表已满足预装条件 | **Pending**；继续使用预装镜像验证 |
| 动态挂载、workspace、用户与工作目录 | 平台规格已有 rootfs/mount 字段，但当前 ADX 预装适配路径未接通完整动态注入/挂载。inline 只接受与预装 profile 一致的意图，包括 `rootfs.workdir` | **Pending**；不修改平台，不静默丢弃挂载意图 |
| 自定义 startup/liveness 健康检查 | ADX 将 Platform Running 视为可用；当前 adxlet 创建路径检查 Execd 控制状态并激活节点路由，但不保证用户业务端口及 Gateway 路由传播已完成 | **Pending**；冷启动可靠性依赖平台健康检查与就绪契约。ADX 不新增端口探测，调用方使用已返回的 Environment ID 重试 |

## inline 文件与命令接口

| 接口/语义 | 已复用的能力 | 尚缺能力及当前边界 |
| --- | --- | --- |
| `POST /api/agent/{id}/exec` | Execd `/invoke` 的 `cmd_run`；字符串命令或字面 argv、工作目录、环境变量、timeout；结果适配为 `returncode/stdout/stderr` | Execd 没有旧 Executor 完整的结构化操作错误契约。命令非零退出仍作为正常执行结果返回；传输中断不自动重放。当前 timeout 默认 60 秒，最大 3600 秒，JSON 响应限制 1 MiB |
| `POST .../files/upload` | multipart `path` 必须先于 `file`；一次 Execd uploadId 请求流式写临时文件，最后 `/upload/commit` rename；文件上限 512 MiB | Execd 未提供 mode 设置、提交前 fsync 与上传取消/临时文件回收的完整契约。非空 mode 明确拒绝；失败不提交目标，但平台可能残留临时分片。不能宣称具有原实现同等的崩溃持久化保证 |
| `GET .../files/download` | Execd 原始字节流；无 Range 或单个显式、开放末尾、后缀 Range。适配层用文件大小规范化范围，不可满足返回 416 | Execd 文件查询与下载不是同一文件句柄快照；文件并发变化时不保证所读版本一致。平台文件错误仍不完全结构化 |
| `GET .../files/list` | Execd `fs_list`，适配为 `items`；支持 recursive、max_depth，默认递归深度 20，平台上限 64 | Execd 缺少原接口的 10,000 条扫描上限、扫描时间预算和完整符号链接语义；现有限制只能限制 Gateway 读回的 JSON，不能限制 Execd 扫描内存。普通文件作为列表目标和文件系统 errno 映射也不等价 |
| `POST .../files/mkdir` | Execd `fs_make_dir` 可递归创建目录 | Execd 固定递归创建，未提供非递归与 mode 语义。因此当前只支持 `recursive=true` 且不传 mode；其他组合明确返回不支持，不能悄悄按递归创建 |

上述缺口需要 Execd 提供相应的文件/命令能力及结构化错误。ADX 不通过执行临时 shell/Python 文件管理脚本绕过平台能力边界，也不把内部 Execd token 返回给调用方。公开接口仍使用旧 JWT/IAM 管理鉴权，连接走 Gateway 的共享路由与 Relay。

## inline 生命周期与查询

- 列表已有可复用能力：Platform `EnvironmentDirectoryService.WatchEnvironments` 的首帧完整快照。Gateway 每次查询读取首帧后关闭订阅，只返回当前认证租户、Running 且带 ADX 执行标识的实例。ADX 不维护额外目录缓存或实例数据库。
- 查询详情从平台记录读取逻辑 ID、运行载体 ID、IP 和资源；能匹配预装 profile 时补充镜像、工作目录、用户与业务环境变量。内部 Execd 凭据、端口注入配置和执行 hash 不返回。平台记录尚不能完整还原旧协议的 trace_id、start_time、rootfs 原始挂载意图、原始端口声明及元戎全部状态/状态消息；无法确认的字段不伪造。
- create 使用旧 Frontend 的 `namespace/name` 规范串生成 UUIDv5，重试保持真实 Sandbox ID。旧规则没有 tenant 段，跨租户同名冲突由平台租户校验拒绝，不能复用别人的实例。平台仍需明确删除后同一指定 ID 的再次创建契约，ADX 不私自生成别名规避。
- 旧 Frontend 在部分创建超时情况下返回已分配 ID，并对删除立即返回“deleted”、后台投递 kill。当前平台没有独立、可靠的异步创建/删除受理回执与取消/fencing 契约；当前适配保留已确认创建/删除才成功的语义，未知结果返回可回查的 ID/错误。**这部分尚未完全等价于旧接口**，不能用 Gateway 内存后台任务代替平台可靠受理。

## Execd 启动锁竞争

2026-09-23 的 `3141e27` 真实容器验证（当时组件名为 RRT，现名 Execd）中已观察到：业务 HTTP 返回 200，但 Execd `/control/v1/status` 持续超时，Platform 因而保持 Creating。与运行二进制相同 Build ID 的调试符号定位到 Execd 主线程在 `entrypoint::Manager::complete_create()` 等待子进程 Mutex；监视线程在 `start_from_environment()` 的 `match child.lock().try_wait()` 分支中持有同一把锁休眠 10ms，并循环抢锁。同步锁等待会阻塞 Execd 的单线程异步运行时，导致控制接口也无法处理请求。

这是 Platform/Execd 的启动可靠性问题。平台修复点是先在短作用域内执行 `try_wait()` 并释放 guard，再处理结果和休眠。本轮不修改 Platform；测试只对尚未终止的同一实例做有界查询，不能保证锁竞争总能在平台启动超时前恢复，也不能通过换 ID 重建掩盖终态失败。

## 创建期限与结果未知

ADX 从入口传递创建的剩余绝对期限，Sandbox HTTP 默认上限为 60 秒；inline 与 Platform RPC 的部署配置可进一步缩短它。Platform RPC 的调度/创建预算字段及 gRPC timeout 均取自剩余预算，不重新获得完整等待时间。超时前已提交的创建仍返回结果未知，调用方只查询或重试原身份。

当前 Coordinator 中央创建只使用 `schedule_timeout_seconds`，尚未将 `create_timeout_seconds` 传入 adxlet 的 assigned-create 路径；adxlet 实际启动上限由自身 `rpc_timeout_seconds` 决定，取消外层 HTTP/gRPC 请求也不代表取消已受理的创建。因此即便 ADX 预算一致，平台仍可能在响应超时后继续创建。这是 Platform 的取消与期限传递缺口，本轮不修改 Platform，也不在 ADX 中增加等待。验证可在外部对同一 ID 做有界查询。

## Environment 的中间记录与删除

流量触发激活时，ADX 先写入 Environment 身份，再向平台提交同一 Sandbox ID。进程中断、创建失败或响应丢失时，可能存在“有 Environment 元数据、无已确认可用 Sandbox”的状态；Active 表示产品记录可用于激活，不代表运行健康。

Environment 统一由 HTTP/WS/SSH 访问触发；查询和列表可能看到上述未完成激活的中间记录，重试使用已返回的原 Environment ID。

平台不能为从未观察到的创建提供取消标记/墓碑，查询不存在并不能排除迟到的创建。因此删除可能保留 Deleting 记录并返回 OutcomeUnknown。需要平台提供按稳定身份 fence 迟到创建、并确认最终删除的能力；ADX 不在缺少确认时删除元数据冒充已清理。

## 验证边界

动态注入、动态挂载、自定义健康检查和冷启动首请求可靠性均未验收。组件测试的模拟 Platform/Execd 不等于真实平台端到端；最新验证证据见 [Agent README](README.md#验证)。
