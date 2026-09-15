# Go Sandbox API 与服务启动

2026-09-15。本阶段把既有 HTTP 兼容层接到 Instance RPC，并提供 `adx-master`、`adx-node-manager` 和 `adx-sandbox-api` 进程入口。生命周期状态机仍在 Node Manager。源码与本地验证以当前未提交工作树为准。

## 请求链路

```text
SDK → Go Sandbox API
  认证：短时摘要缓存 → 缺失时 Master.VerifyApiKey → Redis 摘要记录
  创建：本地兼容字段转换 → Master.CreateInstance → Domain → Node Manager
  删除：本地归属缓存 → Node Manager.DeleteInstance
                         ↑ 缺失时 Master.GetInstance
Node Manager → sandboxd / RRT HTTP / Node Proxy UDS
Node Manager → MasterStateSink → Redis 条件提交
```

Frontend 的归属缓存包含完整 Assignment、节点地址和最后观察结果，有容量上限与有效期。命中后直接访问节点，节点检查租户与完整 generation；缓存不是生命周期权威。连接失败或归属失效时刷新，自动重试最多一次，且只能使用原 Assignment。新的 generation 不会继承旧删除请求。

正在等待确认的删除固定其目标，不因刷新或缓存淘汰更换目标；待确认操作达到上限时拒绝新的删除。已完成操作由节点的状态机与持久化结果处理重复调用。这些待确认记录仅在 Frontend 内存中，不提供 Frontend 重启后找回未确认操作的承诺。

## HTTP 兼容与支持范围

沿用既有 Sandbox URL、请求／响应封装和 SSE 行为。认证接受 SDK 的 `X-Auth`、`X-Auth-Token` 与 Bearer；调用者不能通过请求体或 `X-Tenant-Id` 自报租户。当前适配镜像、隔离 runtime、CPU、内存、磁盘、GPU/NPU 整卡、环境变量和 Instance ID。CPU 使用 millicores、HTTP 内存 MiB 转成内部字节；环境随 Spec 持久化和下发，执行身份及节点部署参数由 sandboxd 适配器最后覆盖。

Go 兼容层内部已有的消息编解码暂时保留，跨进程调用只使用新 `adx.control.v1` 协议。尚未接通的暂停／恢复、快照、网络策略、挂载、入口继承、空闲回收、独立资源上限、公开端口发布、安全模式策略和 HTTP 亲和策略翻译明确拒绝；不会当作已执行。用户命令仍走 RRT HTTP 数据接口。Agent 路由保留，配置 Agent 服务地址时转发给上层 Agent 服务；未配置时返回暂不可用。

## 认证

部署将初始 API Key 放在受保护的文件中，Master 初始化读取并将 SHA256 摘要及租户／管理员身份保存到 Redis。重复启动不更新已有密钥身份。到期时间为 Unix 秒，0 表示不设置到期时间；Master 校验和 Frontend 缓存都受该到期时间约束。缓存不保存明文密钥，未知／无效凭证不进入正向缓存。

组件间必须使用部署提供的 mTLS。Master 按证书指纹识别 Frontend 与具体 Node；身份上下文仅从受信 Frontend 接收。管理员的密钥创建／吊销接口、Edge 认证接线和证书热重载还未实现。

## 服务装配

三个进程均使用 `--config /path/to/config.json`，未知配置项拒绝。配置示例见 `build/config/examples/`，文件路径按进程工作目录解释。Go 对外服务使用 HTTPS；内部 gRPC 客户端校验配置的 CA 与服务端名称。

Master 在绑定监听地址后开启一次 Redis Session，恢复调度目录，装配 MasterService 与 AuthService。

Node Manager 装配真实 Sandboxd、RrtReadiness、UdsRoutes 和 MasterStateSink。当前启动入口读取外部采集源的 JSON 容量观察文件，使用其有效期；它不是硬件自动探测器，也未把 sandboxd 的实例 Stats 冒充节点可用上限。节点自动探测与 sandboxd external collector 的直接适配留待资源采集阶段。

Node 启动后先关闭生命周期入口，以本次进程身份注册并获取 Master 完整目录，与 sandboxd 实际实例对账，恢复资源占用和本机绑定后再开放准入。Master 暂不可用时不清理或重建。详细契约见 [发现与重启对账](recovery-discovery.md)。

入口支持 Redis 注册发现，Frontend 的 Master／认证客户端共用动态 resolver，节点直达缓存继续保留。Node Proxy 全量同步、统一配置生成与 supervisor 尚未装配。直接发送进程退出信号不替代规划中的 CLI stop 实例清理流程。

## 验证入口

```sh
# 先在运行测试的主机平台构建 Go 服务（不是测试模拟服务）
(cd platform/control-plane/sandbox-api && go build -o /absolute/out/adx-sandbox-api ./cmd/adx-sandbox-api)
export ADX_TEST_SANDBOX_API=/absolute/out/adx-sandbox-api
export ADX_TEST_REDIS_SERVER=/absolute/path/to/redis-server
python3 build/ci/run.py frontend-control --jobs 2
```

测试启动真实 Go HTTPS 进程，经过生产 Rust RPC、mTLS、Redis 和 Node Manager 状态机。运行时、就绪检查和本机路由仍由 RPC 测试夹具提供可控实现，因此不算 sandboxd／RRT／Edge 完整平台 E2E。此前的单独 `control-rpc` 入口保持可运行；`frontend-control` 要求实际 API 二进制。

证据目录：`out/ci/frontend-control/`。缓存与 generation 约束先写测试，`red-go.log` 保存实现缺失时的红灯。后续结果记录将明确源码、二进制与测试边界。

## 本轮结果

| 验证 | 结果 | 证据，相对 `out/ci/frontend-control/` |
|---|---|---|
| Go 完整模块测试、vet 与 API 可执行文件构建 | 111 个顶层测试及 93 个子测试通过，vet 通过 | `go-complete.log` |
| 五个 Rust crate 回归 | 94 通过，0 失败，9 个显式环境／性能用例默认忽略 | `rust-final.log` |
| 严格 Clippy，全部目标 | 通过 | `clippy-final.log` |
| 真实 Go HTTPS→Rust RPC→Redis；独立 Master 进程启动、重启 | 2 通过，0 失败，0 忽略 | `integration-complete/result.json` |
| 真实 Redis 存储集成，含初始凭证幂等与旧 Session 拒绝 | 6 通过，0 失败，0 忽略 | `storage-final/result.json` |
| CI harness | 7 通过 | `harness.log` |

Go 测试／构建使用 Linux ARM64 Go 1.25.5 容器，交叉编译 macOS ARM64 API 二进制；RPC、独立 Master 进程与 Redis 7.2.5 在 macOS ARM64 运行。Node Manager 入口通过编译和静态检查，未用真实 sandboxd 启动完整节点。`source-manifest.json` 保存源码和可执行文件 SHA256；最终 RPC 结果另包含 API 与 Redis 二进制摘要。基准提交为 `1e49d86f2123173a8f5358182ca294fab9a9b1e4`，验证对象是其上的未提交工作树。

`integration-complete/frontend-http.log`、`sandbox-api.log`、`master-process-0.log` 和 `master-process-1.log` 分别保存跨语言断言、API 进程以及 Master 首次启动／重启证据。普通 Rust 回归中的忽略用例不算通过；对应真实入口分别实际执行上述 2 项 RPC 和 6 项 Redis 测试。
