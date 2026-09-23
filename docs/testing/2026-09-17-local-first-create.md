# 本地优先创建接线与本地验证

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

2026-09-17，在 `fix/atomic-instance-claim` 工作树完成；基础提交 `6b2d30bea266629a79a0ce6ced46648b7146494e`，测试时包含未提交改动。
原 `api-server-rust` 工作树仍为该提交且干净，未覆盖主线已有代码。原子存储基础改动经复核保留，并在其上接入实际创建路径。

## 结果

- API Server 可配置节点目录订阅和轮转，默认 `central`，通过服务部署配置 `create_mode: "local_first"` 开启。
- Node Manager 共用 Admission 暂留标量资源和整卡，共用同 ID 控制器；持有未知结果的申请可后台重试。
- Master 在同一协调锁中检查节点身份、租户、心跳/会话和硬约束，执行 Redis CAS，并同步内存调度账本后确认启动。
- 中心/本地并发、同节点重复入口及请求取消均保持唯一归属和执行；本地不足时用相同 ID 回退中心。
- 不同规格竞争失败后的暂留泄漏已修复；迟到终态提交导致的 Master 旧资源占用也已通过先红后绿修复。
- 具体模块、恢复协议和 Pack/Spread/队列适用范围见 [契约](atomic-environment-claim.md)。

## 验证记录

环境为本机 macOS ARM64。所有编译使用现有 Cargo 缓存，执行并发为2。
下列日志均位于工作树 `out/ci/local-first/`，干净克隆不包含日志。

| 检查 | 结果 | 证据 |
|---|---|---|
| Master、Node Manager、API Server、调度库相关测试 | 161 通过、45 ignored | `targeted-verified.log` |
| 真实 Redis storage suite | 23 通过；包含侧会话新增9项 | `storage-final/result.json`、`storage-final/02.log` |
| 真实 Redis/mTLS/HTTPS RPC suite | 18 通过、0 ignored | `api-control-final/result.json`、`api-control-final/03.log` |
| Rust API HTTPS 本地优先 | 节点轮转、4并发、同 ID 重放、规格/租户冲突和删除通过 | `api-control-final/local-first-http.log` |
| 原有公开 HTTPS 回归 | 鉴权、生命周期、快照、Agent 转发等通过 | `api-control-final/api-http.log` |
| Clippy，相关包全部 targets，`-D warnings` | 通过 | `clippy-4.log` |
| Docker/K8s 验收驱动回归 | 59 通过 | `e2e-driver-final.log` |
| 格式/差异/配置与脚本语法 | `cargo fmt --check`、`git diff --check`、JSON/Python 语法通过 | 当前源码检查 |

45项默认忽略测试中，23项 storage 和18项 RPC 已由上述真实依赖套件显式执行；其余忽略项不计为已验证。
API 测试二进制 SHA256：`14f1b79777f82d54bce5c1d1d967a1ad57c3f977f9809f38b96a94a1b0510f25`。
Redis 测试二进制 SHA256：`9017e855ae87d02500bb3c8bab84dff77d55156e489c71b90106b4b642af54bc`。

关键负例证据：`scheduler-red.log` 为接口尚不存在时的编译失败；`rpc-1/03.log` 捕获规格冲突后的本机暂留残留；
`late-terminal-red.log` 捕获 Deleted 已落盘但 Master 仍保留100m CPU的账本残留，最终 RPC suite 已通过对应回归。
迟到终态测试通过存储层注入未被协调器观察的提交，不是网络丢包实验。

## 验证边界

真实 Redis、mTLS RPC 和 Rust HTTPS 进程均参与；RPC fixture 的 RuntimeDriver 使用测试实现。
随后已完成新制品真实 sandboxd/runc/RRT 本地双节点8组验收，包含 `local-first`，见 [端到端报告](2026-09-17-local-first-e2e.md)。
上述本地验收使用提交前工作树制品，因此当时不能沿用历史 Buildkite #24 七组的成功。后续已提交并由 [Buildkite #30](2026-09-18-runtime-environment-k8s.md) 使用正式发布包完成八组 K8s 验收；阶段11的正式 K8s 门禁已关闭。
