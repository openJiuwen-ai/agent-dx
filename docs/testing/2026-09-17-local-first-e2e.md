# 本地优先创建：真实双节点端到端验收

2026-09-17，`fix/atomic-instance-claim` 未提交工作树，基础提交 `6b2d30bea266629a79a0ce6ced46648b7146494e`。

本机 Docker Desktop 原生 Linux ARM64 环境，两个隔离节点运行当前源码重新构建的 release 制品；安装发布包内的 Sandbox SDK，经 Edge → Rust API Server → Node Manager/Master 创建真实 sandboxd/runc 实例，命令经过 Edge → Node Proxy → 实例内 RRT。

## 结果

| 用例组 | 结果 | 耗时（秒） |
|---|---|---|
| SDK | passed | 25.796 |
| 鉴权 | passed | 20.940 |
| 资源容量 | passed | 33.615 |
| 放置约束 | passed | 94.875 |
| local-first | passed | 37.131 |
| 节点失联 | passed | 52.734 |
| Node Manager 重启 | passed | 4.892 |
| 停机清理 | passed | 30.408 |

本地优先组确认首两个 Capsule 归属 node1/node2；两个独立 SDK 请求创建同名 Capsule 收敛到同一个 ID；不同 CPU 规格返回冲突；3 个唯一 Capsule 均执行真实 RRT 命令，并在当时的 Master 日志确认 `local_instance_claim`。当前 Capsule/Runtime 协议重构后，同一事件名为 `local_capsule_claim`。组后两个 sandboxd inventory 为空。

容量组确认释放后的 Master 与 Node Manager 资源指标归零且一致；节点失联后清理、进程重启保持后端实例身份、带实例停机均通过。最终 `missing_checks=[]`、`cleanup_errors=[]`，两个测试容器及专用网络已移除。

## 制品与证据

- target：`aarch64-unknown-linux-gnu`，release，dirty=true；Redis 7.2.5。
- sandboxd：`efc201531d7e2e9d69505da151eb66084b61eebf`（PR #56 锁定版本）。
- Node 镜像：`sha256:7763b7a0915cab8d863ab2171c5e3fc9e55f5cb17d9aeec29304fe86326122ff`。
- RRT 镜像：`sha256:bec22160090024aa102b2526cf6a9a0e00b3e52e6190d334048c0dbddf6e1805`。
- package manifest SHA256：`e5786615af61788c8ad36e5099668451526da696066fa4bcfa522d5b1f0e75a3`。

工作树下 `out/ci/local-first-e2e/` 保留：

- `build.log`、`package/manifest.json`：当前代码完整构建及文件校验。
- `source-files.json`、`source-files-run-2.json`：构建源码及第二轮测试脚本的逐文件摘要。
- `bundle-2/bundle.json`：后端二进制哈希、架构与镜像身份。
- `run-2.log`、`run-2/result.json`、`run-2/case-results.json`、`run-2/junit.xml`：逐组日志及总结果。
- `run-2/local-first-result.json`：归属、generation、同名收敛、规格冲突、命令及 claim 日志检查。
- `run-2/metrics-*.json`、`run-2/stop-node*.json`、`run-2/logs-node*/`：资源、后端清理与组件记录。

## 首轮失败与边界

首轮前4组通过；local-first 冲突请求使用 `create_timeout=30`，因未给调度预算预留至少30秒而被 SDK 拒绝，尚未发到服务端。将该测试参数与同组正常创建对齐为150秒，重新准备镜像后完整8组通过；没有改变产品超时和重试行为。产品二进制与 SDK 均复用本轮新构建包。`run-1` 失败及清理证据保留。

本轮是本地双 Docker 节点/runc 验收，不是 Kubernetes/Buildkite 验收，也不覆盖 Firecracker checkpoint、XPU、性能或真实网络丢包。结果未知/延迟写入及中心竞争的详细故障验证仍以 Redis/RPC 套件为证。上述验收使用提交前工作树制品；后续提交 `363e44f` 的 [Buildkite #30](2026-09-18-runtime-environment-k8s.md) 已从干净源码构建并完成正式 K8s 八组验收。
