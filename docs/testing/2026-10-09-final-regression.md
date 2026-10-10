# 2026-10-09 修复回归记录

## 验证边界

本轮验证 H2 CONNECT 收尾、运行期网络策略、Kata 原生 PTY/CLI、RPC 传输错误分类和 supervisor 持续重启。使用远端 x86 Linux/KVM standalone；不涉及集群升级或发布。

主机内核为 Linux 6.8.0-137，32 CPU、123 GiB 内存。AKernel 测试工作树基于 `5302c3d25cac0835666d6a2fb826809a181c97c4`。镜像 `akernel-final-regression:20261009` 的本地 image ID 为 `sha256:cf7a1cc9e2b3e436fb27c9e970f92c47d4c4b9a0773d8421997dc02b2ee266e6`，覆盖 API Server、adxlet、adxctl 及内嵌配置，sandboxd 和 Execd 未改动。

| 二进制 | SHA256 |
| --- | --- |
| adx-apiserver | `ae0d29ad318d236645a89421c8134d01d43596bbd7559febe063353654739e85` |
| adxlet | `6273970ed5d5a2927bf73dc9d92a4a74b9678086064c6174f456de8f84123792` |
| adxctl | `6b1369a8e97fd6fce586a7f89139e2018f8b5ec320f96c2c256adfc9400c661b` |

## 已确认结果

Rust API Server、adxlet、Gateway 和部署组件共 416 项通过、0 项失败、2 项忽略。忽略项分别需要独立 Redis 和系统 OpenSSH。H2 收尾七项使用真实 TCP/h2，包括在同一连接上执行 1,100 个逻辑流；Kata Mount 协议测试包含默认 devpts、显式挂载保留及其他 runtime 不变；supervisor 测试包含连续超过原重启额度及启动失败后的恢复。

两种网络后端的真实 SDK/探针验证均通过：

- bpfnat 和 iptables：更新到默认拒绝策略后，原 Execd TCP 连接保持相同源端口且返回 200，新 TCP 也返回 200；用户出站被拒绝。
- 两种后端的 network-policy 示例矩阵和动态策略更新用例通过。
- 每组结束 Redis Environment 目录、预留及 sandboxd 后端均为空。

原规模混合压力使用 32 个常驻沙箱、目标 20 churn/s、128 客户端在途上限、120 秒负载窗口。22,465 次常驻命令及 36 字节文件读写、2,244 次创建流转全部成功，业务失败为 0。2,401 个到达槽中 157 个未提交，实际 churn 约 18.7/s；含准备和清理总耗时 149.556 秒。此结果不等于达到目标速率，也不等于长时间稳定性验收。

混合压力日志中 `STREAM_CLOSED`、library-reset、内部 reset 上限及文件截断错误计数均为 0。H2 debug 日志另有 8,976 条 `Connection::poll; connection error`，其 error 字段均为 `GoAway(b"", NO_ERROR, Library)`；不能把字符串包含 connection error 的所有记录视为原故障，也不能宣称日志完全没有该字段。

有效凭证下的功能补跑结果：

- runsc：21 个测试组通过，包含 52 项 unittest 及可写层限额、网络策略两个示例矩阵，0 skip。
- OCI rootfs 独立写入、只读 OCI mount、继承 entrypoint：3 项通过。
- RFC6455 WebSocket：直连和反向隧道各 C1/C8/C16，共 6 组通过，逐帧检查正文、Ping/Pong 和 close code 1000。
- HTTP tunnel：9 组通过，包括默认在途限额、提高限额后的 C512、64 MiB 请求体及慢上游。默认限额下的预期 429 与缓存满拒绝新建不是同一语义。
- 该组结束严格 Redis/backend 审计为空。
- Firecracker：9 组、34 项 unittest 全通过，0 skip；包含原生 PTY 和 checkpoint/reload/reverse tunnel。

Kata 初轮通过 8 个完整测试组和 runtime-integration 的 6 个子项；唯一未通过的子项同时请求 `storage_mb=256` 和 checkpoint/reload。实际 Start 请求的 `writable_layer_limit_bytes=268435456` 在当前 sandboxd `internal/server/server.go` 被拒绝：限额只支持 runsc/Firecracker；Kata 也没有实现 `CheckpointHandler`。该子项不适用于 Kata，不应作为 PTY 修复失败。SDK 测试已明确跳过这一子项，命令、文件和 PTY 子项保留。修正后的 Kata 子组为 6 项通过、1 项明确 skip；结合其他八组，支持范围内共有 33 项成功。

进程故障专项：

- adxlet TERM 重启：原 Environment ID、文件、命令及最终删除均成功，验证采用新 PID 和对账 ready 前置。
- sandboxd SIGKILL 重启：失败，120 秒内原 runtime 无法重新访问，持续收到 `503 route absent from synchronized cache`。daemon 重启日志显示恢复了一个磁盘记录，但连接对应 runsc control socket 被拒绝，报告 exit 128；随后 adxlet 清理该后端并撤路由。service 配置的 KillMode 为 process。现有证据不能确定 runsc/gofer 实际退出的原因，不能将此问题归结为单纯路由缓存延迟。
- 故障阶段最终显式删除和严格审计成功，Redis Environment 目录、资源预留及 backend 均为空，测试容器已移除。

因此本轮不能宣称所有故障场景已解决，或者所有运行时功能没有回归。未闭环项为当前测试镜像中 runsc 的 sandboxd daemon 崩溃存活契约；Kata checkpoint/restore 属于不支持的能力，不计作修复失败。

### SIGKILL 时序对照

同一镜像另外执行四轮独立 standalone，每轮保留一个已落盘的对照实例，再创建目标实例。注入只杀 sandboxd 主进程；必须确认 MainPID 已变化且新 daemon 的 `sbox list` RPC 成功，之后分三次验证命令和目标文件。没有把 `systemctl active` 或重启前的旧 Execd 连接成功当成恢复证明。

| 条件 | 创建到 SIGKILL | 新实例恢复 | 已落盘对照实例 |
| --- | --- | --- | --- |
| young-1：下次 cgroup 持久化前 | 786.6 ms | 三次均失败 | 三次均成功 |
| young-2：下次 cgroup 持久化前 | 862.6 ms | 三次均失败 | 三次均成功 |
| mature-1：等待完成 cgroup 持久化 | 8,792.2 ms | 命令、文件三次均成功 | 三次均成功 |
| mature-2：等待完成 cgroup 持久化 | 8,842.2 ms | 命令、文件三次均成功 | 三次均成功 |

四轮最终 Redis/backend 审计均为空。两种镜像的 sandboxd SHA256 同为 `04214d562a3974562c7945b9518a37c39ee2e3e37a1d1612e589627c55aafd0a`，分别为此前测试的 `pr78-operation-cache-v2-20261009` 和本轮 `akernel-final-regression:20261009`。差异不能归结为本轮升级了 sandboxd。

两次 young 实验中，SIGKILL 后立即采集的目标 cgroup 仍有原 PID；直到新 daemon 初始化，目标 PID 才消失。新 daemon 的 cgroup 已用名单只加载一项，而 runtime 磁盘记录恢复两项；目标后端随后出现 control socket `connection refused`、非 active cgroup，已落盘的对照实例继续运行。两次 mature 实验已用名单加载两项，两个原 PID 和后端均保留。这与源码中的恢复清理路径一致：`Allocate` 只更新内存并设 dirty，`keepStoring` 每 5 秒持久化一次；构造器仅将持久化名单视为正在使用，其他 cgroup 进入 `cleanForReuse`，该方法杀掉其中进程。源码核对基于 sandboxd `5970730acc01c25314d7db8322518f6ac4795d0a` 的 `pkg/cgroupmanager/cgroup.go`。

因此该条件在当前环境中可复现：未落盘的新 cgroup 在 daemon 恢复时被当作空闲资源清理。不是仅由杀死父 daemon 立即连带退出，也不证明所有 runsc 实例在 SIGKILL 后都会退出。两轮/条件的样本不能用来估算生产故障概率。本轮尚未修改 sandboxd；修复应让恢复阶段先核对有效 runtime 的 cgroup 归属，再清理真正的空闲资源，不能用延迟故障注入规避缺陷。

此前 SIGKILL 用例仅等待 systemd `active`，没有核对新 PID，且命令成功即结束；那次通过不足以证明 daemon 重启后的存活，需按上述前置条件重新判读。

初次进程故障测试在 adxlet RPC 恢复前进入删除：新 listener 就绪日志为 12:15:40.858Z，测试已在约 3 秒固定等待后结束三次有限 SDK 重试。收到的是可重试 503，不再是错误的 500。后续故障 hook 改为等待新 PID 和 Relay `/readyz` 对账就绪；sandboxd 使用 SIGKILL，并以 `sbox list` 的真实 RPC 成功及 Relay ready 作为恢复前置。没有增加产品请求 deadline 或把固定等待延长后当作故障验收。

## 测试环境失败与补跑

初轮没有为私有 OCI 镜像提供有效凭证，出现 SWR `DENIED`。第二轮使用已有凭证时发现凭证失效；从本地有效 kubeconfig 读取已有 registry Secret 后，固定镜像 digest 的访问检查通过。没有修改 registry 权限、镜像或集群部署。

初轮驱动还继承了启动脚本的 `set -e`，导致失败用例后的审计未执行；补跑已逐项保存 exit code 并继续审计。外层汇总改为主机存在的 `python3`。所有失败记录保留，不计为有效功能回归结果。鉴权失败留下的 Failed 元数据没有资源预留或运行后端；Coordinator 的默认 Failed 保留期为 600 秒，未到期时存在这些记录不证明泄漏。

## 证据位置

ADX 工作树：`out/ci/final-regression-20261009/rust-tests-corrected.log`。

远端 AKernel 工作树 `/root/akernel-worktrees/pr78-report-regression-20261009`：

- `out/ci/report-regression-20261009/final/`：两种网络后端、混合压力及初轮失败证据。
- `out/ci/report-regression-20261009/final-r2/`：修正驱动后，运行时及故障专项。
- `out/ci/report-regression-20261009/final-r3/`：有效凭证下的功能补跑。
- `out/ci/report-regression-20261009/final-r4/`：按运行时能力修正的 Kata 子组及按就绪探测修正的故障专项。
- `out/ci/report-regression-20261009/sigkill-timing/`：四轮时序对照的 `summary.json`、进程/cgroup 快照、daemon 日志和严格审计；总日志为 `/var/log/akernel-sigkill-timing-20261009.log`。
- `/var/log/akernel-final-regression-20261009.log`、`/var/log/akernel-final-regression-r2-20261009.log`、`/var/log/akernel-final-regression-r3-20261009.log`、`/var/log/akernel-final-regression-r4-20261009.log`：各轮执行日志。

本地精简结果副本位于 ADX 工作树 `out/ci/final-regression-20261009/evidence/`。其中 final-r2/runtime 原始汇总仍保留不适用于 Kata 的失败；最终能力范围应结合 final-r4/fault/kata-supported 的明确 skip 判读，不覆盖历史失败证据。

不包含真实 GPU/NPU、S3 rootfs、多节点故障恢复、生产 DNS/TLS 或 Kubernetes 升级验收。原报告的 gofer EBUSY 曾在原规模创建回归中未复现，不能由此认定 gVisor 根因已修复。
