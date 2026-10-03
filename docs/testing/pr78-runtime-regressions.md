# PR78 运行可靠性回归

## 创建失败后的资源释放

ADX 在调用 sandboxd Start 时预留本机资源。Start 失败、超时、应答丢失或成功载荷无效，均由 adxlet 将创建判定为 Failed，不重复启动同一执行。该判定不要求 sandboxd 增加应答标记。

创建进入 Failed 即释放本机标量资源和 GPU/NPU 预留，提交 `resources_held=false` 后尝试清理。清理与尚在等待的 Start 客户端任务串行；查询或删除失败时继续重试，不重新占用已经释放的资源。提交不可用时独立重试补写，清理重试不释放其他实例的预留。Failed 控制器在释放资源后继续定期清理迟到后端，不接纳为 Running，也不重复提交相同 Failed 状态。adxlet 重启通过完整权威目录对账清理残留。公开 API 的应答丢失与后端 Start 失败分开：前者仍复用原 Environment 身份查询或重试。

`platform/adxlet/tests/sandboxd_rpc.rs` 通过真实 gRPC/UDS 与模拟 sandboxd 覆盖失败清理、迟到后端和客户端超时；`platform/adxlet/tests/lifecycle.rs` 覆盖清理／提交失败时释放资源、整卡复用及资源已释放后的 Failed 巡检。这些是组件契约用例，不是实际运行时端到端证据。

## 2026-10-04 实际运行时故障回归

从干净 ADX `2e15df5` 源码构建 Linux ARM64 adxlet，SHA256 为 `b01bb4a98bad51104b5e4874d4522723c81d0af4f1d0297042bcaaa9a1246039`，覆盖已记录的两节点 runc fixture。sandboxd 使用 `eceda17` 修复二进制，SHA256 为 `0d28a3c1afe1ba92b73a789375e46834dc4b12b2dff62be18840e88eae35c852`。其他组件沿用 fixture，属于最新 adxlet 的真实 SDK 故障链路回归，不代替完整正式发布包验收。

gRPC/UDS 代理分别注入执行前 Start 错误、真实 Start 成功后切断应答、切断应答并使 Delete 不可用，以及超时后才执行的迟到 Start。四例均进入 Failed、`resources_held=false`，每例仅一次后端 Start。Delete 不可用期间可观察到一个真实残留后端；取消故障后巡检清空。迟到后端也被巡检清理，没有变成 Running。SDK 对公开创建请求的重试不会再次执行后端 Start。

随后正常 SDK command、文件读写和删除通过。逐例显式删除后 Redis 主目录消失，最终 Redis environment 字段为空、两节点 backend 为空，fixture 清理通过。注意这验证了显式删除和后端清理，未实现 Failed 元数据自动 GC。证据保存在本地 `pr78-failed-create-release-20261003/evidence/`，包含 `failed-create-release.json`、`start-proxy-events.jsonl`、`fix-identity.json` 和 `result.json`；组件回归 369 通过、63 忽略，忽略的 Redis/RPC 用例不计通过。

## Reverse tunnel WebSocket 关闭

Execd 转发真实的 WebSocket close code 和 reason。空关闭帧在内部表示为 1005、空 reason；SDK 将其还原为空关闭帧，不能把保留状态码 1005 写入网络。传输错误或未收到 close 的 EOF 作为错误处理，不能伪装成正常 1000 关闭。隧道通道失败会向连接端返回 1011。

Rust Socket 用例验证两个方向的 1008/code-reason 透传；Python SDK 的真实 WebSocket 用例验证空关闭帧。2026-10-03 的 standalone 验收加载修复后的 Execd，并在实例内校验执行文件 SHA256。direct/tunnel 两条路径分别在并发 1、8、16 下运行 25 次连接，覆盖 0、1 KiB、64 KiB、1 MiB 二进制消息、ping，以及 1000/fixture-finished 关闭握手；全部通过并完成资源清理。证据为本地 `pr78-fixes-20261002/green-005.log`。

## 验证边界

2026-10-03 的本地测试使用 ARM64 Lima Ubuntu 24.04、Linux 6.8 和独立的两节点 Docker fixture。原报告使用 x86、48 CPU 和 AKernel fork gVisor；本地 ARM64 上游 gVisor 运行结果不能证明该 fork 的 gofer 并发故障已修复。运行时选择、bootstrap 来源和实际执行文件 SHA256 必须随证据记录。sandboxd 的守护进程重启与 supervisor stop 是两个不同操作，后者仍按显式停机契约删除本机实例。

## 原报告逐项闭环边界

核对日期：2026-10-04。原报告对应 AKernel `52d0e2f`、ADX `5e62b9f`、sandboxd `31d0749`。以下将修复、未复现和交付验收分开；源码发布不等于正式包或集群已升级。

| 原报告问题 | 当前证据 | 尚未闭环 |
|---|---|---|
| 后台命令短 wait、自然退出 PTY 对象保留 | AKernel `6953519` 修复；271 SDK 单测、永久 standalone 两项和 100 个自然退出 PTY 通过 | 新 SDK 正式制品及部署 pin 整合 |
| Start 失败／应答丢失后持续持有资源 | 创建 Failed 即释放；清理与提交失败、Journaled 降级、GPU/NPU 复用和 UDS 迟到应答均有组件测试 | 同一正式制品及部署 pin 整合；Failed 元数据 GC 尚未实现 |
| runsc gofer `EBUSY` | cn-north-4 六场景累计 8,189 次生命周期成功，未复现 | 原 x86/Linux 6.8/fork runsc 同条件复现及 main A/B；根因未定位 |
| 混合压力文件截断／EOF | 原负载 runc 38,618 个读写周期、2,400 次生命周期通过；集群 runsc 22,859 个周期无错误 | runsc 混合仅提交 1,106/2,401 创建时隙，未完成 20 create/s；原 EOF 根因未定位 |
| tunnel WebSocket close 丢失及并发消息 EOF | 关闭帧已修复；runc direct/tunnel C1/C8/C16 的 50 连接、200 消息、2,048 ping/pong 通过 | runsc 同组合 WebSocket 复测；不能由 close 修复推断消息 EOF 根因已解决 |
| sandboxd 重启后 runsc 实例丢失 | sandboxd 停机保留工作负载已修改；runc 同 backend 与 IO 恢复通过 | 修复制品的 runsc daemon 重启、AKernel 实际停机链路和部分启动隔离资源最终清理 |
| Kata guest 缺少 devpts、PTY／CLI 不可用 | sandboxd `6976ee9` 配对修复，native openpty 与 SDK PTY 通过 | 原 x86 Kata 完整 interrupt、独立会话、close 和 CLI 矩阵 |
| bpfnat 动态 block 后控制面失联 | 真实 TC/BPF 测试及 Firecracker＋bpfnat block/clear 通过 | runsc＋bpfnat 控制端口放行和持续控制链路 |
| S3 根对象空 prefix 忽略覆盖配置 | sandboxd 修复；空／非空 prefix 的 HEAD/Range 读取通过 | 真实 AWS S3／OSS 签名、ACL 和网络互操作 |
| 旧 wait／rrt.sock 文档 | AKernel 当前源文档已同步 wait 契约和 execd.sock | 随正式制品校验部署文档与 pin |

此外，Failed 控制任务与 Redis 记录没有自动保留期限；当前显式 DELETE 才删除主目录，最小删除回执保留十分钟。高并发创建尾延迟、短暂路由 503、测试部署的私有 SWR 凭证缺失和资源／队列接口 404 仍需独立处理。GPU 实机、多节点亲和、整节点故障与共享 checkpoint 跨节点恢复、长期 soak 属于原报告未覆盖的验收边界，不列作已复现缺陷。
