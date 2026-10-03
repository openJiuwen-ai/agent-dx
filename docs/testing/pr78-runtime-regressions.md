# PR78 运行可靠性回归

## 创建失败后的资源释放

ADX 在调用 sandboxd Start 时预留本机资源。Start 失败、超时、应答丢失或成功载荷无效，均由 adxlet 将创建判定为 Failed，不重复启动同一执行。该判定不要求 sandboxd 增加应答标记。

创建进入 Failed 即释放本机标量资源和 GPU/NPU 预留，提交 `resources_held=false` 后尝试清理。清理与尚在等待的 Start 客户端任务串行；查询或删除失败时继续重试，不重新占用已经释放的资源。提交不可用时独立重试补写，清理重试不释放其他实例的预留。Failed 控制器在释放资源后继续定期清理迟到后端，不接纳为 Running，也不重复提交相同 Failed 状态。adxlet 重启通过完整权威目录对账清理残留。公开 API 的应答丢失与后端 Start 失败分开：前者仍复用原 Environment 身份查询或重试。

`platform/adxlet/tests/sandboxd_rpc.rs` 通过真实 gRPC/UDS 与模拟 sandboxd 覆盖失败清理、迟到后端和客户端超时；`platform/adxlet/tests/lifecycle.rs` 覆盖清理／提交失败时释放资源、整卡复用及资源已释放后的 Failed 巡检。这些是组件契约用例，不是实际运行时端到端证据。

## Reverse tunnel WebSocket 关闭

Execd 转发真实的 WebSocket close code 和 reason。空关闭帧在内部表示为 1005、空 reason；SDK 将其还原为空关闭帧，不能把保留状态码 1005 写入网络。传输错误或未收到 close 的 EOF 作为错误处理，不能伪装成正常 1000 关闭。隧道通道失败会向连接端返回 1011。

Rust Socket 用例验证两个方向的 1008/code-reason 透传；Python SDK 的真实 WebSocket 用例验证空关闭帧。2026-10-03 的 standalone 验收加载修复后的 Execd，并在实例内校验执行文件 SHA256。direct/tunnel 两条路径分别在并发 1、8、16 下运行 25 次连接，覆盖 0、1 KiB、64 KiB、1 MiB 二进制消息、ping，以及 1000/fixture-finished 关闭握手；全部通过并完成资源清理。证据为本地 `pr78-fixes-20261002/green-005.log`。

## 验证边界

2026-10-03 的本地测试使用 ARM64 Lima Ubuntu 24.04、Linux 6.8 和独立的两节点 Docker fixture。原报告使用 x86、48 CPU 和 AKernel fork gVisor；本地 ARM64 上游 gVisor 运行结果不能证明该 fork 的 gofer 并发故障已修复。运行时选择、bootstrap 来源和实际执行文件 SHA256 必须随证据记录。sandboxd 的守护进程重启与 supervisor stop 是两个不同操作，后者仍按显式停机契约删除本机实例。
