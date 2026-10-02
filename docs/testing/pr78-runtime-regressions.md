# PR78 运行可靠性回归

## 创建失败后的资源释放

ADX 在调用 sandboxd Start 时预留本机资源。RPC 超时或断线不能证明 Start 已停止，List 暂时为空也不能证明在途 Start 不会随后创建执行实例；这些情况保持结果未知，不提前释放资源。

sandboxd 可以在失败 RPC 的 trailer 中返回 `sandboxd-start-settled: true`。该标记只用于尚未调用执行后端的失败：Start 和该调用的延迟回滚已经返回，并不表示清理一定成功。调用过 runtime Start/Restore 后，即便 Go 函数返回失败，也可能仍有后台执行，不能发送该标记来推断操作已停止。ADX 收到该标记后仍按逻辑 Environment 标签查询物理实例，删除残留并确认没有执行实例，随后才提交 `resources_held=false`。未带此标记的旧 sandboxd 继续采用保守的结果未知行为。成功回复、确定性拒绝及真实传输中断分别由 `platform/adxlet/tests/sandboxd_rpc.rs` 看护。

## Reverse tunnel WebSocket 关闭

Execd 转发真实的 WebSocket close code 和 reason。空关闭帧在内部表示为 1005、空 reason；SDK 将其还原为空关闭帧，不能把保留状态码 1005 写入网络。传输错误或未收到 close 的 EOF 作为错误处理，不能伪装成正常 1000 关闭。隧道通道失败会向连接端返回 1011。

Rust Socket 用例验证两个方向的 1008/code-reason 透传；Python SDK 的真实 WebSocket 用例验证空关闭帧。2026-10-03 的 standalone 验收加载修复后的 Execd，并在实例内校验执行文件 SHA256。direct/tunnel 两条路径分别在并发 1、8、16 下运行 25 次连接，覆盖 0、1 KiB、64 KiB、1 MiB 二进制消息、ping，以及 1000/fixture-finished 关闭握手；全部通过并完成资源清理。证据为本地 `pr78-fixes-20261002/green-005.log`。

## 验证边界

2026-10-03 的本地测试使用 ARM64 Lima Ubuntu 24.04、Linux 6.8 和独立的两节点 Docker fixture。原报告使用 x86、48 CPU 和 AKernel fork gVisor；本地 ARM64 上游 gVisor 运行结果不能证明该 fork 的 gofer 并发故障已修复。运行时选择、bootstrap 来源和实际执行文件 SHA256 必须随证据记录。sandboxd 的守护进程重启与 supervisor stop 是两个不同操作，后者仍按显式停机契约删除本机实例。
