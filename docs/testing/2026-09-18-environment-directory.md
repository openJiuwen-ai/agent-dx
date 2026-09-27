# API Server 实例目录订阅验收

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

2026-09-18，在 `fix/atomic-instance-claim` 工作树完成。API Server 的普通查询和生命周期请求不再逐次调用 Master `GetCapsule`，而是消费 Master 发布的完整实例归属目录。

## 实现契约

- `CapsuleDirectoryService.WatchCapsules` 只接受 API Server 的 mTLS 身份。
- 每次订阅先发送 `reset=true` 的完整目录，后续发送以 `base_revision` 串联的 upsert/delete 增量；帧携带 Master epoch、控制 revision，实例记录携带 Assignment generation。
- API Server 在内存维护 `Capsule ID → CapsuleRecord、Node Manager 地址、Node Proxy 地址`。目录未完成首次同步时返回 Unavailable；已同步目录缺失项直接返回 NotFound。
- 普通流断开时继续使用最近一次完整目录并后台重连；revision 断档、非法 epoch 或损坏帧会清空目录并重新全量同步。
- `MasterService.GetCapsule` 只用于创建后的读后写收敛，以及结果不明时固定原 Assignment 的恢复查询。
- 当前目录只保留仍可查询或操作的归属记录。Redis 可以继续保存 `Deleted` 状态历史，但 Coordinator 在状态提交时发布 delete 增量，并在重启后的全量目录中过滤历史 `Deleted` 记录；公开查询返回 NotFound。重复操作的持久化幂等由 Coordinator 与 adxlet 状态机承担。
- Edge 的路由订阅保持独立，只发布可路由的 Running 实例。

## TDD 与回归

初始红灯 `out/ci/instance-directory/red.log` 记录目录实现尚不存在时的3项失败。内存目录单测随后覆盖全量、增量、删除、revision 断档、epoch 变化和旧版本本地结果保护。

真实 RPC 首轮暴露两项接线问题并保留证据：生命周期测试夹具未装配目录服务，见 `api-control/api-http.log`；终态记录从目录移除导致第二次删除返回404，见 `api-control-final/api-http.log`。前者通过给真实 Coordinator fixture 装配同一个 RoutePublisher 修复；后者当时通过发布 Redis 终态记录处理。2026-09-28 的创建压力验证发现该做法会让长期积累的墓碑进入 API Server 活跃目录并放大内存，当前契约已改为删除增量与全量过滤，第二次删除按公开 API 的 NotFound 语义处理。目标 HTTPS 用例证据见 `read-after-create-final/target-test.log` 与 `read-after-create-final/api-http.log`。

最终验证日志位于工作树 `out/ci/instance-directory/`，干净克隆不包含这些文件。

| 检查 | 结果 | 证据 |
|---|---|---|
| API Server 单测 | 6通过、0失败；其中目录新增4项 | `static-final-3/02-adx-api-server-lib-test.log` |
| 真实 Redis/mTLS Master RPC | 19通过、0失败 | `control-rpc/result.json`、`control-rpc/03.log` |
| 目标真实 HTTPS 生命周期 | 1通过、0失败；完整脚本通过 | `read-after-create-final/target-test.log`、`read-after-create-final/api-http.log` |
| Master 包级回归 | 2通过、0失败 | `static-final/03-adx-master-lib-test.log` |
| 完整 API control suite | 19通过、0失败；两个真实 HTTPS 脚本完成 | `full-final-2/result.json`、`full-final-2/03.log` |
| 严格 Clippy，两个组件全部 targets，`-D warnings` | 通过 | `static-final-3/03-cargo-clippy.log` |
| 格式与差异检查 | `cargo fmt --check`、`git diff --check` 通过 | `static-final-3/` |

## 验证边界

验收包含真实 Redis、mTLS、Master、Node Manager RPC 和独立 Rust HTTPS API Server 进程。测试夹具的 RuntimeDriver 仍为可控实现，因此这组证据验证控制链一致性，不替代 sandboxd、RRT、Edge 的完整平台 E2E。既有 Buildkite 基础 K8s 验收发生在此次目录改造之前，不能作为本次未提交源码的 CI 证据。
