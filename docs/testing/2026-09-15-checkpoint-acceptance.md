# 同节点暂停／恢复本地验收

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

日期：2026-09-15。结论：阶段 1 的真实公共 SDK 本地验收通过。部署为一个 Lima ARM64 KVM 节点，控制面与 Gateway 以独立进程运行，sandboxd 独立托管，实例使用 Firecracker 与 OCI RRT 镜像。

## 验收路径

1. 公共 SDK 创建实例，经 Frontend 调度，命令通过 Edge → Node Proxy → RRT 执行。
2. 暂停成功时 checkpoint 已生成，元数据已写入 Redis，源后端实例已删除。
3. 仅重启 Node Manager，从 Master 对账恢复 Paused 状态。
4. 同 Capsule ID 恢复成功，内存计数器从 92 增长到 223，PID 保持 16，二进制文件内容一致。
5. 显式删除后，Redis 状态为 Deleted、`resources_held=false`，sandboxd 清单及 checkpoint 目录为空，平台正常停止。

代码边界和失败契约见 [Capsule 暂停与恢复](environment-checkpoint.md)。

## 制品身份

| 对象 | 身份 |
| --- | --- |
| ADX 分支 | `feat/instance-pause-resume` |
| ADX 基础提交 | `0dde79ad57583e998389101a763e4d2d825be63e`，包含当前未提交修改，包标记 `dirty=true` |
| 平台发布包 | `out/ci/pause-resume/package-v4`，`manifest.json` 记录逐文件 SHA256 |
| sandboxd | PR #56，`efc201531d7e2e9d69505da151eb66084b61eebf` |
| Firecracker fork | `v1.16.1-akernel.3`，`b9a362d1070ee17991e7388d3b53a9ccce25ecca` |
| virtiofsd | `v1.14.0`，`c2540f8db14caba81c1e37fba23fc7bf2cd7f0dd` |
| RRT OCI 镜像 | `sha256:c959c0b5ee25751911d362c23b76ec94682a2e632d5772b97ab834ebdd230408` |
| Sandbox SDK / Redis | `0.1.0` / `7.2.5` |

## 证据位置

以下为执行工作区的 `out/ci/pause-resume/` 下文件，运行输出不纳入源码包：

- `acceptance-summary.json`：汇总版本、包身份、用例结果与验证边界。
- `fc-r5.log`、`fc-r5/evidence/sdk/result.json`：真实 SDK 五项用例通过。
- `fc-r5/evidence/catalog-paused.json`、`inventory-paused.txt`：暂停持久化及源实例已停止。
- `fc-r5/evidence/catalog-final.json`、`inventory-final.txt`、`result.json`：终态和清理。
- `rust-final-5.log`：122 项定向 Rust/真实 Redis 测试及 Clippy 通过。
- `rpc-final-2.log`：4 项真实 Redis/mTLS RPC 测试通过。
- `frontend-control-1/`、`go-full-2.log`：真实 Frontend HTTP 集成与 Go 回归。
- `linux-build-5b.log`、`package-5b.log`：本次原生 ARM64 编译及打包。

此前失败记录保留：r3 的 OCI 根文件系统未启用 virtio-fs；r4 的 1 GiB filestore 在加载恢复内存时空间不足。最终配置启用 virtiofsd，并以 `filestore_dir_size = ""` 使用原生 Linux 磁盘目录。完整重跑 r5 通过。

本次未执行新增暂停恢复的 Kubernetes Buildkite 验收，也未证明对象存储、跨节点恢复或 SQLite 降级能力。它们仍按后续阶段实施。
