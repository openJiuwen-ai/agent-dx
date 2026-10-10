# AFS 性能对照构建工具

本目录只维护 MooseFS/3FS 构建准备工具，不保存某次运行状态。对照资格仍需单独验收；构建成功不等于正式性能基线可用，也不证明三份同步持久副本语义。

## 输入和运行

- `bin/prepare_3fs_scratch.sh`：从宿主编排，把固定只读 3FS checkout 复制到现有 Linux builder，初始化副本的 submodule。必须显式提供 `REF_3FS`、仓外 `EVIDENCE_DIR` 和 guest 绝对目录 `AFS_BASELINE_GUEST_ROOT`；不依赖个人目录。通过 `AFS_BASELINE_LIMA_INSTANCE` 选择现有 VM（默认 `afs-build`）。
- `bin/build_moosefs.sh`：只在 Linux builder 构建固定官方 MooseFS commit。
- `bin/prepare_3fs_deps.sh`：只在 Linux builder 下载并核验固定 FoundationDB/libfuse 构建依赖。
- `bin/build_3fs.sh`：只在 Linux builder 检查依赖并构建 3FS；现有 ARM 兼容修改必须披露，不能称为 stock 3FS 对照。

版本默认值由 `bin/common.sh` 固定。三个 guest 构建入口默认使用 `$HOME/afs-build/baselines`，可通过 `AFS_BASELINE_GUEST_ROOT` 覆盖。产物和带时间戳日志保存在运行目录，不提交到源码仓。

## 判据边界

MooseFS 持久写语义、3FS 接口/副本/同步屏障和资源资格必须在测量前固定并有实证。历史资格失败和未完成项保持原结论，不降低门槛或转为 PASS。本轮只整理工具，不重新运行基线或扩大性能矩阵。

2026-10-07 小删除及其失败记录的完整说明保存在整改前 Git 版本 `a0d03508c8e87e61acfce501c3d684ad476a5ded` 的本文件及开发者本地归档；通用入口不再承载逐轮过程记录。结果只能复用原版本、原环境和原判据的范围。
