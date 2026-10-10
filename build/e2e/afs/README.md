# AFS E2E 工具（OwnerFs / DFS）

本目录承载从 DMS/AFS 快照迁入 Agent DX 的可维护验收工具。它不是历史证据归档，也不是产品发布包的一部分。

## 范围

保留内容：

- `acceptance/`：标准验收 runner、环境准入、driver、probe 和小规模 case 清单。
- `ownerfs_acceptance.py`：OwnerFs 三节点/远端访问验收入口，默认从目标仓根目录定位 `afs/common/protocol/proto`。
- `scripts/dfs/r1_e2e.py`：DFS R=1 小规模本地 E2E 驱动。
- `scripts/ownerfs/`：OwnerFs workspace 控制，以及 bind ON 两节点 local-file 恢复验证入口。
- `scripts/check-rdma-cancellation.py`：RDMA 取消路径辅助检查。

不保留内容：

- 源仓旧 `scripts/deploy/` 产品安装器和 release 包流程；目标仓统一使用 `build/release` 与部署适配层。
- 旧 VM 过程证据、checkpoint、日志、压缩包、镜像和历史失败原件；这些留在 Agent Runtime 工作区上一级本地归档。
- 依赖旧包身份、源仓部署器或历史环境状态的一次性验证脚本。

## 维护边界

- 本机 Lima YAML、IP/MAC 租约、实际运行 lock 和旧阶段专用驱动不作为仓内公共配置；原件、失败记录和路径/校验和索引在开发者源码树外归档，并可从整改前 Git 版本追溯。
- `runner.py --list` 不需要环境；执行必须显式提供 `--lock /path/to/run/acceptance.lock.json`。无机器身份的 `acceptance/acceptance.lock.example.json` 只说明结构，不能直接形成正式 PASS。
- 旧实验室原始观测和紧耦合判定器已归档仓外；保留底层参数化探针及合成负例。通用完整 ENV verifier 未实现，full 明确 BLOCKED，不因移出历史工具放行。
- 跨节点锁、锁取消、bind/native↔FUSE 锁域及 RDMA 专题工具保留为后置研究/回归资产，不是本次迁移的前置，也不证明这些能力受支持。本机 ext4 锁成功不能代替分布式锁验收。
- 旧 W1/W2/S5 批次诊断及旧并发判定器退出当前入口。维护中的 `owner_remote_small.py`、`owner_remote_write_small.py` 等测量工具继续保留；工具输出不自动满足吞吐与独立操作时延双目标。

## 常用入口

在目标仓根目录运行：

```bash
python3 -m unittest discover -s build/e2e/afs/acceptance -p 'test_*.py' -v
python3 build/e2e/afs/ownerfs_acceptance.py --help
python3 build/e2e/afs/scripts/dfs/r1_e2e.py --help
python3 build/e2e/afs/scripts/ownerfs/native-workspace-control.py --help
python3 build/e2e/afs/scripts/ownerfs/bind-two-node-localfile.py --help
```

真实 FUSE、权限、runc、RDMA、MooseFS、3FS 和多 VM 验收只在 Linux 环境执行。macOS 仅用于静态检查、路径回归和文档整理。目标仓二进制必须通过本仓构建产生，历史 DMS/AFS 证据只能作为迁移来源和判据参考。

### OwnerFs workspace bind ON 两节点 local-file 验证

该入口用于迁移后最小真实运行核对：在一台 Linux VM 上启动 `afs-meta`、`node-a`、`node-b`，使用 local-file Meta 和 mTLS，先通过 FUSE 创建 Home workspace，再对 `node-a` 开启 OwnerFs workspace bind mount，验证本地 bind 与远端 FUSE 的读写、权限、错误传播、正常停止、local-file Meta 重启恢复和卸载排空。它不声明性能达标，也不声明跨节点锁能力。

运行前需先在 Linux 上构建目标二进制，例如：

```bash
make ADX_WITH_AFS=1 JOBS=2 afs-build
sudo python3 build/e2e/afs/scripts/ownerfs/bind-two-node-localfile.py \
  --binary-dir "${CARGO_TARGET_DIR:-target}/debug" \
  --work-root "/var/tmp/adx-dfs-bind-two-node-$(date +%Y%m%dT%H%M%S)"
```

前置条件：Linux root 权限、`/dev/fuse` 可用、`openssl`、`findmnt`、`fusermount3` 可执行，`127.0.0.1/2/3` 上 `26400/26401/26500/26501` 端口空闲，`--work-root` 父目录至少 1GiB 可用空间。脚本会在 work root 写入 `identity.json`、`checks.json`、`waits.json`、进程日志和最终 `result.json`。

### DFS 一写多读最小入口

迁移的有限功能回归可直接启动同一 Linux VM 上的 Meta 和三个独立 Node：

```bash
sudo python3 build/e2e/afs/scripts/dfs/three_node_core.py \
  --repo "$PWD" --binary-dir "$CARGO_TARGET_DIR/debug" \
  --work-root /var/tmp/adx-dfs-three-node-RUN
```

目录必须全新；要求 root、ext4、`/dev/fuse`、OpenSSL、findmnt 和 FUSE 卸载工具，使用独立的 127.0.0.1–4 端口。A 连续写入三个 64KiB 版本并 fsync，B/C 同时独立读回，检查删除可见性及实际正常退出/卸载。配置为一份必需持久副本；这只证明小规模一写多读功能，不证明跨主机、三份同步副本、3FS 性能持平或崩溃恢复。完整运行和失败证据写入外部 `--work-root`，不提交原始日志。

DFS 当前保留的是小规模一写多读的维护驱动，不负责启动集群生命周期，也不声明 3FS 性能持平。运行时先由部署层准备三个已挂载的 `afs-dfs` 根目录、同一候选身份文件和同一个 C I/O 探针，然后按 cohort 协议组织：

```bash
# 三节点各自的 worker 命令模板；coordinator 负责 START/C_DONE/FINAL 协议
python3 build/e2e/afs/acceptance/dfs_multinode_small.py worker \
  --operation write --batch 1 --member A \
  --session-token "$SESSION" --cohort-token "$COHORT" \
  --dfs-root /mnt/adx/dfs --io-tool /path/to/dfs_unique_io \
  --identity /path/to/identity.json --output /path/to/evidence/A-write

python3 build/e2e/afs/acceptance/dfs_multinode_small.py worker \
  --operation read --batch 1 --route r1 --member B \
  --session-token "$SESSION" --cohort-token "$COHORT" \
  --dfs-root /mnt/adx/dfs --io-tool /path/to/dfs_unique_io \
  --identity /path/to/identity.json --manifest /path/to/A-write/summary.json \
  --output /path/to/evidence/B-read

python3 build/e2e/afs/acceptance/dfs_multinode_small.py coordinator \
  --operation read --batch 1 --route r1 \
  --session-token "$SESSION" --cohort-token "$COHORT" \
  --output /path/to/evidence/ctl-read
```

实际多节点运行由外层 harness 连接三个 worker 的 stdin/stdout。若只需单 writer + 两 reader 的基础同步读辅助，可使用 `dfs_manyread_small.py writer/reader/coordinator`（writer／reader 必须显式传入本轮 `--product-source-commit` 和 `--compiler-input-map`，两端一致）；两者都只产生功能和测量证据，不自动给出性能达标结论。

### 测试探针边界

`afs/Cargo.toml` 中的 `afs-workspace-probe` 是 `tests/support/workspace_probe.rs` 里的 Cargo example，只供验收流程显式构建后放入测试 rootfs。`build/e2e/afs/acceptance/prepare-workspace-rootfs-linux.py` 会校验探针和 busybox 的 sha256、复制运行依赖，并输出测试 rootfs manifest。普通产品构建和试用包不依赖、也不携带该探针；OwnerFs bind 核心和宿主 bind 逻辑不调用它。

## 源到目标映射

| 来源路径 | 目标路径 | 处理 | 原因与影响 |
| --- | --- | --- | --- |
| `tests/acceptance/runner.py`、`drivers/`、`probes/`、`suites/`、`cases.json` | `build/e2e/afs/acceptance/` | 保留 | 当前标准验收和小规模回归仍读取这些入口。路径定位已调整为目标仓根目录。 |
| `tests/ownerfs_acceptance.py` | `build/e2e/afs/ownerfs_acceptance.py` | 保留 | OwnerFs 远端/三节点验收入口，proto 默认路径已改为 `afs/common/protocol/proto`。 |
| `scripts/dfs/r1_e2e.py` | `build/e2e/afs/scripts/dfs/r1_e2e.py` | 保留 | DFS R=1 小规模 E2E 驱动。 |
| `scripts/ownerfs/native-workspace-control.py` | `build/e2e/afs/scripts/ownerfs/` | 保留 | OwnerFs bind/workspace 场景的测试控制工具；旧 W1/W2/S5 诊断入口已归档。 |
| 来源快照的限定两节点 local-file bind ON 断言 | `build/e2e/afs/scripts/ownerfs/bind-two-node-localfile.py` | 迁移并改写 | 仅保留运行断言和证据输出；旧 `scripts/deploy/afs-trial-config`/包身份依赖被替换为目标仓二进制、脚本内临时配置和 mTLS 材料生成。 |
| `scripts/check-rdma-cancellation.py` | `build/e2e/afs/scripts/check-rdma-cancellation.py` | 保留 | RDMA 取消路径辅助检查。 |
| 旧 `tests/acceptance/*-linux.py` 中依赖源仓 release 包、`scripts/deploy/afs-processctl`、旧 package identity 的一次性驱动 | 未迁入 | 目标仓改由 `build/release`、部署适配和 `adxctl` 交付；这些脚本在源仓本地归档保留历史证据，不作为目标仓可运行入口。 |
| 源仓 `.github/`、`.codex/`、`.omx/`、`development/`、历史日志、checkpoint、压缩包、VM 镜像 | 未迁入 | 过程资产不进入目标代码仓，避免负担和证据身份混淆。 |
