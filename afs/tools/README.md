# AFS 工具与探针（`afs-tools`）

本目录是度量工具和诊断探针，**不是产品二进制，不进任何发布包**。
发布构建只取主 crate `afs` 的 `afs-meta`、`afs-node`（`build/release/build.sh`、
`.buildkite/build-component.sh`）；`afs-tools` 只随 `ADX_WITH_AFS=1` 的 AFS
检查（`make afs-lint`、`make afs-test`、`.buildkite/component-tests.sh afs`）编译和测试，
默认 Agent DX 构建通过 `AFS_EXCLUDES` 排除它。

按需显式构建：

```sh
cargo build --locked --release -p afs-tools --bin <名字>
```

二进制名沿用迁入前的名字，产物在 `target/<profile>/<名字>`。

| 二进制 | 源文件 | 用途 |
| --- | --- | --- |
| `afs-bench` | `src/bin/afs-bench.rs` | DFS 双臂（FUSE 挂载点 / SDK uds）同尺并发读写基准，见 [docs/testing/afs.md](../../docs/testing/afs.md) |
| `afs-meta-bench` | `src/bin/meta-bench.rs` | 只测 Meta 元数据操作的度量工具，见 [build/e2e/afs/perf/meta-bench.md](../../build/e2e/afs/perf/meta-bench.md) |
| `afs-perf-denominators` | `src/bin/perf-denominators.rs` | BLAKE3、TLS 回环等性能分母测量 |
| `afs-workspace-probe` | `src/bin/workspace-probe.rs` | OwnerFs workspace 验收用的容器 idle init 与视图观察器，由 `build/e2e/afs/acceptance/prepare-workspace-rootfs-linux.py` 放入测试 rootfs |
| `usrbio_bench` | `src/bin/usrbio-bench.rs` | usrbio 数据面吞吐测量；payload 生成器带防块去重单测（`cargo test -p afs-tools --bin usrbio_bench`） |
| `usrbio_pipeline_probe` | `src/bin/usrbio-pipeline-probe.rs` | 同一内存 Backend 下的 USRBIO-only 受控对比 |
| `entrance_probe` | `src/bin/entrance-probe.rs` | 同一 Backend 下 FUSE 与 USRBIO 前端的诊断对比 |

`src/bin/entrance_probe/` 是后两个探针共用的子模块：`native.rs` 是冻结的参考客户端；
`direct_hook.rs` 是早期 backend/sync 消融实验的片段，不参与编译。

`probes/` 放非 Cargo 的探针与说明：

- `write_delivery_bench.c`：写交付延迟复现负载，见
  [docs/performance/afs-write-early-reply-20261029.md](../../docs/performance/afs-write-early-reply-20261029.md)。
- `entrance_probe.md`：`usrbio_pipeline_probe`、`entrance_probe` 的模式与口径说明。
