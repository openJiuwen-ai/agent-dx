# Cargo 构建缓存

本地 Makefile 和 `build/ci/run.py` 默认将 Rust 构建指向主 checkout 下的共享缓存：

```text
<primary-checkout>/.adx-cache/
├── cargo-target/<host>-rust<version>/
└── sccache/
```

所有 Git worktree 使用同一个按宿主架构和 Rust 版本分桶的 `cargo-target`。共享目录关闭 Cargo incremental，避免每个 worktree 长期累积一套增量状态。Cargo 会锁住共享 build directory，因此并行调用可能等待，但不会并发写坏目录。

如果系统存在 `sccache`，Makefile 会自动设置 `RUSTC_WRAPPER`，使用上限为 20 GiB 的共享编译缓存。没有安装 `sccache` 时仍可共享 Cargo target，但清理 target 后需要重新编译。

查看当前解析出的路径和占用：

```sh
make cargo-cache-info
```

为直接运行的 Cargo 命令加载共享环境：

```sh
eval "$(python3 build/cache/cargo_cache.py env --mode shared)"
cargo test --locked --workspace --all-features
```

## 必须隔离的构建

发布、打包、准备长时间运行的本地二进制，或者需要与另一 worktree 并行构建时，使用独立 target。这样同名的 `target/debug/*` 或 `target/release/*` 不会被另一分支的后续构建覆盖：

```sh
eval "$(python3 build/cache/cargo_cache.py env --mode isolated)"
make platform-release JOBS=2 PYTHON="$PWD/.venv/bin/python"
```

`make platform-release` 会拒绝使用默认共享 target。Buildkite 已提供按架构和工具链分桶的显式 `CARGO_TARGET_DIR`，不受本地默认值影响。

可通过 `ADX_BUILD_CACHE_ROOT`、`CARGO_TARGET_DIR`、`CARGO_INCREMENTAL`、`SCCACHE_DIR` 和 `SCCACHE_CACHE_SIZE` 覆盖默认值。切换 Rust 版本或宿主架构会自动进入新的 target 分桶；旧分桶不会自动删除，回收前应确认没有 Cargo/Rust 进程正在使用目标目录。
