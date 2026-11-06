# DFS 通用分发 E2E

测试普通文件即可验证扩散和并行读取；Nydus 只作为另一个消费者。
本目录是独立实验入口，不是默认 ADX 构建／发布内容。

## 环境与输入

使用已登记的 Linux ext4 环境，1 mgmt + 4 worker，地址为
10.203.0.10、20、21、22、23。保存 adx-vm open JSON 为
`$TASK/environment/open.json`；`$TASK` 是本次独立目录，`$CLI` 是 adx-vm 绝对路径。
macOS 只运行编排；FUSE 和文件正文都在节点原生 `/workspace`。

1. Linux 执行 `cargo build --locked -p afs --bin afs-node --bin afs-meta`，
   将两二进制放到 `$TASK/artifacts/afs/`。记录优化档位、源码及二进制 SHA。
2. 将 `configs/*.toml` 复制到 `$TASK/artifacts/afs-configs/`；
   运行 `python3 ../nydus/prepare_tls.py "$TASK/artifacts/afs-tls" --four-workers`。
   **配置只有一个持久副本，专门区分缓存供数和持久副本；不是生产可靠性配置。**
3. 按 [Nydus 实验](../nydus/README.md) 准备工具、Registry、观察代理和转换后的 fixture；
   将其脚本与本目录 `bench_*.py`、`process_resources.py` 复制到 `$TASK/artifacts/scripts/`。
4. 用 `../nydus/run.py --workspace "$TASK" --adx-vm "$CLI" start ROLE`
   依次启动 meta、node-a、node-b、node-c、node-d。

## 普通文件

先在 A 的 `/workspace/afs/mount/distribution-one.bin` 写入
`bytes(range(256))*4096` 并 fsync；在 B 读完整内容，让它安装一份完整缓存。
固定 SHA256 为 `fbbab289f7f94b25736c58be46a994c441fd02552cc6022352e3d86d2fab7c83`。

```sh
python3 probe_replica.py --workspace "$TASK" --adx-vm "$CLI"
python3 probe_replica.py --workspace "$TASK" --adx-vm "$CLI" --multi-source
python3 check_mutations.py --workspace "$TASK" --adx-vm "$CLI"
python3 check_bad_cache.py --workspace "$TASK" --adx-vm "$CLI" --mode corrupt
python3 check_bad_cache.py --workspace "$TASK" --adx-vm "$CLI" --mode missing
python3 check_pressure.py --workspace "$TASK" --adx-vm "$CLI"
```

第一步暂停 A，C 只能复用 B，finally 恢复 A；第二步 D 首读，至少两节点发送正文，
保存三方计数并验证 hash。重复第二步须换一个新文件，避免 D 本地命中。
并发重叠由 focused 单测证明，计数只证明真实来源参与，不等同于网络同时传输的抓包。
随后检验新版本、truncate 后零空洞、unlink；坏副本必须回退并由维护线程撤销；
80 MiB 读取超过 64 MiB 缓存预算，检查 catalog 记账和另一个新读者的完整 hash。
`check_bad_cache.py` 使用固定测试文件名，只在独立环境运行；重复前用 AFS unlink 删除旧测试文件。

Nydus 首次 HEAD/GET、同范围合并、短读／503／超时沿用 `check_http.py`。
一个持久副本时，`check_failover.py` 的已提交范围必须先在 B 读取形成缓存，
再停 A；不要把原两持久副本 fixture 的条件悄悄替换成一副本。
`probe_origin_recovery.py` 专门记录所有已提交副本不可用后的缺口，输出不是恢复通过门禁；
不会删除原持久数据，finally 重启 A。

## 对照

固定 Dragonfly client v1.5.11、scheduler v2.5.3；release URL 和 SHA 见 `tools.json`。
只提取 dfdaemon、dfget、scheduler 到 `$TASK/artifacts/dragonfly/`，将 configs 的 YAML 同步过去。
不要把源码 checkout 版本当成运行版本。

```sh
python3 dragonfly.py --workspace "$TASK" --adx-vm "$CLI" start
python3 run_startup_bench.py --workspace "$TASK" --adx-vm "$CLI" --rounds 3
python3 run_startup_bench.py --workspace "$TASK" --adx-vm "$CLI" \
  --backends dragonfly --dragonfly-policy auto --output startup-benchmark-auto.json
python3 run_startup_bench.py --workspace "$TASK" --adx-vm "$CLI" \
  --backends registry-direct --output startup-benchmark-registry-direct.json
```

每次启动 Nydus 用新本地缓存，预装 bootstrap、关闭预取，计时到 startup.txt+hot.bin 校验完成。
AFS 每轮新 manifest；Dragonfly 每轮清自己的已停止 peer 缓存；Registry 本身热缓存。
`always` 强制 P2P；`auto` 的小请求可能直接回源，必须同时报告。bootstrap 获取不在计时内。
Registry 的 1-byte stat GET 与正文传输分开计数；`body_503` 允许 stat，不代表完全离线。
`registry` 经观察代理；`registry-direct` 绕过代理，用于隔离测试代理的额外成本，
启动测试不采集其源站字节（JSON 为 null，不能解释为零回源）。Dragonfly 重置会在计时外等待端口可重新绑定。
在 mgmt 运行 `/artifacts/scripts/bench_ranges.py` 可测热缓存 64 KiB HTTP 范围读取：
并发 1/8/32，各 128 请求，报告吞吐、P50/P95/P99、回源量；这不是 Nydus 完整工作负载。
Python HTTP 客户端、代理 backlog、单 VM 和 debug/release 差异均可能限制结果。

## 门禁与收尾

Linux focused：`cargo test --locked -p afs --lib distribution_`；
然后完整 AFS lib、workspace fmt 和 Clippy。Python 执行
`make python-check PYTHON_CHECK_BASE=<实际基线>`。保留失败记录、最终 source manifest、
配置、bin SHA、原始结果、必要日志；不要提交二进制或密钥。

先正常卸载所有 Nydus，再 stop 四个 Node/Meta、Dragonfly、Registry 和观察代理。
核对进程退出和证据后，清理自己的正文、缓存和重复下载包，归还运行／编译会话及登记。
不要清共享 Cargo 缓存、公共镜像或他人的工作区。
