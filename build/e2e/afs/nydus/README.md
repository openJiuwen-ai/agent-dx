# Nydus Connector E2E

拓扑：真实 OCI Distribution Registry、测试观察代理和 Meta 在 mgmt；AFS A/B/C 在三个独立节点；原生 Nydus `http-proxy`、真实 FUSE 挂载。示例使用 adx-vm 的 `10.203.0.10/20/21/22`，运行数据必须在原生 ext4 `/workspace`，`/artifacts` 和 `/output` 仅用于交换。单 VM namespace 验证不等于跨物理机 HA 或性能资格。

## 准备

1. 按公共 adx-vm 规则 reserve/open 三个 worker，保存返回 JSON 为 `$TASK/environment/open.json`；由其提供 session 路径。编译与运行会话分别登记，结束后 close/release。不要使用其他人的活动 VM。
2. 在 Linux 构建 `cargo build -p afs --bin afs-node --bin afs-meta`，将二进制放到 `$TASK/artifacts/afs/`。构建镜像和基线规则见 [AFS 入口](../../../../afs/README.md)。
3. 根据 [tools.json](tools.json) 下载并校验 Nydus v2.4.5 和 Registry v3.1.2 的 ARM64 发布包，把 `nydusd`、`nydusify`、`nydus-image`、`registry` 放到 `$TASK/artifacts/tools/`。这不是编译 AFS 的依赖。
4. 将本目录 Python 脚本复制到 `$TASK/artifacts/scripts/`；四份 TOML 放到 `$TASK/artifacts/afs-configs/`，registry.yml 放到 `$TASK/artifacts/`。配置使用 CRC32C 和两个同步副本，三个 Node 都启用同一 source。
5. 执行 `python3 prepare_tls.py "$TASK/artifacts/afs-tls"`，生成本次独立证书和测试用 Nydus 路由令牌，脚本拒绝覆盖已有身份。管理节点的 HTTP 探针使用令牌，原生 nydusd 在 worker 内走回环。创建 `$TASK/evidence`、`$TASK/artifacts/nydus-configs`。

运行节点命令通过 `adx-vm exec --session "$SESSION" --node NODE -- COMMAND...` 执行。在 mgmt 启动下面两个后台服务，保存 PID 和日志：

```sh
mkdir -p /workspace/registry /output/logs
/artifacts/tools/registry serve /artifacts/registry.yml
python3 /artifacts/scripts/registry_proxy.py
```

随后在 mgmt 执行 `python3 /artifacts/scripts/prepare_fixture.py`。它发布小型 OCI 镜像，调用原生 nydusify 转换并重新发布，再提取 bootstrap；不生成任何分段校验清单。正文只保留在任务 ext4；输出保留 manifest、bootstrap、期望文件 hash 和转换日志。

## 运行和判定

宿主执行（`$CLI` 是实际 adx-vm 路径）：

```sh
python3 run.py --workspace "$TASK" --adx-vm "$CLI" start meta
python3 run.py --workspace "$TASK" --adx-vm "$CLI" start node-a
python3 run.py --workspace "$TASK" --adx-vm "$CLI" start node-b
python3 run.py --workspace "$TASK" --adx-vm "$CLI" start node-c
```

在 mgmt 执行 `python3 /artifacts/scripts/check_http.py`。所有断言必须成功，结果在 `/output/http-results.json`：

- 首次 HEAD 只准备元数据；首次直接 GET 不依赖预准备。
- 非回环的未授权 HEAD／GET 返回 403，且没有发起回源。
- B 冷读由 A 回源；C 复用时没有新增源站正文请求。
- 并发准备收敛为一个目录；同范围六个请求只有一次回源。
- 短响应、503、超时、manifest digest 错误都不能伪装为成功，恢复后可重试。
- 未声明 blob 返回 404；越界 Range 返回 416。

真实挂载：

```sh
python3 run.py --workspace "$TASK" --adx-vm "$CLI" mount worker-2 check-b afs
python3 run.py --workspace "$TASK" --adx-vm "$CLI" probe worker-2 check-b
python3 run.py --workspace "$TASK" --adx-vm "$CLI" mount worker-3 check-c afs
python3 run.py --workspace "$TASK" --adx-vm "$CLI" probe worker-3 check-c
```

在 C 挂载前可将共享输出的 `origin-control.json` 设为 `{"mode":"503"}`，记录 `logs/origin.jsonl` 正文请求数；C 内容 hash 必须正确且计数不增长。完成后恢复 `{}`。脚本默认验证 startup.txt 和 hot.bin；完整正文验证可在挂载点读取 cold.bin，对照 `fixture/expected-files.json`。Nydus 自带预取关闭、内容校验打开，每个 mount case 使用全新空缓存目录。

`python3 check_failover.py --workspace "$TASK" --adx-vm "$CLI"` 会准备新镜像、停掉 A、验证已提交范围由持久副本供数，然后有界重试缺失范围，检查新 owner B 的回源记录，再启动 A。租约未过期时读缺失范围可以失败；约 30 秒租约到期后才能接管。暂停旧 owner 的独立竞态使用 `check_owner_race.py`：先把测试 Node 的 `[connectors.nydus].timeout_ms` 和 `[dfs_read_pipeline].task_timeout_ms` 设为 60000 并重启，生产租约参数保持默认。代理只挂起 A 的回源，在回源已进入后 SIGSTOP A；B、C 同时请求并竞争过期租约，随后恢复 A 并放行旧响应。必须两读者得到正确数据、仅一个新 owner 回源、旧请求失败，关闭源站后两节点仍复用数据。该脚本只操作本任务 PID，finally 保证恢复暂停进程。A 恢复后可能按既有会话失效规则退出；这不是自动重新入网。后续测试须重新启动 A，确认 FUSE mountpoint 后再读写，避免误测底下的本地空目录。

## 收尾与证据

先 unmount 两个 Nydus case，再 stop 三个 Node 和 Meta，停止观察代理及 Registry；导出日志、JSON、配置、源码身份和二进制 hash。可用普通文件操作删除测试镜像目录，验证 unlink/rmdir 与既有 GC。测试产生的 registry 正文、转换中间文件、Nydus 缓存、任务 AFS 数据和重复工具副本在退出后清理；不要清理公共构建缓存或其他任务数据。最后归还会话和环境。

发布成功但回复丢失、确认服务暂不可达由 `node::connectors::nydus::service::tests` 故障注入验证；文件提交回复丢失后的复用由 `lazy_fill_lost_commit_reply_reuses_committed_bytes_on_retry` 验证。它们是组件级边界测试，不冒充真实网络丢包实验。

实现测试入口：Linux `cargo test -p afs --lib lazy_fill_`、`cargo test -p afs --lib node::connectors`；完整 lib 测试应把 TMPDIR 指向原生 ext4。关闭 DFS feature 的基线仍有未使用代码警告，与该模块新增告警分别记录。
