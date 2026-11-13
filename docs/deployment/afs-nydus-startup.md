# 用 AFS 加速 Nydus 沙箱启动

本教程在已有 ADX 环境里，用两台 Linux worker 验证同一个 RAFS v5 镜像的冷读和预热。第一台上的 AFS Node 做种子，第二台上的 sandboxd 通过本机 AFS Node 读取。镜像正文按需读取，也可以在启动批次前铺好。继续使用原版 sandboxd、distill_fs 和 gVisor。

## 准备环境

- 在 Linux 从 ADX 仓库的 `master` 构建，选择带 AFS 的发布包。两台机器部署同一版本的 AFS Node，并与同版本的 Meta 连接。部署步骤、原生依赖和 TLS 见 [AFS 部署](afs.md)。
- worker 使用 sandboxd 的 Nydus 镜像路径和 `runsc`。配置 [ADX 运行环境](runtime-environment.md)，确保沙箱能启动 Execd。客户端安装 [Environment SDK](../../platform/sdk/environment/python/README.md) 并配置服务地址和认证。
- 使用你有权限的 Registry 和一个已发布的 RAFS v5 镜像。记录具体平台的 manifest SHA-256。若从现有 OCI 镜像转换，可用 `nydusify convert --fs-version 5 --source "$OCI_IMAGE" --target "$NYDUS_TAG"`，再从 Registry 获取具体 manifest 的 digest。选一条能读取镜像内文件并有固定输出的命令，例如 `/bin/sha256sum /startup.txt`。

```sh
ADX_WITH_AFS=1 make platform-release
```

## 配置 AFS 和 sandboxd

把以下设置合并到每台 AFS Node 的现有 TOML。示例使用 1 GiB 读缓存，容量至少应覆盖打算留在节点上的启动范围。保持默认来源缓存。副本供数和并行读参数也适用于普通 DFS 文件。启用 `dfs_read_cache_bytes` 时，按现有配置约束使用原生 DFS，且关闭 `dfs_peer_cache`。

```toml
# 合并到 Node TOML 顶层。端口改成你现有的 Node REST 端口。
rest_listen = "127.0.0.1:7401"
dfs_read_cache_bytes = 1073741824
dfs_read_parallelism = 4
dfs_read_stripe_bytes = 65536

[connectors.nydus]
enabled = true

[connectors.nydus.sources.startup]
registry = "https://registry.example.com"
repository = "team/rafs"
registry_proxy = true
```

供数节点使用一致的 source 名、Registry URL 和仓库名。需要认证时，按 [Connector 配置](../../afs/CONNECTORS.md#配置与调用)在节点本机设置 Registry 凭据。默认使用 HTTPS 回源；隔离测试的明文 Registry 需要显式配置 `allow_insecure_http = true`。

将下面的 `proxy` 对象合并到 sandboxd 使用的 Registry 后端模板中的 `registry` 对象。保留原镜像引用。sandboxd 和 AFS Node 要能通过同一网络命名空间的回环地址通信，且 sandboxd 的镜像格式探测仍需访问原 Registry 的 manifest。已验证的客户端到 AFS 代理链路使用 HTTP。强制经代理用 HTTPS `CONNECT` 传输正文时，当前入口返回 `404`。

```json
{
  "proxy": {
    "url": "http://127.0.0.1:7401",
    "use_http": true,
    "fallback": false
  }
}
```

## 采样、预热和对照

1. **采样。** 在隔离的 worker 上从冷态启动一次。采样时允许从 Registry 或已有 AFS 副本读取。按[工具说明](../../tools/nydus-startup/README.md)保留 Registry 返回的原始 manifest、回环请求抓包和 SDK 时间戳。运行 `sample_ranges.py` 生成范围清单。用相同镜像、相同命令再做一次独立冷启动，将实际范围和清单比对，记录覆盖率、漏抓和多抓的字节。
2. **备种。** 用 `prewarm.py` 在种子 A 的本机 AFS Node 上读取清单范围。确认 A 能供数后开始计量。冷读对照让目标 worker 清空本次 AFS 测试缓存和 sandboxd 镜像缓存后直接启动。预热对照从同样冷态开始，先对目标 worker 的本机 AFS Node 执行同一清单。重复读取确认内容哈希一致，且隔离窗口的 Peer 正文字节增量为零。两组的 sandboxd bootstrap 都从冷态开始。
3. **发压。** 用相同镜像、命令、worker 落点和资源配额重复两组。可用下面的 SDK 示例记录一条成功样本，也可在一个客户端并发发起多条。用该客户端的单调时钟，从最早的 SDK 提交算起，统计一秒内退出码和输出都正确的首条命令数。另记从开始铺热到最后一条命令成功的墙钟时间。
4. **看流量。** 在铺热、复读和启动窗口分别取各 AFS Node `/metrics` 快照。核对种子的 `afs_dfs_payload_bytes_total{direction="send",operation="read",transport="grpc"}` 与读端的 `afs_dfs_crc32c_read_bytes_total{stage="trusted_segments_receive"}` 增量。统计实际跨宿主机的发送，单列 config、bootstrap 和 Registry manifest。热启动时如果仍有额外正文流量，记录它对应的范围。用 Registry 访问日志核对 blob 请求。测试期间只封锁正文时，应保留 manifest 的可达性。

用 SDK 已配置的连接，在抓包所在 worker 运行以下示例。替换镜像、目标节点、镜像内命令和期望输出。输出可以直接作为采样工具的 `startup.json`。

```sh
export TEST_IMAGE='registry.example.com/team/rafs@sha256:<manifest-digest>'
export TEST_NODE='<ADX-worker-id>'
export TEST_COMMAND='/bin/sha256sum /startup.txt'
export TEST_EXPECTED='<sha256sum 的完整预期输出>'

python3 - <<'PY' > startup.json
import json
import os
import time
from adx_environment import CommandStatus, Environment

image = os.environ["TEST_IMAGE"]
sample = {"submit_wall_ns": time.time_ns(), "submit_ns": time.monotonic_ns()}
with Environment(image=image, runtime="runsc", node_id=os.environ["TEST_NODE"],
                 cpu=500, memory=512) as environment:
    result = environment.commands.run(os.environ["TEST_COMMAND"], timeout=60)
    sample.update(command_ns=time.monotonic_ns(), command_wall_ns=time.time_ns(),
                  exit_code=result.exit_code, stdout=result.stdout,
                  ok=(result.status == CommandStatus.SUCCEEDED and result.exit_code == 0
                      and not result.stdout_truncated
                      and result.stdout.strip() == os.environ["TEST_EXPECTED"]))
print(json.dumps({"digest": image.rsplit("@", 1)[1], "samples": [sample]}))
if not sample["ok"]:
    raise SystemExit(1)
PY
```

### 多 worker 分发

首轮用单种子直供。双机要比较直供与接力时，可以在第一台宿主机上运行独立的种子 A 和 worker C，在第二台运行 worker B。每个 AFS Node 使用独立身份、端口和数据目录。可以用仓内 [分发工具](../../tools/nydus-startup/README.md#分批派发)并行派发，也可以按批次接力。接力时，上层需要确认上游节点已发布这批范围的副本，并能被下游发现。计划控制派发顺序，AFS 仍根据 Meta 的候选自主选源。要证明 B 给 C 供数，需要核对 B 发、C 收和来源记录。双机实验通过隔离网络规则约束了可达来源。

## 已有双机结果

对照环境为两台 x86 Linux 云机，ADX 为 `6a87d251`，sandboxd 为 `b5dab295`，distill_fs 为 `v0.1.2`，gVisor 为 `release-20260817.0-akernel.5`。每个 worker 4 vCPU、8 GiB；每个沙箱 500m CPU、512 MiB。使用同一个约 1 GiB 的 RAFS v5 镜像，首命令校验镜像内文件。以下是功能和性能对照样本，实际结果随镜像、命令、宿主机和网络变化。

- 同镜像的两条命令分别产生 **18,507,370 B** 和 **18,507,362 B** 的启动范围。各做两次独立冷启动，覆盖率均为 100%，漏抓和多抓均为 0 B。交换清单后覆盖率为 71.62%。
- 每组四次单实例启动，首命令中位数从冷读 **875 ms** 降到预热后的 **549 ms**。启动期跨机镜像正文从约 **18.51 MB** 降到 **0 B**；每次仍读取 **6,367 B** 的 config 和 bootstrap。首次铺入范围用时 **0.505–0.528 s**。计入复读、实验取证和调度，从铺热到首命令成功为 **18.63–19.25 s**。
- 两个目标 worker 每批各启动 6 个沙箱，直供和接力各做两批，四批均在一秒内完成 **12/12** 条首命令。直供最慢 worker 热好用 **9.62–10.22 s**，含铺热的总时间 **18.11–18.47 s**。接力用 **15.74–19.50 s**，含铺热的总时间 **24.13–27.93 s**。种子发送量约减半；该拓扑种子与其中一个 worker 同机，物理跨机流量由 **21.40–22.48 MB** 增至 **46.78–47.43 MB**。总时间包含热好后约 **7.6 s** 的实验取证和调度。5000 个首条命令同一秒成功仍需在更大的环境验收。

回退时停止新的预热批次，把 sandboxd 的 Registry proxy 恢复到之前的配置，用一条真实冷启动确认后再恢复批量发压。
