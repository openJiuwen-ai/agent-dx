# Nydus 启动范围采样与预热

这组 Python 3.9+ 工具从一次成功的真实启动生成正文范围清单，并通过目标 worker 的 AFS Node
按清单读取正文。工具使用标准库，可从任意目录调用。运行时需要启用 Nydus connector 的 Registry
只读入口。sandboxd 的 Registry 代理和工具应
使用同一 worker 的 AFS Node、同一个已配置仓库。部署、沙箱生命周期和流量监控由调用方负责。

## 从一次冷启动采样

在隔离的 worker 上，用 tcpdump 记录明文 HTTP/1.x Registry 请求。采样器读取 Linux IPv4
Ethernet pcap，要求完整请求头，遇到截断或分段请求会报错。采样时避免让其他启动或下载共用
捕获端口。用和抓包相同机器的 SDK 启动沙箱，执行真实命令并核对退出码和输出。保留：

- Registry 返回的原始 manifest 字节，SHA-256 必须等于 SDK 使用的镜像摘要。
- 例如以 `tcpdump -i lo -s 0 -w startup.pcap 'ip and tcp dst port 7401'` 获得的完整抓包。
  停止后检查 tcpdump 丢包计数。抓包可能包含认证信息，应保存在受限目录。
- SDK 的 `startup.json`。顶层 `digest` 是镜像摘要，`samples` 含一条成功样本，字段为
  `ok: true`、`exit_code: 0`、`submit_wall_ns`、`command_wall_ns`、`submit_ns` 和
  `command_ns`。分别用 Python 的 `time.time_ns()` 和 `time.monotonic_ns()` 在 SDK
  提交前及收到成功命令结果时记录两个时点。

```sh
python3 tools/nydus-startup/sample_ranges.py \
  manifest.json startup.pcap startup.json events.json \
  --plan startup-plan.json --workload alpha \
  --command '/bin/sha256sum /alpha.bin' --training-run alpha-01
```

清单绑定镜像 manifest digest 和启动用例，按 blob 合并重叠、相邻的正文范围。config 和 bootstrap
留在观测文件里。范围代表该窗口的观测请求，包含运行时预取或对齐产生的请求。输出使用新文件名。
用同一镜像和命令做独立冷启动，保存新的 `heldout.pcap` 和 `heldout.json`。生成实际请求范围并与冻结清单对比：

```sh
python3 tools/nydus-startup/sample_ranges.py \
  manifest.json heldout.pcap heldout.json heldout-events.json
PYTHONPATH=tools/nydus-startup python3 - <<'PYCODE'
import json
from pathlib import Path
from sample_ranges import body, compare, read

plan = read(Path("startup-plan.json"))
observed = body(read(Path("heldout-events.json")))
print(json.dumps(compare(plan["ranges"], observed), indent=2))
PYCODE
```

`coverage_pct` 以独立启动的实际请求字节为分母，`missing_bytes` 和 `extra_bytes` 分别表示漏抓和多抓。

## 预热指定 worker

```sh
python3 tools/nydus-startup/prewarm.py \
  --manifest manifest.json --plan startup-plan.json \
  --workload alpha --command '/bin/sha256sum /alpha.bin' \
  --repository lab/nydus --endpoint http://127.0.0.1:7401 \
  --receipt warm-01.json
```

在目标 worker 上执行，或把 `--endpoint` 指向可访问的目标 AFS Node。启用了入口认证时用
`--token-file` 提供 bearer token。工具验证清单的镜像、命令和正文范围，向 Registry 入口发
Range GET。每段检查 206、Content-Range、长度和 blob 摘要响应头，收据保存收到的范围字节
SHA-256。读取发生错误时退出非零，HTTP 阶段已完成的范围保留在收据里。重复运行使用新的收据文件。
在隔离窗口比较两次收据的范围哈希和 AFS Peer 计数，确认复读没有新增跨机正文。启动对照应分别
记录铺热耗时、SDK 提交到首条命令成功的耗时和两阶段的跨机流量。

## 分批派发

用 `python3 tools/nydus-startup/distribute.py plan.json` 执行预热命令。计划使用 `nodes`、`seeds` 和
`waves` 描述节点、已准备的种子和批次。下面示例用 SSH 到两台目标 worker 执行各自已准备的命令。
将主机和命令替换成你的部署。每个 `warm` 命令调用上面的预热入口并检查本地复读；接力时，`ready`
命令检查前一批节点的对应副本能否供下游发现。

```json
{
  "nodes": ["A", "B", "C"],
  "seeds": ["A"],
  "waves": [{"B": "A"}, {"C": "B"}],
  "commands": {
    "B": {"warm": ["ssh", "worker-b", "warm-image"], "ready": ["ssh", "worker-b", "check-image-source"]},
    "C": {"warm": ["ssh", "worker-c", "warm-image"]}
  }
}
```

直供时用 `"waves": [{"B": "A", "C": "A"}]`。同一批中的命令并发执行，有命令失败时停止派发下一批。
计划中的上游用于控制派发顺序，AFS 读取仍按现有来源视图选源。启动与流量验收见
[部署教程](../../docs/deployment/afs-nydus-startup.md)。

## 使用管理接口准备镜像

`startup.py` 使用部署提供的管理地址和凭据。启用节点发现和任务接口后，工具可自动发现节点、完整铺种并向 worker 逐台下发启动范围。

以下是工具配置 JSON。Registry 上传凭据由 nydusify 获取。受限 Registry 读取 manifest 时可增加 `registry_token_file`。管理 token 文件由运维部署，和 Registry 凭据分开。

```json
{
  "meta_admin_url": "https://afs-meta-admin.example.com",
  "meta_admin_token_file": "/run/secrets/afs-meta-admin",
  "node_admin_token_file": "/run/secrets/afs-node-admin",
  "deployment": "adx-prod",
  "source": "startup",
  "publish_target": "registry.example.com/team/rafs:startup",
  "platform": "linux/amd64",
  "seed_node_ids": ["seed-a"]
}
```

`source` 必须对应节点上已启用 Registry proxy 的来源。该来源必须能够读取已发布镜像所在仓库。测试可在部署时配置临时 Registry。若上传和读取使用不同地址，`manifest_registry_url` 指定 SDK 也会访问的同一仓库读取地址。明文实验 Registry 需在隔离环境配置 `plain_http: true`。

```sh
startup() { python3 tools/nydus-startup/startup.py --config deployment.json "$@"; }

startup convert --image registry.example.com/team/oci:app --output prepared
startup discover
startup seed --image prepared/image.json --node-id seed-a
startup prewarm --image prepared/image.json --plan startup-plan.json
```

`convert` 输出具体平台的 `image.json` 和 Registry 原始 `manifest.json`。输出的 digest 引用可交给原版 ADX SDK。未给 `--node-id` 时，`seed` 使用配置的 `seed_node_ids`，或把发现时在线且有 DFS 存储的集群节点全部作为目标。工具只向一台可受理铺种的 Nydus Node 提交，并轮询该 Node 的任务。预热默认选当前部署中注册了 ADX worker ID 且启用 Nydus 的在线节点，逐台提交并检查本机缓存。

也可在隔离的冷采样 worker 内直接运行下面的命令。该 worker 与其 sandboxd 和 AFS Node 共用网络命名空间，工具需访问现有 ADX SDK、`tcpdump`、`nydusify` 和管理入口。ADX 连接沿用 SDK 环境变量。采样绑定部署注册的 ADX worker ID，先运行真实命令校验内容，再为已发现的 worker 预热。

```sh
startup prepare --image registry.example.com/team/oci:app --output prepared \
  --sample-worker afs-worker-b --expect-stdout "$EXPECTED_STDOUT" -- /bin/sha256sum /startup.txt
```

`ready` 在铺种时表示指定持久副本已在 Meta 登记，在预热时表示任务完成时范围已可在对应 worker 本机读出。长时间后的缓存状态随容量和淘汰变化。
