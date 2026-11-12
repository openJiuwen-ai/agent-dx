# Nydus 启动范围采样与预热

这组 Python 3.9+ 工具从一次成功的真实启动生成正文范围清单，并通过目标 worker 的 AFS Node
按清单读取正文。工具使用标准库，可从任意目录调用。运行时需要启用 Nydus connector 的 Registry
只读入口（通过双机验证的 AFS Node 版本含 `b3a776e3`）。sandboxd 的 Registry 代理和工具应
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
python3 build/e2e/afs/nydus_startup/sample_ranges.py \
  manifest.json startup.pcap startup.json events.json \
  --plan startup-plan.json --workload alpha \
  --command '/bin/sha256sum /alpha.bin' --training-run alpha-01
```

清单绑定镜像 manifest digest 和启动用例，按 blob 合并重叠、相邻的正文范围。config 和 bootstrap
留在观测文件里。范围代表该窗口的观测请求，包含运行时预取或对齐产生的请求。输出使用新文件名。
用独立冷启动检查范围覆盖后，再将清单交给预热工具。

## 预热指定 worker

```sh
python3 build/e2e/afs/nydus_startup/prewarm.py \
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
