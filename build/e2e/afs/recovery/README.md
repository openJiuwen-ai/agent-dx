# AFS 来源恢复与缓存对账 E2E

沿用 [Nydus 准备步骤](../nydus/README.md)，使用 [distribution 四节点 R1 配置](../distribution/configs/)。所有正文在独立 Linux ext4；共享目录仅放程序和证据。每个 Node 配置同一 lazy handler，缓存显式开启；不是生产缩减持久副本数的建议。

宿主执行（`TASK` 为已登记的独立工作区，`CLI` 为公共 adx-vm 路径）：

```sh
python3 check_origin.py --workspace "$TASK" --adx-vm "$CLI"
python3 check_cache.py --workspace "$TASK" --adx-vm "$CLI"
```

- `check_origin.py`：新镜像 HEAD 预建、A 填充 4 KiB，停止唯一持久节点 A；B 等待既有租约到期，恢复描述文件和正文，断言原 Chunk 身份／描述不变。再填新范围，并禁止 Registry 正文请求，C 必须复用成功。finally 重启 A。
- `check_cache.py`：A 写普通文件，B 读取形成缓存；B 重启完成注册后、首个维护 tick 前，仅阻断 B→Meta 的 TCP 7400。缓存发布失败后强制终止 B，解除故障并重启；无需再读 B，暂停 A 时冷节点 C 必须读到正确内容。随后删除准确选定的 B 缓存文件，在 Meta 不可达时再次终止／重启 B，必须收到撤销确认并清除持久待办。脚本只动自己登记的四节点会话；finally 删除专用 iptables 规则并恢复暂停进程。
- `catalog_probe.py`：只读检查点与完整 WAL 行，供两项测试核对记录；不修改存储，不是产品诊断 API。

完整回归同时运行 `nydus/check_http.py`、真实 Nydus B/C 全新缓存挂载、`nydus/check_owner_race.py`（该项 Node 的 connector/read budget 临时设为 60 秒，租约配置不变）和 `distribution/probe_replica.py --multi-source`、`distribution/check_mutations.py`。故障测试须顺序执行；owner race 后旧节点可能按 session fencing 退出，确认并重新启动后再执行其他用例。

Linux gates：`recovery_`、`lazy_fill_`、`distribution_`、`node::connectors`、完整 AFS lib、fmt、workspace Clippy。丢失确认的确定性窗口由组件故障注入证明；真实 E2E 使用 Meta 断联及 Node 崩溃，不把两者混称为精确丢包测试。

证据包括固定源码／二进制身份、原始失败与通过日志、JSON、源站请求记录和配置。节点切换约受默认 30 秒租约限制；没有立即切换或固定恢复时间承诺。来源绑定／Registry 对象仍可用、Meta 元数据完整是恢复前提。结束后先停服务和卸载，再清理本任务正文与重复产物，close/release 环境；保留一份必要证据和最终程序。
