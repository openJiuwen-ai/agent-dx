# Agent FS（AFS） 部署与配置

Agent FS（AFS） 是 Agent DX 的可选文件系统组件。默认部署配置、默认发布包和默认 supervisor 服务都不包含 AFS 二进制、配置示例或 FUSE 依赖；只有显式设置 `ADX_WITH_AFS=1` 的文件系统包和部署入口才包含这些内容。

## 发布包合同

默认包不得包含：

- `bin/afs-meta`
- `bin/afs-node`
- `etc/examples/afs/`
- Agent FS（AFS） 专用端口、数据目录、挂载点或探针程序

显式文件系统包必须在 `manifest.json` 中记录 `with_afs: true`，并包含 `afs-meta`、`afs-node` 和 `etc/examples/afs/*` 的摘要。测试辅助程序只由验收流程按需构建和放入测试环境，不加入普通试用包。
组包完成后的校验与安装前／暂存后的校验共用随包
`lib/package_manifest.py` 合同；AFS ON、默认 OFF、旧 `with_dfs`、文件清单、摘要和
主机架构不会由两份独立规则分别解释。

## 配置示例

发布包配置示例来自 [build/config/examples/afs/](../../build/config/examples/afs/)，源码级示例保留在 [afs/examples/](../../afs/examples/)。部署层只负责把 AFS TOML 交给 `afs-meta` 和 `afs-node`，以及进程启动、健康查询、正常停止和卸载；它不改写 AFS 自有配置 schema。

Meta 后端优先级：

1. `memory`：一次性演示，重启不保留状态；
2. `local-file`：当前可靠性基线，中心节点可正常重启恢复；
3. `etcd`：资源和可靠性专题后置；
4. `redis`：最低优先级，后置。

当前试用建议使用 `local-file`。发布包内 `etc/examples/afs/meta.toml` 已显式设置 `meta_store = "local-file"`，并把 Meta 状态放在 `/opt/adx/data/afs/meta`，避免因代码默认值误连 etcd。使用 `memory` 时，验收报告必须明确“Meta 重启后不保留状态”。

单机试用前先准备配置、数据、运行时、挂载目录和本机临时证书：

```sh
sudo install -d -m 0755 /opt/adx/config/afs /opt/adx/run/afs /mnt/adx/ownerfs /mnt/adx/dfs
sudo install -d -m 0700 /opt/adx/data/afs/meta /opt/adx/data/afs/node-a
sudo cp /opt/adx/current/etc/examples/afs/meta.toml /opt/adx/config/afs/meta.toml
sudo cp /opt/adx/current/etc/examples/afs/node.toml /opt/adx/config/afs/node.toml
sudo /opt/adx/current/etc/examples/afs/prepare-local-tls.sh
sudo cp /opt/adx/current/etc/examples/afs/deployment-ownerfs-local.yaml \
  /opt/adx/config/deployment.yaml
sudo adxctl validate
```

证书脚本只创建 30 天有效的本机隔离试用 CA、Meta 和 `node-a` 身份，并在已有
任一目标文件时拒绝覆盖；它不分发固定私钥，也不适用于生产或多主机扩容。生产部署
必须由自己的 CA 为每个 Node 签发独立身份，并在 Meta 和 peer 的
`trusted_node_certs` 中把证书精确映射到该 Node ID。不要复制 Node 私钥到其他主机。

示例 Node 配置使用 mTLS `https://` endpoint、`/opt/adx/data/afs/node-a` 和
`/opt/adx/run/afs/node-a.sock`。前台运行 `sudo adxctl run` 后，在另一终端用
`sudo adxctl status` 检查 Meta 与 Node 都返回 `status=ready`，并用
`sudo adxctl stop` 正常停止。Supervisor 按 Meta→Node 排序启动；Node 在尚未挂载或
暴露本地 API 前对首次 Meta 注册做有界、可取消重试。预算耗尽、证书/身份拒绝或运行后
心跳/排空失败仍然失败关闭，不会无限重试或盲目重启。

如果启用 FUSE 挂载，先确认 `/mnt/adx/ownerfs` 与 `/mnt/adx/dfs` 为空目录，再在
`node.toml` 中启用对应 `ownerfs_mount` 或 `dfs_mount`。挂载所有者捕获的 canonical
path、mount ID 和 source 同时作为 readiness 身份；挂载被替换时健康检查继续失败关闭。

## OwnerFs workspace bind ON

普通发行默认关闭 workspace bind。当前实际试用场景需要显式开启，示例字段以 `build/config/examples/afs/node.toml` 为准；历史 `native_workspace` 命名只作为兼容入口，不能作为新的核心能力名称。

开启条件：

- 先通过 OwnerFs 正常创建并授权 workspace，再开启 bind；不要在 Home/root/epoch 尚未固定时直接打开开关；
- workspace 必须是已授权的本地 Home workspace；
- bind 源必须是 Home 上的底层真实目录；
- bind 目标必须是 OwnerFs FUSE 根下对应的一级目录；
- 同一 Node 只启用管理员控制的 host bind entry；
- 停机、root/epoch 变化或授权失效前，先停止受管用户并排空引用。

## 运维边界

启动前一次性检查二进制身份、配置 SHA、端口、TLS、数据目录、运行目录、日志目录、挂载目录、FUSE 设备、`fusermount3`/`umount`、旧 PID、旧 UDS、旧挂载和磁盘余量。遇到环境阻塞时保存证据并停止受影响项，不把环境修补混入产品结论。

正常停止应记录进程退出状态，确认 FUSE mount 和 bind mount 已卸载，UDS/PID/临时 claim 无遗留。`local-file` Meta 的恢复检查至少要覆盖写入、sync/close、正常停止、重启、重新打开和内容校验；这不等于 crash recovery、HA、多 Meta 或复杂可靠性通过。
