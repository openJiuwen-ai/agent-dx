# 验收工具可移植边界

本目录是目标仓内维护中的 AFS 验收工具入口（OwnerFs / DFS），支持在一个独立 clone 中直接读取当前源码树：

- 默认验收合同：`docs/development/afs-plan.md` 和 `docs/migration/2026-10-09-afs-snapshot.md`。
- 默认 case manifest：`build/e2e/afs/acceptance/cases.json`。
- 实际 lock：运行时必须显式传入 `--lock`，保存在仓外；仓内 `acceptance.lock.example.json` 只提供无机器身份的 PREPARING 结构，不能证明环境 READY。
- 默认运行产物：`.local/acceptance/<run-id>/`，该目录被 `.gitignore` 排除。

Linux 才运行真实文件系统、FUSE、runc、RDMA、MooseFS、3FS 和多 VM 验收。macOS 只用于编辑、静态检查、整理输入和生成文档。新的机器或 VM 必须重新观测网络、身份、候选二进制和容量；不要把历史 SHA、PID、VM 路径或失败日志复制成当前结果。

实际 VM 配置、IP/MAC 租约和旧环境锁属于本地资产，不随源码分发。新环境复用 ADX 部署体系；角色、架构、内核、CPU/RAM、磁盘及约束从实际观测填写到本轮输入，不能从历史值推断。

维护规则：

1. 测试输入使用临时目录和合成身份；不导入个人运行观测或源码快照。
2. 历史过程证据、失败原件、checkpoint 和大型日志保存在开发者维护的仓外归档，不回流到产品树。
3. runner、drivers、probes、suite selectors 和 rootfs 准备脚本可以留在本目录；生成结果、VM 数据、TLS 私钥、二进制和源码构建缓存不得提交。
4. 没有已验证 lock、候选身份和环境准入时，不得把任何 release driver 标记为最终 READY。

LTP driver 的通用安装默认路径为 `/opt/afs-tools/ltp-install`，现有环境须通过 `--ltp-install` 显式传入实际目录；不再默认引用开发者 home。挂载隔离示例使用 `/etc/afs-acceptance` 和 `/opt/afs-acceptance`。

旧固定实验室 ENV 判定器及其原始观测、旧 3FS 固定集群实验已完整归档仓外。`environment.py`保留路径／校验和／格式检查并明确 full BLOCKED，后续通用验证器不能绕过真实准入。底层网络／verbs 探针继续由显式参数运行；单元测试使用合成地址与身份，不代表真实运行通过。宿主路径拒绝测试使用虚构用户，拒绝检查本身保留。

标准套件的通用默认根为 `/var/lib/afs-acceptance/suites-reference`；可通过 `--suite-root`、`--fsx-binary`、`--expanded-tsv` 等参数覆盖。准备脚本通过 `STATE_ROOT` 指定状态目录，仍必须是真实 Linux ext4，不能用共享目录替代。

小规模 OwnerFs 写对照必须传入 `--product-source-commit`（40位commit）、`--compiler-input-map`（64位输入清单SHA）和 `--moose-source`（本轮预期的 `mfs#...` 挂载源）；DFS同步读／删除工具的 writer 与 reader/checker 传入相同候选身份，跨候选清单拒绝复用。身份标注为 `caller supplied identity`，须由本轮二进制／构建证据绑定，不能把参数本身当作实际进程身份观测。工具继续观察实际挂载并严格匹配预期源。
