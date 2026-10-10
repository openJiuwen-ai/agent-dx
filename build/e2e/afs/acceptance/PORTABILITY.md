# 验收工具可移植边界

本目录是目标仓内维护中的 AFS 验收工具入口（OwnerFs / DFS），支持在一个独立 clone 中直接读取当前源码树：

- 默认验收合同：`docs/development/afs-plan.md` 和 `docs/migration/2026-10-09-afs-snapshot.md`。
- 默认 case manifest：`build/e2e/afs/acceptance/cases.json`。
- 实际 lock：运行时必须显式传入 `--lock`，保存在仓外；仓内 `acceptance.lock.example.json` 只提供无机器身份的 PREPARING 结构，不能证明环境 READY。
- 默认运行产物：`.local/acceptance/<run-id>/`，该目录被 `.gitignore` 排除。

Linux 才运行真实文件系统、FUSE、runc、RDMA、MooseFS、3FS 和多 VM 验收。macOS 只用于编辑、静态检查、整理输入和生成文档。新的机器或 VM 必须重新观测网络、身份、候选二进制和容量；不要把历史 SHA、PID、VM 路径或失败日志复制成当前结果。

实际 VM 配置、IP/MAC 租约和旧环境锁属于本地资产，不随源码分发。新环境复用 ADX 部署体系；角色、架构、内核、CPU/RAM、磁盘及约束从实际观测填写到本轮输入，不能从历史值推断。

维护规则：

1. `build/e2e/afs/acceptance/fixtures/` 只保留回归测试实际读取的小输入。
2. 历史过程证据、失败原件、checkpoint 和大型日志保存在源码仓上一级 `local-archive/`，不回流到产品树。
3. runner、drivers、probes、suite selectors 和 rootfs 准备脚本可以留在本目录；生成结果、VM 数据、TLS 私钥、二进制和源码构建缓存不得提交。
4. 没有已验证 lock、候选身份和环境准入时，不得把任何 release driver 标记为最终 READY。

LTP driver 的通用安装默认路径为 `/opt/afs-tools/ltp-install`，现有环境须通过 `--ltp-install` 显式传入实际目录；不再默认引用开发者 home。挂载隔离示例使用 `/etc/afs-acceptance` 和 `/opt/afs-acceptance`。

网络/verbs 环境判定器仍核对原有固定观测协议，其中的原 VM 路径和 collector hash 是历史输入格式的一部分，不能通过本轮整理篡改。它们不是新环境的自动创建配置；新环境不匹配时须明确登记待准入，不以旧 fixture 冒充实际资格。测试中的 `/Users/...` 路径仅用于验证拒绝宿主路径。
