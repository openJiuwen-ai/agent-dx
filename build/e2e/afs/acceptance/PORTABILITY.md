# 验收工具可移植边界

本目录是目标仓内维护中的 AFS 验收工具入口（OwnerFs / DFS），支持在一个独立 clone 中直接读取当前源码树：

- 默认验收合同：`docs/development/afs-plan.md` 和 `docs/migration/2026-10-09-afs-snapshot.md`。
- 默认 case manifest：`build/e2e/afs/acceptance/cases.json`。
- 默认 lock：`build/e2e/afs/acceptance/acceptance.lock.json`，当前只表示 `PREPARING`，不能证明环境 READY。
- 默认运行产物：`.local/acceptance/<run-id>/`，该目录被 `.gitignore` 排除。

Linux 才运行真实文件系统、FUSE、runc、RDMA、MooseFS、3FS 和多 VM 验收。macOS 只用于编辑、静态检查、整理输入和生成文档。新的机器或 VM 必须重新观测网络、身份、候选二进制和容量；不要把历史 SHA、PID、VM 路径或失败日志复制成当前结果。

VM 模板保留原始观察中的角色、架构、内核、CPU/RAM、磁盘和约束。迁移到新机器时，只允许替换机器相关值，例如宿主路径、guest 用户和本地镜像缓存 URL；替换后必须重新准入。

维护规则：

1. `build/e2e/afs/acceptance/fixtures/` 只保留回归测试实际读取的小输入。
2. 历史过程证据、失败原件、checkpoint 和大型日志保存在源码仓上一级 `local-archive/`，不回流到产品树。
3. runner、drivers、probes、suite selectors 和 rootfs 准备脚本可以留在本目录；生成结果、VM 数据、TLS 私钥、二进制和源码构建缓存不得提交。
4. 没有已验证 lock、候选身份和环境准入时，不得把任何 release driver 标记为最终 READY。
