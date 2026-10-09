# 固定回归输入

这里保留网络与 verbs 环境判定器实际消费的固定输入，供独立 clone 在 Linux 运行回归。当前共 185 个输入文件；不是候选版本的验收结果，也不证明当前 VM、网络、RDMA 或产品已通过。

原始提交为 `6b75d78a1c9350553770394b13384e01a9b292c0`：

- `network-preparation/` 取自原 `development/evidence/20261001-network-preparation/`，由 `test_environment_network.py` 读取。
- `verbs-preparation/` 取自原 `development/evidence/20261001-verbs-preparation/`，由 `test_environment_verbs.py` 读取。

只保留谓词使用的 receipt、原始片段、hash 绑定输入和保护对象观测；完整原件和失败记录留在仓库外归档及原 Git 提交。冻结的 Python/shell 输入用于验证源码与观测绑定，不是继续维护的工具副本；维护版本在 `build/e2e/afs/acceptance/` 和 `build/e2e/afs/acceptance/probes/`。不向这里加入每轮运行输出、TLS 私钥或 VM 数据。
