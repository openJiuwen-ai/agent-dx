# 2026-09-25 cn-north-4 定向 Full E2E

本次在 cn-north-4 集群以真实 Kubernetes Pod、Redis、sandboxd、Execd 和公开 Python Sandbox SDK
运行 11 项可靠性场景。基础出包采用
[Buildkite #95](https://buildkite.com/agent-dx/agent-dx/builds/95)；产品与测试驱动均为提交
`16298a4612f58edaf85bbcfcbd4c647c6287373b`。制品清单中 sandboxd revision 为
`efc201531d7e2e9d69505da151eb66084b61eebf`。运行前逐一核对了基础包制品 SHA256 和
镜像清单关联的 bundle SHA256。构建的 source、SDK、平台构建、镜像与 L0 作业均通过。

最终完整定向批次耗时 1097.502 秒，`status=passed`，11/11 检查通过、无缺失检查、无失败场景、
无清理错误；JUnit 为 14 项、0 失败、0 跳过。每个场景的 node1 与 node2 Pod 均分别落在
`192.168.10.48` 和 `192.168.10.192`，不是同一物理 worker 的两个 Pod。运行后再次查询集群，
没有遗留 `adx-e2e-` 测试命名空间。

| 类别 | 通过的场景 |
|---|---|
| 创建及调度 | `create-unknown-query`、`schedule-deadline` |
| 命令结果与容量 | `command-response-cut`、`command-registry-capacity`、`command-expiry` |
| 命令流及能力 | `command-watch-unavailable`、`command-watch-query-unavailable`、`command-unsupported-feature` |
| 文件传输 | `upload-response-cut`、`download-response-cut` |
| 节点恢复 | `sqlite-node-restart` |

首次完整定向运行在旧提交 `88dd224` 上执行 10 项，6 项通过、4 项失败：管理员队列查询用了租户
API Key；下载断流注入早于路由同步；SQLite 重启检查读取了错误的 runtime ID 层级；过期命令
被 Execd 误报为从未存在。这些失败均先保留为测试或产品红灯，再修正。提交 `de96757` 修复
前三项测试链路并补充 Watch 与查询同时故障的断言；提交 `16298a4` 为 Execd 增加有界过期 ID
记录和 `COMMAND_EXPIRED` 映射。修复后 5 项定向复测通过，随后上述 11 项完整定向批次通过。

本地原始证据保存在 `out/ci/targeted-full-suite-0925/`：`bundle95/bundle.json`、
`bundle95/registry-images.json`、`cn-north-4-run5/result.json`、
`cn-north-4-run5/placement.json`、`cn-north-4-run5/junit.xml` 和
`cn-north-4-run5-driver.log`。该目录为运行产物，未提交到 Git；可复查 Buildkite #95 的基础包
制品来源以及本地逐场景日志。此前红灯和复测证据分别位于同目录的 `cn-north-4-run3/`、
`cn-north-4-run4/`。

本次是 11 项定向故障批次，没有同次重跑完整 Full 十一组，也没有运行三 VM、Firecracker、
真实 GPU/NPU 或长时间稳定性测试；这些项目保留各自的门禁状态。
