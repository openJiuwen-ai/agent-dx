# 日志与Trace正式Kubernetes验收

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

[Buildkite #21](https://buildkite.com/agent-dx/agent-dx/builds/21) 的编译、镜像发布、独立Kubernetes E2E全部通过。验收提交为 `b3145d6d43d04c06dbc85a91928d441814111d87`，发布包dirty=false，release manifest、bundle与镜像引用身份一致。

## 部署与用例

运行namespace：`adx-e2e-89f162dc8e06`。两个Pod位于同一宿主 `10.244.128.160`，验证了跨Pod调用与进程故障，未验证跨宿主故障。FC profile按当前范围关闭。

| 用例组 | 结果 | 秒 |
| --- | --- | ---: |
| sdk | PASS | 30.489 |
| auth | PASS | 21.199 |
| capacity | PASS | 33.702 |
| placement | PASS | 95.256 |
| node-failure | PASS | 54.397 |
| restart | PASS | 5.281 |
| stop | PASS | 31.479 |

JUnit含清理共8项，无失败、错误、跳过；`cleanup_errors=[]`、`missing_checks=[]`。节点失联用例验证旧执行失效、路由撤销、健康节点不受影响，以及节点恢复后的清理。

## 实际采集结果

| 检查 | node1 | node2 |
| --- | ---: | ---: |
| 唯一Span | 2323 | 950 |
| queue→execute父子关系 | 664 | 434 |
| 完整创建Trace | 9 | 在node1聚合检查 |
| RRT接收远端上下文 | PASS | PASS |
| 日志恢复唯一探针 | 40/40 | 40/40 |
| Collector重启及后端故障恢复 | PASS | PASS |
| 后端503期间公共SDK生命周期 | PASS | PASS |
| 采集载荷无测试凭证 | PASS | PASS |

完整创建Trace包含Edge、Sandbox API、Master、Node Manager、实例队列/执行及状态提交。父子校验以Trace ID与Span ID联合索引进行；数据路径验证RRT收到远端上下文。创建、执行和删除分别属于各自请求Trace，以Capsule ID关联。

原有资源与Gateway指标、日志滚动压缩同时通过：allocated/queued/released三个时点的Running、预留CPU毫核、排队数量分别为(2,4000,0)、(2,4000,1)、(0,0,0)，Master及两节点账本一致。两个节点均验证gzip归档及日志I/O健康状态。

## 制品与证据

- sandboxd revision：`efc201531d7e2e9d69505da151eb66084b61eebf`。
- Collector：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:b5cf983651c32c3ca13f936deb51742015a54d121f388cac248923ddeb8cc9fc`（0.161.0，linux/amd64）。
- node镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:a6fdb63918af13aa53bdc87593e47199bba5aaf0ce46f90687dc770c433703f8`。
- rrt镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:31c45a3b0dec4d3826bf03d1647395aa21d44eead2ed6e4020bbd5cbb57542dd`。
- Buildkite日志包含部署指令、逐组RUN/PASS及METRICS/COLLECTION/TRACE PASS。产物为`out/buildkite/acceptance/`中的result.json、case-results.json、junit.xml、placement.json及每节点采集记录，累计汇总为`out/buildkite/summaries/e2e.json`。
- 下载后的核验摘要：`out/ci/stage-7/preflight/build-21-evidence/verified.json`；本地完整日志：`out/ci/stage-8/k8s-21/`。

## 验收范围与后置事项

统一的实时队列满丢弃指标按用户决定后置，不阻塞本期阶段8完成；现有导出成功/失败Span计数与SDK队列丢弃不是同一口径。真实GPU/NPU、FC双克隆问题、跨宿主及长稳验证继续按原待办推进。当前验收不覆盖任意长时间中断、Pod删除后采集状态持久化、SDK事务根Span或用户进程内部Span。
