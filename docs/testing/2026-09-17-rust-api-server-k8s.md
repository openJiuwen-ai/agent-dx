# Rust API Server 与调度 Shard 正式 Kubernetes 验收

[Buildkite #24](https://buildkite.com/agent-dx/agent-dx/builds/24) 的 release 构建、镜像发布和独立 Kubernetes E2E 全部通过。验收代码提交为 `385b698043f0b62fac943a43da3de17bf59f6441`，分支 `refactor/api-server-rust`。后续文档提交不改变本次验收代码。

## 实现范围

Rust `adx-api-server` 替换 Go HTTP 服务，公开 Sandbox HTTP/SDK 契约保持，直接调用 Instance RPC；API Key、归属缓存、不可变操作目标重试、SSE、快照接口和 Agent 流式转发已迁移。内部协议拆分为 `instance.proto`、`instance_types.proto`、`snapshot.proto`、`credentials.proto`、`routes.proto`，删除旧 Frontend protobuf 适配。

调度层命名为 Scheduling Shard：`ShardScheduler`、`shard_id`、`scheduler_shards`。Global 轮转 → Shard Filter/Score → Node Manager 准入保持。CLI、证书身份、配置、统一包、K8s 部署和指标消费者同步迁移，具体升级步骤见 [迁移记录](rust-api-server.md)。

## 用例与部署

namespace：`adx-e2e-dd9067e81afe`。两个 Pod 在同一宿主 `10.244.128.160`，验证跨 Pod 通信和进程故障；未验证跨宿主网络分区。

| 用例组 | 结果 | 秒 |
| --- | --- | ---: |
| sdk | PASS | 29.888 |
| auth | PASS | 21.150 |
| capacity | PASS | 34.004 |
| placement | PASS | 95.557 |
| node-failure | PASS | 56.601 |
| restart | PASS | 5.332 |
| stop | PASS | 29.978 |

JUnit 包括清理共8项，失败、错误、跳过均为0；`cleanup_errors=[]`、`missing_checks=[]`，namespace删除已验证。节点失联场景包含旧执行失效、路由撤销、健康节点继续工作，以及返回节点清理旧执行。

资源指标三个时点（Running、已分配CPU毫核、队列长度）为 `(2,4000,0)`、`(2,4000,1)`、`(0,0,0)`，Master/Node账本一致。Gateway Metrics、两节点日志滚动/采集和Trace检查通过，完整数据见本次产物。

| 可观测检查 | node1 | node2 |
| --- | ---: | ---: |
| 唯一 Span | 2413 | 970 |
| queue→execute 父子关系 | 680 | 444 |
| 完整创建 Trace | 9 | 在 node1 聚合核对 |
| 恢复后唯一日志探针 | 40/40 | 40/40 |
| 可读 gzip 归档 | 109 | 24 |

创建链路包含 `adx-api-server`；两节点均通过 RRT 远端上下文、Collector 重启补传、后端故障期 SDK 生命周期及凭证不进入采集载荷检查。

## 制品身份

发布包 `dirty=false`、`target=x86_64-unknown-linux-gnu`、`profile=release`；release manifest 与 bundle 内包记录相同，registry记录的bundle摘要、镜像ID均匹配。

- API Server SHA256：`ce02190859a4f499fb90f4d11f1da52b996364f4088242605abae814017b4085`；发布包包含 `bin/adx-api-server`。
- sandboxd revision：`efc201531d7e2e9d69505da151eb66084b61eebf`。外部后端复用已通过的#21同版本、同架构制品，逐文件校验SHA256；ADX产品由本次代码重新构建。
- node：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:249b9bcc266e3e08bf7615c2538791a950213f7c3b137c3a0086012ccd95294f`。
- rrt：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:65e7ad75a617a8d4280d52019c4a826fdbdf6fbd1c33fae4f36d6eac608623c2`。

流水线保存309项产物；主要证据在 `out/buildkite/acceptance/`：`result.json`、`case-results.json`、`junit.xml`、`placement.json` 和每节点采集记录。构建/镜像/部署汇总在 `out/buildkite/summaries/`。本地已下载核验记录：`out/ci/api-server/buildkite-24-evidence/out/buildkite/verified.json`；完整日志为 `out/ci/api-server/buildkite-24-platform-*.log`。

## 本地回归与边界

工作区370项通过、29项环境/性能用例默认忽略；58项E2E驱动、7项CI驱动、14项实际Redis存储、11项实际Redis/mTLS RPC及生产API二进制HTTPS契约通过；严格Clippy通过。名称含空格、加号和百分号的路径解码回归先失败后通过；暂停/恢复重试、快照CRUD、克隆和九条Agent流式转发也通过本地HTTPS测试。

本次正式K8s使用runc基础七组，不包含Firecracker暂停/快照、真实GPU/NPU、跨宿主故障或长稳。既有FC与设备待办不因本次迁移通过而关闭；Agent路由转发测试不代表Agent业务后端完成迁移。见 [剩余事项](control-plane-remaining.json)。
