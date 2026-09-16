# 实例数量与资源分配Metrics验收

首批功能提交 `5f592cf57c9c0138fc3a22784c48e75a9c80f4fb`，已推送 `ci/control-plane-k8s-20260916`。指标口径和采集配置见[实例资源Metrics](instance-resource-metrics.md)。

## 实现与本地验证

Master新增可配置HTTP `/metrics`，提供节点/Domain维度的实例状态计数、队列长度和分配账本。Node Manager补齐资源容量、已预留、可用、超额占用与观测有效性。CPU、内存、磁盘和GPU/NPU整卡分别统计，实际使用量沿用已有采样指标。

先复现容量指标缺失的RED断言，再实现导出。53项不同Rust检查通过：core 6、Node生命周期17/资源4、Master恢复5/公平性2、真实Redis/mTLS RPC 11、CLI配置8；Clippy零警告。运行脚本最初漏设证据目录导致进程用例失败，补齐后该项通过；保留失败日志。53项Python驱动检查通过。

Linux ARM64全bins构建、统一发布包及本地双节点真实runc公共SDK七组全部通过，清理无残留。该本地包标记dirty，Go API/SDK/Redis复用已验证包，Rust程序本轮重建；正式CI则从已提交源码重新构建ADX制品。

## 正式K8s验收

[Buildkite #17](https://buildkite.com/agent-dx/agent-dx/builds/17) 的构建、镜像发布与独立Kubernetes E2E全部通过。包清单commit与本次提交一致、dirty=false，release manifest和bundle身份核对通过。

| 用例组 | 结果 | 秒 |
| --- | --- | ---: |
| sdk | PASS | 28.374 |
| auth | PASS | 21.348 |
| capacity | PASS | 34.253 |
| placement | PASS | 96.108 |
| node-failure | PASS | 57.302 |
| restart | PASS | 5.231 |
| stop | PASS | 21.328 |

JUnit含清理共8项，无失败/错误/跳过。运行namespace为 `adx-e2e-23951283dc94`，最终 `cleanup_errors=[]`、`missing_checks=[]`。两个Pod同处宿主机 `10.244.128.160`（IP分别为 `10.245.14.235`、`10.245.2.141`），不代表跨宿主故障验证；FC本轮关闭。

## 实际抓取结果

| 时点 | Running实例 | 已分配CPU毫核 | 等待请求 |
| --- | ---: | ---: | ---: |
| 两节点满载 allocated | 2 | 4000 | 0 |
| 第三请求排队 queued | 2 | 4000 | 1 |
| 删除完成 released | 0 | 0 | 0 |

三个时点都通过HTTP抓取Master和两个Node Manager。每个节点的CPU、内存、磁盘capacity/reserved/available/overcommitted逐项一致；父任务对下载后的原始抓取文本再次解析核对，结果一致。

GPU/NPU库存、保留分配与丢失设备的统计规则由组件检查覆盖，实卡验收仍是阶段5独立待办。日志采集、Trace、高基数明细配置及日志滚动压缩仍按后续规划推进。

## 证据

- `out/ci/stage-8/metrics/`：red.log、green-1.log、green-2.log、acceptance.log及本地local结果。
- `out/ci/stage-7/preflight/build-17-evidence/verified.json`：正式验收汇总。
- 同目录 `acceptance/node1/metrics-{allocated,queued,released}.json`：三个时点的Master/Node原始抓取文本。
- Node镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:6d6bfcecbb88fe6e6d542ea588696661871e9d348deed7de1fda180b275b9173`。
- RRT镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:0bc41ee4565179915155ea487ad5d550268278ec3bac0419cf4086d77f9bf9a3`。
- sandboxd固定revision：`efc201531d7e2e9d69505da151eb66084b61eebf`。
