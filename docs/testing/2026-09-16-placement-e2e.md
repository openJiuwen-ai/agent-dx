# 双节点放置约束端到端验收

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

2026-09-16，统一 package-v17 的 Linux ARM64 release 制品在本地 Docker 双节点环境通过六组基础验收。产品调用链使用安装后的 Sandbox SDK、Edge、Frontend、Master/Domain、Node Manager、Node Proxy、外部 sandboxd 和真实 RRT；只读 Redis 目录用于核对实际归属和资源释放。

## 结果

| 用例组 | 结果 | 耗时 |
| --- | --- | --- |
| SDK 创建、查询、命令、文件、删除 | 通过 | 22.321 秒 |
| API Key 和租户隔离 | 通过 | 10.748 秒 |
| 容量不足排队、释放后调度 | 通过 | 33.542 秒 |
| 双节点放置约束 | 6/6 通过 | 94.838 秒 |
| Node Manager 进程重启与对账 | 通过 | 35.125 秒 |
| supervisor 停机清理 | 通过 | 21.117 秒 |

放置组分别验证：实例亲和 OR → node1；实例反亲和 → node2；加权节点偏好 → node2；有序节点偏好 → node2；node_id 约束所有 OR 分支 → node2；已有实例的硬反亲和阻止新实例进入 node1 → node2。每项都核对实际节点，并执行真实 RRT 命令。两个固定归属的标签实例作为前五项的等量基础占用，反向反亲和用例额外设置 node1 高权重偏好，验证硬约束生效。

九个放置测试实例最终均为 Deleted 且不占资源，两个 sandboxd 清单为空。本轮容器与网络清理成功，缺失用例和清理错误均为零。未调整 Frontend 重试或 Node Proxy 超时；运行日志保留路由发布期间由现有 SDK 重试处理的短暂 503。

## 门禁变更

`build/e2e/placement.py` 是本地与 Kubernetes 共用的真实用例组；`build/e2e/run.py` 将 placement 加入必需集合。只完成旧五组不能通过门禁。Kubernetes JUnit 分列六个用例组和清理结果，用例日志输出预期/实际节点与执行结果。

TDD 首先确认旧门禁错误地允许缺失 placement 的运行通过；修正后本地/Kubernetes 驱动的 48 项契约测试全部通过。测试结果属于驱动契约验证，与上面的真实双节点结果分别记录。

## 制品与证据

- 发布包：`out/ci/pause-resume/package-v17/manifest.json`，基准 commit `0dde79ad57583e998389101a763e4d2d825be63e` 加未提交修改，`dirty=true`，`aarch64-unknown-linux-gnu / release`。
- sandboxd：PR #56 的 `efc201531d7e2e9d69505da151eb66084b61eebf`，本轮使用 runc。
- 节点镜像 ID：`sha256:61b6724067768027197596e600b02a48b66b2ee7efcd2290260317c030581367`。
- RRT 镜像 ID：`sha256:b0ae1641a392ff1d4cf22c35b9e60147953419de6af0dbaaa92bb114e669ca81`。
- 本轮 ID：`adx-e2e-b481bd81e47d`。
- `out/ci/stage-7/placement/local.log`：构建、部署和用例完整日志。
- `out/ci/stage-7/placement/bundle/bundle.json`：包、后端和镜像校验信息。
- `out/ci/stage-7/placement/local/result.json`：六组门禁与清理结果。
- 同目录 `placement-result.json`、`case-results.json`：实例归属、命令断言及分组耗时；节点日志也保留在该目录。
- `out/ci/stage-7/placement/contracts.log`：48 项驱动契约测试；`red.log`：初始失败证据。

这是同一 Docker 主机上的两个逻辑节点，未证明跨物理宿主网络、GPU/NPU 或混合负载性能。本轮不涉及 Firecracker 双克隆修复，也未触发正式 Buildkite/K8s；正式验收需要已提交版本的干净构建及其镜像交接。本次本地通过不关闭这些剩余项。
