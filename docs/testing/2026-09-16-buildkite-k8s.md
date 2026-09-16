# Buildkite #15 基础 Kubernetes 验收

2026-09-16，[Buildkite #15](https://buildkite.com/agent-dx/agent-dx/builds/15) 的构建、镜像发布与独立 Kubernetes E2E 三个步骤全部通过。按本轮决策，正式流水线运行基础 K8s 七组；Firecracker 继续本地验收，`ADX_E2E_CHECKPOINT=0`。

## 用例与清理

| 用例组 | 结果 | 秒 |
| --- | --- | --- |
| sdk | PASS | 32.967 |
| auth | PASS | 21.199 |
| capacity | PASS | 33.903 |
| placement | PASS | 95.908 |
| node-failure | PASS | 57.302 |
| restart | PASS | 5.582 |
| stop | PASS | 21.114 |

放置约束包括实例亲和 OR、实例反亲和、加权节点偏好、有序节点偏好、每个 OR 分支保留 node_id 约束、反向实例反亲和，共 6/6 通过。所有用例结束后资源释放。

节点故障组暂停 Node Manager 心跳，验证旧执行失效、节点不可用、路由撤销，健康节点仍可执行；恢复报到后 backend 清空，清理结果提交成功。此组使用 runc，不验证 checkpoint 迁移。

`result.json` 为 passed，七组无缺失，`cleanup_errors=[]`。JUnit 包含七组与清理共 8 项，无失败、错误或跳过。namespace `adx-e2e-5e32139b3170` 删除耗时 130.5 秒，随后查询确认不存在。

## 部署落点

| Pod | 宿主节点 | Pod IP |
| --- | --- | --- |
| node1 | 10.244.128.160 | 10.245.14.235 |
| node2 | 10.244.128.160 | 10.245.2.141 |

这是两个真实 K8s Pod、两个逻辑执行节点，位于同一宿主节点；未覆盖跨宿主机网络与宿主机故障。日志包含 namespace/Pod 部署、平台就绪、逐用例 RUN/PASS、诊断收集、停止实例和 namespace 删除过程。

## 源码与制品

- 已验证提交：`85d89e841c8b6622e1f1c1892c55c15f10465ce8`，分支 `ci/control-plane-k8s-20260916`，已推送 origin。
- CI 工作树：`worktrees/control-plane-k8s-20260916`。从开发工作树生成独立提交，逐文件核对源码一致；原开发工作树保留。
- 发布包：`x86_64-unknown-linux-gnu / release`，manifest 的 commit 与构建一致，`dirty=false`；bundle 内包清单与 release manifest 完全一致。
- `adx-release.tar.gz`：43634479 bytes；SHA256 `22030c41e9db38524ce8fb8b7c8233082f1783fabc12078b3ac1bb8785e060ab`。
- Node 镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:28fc26961f69fa762b88d0b858a6dd301b12eee6bd0d954477c62759136b2468`。
- RRT 镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:afe22fae1375f67f7b28cf522a9fda067159a0617e6429f3f030088e64667e4e`。
- sandboxd：`efc201531d7e2e9d69505da151eb66084b61eebf`，复用固定 backend 构建的制品，并在本轮校验 revision、架构、文件清单及 SHA256。ADX 组件由本轮源码构建。

流水线上传 141 项制品。精选证据保存在 `out/ci/stage-7/preflight/build-15-evidence/`，其中 `verified.json` 汇总身份、用例和落点；完整清单为 `artifacts-15.json`，最新 E2E 日志为 `job-15-platform-e2e-4.log`，收集核对日志为 `collect-15.log`。进度页将阶段 7 按本轮基础 K8s 范围标为完成。

FC 的暂停恢复、快照和跨节点恢复保留已有本地证据。将来接入正式 K8s FC profile 时，仍需提供相同架构的 runtime kit、选定可用 KVM worker 并运行全部 FC 用例；本次通过不替代这些条件。
