# OCI 运行环境与本地优先创建 K8s 验收

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

2026-09-18，[Buildkite #30](https://buildkite.com/agent-dx/agent-dx/builds/30) 对提交
`363e44f1b28f1a792a4b9935c5bd27034fba9926` 完成正式 Kubernetes 验收。`platform-build`、
`platform-images` 和 `platform-e2e` 三个步骤全部通过；发布清单记录 `dirty=false` 和
`x86_64-unknown-linux-gnu`。本轮 K8s 使用 OCI 运行环境，本地／独立进程部署继续支持
发布包内 EROFS，两条产品路径同时保留。

## 运行环境结果

流水线发布不可变 RRT OCI image，并以同一个 digest 配置默认 rootfs 与自定义用户镜像的
bootstrap。SDK 组验证：

| 模式 | 根文件系统 | RRT 来源 | 结果 |
|---|---|---|---|
| `default` | RRT OCI image | 根文件系统内统一入口 | passed |
| `runtime-only` | RRT OCI image | 请求显式携带同一运行环境 | passed |
| `custom` | 不含 RRT 的普通用户 image | RRT OCI image 只读递归 bind 到 `/__adx` | passed |

`custom` 路径要求 OCI spec 的 mount options 同时包含 `ro` 和 `rbind`。只设置
`type=bind, options=[ro]` 不会产生 bind 标志，runc 会以单独的 `MS_RDONLY` 挂载并返回
`ENODEV`；该问题由 #29 捕获，提交 `363e44f` 修复。此参数差异只适用于 OCI image
mount，本地 EROFS 仍由 sandboxd 按 `type=erofs, options=[ro]` 处理。

## 八组端到端结果

| 用例 | 结果 | 耗时（秒） |
|---|---:|---:|
| sdk | passed | 14.281 |
| auth | passed | 11.649 |
| capacity | passed | 4.303 |
| placement | passed | 5.355 |
| local-first | passed | 8.341 |
| node-failure | passed | 37.351 |
| restart | passed | 5.733 |
| stop | passed | 11.379 |

`result.json` 为 8/8 passed，`missing_checks=[]`、`cleanup_errors=[]`。JUnit 共 9 项
（八组用例加清理），0 失败、0 跳过；namespace 删除已确认。`local-first` 验证 API Server
入口节点轮转、同 ID 并发收敛、冲突规格拒绝、真实 RRT 命令、原子 claim 日志与物理清理。

## 制品与部署身份

- ADX release SHA256：`a703c15e4d78c4cc36af01b82aca14f2443bcd153a35c3d1993144dde4d30bdb`。
- Node image：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:9527862a7e77fd0d7ad0c2f4e96a79c0070db7318b0b038875bf21c027ee8d74`。
- RRT image：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:3467204e59e83e8e608ef7a4a6c72a2812f43b29d647a44cdd445269a30fcf5d`。
- sandboxd：`efc201531d7e2e9d69505da151eb66084b61eebf`，目标
  `x86_64-unknown-linux-gnu`。构建复用 #27 后端制品前，逐文件 SHA256 及仓库
  `verify_backend.py` 均通过；ADX 产品与 SDK 仍从本轮提交重新构建。

两个测试 Pod 位于同一物理宿主 `10.244.128.160`，Pod IP 分别为 `10.245.2.141` 和
`10.245.6.34`。该结果验证 K8s 进程部署、跨 Pod 路由和节点进程故障，不作为跨物理宿主
网络分区或宿主机故障证据。Firecracker checkpoint/snapshot 仍按当前决定在本地 KVM 环境
验收，本轮未启用 K8s FC profile。

## 失败记录边界

- #28 在构建固定 runc 时访问 GitHub 超时，未进入 K8s；属于外部依赖下载失败。
- #29 构建、镜像和 OCI 默认路径通过，但 `custom` 模式暴露缺少 `rbind`，因此整轮失败。
- #30 使用修复后的已提交代码和校验过的固定后端制品，才是本功能的正式通过记录。

本地证据保存在 `out/ci/oci-runtime/buildkite-rbind/`，包括三阶段日志、`result.json`、
`case-results.json`、`junit.xml`、`placement.json`、`runtime-environment-result.json`、
发布/镜像汇总和 `verified-acceptance.json`。`out/` 不进入 Git，干净克隆通过上面的
Buildkite 链接和固定制品身份复核结果。
