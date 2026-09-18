# Collector SWR 镜像同步

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

- 验收：[Buildkite #20](https://buildkite.com/agent-dx/agent-dx/builds/20)，独立 `collector-sync` 步骤通过。
- 同步脚本提交：`bb3f8e5175e058def452d6f0770bf26c665ee869`。
- 上游版本：OpenTelemetry Collector Contrib `0.161.0`，平台 `linux/amd64`。
- 上游索引：`sha256:fd328de2552466ad78385e1b1289c3f2402b1c45f265b252aab1955b42845ac1`。GHCR 与原 Docker Hub 索引相同。
- 原生 manifest：`sha256:b5cf983651c32c3ca13f936deb51742015a54d121f388cac248923ddeb8cc9fc`。
- SWR tag：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e:collector-0.161.0-amd64-b5cf983651c32c3c`。
- 固定引用：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:b5cf983651c32c3ca13f936deb51742015a54d121f388cac248923ddeb8cc9fc`。

同步使用 CI 现有 SWR Secret，经 GHCR 拉取原生 manifest，推送后按同一摘要回拉成功。日志包含每层推送结果、远端摘要和最终 `status: passed`。Buildkite 产物 `out/buildkite/collector-sync/` 保留 `result.json`、`sync.log`、`summary.md`、`dockerd.log`。

正式 CI 从该 SWR 固定引用拉取；显式镜像及上游镜像站覆盖方式继续有效，本地 ARM64 构建保留原多架构来源。Collector 部署方式与采集配置不变。#20 这一步只验证镜像同步，未运行产品编译和 Kubernetes E2E；后续日志与 Trace 已由 [Buildkite #21](2026-09-17-observability-k8s.md) 验收，并继续在 [Buildkite #30](2026-09-18-runtime-environment-k8s.md) 的八组基础 K8s 中通过。
