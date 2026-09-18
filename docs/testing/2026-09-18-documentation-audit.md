# 文档与当前实现核对（2026-09-18）

源码基线为 `363e44f1b28f1a792a4b9935c5bd27034fba9926`。本次按受版本管理的源码、配置、
构建脚本和 [Buildkite #30](2026-09-18-runtime-environment-k8s.md) 产物核对当前说明；历史报告
保留当时提交、成功和失败事实，并在会影响当前判断的位置补充后续结果。

## 核对结论

- 产品同时支持本地 EROFS 与不可变 OCI image 运行环境。Kubernetes 基础流水线使用 OCI，
  standalone／本地进程路径继续覆盖 EROFS。
- 自定义用户镜像的 OCI bootstrap 使用 `ro+rbind` 挂载到 `/__adx`；EROFS 仍按
  sandboxd 的 `erofs+ro` 契约执行。
- 当前基础 K8s 门禁为八组：sdk、auth、capacity、placement、local-first、node-failure、
  restart、stop。#30 三阶段、三种 OCI 运行环境模式和清理全部通过。
- 阶段11的正式 K8s 门禁已完成；阶段12只剩 Firecracker 新运行环境入口与快照恢复复验。
- 两个 #30 测试 Pod 位于同一物理宿主，因此没有把该结果表述为跨宿主故障隔离证明。
- FC 双克隆网络、GPU/NPU 实卡和真实服务混合长稳仍为当前未完成项；K8s FC、模板预热、
  证书热重载和统一实时 Trace 队列丢弃指标保持既定后置状态。

## 本次修订

- 当前架构 SVG 沿用最初版式，仅将 Go Sandbox API／Domain 命名更新为 Rust API Server／Shard；本地优先创建和 EROFS／OCI 等实现细节保留在目录正文。
- 根中英文 README、Buildkite 和 Kubernetes E2E 指南更新到 #30、八组及 OCI worker 前置条件。
- 运行环境部署说明补齐 EROFS/OCI 双路径和 OCI 递归只读 bind 语义。
- 新增 #30 的提交、制品 digest、三种运行环境、八组用例、清理和部署边界记录。
- 本地优先创建、运行环境、Collector 历史报告补充后续正式验收，避免把当时的待办读成当前状态。
- 路线图、当前实现、CI 契约和进度页数据同步：阶段11改为完成，阶段12仅保留 FC 复验。

迁移文档中的旧 Go Frontend、Domain 命名和早期“尚未实现”属于带日期的导入事实，没有改写为
当前实现说明；当前模块名称和能力以 [实施总览](control-plane-implementation.md)、
[路线图](control-plane-roadmap.md) 与 [剩余事项](control-plane-remaining.json) 为准。

## 检查结果

```bash
python3 build/docs/check.py --output out/docs-audit/2026-09-18/check.json
python3 build/docs/render_architecture.py --check
python3 -m json.tool docs/testing/control-plane-remaining.json
git diff --check
```

检查覆盖 89 份 Markdown／HTML／SVG 文档、453 个本地链接或 Markdown 锚点、22 个 JSON
示例和 1 个 SVG，0 错误。架构图生成一致性、剩余事项 JSON 解析和差异空白检查均通过。
另检索“正式 K8s 待执行”“最新 #24”“七组共享”“K8s worker 必须 EROFS”等容易漂移的
当前表述，没有发现与 #30 冲突的说明。

检查输出保存在本地 `out/docs-audit/2026-09-18/`，不进入 Git。文档检查不重新执行产品测试；
端到端结论直接引用 #30 的已下载 `result.json`、JUnit、placement、运行环境结果及三阶段汇总。
