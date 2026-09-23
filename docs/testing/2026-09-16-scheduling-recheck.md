# 当前管控面调度基线复测（2026-09-16）

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

使用当前工作树重新编译 ADX compare，与已保存的历史 FunctionSystem 基准二进制在同一个 Linux ARM64 容器内顺序交替执行。1 轮预热、7 轮正式测量、6 类场景全部通过，所有放置正确性、报告成功与最终实例排空断言通过。

旧侧是已保存的 `feature/distribute_env` 报告制品，未重建该分支最新 HEAD。负载与计时边界继承 [原比较说明](scheduling-baseline-comparison.md)，它是调度器/进程内队列微基准，不能替代真实平台或纯 Filter/Score 同口径内核测试。

## 七轮中位数

| 场景 | 旧侧 QPS | ADX QPS | 旧侧 P99 | ADX P99 |
| --- | ---: | ---: | ---: | ---: |
| 批量，开启复用 | 108,611 | 268,953 | 未测 | 3.391 ms |
| 单请求，无周期更新 | 11,071 | 30,440 | 0.107 ms | 0.044 ms |
| 单请求，目标500更新/秒 | 10,907 | 30,385 | 0.112 ms | 0.045 ms |
| 持续生命周期，开启复用 | 14,402 | 21,721 | 367.810 ms | 230.214 ms |
| 冲突重试 | 5,759 | 15,663 | 0.105 ms | 0.043 ms |

关闭复用的持续场景：旧侧 14,343 QPS，ADX 14,014 QPS（-2.3%）；此模式基本持平，不能概括为所有场景都有明显提升。开启复用的持续场景提升约51%，P99下降约37%。单请求场景旧测试只提供 no_aggregate，不能声称两侧均开启聚合。

更新平均耗时旧侧 36.530 µs；ADX 按 QPS 倒数为 23.247 µs。更新内容不改变可行节点，两侧报告协议不同；原始字段和完整命令一并保存。

## 制品和复现

- ADX 基础提交：`0dde79ad57583e998389101a763e4d2d825be63e`，包含未提交修改；逐文件 SHA256 记录于 provenance。
- ADX binary SHA256：`cf1bb62b7694ccca794437b9d4cd6daa8420c9320c79f188c33590b759bc0e92`。
- 历史 binary SHA256：`8d78ae746a6c05edb3455b87d7634872e36207829d48c5f7f2663ca092f3ac26`，与前次报告一致。
- Rust 1.95.0，Linux ARM64 release；容器 CPU affinity 0–3、6 GiB；构建结束后才开始顺序测量。
- 镜像：`yr-openeuler22-compile-localbuild:8.5.0-cc`（复用既有测试环境）。

证据目录 `out/ci/stage-5/`：`provenance.json`、`driver.log`、`rounds/summary.json`、`rounds/raw.json`（144条）、`rounds/commands.json`。本次未覆盖 GPU/NPU 真实卡、HTTP 亲和翻译、多 Domain 并发、长期公平性或完整资源报告服务链路，因此不据此宣布阶段5全部完成。
