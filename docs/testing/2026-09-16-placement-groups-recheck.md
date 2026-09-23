# 放置条件组接线后的调度复测（2026-09-16）

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

新增HTTP/SDK实例标签、实例亲和条件组和偏好评分后，重新构建当前ADX compare。与保存的历史 `feature/distribute_env` 二进制在同一Linux ARM64容器内交替执行：1轮预热、7轮测量、6类场景、18个配置组全部断言通过。FC验收停止后才开始本次构建和顺序测量。

## 七轮中位数

| 场景 | 历史 QPS | ADX QPS | 历史 P99 | ADX P99 |
| --- | ---: | ---: | ---: | ---: |
| 批量，开启复用 | 104,417 | 266,179 | 未测 | 3.407 ms |
| 单请求，无周期更新 | 10,356 | 29,988 | 0.116 ms | 0.047 ms |
| 单请求，目标500更新/秒 | 10,367 | 29,322 | 0.121 ms | 0.051 ms |
| 持续生命周期，开启复用 | 14,384 | 19,169 | 370.581 ms | 261.002 ms |
| 冲突重试 | 5,283 | 14,442 | 0.117 ms | 0.050 ms |

开启复用的持续生命周期QPS提高33.3%，P99下降29.6%；关闭复用时历史14,176 QPS、ADX13,071 QPS，ADX低7.8%。不能概括为所有路径都更快。单请求场景历史侧仅有no_aggregate结果。

更新平均耗时历史42.491 µs；ADX按QPS倒数为23.641 µs。更新内容不改变候选可行性，两侧报告协议不同。批量历史P99没有采集，不能把其原始0字段当成零延迟。

相对[前一次ADX测量](2026-09-16-scheduling-recheck.md)，持续场景由21,721降至19,169 QPS，批量由268,953变为266,179 QPS；这是两次测量的观察值，未在同一轮交替运行修改前后的ADX二进制，不据此把差异直接归因于新增插件。

## 边界与身份

这是普通调度路径和进程内报告闭环微基准，新放置条件通过另外的行为测试验证。本次负载没有真实GPU/NPU、多Domain并发、混合亲和负载或长时间公平性；不等于完整服务链性能，也没有重建历史分支最新HEAD或完成纯Filter/Score的同口径配对比较。

- ADX基础提交 `0dde79ad57583e998389101a763e4d2d825be63e`，包含未提交修改；provenance保存相关逐文件SHA256。
- ADX binary SHA256 `8990de3cb5024d98c4fab594f0a360227e50d5c5a76acf005b3bc51d83212368`。
- 历史 binary SHA256 `8d78ae746a6c05edb3455b87d7634872e36207829d48c5f7f2663ca092f3ac26`。
- Rust 1.95.0，Linux ARM64 release，CPU affinity 0–3、6 GiB；既有镜像 `yr-openeuler22-compile-localbuild:8.5.0-cc`。
- 证据根目录 `out/ci/stage-5/groups/performance/`：`provenance.json`、`rounds/summary.json`、`rounds/raw.json`（含预热共144条）、`rounds/commands.json`（128条进程调用）。完整日志为上级 `performance.log`。
- 执行脚本 `out/ci/stage-5/groups/performance/run.sh`，计时边界见[原比较说明](scheduling-baseline-comparison.md)。

真实FC新放置用例通过，但整套运行在后续双克隆网络操作失败，见[网络取证](2026-09-16-fc-clone-network.md)。阶段5保持进行中。
