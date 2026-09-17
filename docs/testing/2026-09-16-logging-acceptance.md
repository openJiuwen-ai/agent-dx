# 组件日志滚动与压缩验收

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

功能提交 `3985f4b791f4e241e01e061f3174e1c895b2c12e`，分支 `ci/control-plane-k8s-20260916`。配置、保留范围及失败行为见[日志契约](log-rotation.md)。

## 行为与本地检查

Supervisor 接管托管进程的 stdout/stderr，按大小或时间关闭并滚动文件；后台将已关闭文件压缩为 gzip，并按组件限制历史数量、年龄和实际占用字节。正常重启和停止等待日志收尾，状态接口报告写入/压缩错误。

先用配置契约测试确认原实现拒绝 `logging` 字段，再实现功能。首轮16项macOS Rust测试通过，Clippy指出测试中无效的结构体默认展开；修正后macOS 16项、Linux 17项、Clippy及53项Python驱动测试通过。Linux额外检查 `/dev/full` 的 ENOSPC 返回。

覆盖 stdout/stderr 连续输出在滚动、压缩和重启后的逐字节一致性，空闲时间滚动，保留清理仅作用于所属组件，以及压缩发布失败后原文件保留与恢复。文件写入失败的字节计数为失败批次上界；没有执行整盘耗尽、磁盘I/O挂起或断电测试。

Linux ARM64全bins构建和统一打包通过。本地发布包标记 `dirty=true`，Rust程序重新构建，未改动的Go API/SDK/Redis沿用已验证包。`out/ci/stage-9/logging/source.json`核对22个提交文件与验收工作树一致。

本地真实runc双节点公共SDK七组全部通过：SDK、认证、容量、放置、节点失联、重启和停机；`cleanup_errors=[]`、`missing_checks=[]`。节点1/2分别40/6个gzip归档均可解压，8个服务的最终捕获状态无报告错误，`failed_bytes=0`，无压缩临时文件残留。Master和两节点在满载、排队、删除后的资源指标核对继续通过。

## 正式基础K8s

[Buildkite #18](https://buildkite.com/agent-dx/agent-dx/builds/18)的构建、镜像发布和独立K8s E2E均通过。正式包commit与功能提交一致，dirty=false，release manifest与bundle身份一致。FC关闭。

| 用例组 | 结果 | 秒 |
| --- | --- | ---: |
| sdk | PASS | 27.273 |
| auth | PASS | 21.149 |
| capacity | PASS | 33.805 |
| placement | PASS | 95.055 |
| node-failure | PASS | 53.859 |
| restart | PASS | 5.631 |
| stop | PASS | 21.216 |

JUnit含清理共8项，无失败、错误或跳过；`cleanup_errors=[]`、`missing_checks=[]`。运行namespace为`adx-e2e-c88a8cefff19`；两个Pod同处宿主机`10.244.128.160`，IP分别为`10.245.5.174`和`10.245.7.63`。这不代表跨宿主或FC验收。

K8s两节点分别40/6个gzip归档通过解压检查，8个服务最终日志状态均为`error=null`、`failed_bytes=0`，无压缩临时残留。三个时点`allocated`、`queued`、`released`的Master/Node指标核对继续通过。

镜像步骤的制品下载耗时较长但最终成功。构建通过后，本地首次读取release manifest失败；只读重试同一次构建成功，未因此重新触发CI。

Node镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:c1e2ef7bd593d05a775f689827cb3b4076911195a17b996ffb9d8ad883c07b04`。

RRT镜像：`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e@sha256:3c1966f17b4b8e9823d0f444e25ace0f321ccac1bed3d75f8af080e10586180b`。

sandboxd固定revision：`efc201531d7e2e9d69505da151eb66084b61eebf`。


## 证据位置

- `out/ci/stage-9/logging/red.log`：缺失配置契约的失败。
- 同目录`green-1.log`、`acceptance.log`：初次Clippy问题及修正后完整本地验收。
- 同目录`local/result.json`、`local/case-results.json`、`local/logging-node1.json`、`local/logging-node2.json`：用例、清理与最终日志状态。
- 同目录`watch-18.log`、`collect-18.log`、`collect-18-r2.log`：正式CI监控、首次读取失败及重试成功。
- `out/ci/stage-7/preflight/build-18-evidence/verified.json`及`acceptance/node*/logging-node*.json`：核对后的正式结果和两节点日志状态。
