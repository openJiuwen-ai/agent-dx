# 【已定方案】ADX 进程升级 Socket 路径修复

初版修复日期为 2026-11-14，完整 Pod UID 路径补测日期为 2026-11-19。修复前代码依据：`8fa64c1c`。

状态：已按用户选择的最小方案修改代码，本项本地 90 项测试、工作区格式和 Clippy 检查通过；2026-11-19 已在北京完成独立 Relay publisher 的三 Env 短请求与同一条 SSE 跨实际切换复测。提交、推送状态以 Git 记录为准。**Relay 缩短升级路径；adxlet 保留两个用途，启动 socket 改用固定短地址，管理地址不变。**

范围只限 socket 调整，沿用已有启动身份、注册接管、发布及回退机制。

## 1. adxlet：保留用途，只改启动 socket 路径

```text
管理地址：<state_dir>/<service_id>-admin.sock  # 保持原样
启动地址：<state_dir>/<service_id>-start.sock  # 替代原长路径
```

修复前启动地址为 `<state_dir>/rollout/<task_id>/bootstrap-<attempt>.sock`。现在仅改地址，仍由 adxctl 监听，新 adxlet 读取现有启动信息后按原顺序初始化；启动通道使用后按原方式清理。正向和回退使用同一短命名规则。

完整 task ID、attempt、node_session 继续保存在任务记录、回执及既有启动信息中，不靠 socket 名字区分身份。复用原任务独占、同 attempt 最多一个 Child 和 PID/UID 校验；固定地址遇到活动监听、普通文件或符号链接时拒绝占用，不覆盖。

准备阶段检查目标和原版的启动路径，实际绑定前复查。**不合并两个用途，不新增身份交付 RPC，不改变管理服务启动时机和进程交接流程。**

## 2. Relay：只缩短升级路径

独立 Relay 仍保留原有新旧进程交接与旧连接收尾流程，只替换过长的路径：

```text
Relay 新版：     <state_dir>/s/r-<relay_key>/route.sock
Relay 回退进程： <state_dir>/s/o-<relay_key>/route.sock
```

`relay_key` 为 24 个小写十六进制字符。将 `["upgrade-uds-v1","relay",完整task_id]` 编码为紧凑 UTF-8 JSON，计算 SHA-256，取前 12 字节编码。同一任务的路径稳定。使用北京现场的 `/work/supervisor-<36 字符 Pod UID>` 作为根目录时，新版和回退路径均为 93 字节。2026-11-19 复测中，旧的 32 字符摘要拼出的路径为 101 字节，在交权前被 100 字节上限拒绝。

任务和制品继续放在 `<state_dir>/rollout/<完整task_id>/...`，完整身份继续保存在记录和请求中。准备阶段同时检查 Relay 正向、回退路径的完整字节长度，超过现有 100 字节限制就报错，不能先停止旧进程。

复用现有私有目录、权限、归属及拒绝覆盖检查；仍在使用的 Relay socket 不随任务完成而删除。本项不增加映射表、配置或历史目录清理机制。

## 3. 修改与统计边界

- Rust 修改限于 `platform/deployment/src/rollout.rs`、`rollout/relay.rs`、`rollout/adxlet.rs`：替换路径拼接、复用长度检查、保护固定启动地址，以及对应聚焦测试。
- 不修改 supervisor、adxlet 本身、协议类型或 Coordinator；保留现有启动信息环境变量和传输方式，不再实施合并地址或新增 RPC 的路线。
- 代码量只统计 socket 的监听、连接、路径、清理及直接调用适配；相关测试单列。删除旧实现与新增实现分别统计，净行数以实际 diff 为准，不把已有升级功能算作本次新增。
- 保留任务记录、固定制品、进程归属、重复启动保护、原阶段期限和恢复核实；publisher/Coordinator 职责、PR178 Pod 升级及独立 Relay 交接流程沿用。

## 4. 聚焦验证

已按 AGENTS.md 先补失败用例再实现，覆盖：

1. Relay 正向及回退路径在 64 位 task ID 下可实际 bind/connect，完整任务身份保留。
2. 覆盖稳定派生、100/101 字节及多字节目录边界，超长路径在旧进程退出前失败；保留归属及拒绝覆盖检查。
3. adxlet 在 64 位 task ID、attempt 下，正向和回退仍沿同一固定短启动地址完成交付，完整身份和原期限不变；丢 Start/Complete 回复时不重复创建进程。
4. 固定启动地址拒绝覆盖活动监听、普通文件和符号链接；可处理已无监听的旧 socket 文件，管理地址不受启动清理影响。

已完成聚焦测试、格式和静态检查，同步使用说明并保留日志。本项证据见 `out/adx-socket-fix-20261114/RESULT.md`。2026-11-16 的 Node0 空 Pod 正常 verify 与 abort 收口属于独立验证，不作为本项进程 socket 或独立 Relay publisher 的专项集群验收。
