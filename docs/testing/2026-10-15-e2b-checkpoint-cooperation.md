# E2B guest 检查点协作验收结论（2026-10-15）

本记录保留 guest freeze/unfreeze、Firecracker 恢复和共享检查点的验收边界。详细版本、物理 worker、故障定位与资源释放摘要可参考 [个人回顾](https://gitcode.com/mhsong2/personal-docs/blob/main/retrospectives/2026-10/2026-10-19-adx-e2b-lifecycle-validation.md)。

## 已验证范围

- 固定 envd `0.9.0` 的 `/freeze`、`/unfreeze` 和恢复 `/init` 与 Execd checkpoint 协作；prepare/abort、response 丢失后的清理和运行中 checkpoint 均有组件覆盖。
- 在两个隔离物理 KVM worker 上，SDK `2.52.0` 完成 Pause→Connect、跨节点恢复、snapshot 双 clone、凭据隔离、时钟恢复和组件重启后的数据路径检查。
- 后续定向验证确认两条不同路径：已持久 source-session fence 的 Deleting Reusable Snapshot 可由同 scope、完成对账的节点接管；而 Deleted outbox 中已捕获、且 backend 已停止的 artifacts 可委派清理，不再依赖 owner fence。恢复中引用继续保护对象。
- 已复现的 Relay CONNECT 504 路径归因于 snapshot clone 后的 TAP/FDB 漂移；修复后的定向 KVM 用例通过。该结论不外推所有历史 504。

## 证据边界

| 项目 | 结论 |
| --- | --- |
| 代码/组件检查 | 覆盖 Execd、adxlet、API Server 的聚焦测试和 Linux 构建；不替代真实 VM 验收 |
| KVM 运行验证 | 来自原分支的多个历史版本；最终 Deleted cleanup 七案例使用 `eec4bd1`，其报告为 `1bce7bc`；本轮精简前产品记录为 `e237878` |
| 删除接管 | 验证上述 fenced Snapshot 和 Deleted outbox 路径，不证明非 Deleted、未登记 foreign-owner 历史孤儿的自动删除 |
| Relay | 验证已复现的 FDB 漂移路径，不证明任意网络、VMM 或负载下的连接可用性 |

`full_stage3_accepted=false` 仍是当前边界：未覆盖运行中 VM 的硬断电恢复、所有故障组合、长期压力、全部 SDK/API 变体或主线整合后的 KVM 回归。不要将这份历史证据作为当前主线发布验收。

## 契约摘要

checkpoint prepare 是 guest 协作准备，不替代 sandboxd 对源执行、artifact 和恢复身份的权威管理。freeze 请求为 best-effort；abort 或已确认源继续运行时，只有 `/unfreeze` 成功才恢复 Running。未知 handoff 结果不授权重复 capture 或盲目解冻。

恢复沿既有 `/init` 重新建立 guest 服务和路由；失败时不得发布可用路由。Firecracker 的 host、initrd 与 checkpoint 必须来自同一固定 sandboxd revision、补丁集合和 provenance，旧快照不因此承诺升级兼容。

Deleting Reusable Snapshot 的接管要求 source-session 已持久 fence、无引用、scope 相同且候选节点完成对账。Deleted outbox 的 artifacts 已在 backend 停止后捕获，可按委派任务清理而无需再次 owner fence。local、旧无 scope、非 Deleted 或未登记 foreign-owner 对象继续保守保留；不能因本次接管验收而放宽通用 orphan GC。

下次主线 KVM 验收应至少重跑 Pause/Connect、普通与断连 snapshot、双 clone、owner-exit 删除接管和恢复中引用保护，并记录运行中硬断电与长期压力尚未覆盖的边界。
