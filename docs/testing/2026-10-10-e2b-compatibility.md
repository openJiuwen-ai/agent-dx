# 2026-10-10～12 E2B 兼容验收结论

本记录保存 E2B 基础链路的历史验收结论。实现和当前部署约束见 [E2B 兼容入口](../deployment/e2b-compatibility.md)；详细版本、环境和故障摘要可参考 [个人回顾](https://gitcode.com/mhsong2/personal-docs/blob/main/retrospectives/2026-10/2026-10-19-adx-e2b-lifecycle-validation.md)。

## 已验证范围

- 在 `cn-north-4/akernel-cce` 的隔离 runc guest 中，Python E2B SDK `2.52.0` 与 envd `0.9.0` 完成创建、联合就绪、命令、环境变量、文件、PTY、应用端口、metadata 分页、租户隔离和删除。
- 验证覆盖绝对期限、Connect 不缩短期限、流式命令跨原到期点续期，以及 envd 退出后的 Failed、路由撤销和清理。
- CCE Linux builder 的 Rust workspace 记录为 772 passed、0 failed、84 ignored；Clippy `-D warnings`、格式检查和 E2B backend/package Python suite 均通过。ignored 不计为通过。
- Coordinator 与 Redis 受监管重启后，既有期限、文件和命令路径恢复；Redis 重启期间曾出现一次 503，故不宣称零中断。

## 固定输入与限制

| 输入 | 已记录版本 |
| --- | --- |
| E2B SDK / envd | `2.52.0` / `0.9.0` |
| envd infra | `92197909dce5a1bef33e764ae4af76f0732fd7a8` |
| sandboxd | `efc201531d7e2e9d69505da151eb66084b61eebf` 与受信任 guest metadata 补丁 |
| 产品证据来源 | 原分支 `smh/e2b-compatibility` 的历史验证；本轮精简前记录位于 `e237878` |

- 该环境没有 `/dev/kvm`，因此不证明 Firecracker 内存 Pause/Resume、snapshot clone 或恢复前 MMDS 更新。
- sticky `/tmp` 中 UID 1000 文件覆盖仍会触发 Linux `protected_regular` 限制；`/home/user` 和非 sticky 子目录已验证可用。
- 本记录不是当前 `upstream/master` 集成结果的运行验收；合并后的组件与 KVM 验收必须重新执行。

## 使用此证据时

E2B worker 需要固定 envd 工件、受信任 runtime profile 和 E2B 专用 OCI 基线；原生 worker 不继承这些 guest 权限。SDK 经 HTTPS Ingress 访问，测试使用独立租户和隔离 runtime，不升级既有业务 deployment。

创建成功的判据是 Execd 运行、envd 初始化与版本/鉴权探测均完成并已发布路由。仅有 VM 或 Execd Running 不是 E2B Ready。envd 访问 token 和应用流量 token 独立，凭据不得由用户环境变量覆盖。

下次验收应使用当前构建的 Linux 工件重新覆盖基础 smoke；若声明 Firecracker 支持，还要验证暂停后的内存/进程连续性、恢复后路由归属、MMDS 写入失败不运行及 clone 的新凭据。

## 最小复验集

基础 smoke 应使用独立测试 deployment，并在清理前确认测试 sandbox 已删除。它至少应覆盖：

1. 创建后通过命令、文件和 PTY 证明数据面已就绪；
2. 使用第二租户验证管理面隔离，并验证 envd/私有应用 token 不能互换；
3. 续期与到期分别验证，避免把活动误判为期限自动延长；
4. 终止 envd 后确认路由撤销、Failed 和资源清理，而不是只检查客户端报错。
