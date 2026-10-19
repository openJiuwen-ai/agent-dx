# E2B API Key 与网络适配验收结论（2026-10-15）

本记录保留 API Key 和 IPv4/CIDR 出站策略的验收范围。详细版本、fixture 和排障摘要可参考 [个人回顾](https://gitcode.com/mhsong2/personal-docs/blob/main/retrospectives/2026-10/2026-10-19-adx-e2b-lifecycle-validation.md)。

## 已验证范围

- API Key 覆盖创建、列表、撤销、tenant 隔离、一次性原文返回、缓存绕过和 Redis 持久化；原文不写入存储。
- 网络覆盖 DTO/转换、创建时策略、Running 状态动态替换、revision 冲突、空更新恢复默认允许及不支持能力拒绝。
- 固定 sandboxd 的 iptables conformance 为 30/30；隔离 CCE runsc/systrap guest 的 E2B REST→ADX→sandboxd 实测为 13 项，覆盖 allow/deny、替换、默认拒绝和两个沙箱隔离。

## 版本与边界

| 输入 | 已记录版本 |
| --- | --- |
| E2B SDK / envd | `2.52.0` / `0.9.0` |
| sandboxd | `efc201531d7e2e9d69505da151eb66084b61eebf` 加受信任 guest metadata 补丁 |
| 运行时 | runsc systrap；runc 非空 ACL 与动态更新明确拒绝 |
| 产品证据来源 | 原分支历史验证；本轮精简前记录位于 `e237878` |

- 组件/HTTP mock 和 runsc ACL 证据不证明 Firecracker 恢复、跨节点部署、已有 TCP 连接的策略切换或主线整合后的运行结果。
- IPv6、域名/通配符和 egress proxy 规则明确拒绝；本记录不声明 IAM、卷或上层 Code Interpreter 能力支持。

## 契约摘要

API Key 的普通管理请求使用 ADX 管理员凭据并限制在所属 tenant；创建响应中的 key 只返回一次，存储只保存 digest、metadata 和撤销 tombstone。撤销后的下一次 E2B 管理请求实时校验；已建立连接和独立 envd/应用 token 不因平台 key 撤销而被广播断开。

网络策略只处理 IPv4 地址或 CIDR。allow 优先级高于 deny，默认允许，`allow_internet_access:false` 改为默认拒绝。`PUT /sandboxes/{id}/network` 是对出站策略的替换，空对象恢复默认允许；仅 Running 沙箱可更新，revision 竞争返回 409。

真实节点验收必须启用 sandboxd ACL，并在专用 worker 验证规则实际阻断报文。runc 的非空策略和动态更新仍拒绝；不要把 runc 基础连通性或 API 204 当作 ACL 生效证明。
