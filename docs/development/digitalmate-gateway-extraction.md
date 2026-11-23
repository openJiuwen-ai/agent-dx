# DigitalMate Gateway 独立部署执行计划

## 交付范围

DigitalMate 仓负责独立服务、镜像及业务接口。只保留 digitalmate 模板相关入口，以及账号、配置中心、Composio、邮件摄取与已启用的推送模块。删除原 Gateway 的 JiuwenSwarm 协议转换、E2A、附件 HTTP 接口和 Orbit 适配。DigitalMate 的附件上传、读取及产物读取继续通过业务 WebSocket。迁移须保留已有 tenant、用户身份、Binding ID 和 workspace 的派生规则、业务数据库与凭据。

北京四 beijing-test 的 adx-jiuwen-test 为本次验收环境。本文记录目标行为和验收进度，实施完成前应以源码和部署证据判断实际可用能力。

## 北京 test 的模板基准

本次部署目标是北京四测试集群，命名空间为 `adx-jiuwen-test`。香港 test0 只用于读取模板基准。2026-11-23 从 test0 Agent v2 读取到 `digitalmate:production` 以 100% 权重指向数字版本 **64**。镜像 digest 为 `sha256:ee9c7179e4a264e25da6d8b1b00d4ee59347ffbd026253ddfee8639b3987c2ba`，北京 SWR 中已核对到相同的 linux/amd64 manifest。模板通过 `DIGITALMATE_MODEL_CONFIG_NAME=DIGITALMATE_MODEL_CONFIG` 读取模型配置。切换前，北京 test 的共享别名指向版本 17。独立验证和共享发布均沿用 test0 的应用镜像、服务端口、就绪检查与生命周期，按北京 test 替换租户、工作区根目录、服务地址、CA 和网络规则。业务密钥由各环境单独配置。

## AgentDX 接口契约

| 接口 | 运行时调用者及用途 |
| --- | --- |
| /agent/ws 加业务路径 | 前台会话、附件与产物。首次握手触发建绑、启动或唤醒。 |
| /agent/http 加业务路径 | 连接邮箱、查询批次回执、提交邮件。首次请求可以建绑。 |

Gateway 由可信会话或有效订阅确定模板、端口、Binding ID 和 workspace。保留模板目标 URN、X-ADX-Binding-Id 和首次初始化的 X-ADX-Parameters。所有 Agent v2 管理及 HTTP/WS 接口只接受 X-ADX-AUTH 进行平台鉴权。平台凭据缺失、无效、重复均拒绝。转发时移除平台头，沙箱业务 Authorization 原样传递。其他产品 API 的认证独立。配置和平台凭据只由服务端持有。

删除公开的版本和 Alias 两种 Binding resolve 路径、文档、测试及脚本调用。保留流量触发的内部 Activator 解析。盘点 CLI、模板发布及运维实际调用方后精简管理接口。新增接口须有具体业务调用方并重新评审。DigitalMate 运行时不查 Binding、模板或 Environment，不使用跨别名复用，不新增文件 API。

## 实施顺序与验收

1. **AgentDX 协议。** 先写鉴权与转发契约测试，修改所有入口及客户端，删除公开 resolve。把原先靠 resolve 触发的集成断言迁到真实流量入口，合并重复测试。分别验证 HTTP 和 WS 后端能收到业务头且收不到平台头，首次建绑、就绪等待、断开和已发送请求的处理符合原转发契约。
2. **DigitalMate 服务。** 在 server/digitalmate-gateway 建独立服务，迁入账号、隐私 HTTPS 服务、Composio、邮件 worker 与华为 Push、配置工具及对应测试。实现 HTTPS/WSS Agent v2 客户端、会话撤销、健康检查与后台任务退出。通过业务接口调用上游，清除对共享 Ingress、Activator、Execd 的进程内依赖。
3. **业务授权。** 登录验证后按租户、用户和 workspace 签发短期凭据。配置读取及续期核对业务授权。删除前台连接器、授权签发、验证、续期和后台投递中所有 Binding/generation 校验。保留业务操作权限、邮箱连接归属、用户同意、订阅状态和批次摘要核对。存量订阅 authority_ref 需继续有效。模型 Key 由沙箱读取配置中心。配置凭据 5 分钟有效、最长 30 天恢复。邮件每批 60 秒短期凭据。提交结果未知时先查同一批次回执。
4. **部署。** 按下节计划在 DigitalMate 仓维护独立 Helm chart、各环境配置、Deployment、Service、Secret 引用以及业务与隐私入口。新 Gateway 只持业务依赖及平台租户凭据。核对北京 test 已部署的镜像、账户库、数据库读写兼容性、路由与 worker 归属；先验证新实例，再切业务流量并交接 worker，记录可回切的镜像及 Helm revision。保留既有工作区、绑定及共享存储。
5. **北京端到端。** 用模拟身份提供方验收登录，保留真实账号服务、PostgreSQL、配置中心、AgentDX 和沙箱。验证首次 WS、原 workspace 重连、休眠唤醒、退出撤销和附件上传读取，核对模型配置读取与续期。在启用推送的环境验收推送。按本次范围，北京 test 暂缓真实邮箱及离线收件验收。记录镜像版本、日期、真实参与方、测试身份或替代服务、通过及失败项目。核验独立 Gateway 对 AgentDX 的运行时请求仅走上述两个入口。
6. **清理及示例。** 从 AgentDX 删除原内嵌业务代码、专用依赖、路由、测试、旧配置和部署资料。保留通用平台能力。新增可运行的最小业务网关 example 与 quickstart，并从 README 链接。示例使用演示身份和真实 HTTP/WS 协议，涵盖模板准备、稳定 Binding ID、首次流量建绑、两种请求头、后端观察值和预期响应。按 quickstart 从干净目录运行，执行受影响测试、工作区 Rust 检查、文档检查和 git diff --check。

## DigitalMate 仓的 Helm 部署包（待实施）

- 部署包放在 DigitalMate 仓的 `charts/digitalmate-gateway/`，提供 `Chart.yaml`、默认 `values.yaml` 和 `templates/`。目录布局、资源命名及 Secret 引用方式参考 `agent-dx-iac`。部署 Gateway 时直接使用 DigitalMate 仓的 chart。
- 每个环境提供 `environments/<环境>/value.yaml`，沿用参考仓的单数文件名。当前环境清单为北京测试 `test`、香港测试 `hk-test-0` 至 `hk-test-9` 及生产 `prod`。共用默认值写入 chart；环境文件记录镜像仓库、租户、模板引用、AgentDX 地址、入口、业务模块开关和已有 Secret 的名称等差异。
- 部署阶段用每份环境配置执行 `helm lint` 和 `helm template`，核对业务路由、TLS、Secret 引用和 worker 启用条件。实际安装与端到端验收范围为北京测试环境。此次补充只登记计划，Helm 实现延后。

## 进度与交接

- AgentDX 已修改 Agent v2 的平台鉴权头、CLI 和公开 resolve 路由，添加业务网关示例。鉴权回归已在旧代码复现并在修改后通过；Agent API、CLI 和 embedded_stack 的本地检查已通过。AgentDX Gateway 库测试 403 项通过、36 项忽略，工作区格式检查及 Clippy 通过。
- DigitalMate 独立服务 release 已构建。Rust 最终本地测试 151 项通过、26 项忽略（含单独在北京执行的验收用例），另在临时 PostgreSQL 16 执行其中三项授权与投递测试，全部通过。Python 邮件身份测试 30 项通过。


### 北京 test 独立实例验收记录（2026-11-23，共享切换前）

- 验证使用独立 Deployment、Service、PostgreSQL 和 Activator 状态。北京共享入口与共享模板继续服务原有测试。香港 test0 只读。北京测试沙箱使用 test0 版本 64 的应用镜像；北京独立 Activator 发布的数字版本为 3，同一镜像 digest。版本 1 和 2 在验证中分别发现遗漏环境变量和临时 CA 证书用途问题。
- 登录仅在测试进程内替换华为身份提供方。账号服务按正式登录流程签发会话并登记模型配置，随后通过同一 Pod 的独立 Gateway 进入真实 AgentDX 和 DigitalMate 沙箱。数据库和凭据根密钥为本次验证专用。
- 已通过会话查询、业务 WebSocket、房间访问、附件上传和原字节读回、断开后重连并读取同一附件、退出后会话返回 401 和活动 WS 断开。新模拟用户首个 WS 在北京候选版本 3 创建 Binding 和沙箱，配置中心记录到沙箱请求 get 和 renew 均为 200。另一个测试用户的沙箱经确认暂停后，以业务 WS 唤醒，同一 Environment 回到 running。北京 test 的 Huawei Push 未启用，实际收件按本次范围暂缓。该阶段使用验收账户库。共享入口的后续切换见下节。
- 已从 AgentDX 工作树删除内嵌 Gateway、旧业务路由、JiuwenSwarm 适配和相关用例。清理后的 Gateway 全特性测试 224 项通过、6 项忽略，workspace Clippy 与格式检查通过。修复受管 HTTP/2 转发重写路径丢失 Host 的问题后，北京使用 adx CLI 发布示例模板，以示例请求实际沙箱：HTTP 返回 200 和预期文本，WS 返回 101 且原文回显。沙箱端同时校验了业务 Authorization 和平台头的移除。

| 北京 test 项目 | 观测结果 |
| --- | --- |
| Agent v2 平台鉴权 | 同一模板查询，仅带 `X-ADX-AUTH` 返回 200，仅带 `Authorization` 返回 401。 |
| 真实沙箱访问配置中心 | 私有 HTTPS 服务的操作审计记录了 `get` 和 `renew` 返回 200，握手完成后收到了房间与附件的业务响应。 |
| 登录及会话撤销 | 华为身份提供方使用进程内 mock；实际账号数据库、模型配置注册与会话校验均参与。退出后查询返回 401，活动 WS 断开。 |
| 独立业务网关示例 | 用 CLI 发布 gateway-echo 版本 2，首次 HTTP 流量访问成功；WS 消息 hello 原样回显。 |
| 运行版本 | 独立 DigitalMate Gateway 镜像 `sha256:8d23a12e1f79dfdbfe34775d81e1c4e116d7ef013347bd5f6b0e4980f1b77049`；独立 AgentDX Ingress 镜像 `sha256:2374614a250df79830ac0497b7fb7cae8aa530fe09ae96ee386d251d6419497a`。 |
| 本轮范围 | 北京 test 的独立服务。真实邮箱收件暂缓，华为 Push 未启用。正式 Helm chart 及各环境 value.yaml 按要求登记在计划中。 |

AgentDX 工作树的 `out/gateway-extraction/` 保存脱敏验证记录，主要文件为 `beijing-final-pair.log`、`beijing-wake.log`、`beijing-privacy.log`、`beijing-final-auth.log` 和 `beijing-final-example.log`。测试用户和 SFS 工作区均为验收新建。“重连原工作区”是在同一测试用户下读取刚写入的附件；此次未迁移北京共享库中的既有用户。

### 北京 test 共享入口切换（2026-11-23）

CCE Ingress `adx-jiuwen-test` 的 `/api/v1/auth` 与 `/digitalmate/ws` 已指向 `digitalmate-gateway:80`。旧 `/ws`、`/file-api`、`/orbit/ws` 路由已移除，`/orbit-profiling/ws` 继续由独立 profiling 服务处理。共享 Ingress 已移除内嵌业务配置、业务专用挂载和原 `jiuwen-gateway` Service。

独立 Gateway 通过 `https://ingress:8444` 访问 AgentV2。平台原监听端口 8443 继续承担 Environment 流量，原监控入口继续返回指标。8444 使用平台 TLS 证书和共享 Activator 状态，模板管理请求仅带 `X-ADX-AUTH` 返回 200，仅带 `Authorization` 返回 401。原 8443 上同一模板管理请求返回 404。北京共享 `digitalmate:production` 已从版本 17 切到版本 **18**，使用 test0 版本 64 的同一应用镜像。沙箱通过独立 Gateway 的私有 HTTPS 端口 18444 访问配置中心。

| 验证范围 | 结果 |
| --- | --- |
| 公网业务链路 | 校验 ELB 证书，使用模拟华为身份登录。会话查询通过，首个 WS 在共享 Activator 创建绑定并启动版本 18 的沙箱。附件上传和逐字节读回通过；重连可读同一房间和附件；退出后会话返回 401，已打开的 WS 关闭。沙箱请求配置中心 `get` 和 `renew` 均返回 200。 |
| 配置与账号库 | mock 验收期间，独立 Gateway 临时使用验收账户库和验收密钥。随后恢复北京共享 `postgres/adx_accounts` 和原账户加密密钥。在 PostgreSQL 核实独立 Gateway Pod 以 `adx_accounts_runtime` 连接，并核实表权限。恢复后，公网会话查询、WS 均拒绝无效会话并返回 401。未使用共享库已有用户做登录复验。 |
| 流量唤醒 | 暂停本轮创建的沙箱后，通过共享 AgentV2 发送业务 HTTP 请求，返回 200。同一个 Environment 恢复 running，验收结束后已再次暂停。 |
| 后台模块 | 北京原共享配置没有启用邮件 worker 和华为 Push。真实邮箱收件按约定暂缓。 |

共享发布使用 Helm revision `digitalmate-gateway/6`、`digitalmate-privacy-egress/1`、`adx-ingress/48` 和 `adx-activator/19`。原平台 revision 分别为 `adx-ingress/45` 和 `adx-activator/18`。本次使用北京已安装的 Helm chart 配合 post-renderer 更新共享组件。再次升级时应带上这次的渲染改动，直至部署仓收录。正式的多环境 chart 仍按原计划延后。

北京测试环境的发布记录保存了切换前路由、别名 revision、Helm 发布记录及渲染脚本。回切时先核对这些资源的最新 revision；通过仍可用的 AgentV2 监听端口用最新 revision 将别名改回原版本，恢复旧组件并确认就绪后，再恢复原业务路由。共享数据库、共享工作区及历史 Binding 均保留。

原独立验证使用的五个临时 Helm release 和三个临时 Secret 已清理，验收专用数据库已在受保护目录备份。共享服务和存储继续运行。

本地 `out/gateway-extraction/` 目录中的 `beijing-shared-public.log`、`beijing-shared-contract.log`、`beijing-shared-final.log` 和 `beijing-shared-wake.log` 分别记录公网验收、平台鉴权、最终账户库连接和沙箱唤醒结果。
