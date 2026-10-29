# Agent 别名目标态与灰度升级方案

状态：已实现。本文描述当前数字版本、Alias 目标态、AgentBinding 双 Environment 引用和 Activator 后台收敛的实现；管理页面与 Platform 优雅退出不在已完成范围。接口和部署配置分别见 [Agent README](../../agent/README.md) 与 [Activator README](../../agent/activator/README.md)。

## 1 职责与目标态

Alias 保存最新的完整版本分布。管理端调整权重后，Activator 主动推进现有 Binding 的版本切换，不需要提交发布任务、执行命令或确认切换。当前管理页面尚未提供，配置通过 Ingress 的租户鉴权 API 读写。

| 层 | 已实现职责 |
| --- | --- |
| Ingress / Gateway | 公开模板、Alias 和 Binding API；鉴权、逻辑 Binding 身份选择、解析结果校验；HTTP/WS/SSH 及文件转发 |
| Agent / Activator | 模板发布、Alias 选版、服务协议/端口校验、Binding 持久化；候选准备、current 切换和旧 Env 清理 |
| Agent Store | Template/AliasConfig/AgentBinding 元数据、revision CAS、引用索引、恢复索引、注册目录和变更订阅 |
| Platform | Environment 创建、调度、运行与暂停恢复、Volume 准入、路由发布和实际删除 |

Activator 通过 Environment 能力接口访问生命周期：独立部署使用 API Server HTTP 接口，嵌入式部署调用 Environment 应用服务。用户业务流和文件正文使用共享 Gateway/Relay 数据面，不经过 Activator，也不读取 Platform 私有存储。

## 2 数字版本与统一引用

`POST /api/agent/v2/templates` 接收不含 version 的 TemplateDraft。服务端在 tenant/template 范围内，通过同一 CAS 事务推进持久化序列并保存不可变模板快照。每次成功发布分配一个新版本，包括重复内容的发布；结果未知时不自动重放发布。

版本采用规范的正 u64 十进制字符串，拒绝 0、前导零和溢出。外部引用中的纯数字只能解释为版本，其他合法引用解释为 Alias；Alias 名不能为纯数字。不提供隐式 latest、版本查询失败后的 Alias 回退或单独的 alias-binding 类型。

统一目标格式为：

```text
urn:adx:template:<template>:<reference>
urn:adx:binding:<template>:<reference>:<binding_id>
```

数字 reference 示例为 `1`，Alias 示例为 `production`。template 目标由入口生成新 Binding ID；binding 目标创建或复用指定逻辑实例。tenant 来自认证，不放入 URN。已有字符串版本、索引及序列的转换由独立运维流程完成，服务启动不扫描或自动迁移旧数据。

## 3 Alias 配置与稳定选择

AliasConfig 保存 tenant、template、alias、seed 和 versions；存储 revision 放在返回的 AliasSnapshot 外层。示例：

```json
{
  "tenant": "tenant-a",
  "template": "orbit",
  "alias": "production",
  "seed": "stable",
  "versions": [
    {"version": "1", "weight": 9},
    {"version": "2", "weight": 1}
  ]
}
```

所有版本必须已发布且不能重复。weight 为非负 u64，允许单项为 0；总和必须为正且不能溢出 u64，不要求等于 100 或 10000。各版本的目标比例为 `weight / sum(weights)`。

选择使用 tenant/template/alias/seed/binding_id 的稳定 SHA-256 值，按数字版本排序后映射到累计权重区间。revision 不参与选择；列表重排和权重同比缩放不改变分配。seed 在同一 Alias 生命周期内不可修改，避免意外重新分组。灰度单位是 Binding，不逐请求随机抽样，也不保证实际请求流量与权重精确相等。

公开管理接口为：

| 操作 | 接口 |
| --- | --- |
| 读取 Alias | `GET /api/agent/v2/templates/{template}/aliases/{alias}` |
| 创建或更新 Alias | `PUT /api/agent/v2/templates/{template}/aliases/{alias}` |

PUT 请求体只包含 seed、versions 和可选 expected_revision。Ingress 从认证确定 tenant，从 URL 确定 template/alias，不允许请求体覆盖身份。创建时 expected_revision 为空，更新必须传上次读取的 revision；并发冲突返回 409，需要重读后编辑。

两种 Activator 部署模式使用同一公开接口。独立 Activator 的内部 aliases/put 仅承担服务间 RPC，配置工具不需要直连该端点或 Redis。写入成功表示目标态已持久化，不表示全部 Binding 已完成切换。

## 4 持久化模型与身份

AgentBinding 代表长期存在的逻辑实例，使用同一模型承载数字版本和 Alias 引用。

| 字段 | 含义 |
| --- | --- |
| scope | tenant、template、typed template_ref 和 binding_id；template_ref 为 version 或 alias |
| generation | Binding 生命周期标识，版本升级期间保持不变；删除重建后变化 |
| current | 当前实际使用的 version 和 environment_id |
| secondary | 可选的另一个 Environment 引用，intent 为 preparing 或 retiring |
| phase | 产品状态 active 或 deleting，不复制 Platform 运行状态 |
| parameters | 首次创建保存的初始化参数，后续版本复用 |
| volumes | 本 Binding 生命周期固定的 Volume ID → UID 映射 |

current 与 secondary 保存在同一记录中并通过 CAS 更新，最多保留两个 Env 引用。AliasConfig 只有最新目标分布，不保存 target_version、执行进度或操作流水；执行进度由 Binding 自身承载，不引入 AliasBinding、BindingRouteState 或 Rollout 对象。

首次 current 的 EnvID 沿用 `adx-<generation>` 生成方式，候选使用新保存的 `adx-<UUID>`。激活、切换、恢复与删除均使用持久化 Env 引用，不根据 binding_id 反推 EnvID 或所有权，避免新增对 ID 算法的依赖。

## 5 访问解析与文件操作

HTTP 使用 `/agent/http`，WS 使用 `/agent/ws`；SSH 使用统一目标 URN。Ingress 选择逻辑 scope 后，将协议、可选端口和初始化输入交给 Activator resolve。

- 成功激活缓存命中时，Activator 校验请求服务并返回缓存目标。
- 冷查询发现已有 Binding 时，使用 current 的真实版本，不在每次请求重新按 Alias 权重选择。
- Binding 不存在时，Activator 才根据数字版本或 Alias 目标态选择实际版本，校验服务、参数与规格，CAS 保存身份后调用 Environment 能力。
- Environment 操作前确保所需 Volume、固定 UID；创建或重试沿用已保存 EnvID。迟到结果必须核对 generation、phase 和对应引用，不能清理另一个生命周期的 Env。

协议与端口唯一匹配校验在 Activator 内完成，无效服务不会触发首次 Binding 创建。普通数字版本与 Alias 的全热解析均为一次 Gateway→Activator RPC，Activator 不访问 Agent Redis 或 Environment API。Gateway 不缓存 Binding/Target，Activator 扩展已有成功激活缓存，默认 200000 条、5 小时滑动 TTL。

Orbit/Jiuwen 业务 WS 入口另有初始化前查询：先检查 Binding，只有缺失时才生成模型与 workspace 参数，再调用 resolve。该业务适配成功路径在独立模式下通常包含一次 Binding 查询和一次 resolve，不属于上述单次通用热解析。

文件上传下载使用 Activator 的只读 binding_view，一次取得 Binding 及其 current 版本模板，不根据 Alias 最新权重另选模板，不创建 Binding 或 Environment。Ingress 按这一组结果选择 EnvID、工作目录和下载配置，再完成业务会话、下载 token 与文件范围校验；正文经共享文件能力和 Relay/Execd 传输。已有暂停 Env 的唤醒由共享数据面请求 Platform 完成。

当前 `/file-api/upload`、`/file-api/download` 从 DigitalMateGateway 的 jiuwen 配置生成 scope；`/orbit/ws` 使用独立 orbit 配置，文件接口不会因此自动切换模板。

## 6 升级、回退与清理

| 阶段 | current | secondary | 行为 |
| --- | --- | --- | --- |
| 稳定 | V1 / E1 | 无 | 访问使用 E1 |
| 准备 | V1 / E1 | V2 / E2，preparing | 先保存候选，再准备 Volume 和 Env；访问仍使用 E1 |
| 已切换 | V2 / E2 | V1 / E1，retiring | 新解析使用 E2，后台清理 E1 |
| 收敛 | V2 / E2 | 无 | E1 删除和卷关联释放确认后清空 secondary |

提升候选要求 Platform 观察为 ready Running，或已初始化的 Paused。切换 CAS 同时检查 Binding generation/Active/current/secondary 和最新 Alias revision，防止提升过时目标；切换后失效本地激活缓存，Binding 通知使其他副本失效缓存。

目标连续变化时，先退役不再需要的 secondary，再根据最新目标准备下一候选，不分配第三个槽位。回退通过更新 Alias 权重表达，使用相同状态转换；已标记 retiring 的旧 Env 不会重新提升，需要时准备新的 Env。generation 和已保存初始化参数不随版本切换而变化。

新 Env 删除或清理只有在 Platform 确认 Deleted 或明确 NotFound 后，才释放其卷关联并清空相应引用。超时、断连或结果未知保留引用，后续沿用同一身份重试。该过程不删除仍被引用的 current，也不因普通进度变化将有效 Env 当作迟到结果清理。

## 7 事件、成员与恢复

独立和嵌入式模式配置 registration 后，自动启动成员注册/续租、Redis 订阅及后台收敛；没有单独的 reconciliation 部署开关。省略 registration 时只按请求处理，不具备后台主动升级与成员接管。默认后台并发 8，单次工作期限 60 秒，成员刷新间隔 5 秒，均为内部默认值。

Redis 使用 Keyspace Notifications，部署需开启 `notify-keyspace-events 'K$g'`，或包含所需事件的更大集合。Activator 检查配置但不自动修改 Redis；使用独立订阅连接，先确认订阅再恢复扫描。通知只负责唤醒，最新持久化状态始终是权威来源，不依赖消息逐条可靠送达。

| 触发 | 读取与处理范围 |
| --- | --- |
| Alias 变更 | 分页读取该 tenant/template/alias 的 Binding 引用索引，再按 owner 分工 |
| Binding 变更 | 失效该 Binding 的本地缓存，仅将该对象交给 owner 入队 |
| 启动、订阅重连 | 全量恢复扫描持久化 Binding |
| 成员列表变化、租约过期、缩容 | 重新分配 owner，失效缓存并进行恢复扫描 |

没有周期全量 Binding 扫描、每 Binding 工作租约或全局主节点。成员列表定期刷新，正在处理的对象按结果短暂退避继续推进。订阅恢复期间 readiness 返回 503，Alias 热缓存不作为最新状态使用。

owner 按完整逻辑 scope 和共同成员集合做 Rendezvous Hash 分配，用于减少多个副本重复触发操作；正确性仍依赖 Redis CAS 和 Platform 幂等。独立模式请求也按 scope 做亲和路由；嵌入式请求直接调用当前进程，后台 owner 分工不要求请求转发给其他嵌入式实例。没有内部 HTTP 接口的嵌入式成员不能混入远程 ActivatorClient 的请求发现目录。

## 8 异常与删除意图

| 场景 | 已实现处理 |
| --- | --- |
| 目标版本模板缺失，尚无 secondary | 返回错误，不分配候选 EnvID；后台记录失败并重试 |
| Preparing 候选的模板缺失 | 记录 scope、版本和 EnvID 的错误；CAS 标记 retiring，清理候选 Env/卷关联，确认后清空 secondary，保留 current |
| 候选清理失败或结果未知 | 保留 retiring 引用继续重试，不把模板 NotFound 当作已完成 |
| 候选尚未就绪 | 保留 preparing，后续继续核对 |
| 候选 Failed/Deleted、并发 CAS 冲突 | 返回错误并记录，重试或根据最新目标继续收敛，不强制提升 |
| 用户删除 Binding | 先保存 deleting，清理 current 和 secondary；确认清理后条件删除元数据与索引 |
| 前台删除失败或结果未知 | 已持久化 deleting 时，owner 接续后台删除；未保存意图时不能认为删除已提交 |
| 暂时故障、冲突、未知结果 | 默认约 5 秒后重试；尚需下一步时约 1 秒继续 |
| Invalid/Unsupported | 记录阻塞警告，停止主动重试，等待相关变更或恢复扫描 |

未配置 registration 时，失败删除仍依赖请求方重试。可通过 Binding GET/list 查看 phase、current、secondary，并结合 `Binding convergence waiting or blocked` 日志观察阻塞原因。

发送前目标失效沿用共享数据面的 bypass 重试。HTTP/SSH 固定首次选定的 generation；WS 只在未发送后端握手和业务字节，且权威确认 Binding 删除或换代时，允许在原期限内刷新一次。已发送的业务数据不重放，已有连接不会因为元数据切换而被重新绑定到另一个 Env。

## 9 Volume 与能力边界

受管 Volume 的定义、准入和实际使用由 Platform 负责，见 [Volume API](../../platform/api/http/volumes.md)。Agent 在创建前确保卷定义和 Env 关联，固定返回的 UID；升级先建立新 Env 关联，旧 Env 删除确认后释放旧关联。清除 Binding 或卷定义不等于删除底层工作区数据。

Orbit 模板已声明受管共享工作区 Volume；首次创建保存的 workspace 参数在升级中复用。固定版本转 Alias 时，保留既有 Binding、scope 索引、generation、Env 引用、参数和数据的调整属于独立运维流程，代码不自动迁移。其他 Binding/agent_type 共用卷时也须遵守 Platform 准入，Agent 不实现跨 Binding 卷调度。

Platform 优雅退出尚未实现：Activator 切换 current 后直接调用既有 delete，不统计连接、不等待排空；当前实际删除直接清理运行时，旧连接可能中断。独享卷也可能等待旧执行释放，不承诺强制抢占或零空窗升级。Agent 不增加 Environment 健康恢复、checkpoint 自动回收或跨宿主机数据迁移逻辑。

数字版本、Alias、双引用收敛、Ingress 接口和缓存边界已通过组件、临时真实 Redis 及指定镜像内构建验证。这些证据不等同于当前数字协议的真实 Orbit/SFS、跨宿主机、集群或连接排空验收；管理页面和这些运行场景的验收仍在已完成范围之外。
