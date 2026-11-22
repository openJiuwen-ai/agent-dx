# AFS 版本兼容设计与验收规则（评审稿）

本稿用于集中评审。对影响兼容性的变更，建议分别说明三件事。**哪些版本能互通、哪些现有状态能读写、退回时哪些已确认写入会保留。** 兼容结论绑定角色、协议或格式版本、功能启用状态及制品 SHA。MR 评审和发布说明使用同一口径。对应场景在 [AFS 测试用例与编写 Prompt](afs-case-prompts.md) 按真实用例 ID 登记。

## 一手资料核对

| 项目 | 已核实的实现与测试 | 对 AFS 的启发 |
| --- | --- | --- |
| JuiceFS CE | 自建两份客户端二进制，在 Redis、MySQL、TiKV 等元数据后端和 MinIO 上运行 Hypothesis 状态机，覆盖不同版本执行格式化、配置、挂载、读写、导出和导入。用例有按版本过滤的前置条件。Volume 格式校验区分元数据版本与客户端最低/最高版本。官方同挂载点平滑升级要求两侧 ≥ v1.2。 | 固定真实制品，先做确定性双向矩阵，再扩随机操作序列和后端。工作流会把超时 124 转为成功，AFS 报告维持独立的超时和未完成状态。客户端重挂载的证据只支持该升级方式。 [兼容 CI](https://github.com/juicedata/juicefs/blob/0b90c7db5a929ae6adc5faad948d108efd2c99f9/.github/workflows/version_compatible_hypo.yml) · [状态机](https://github.com/juicedata/juicefs/blob/0b90c7db5a929ae6adc5faad948d108efd2c99f9/.github/scripts/testVersionCompatible.py) · [格式校验](https://github.com/juicedata/juicefs/blob/main/pkg/meta/config.go) · [升级手册](https://juicefs.com/docs/community/administration/upgrade/) |
| 3FS | 同一个布局 seed 在不同 libstdc++ 上可能导向不同复制链，项目通过显式 `SHUFFLE_METHOD` 保持既有集群的布局算法，测试包含已知分歧 seed。恢复设计将重启存储目标标记离线，恢复期间继续其他正常业务，数据恢复完成后才恢复目标服务。源码公开了链状态恢复测试和故障模型，核查范围内未找到两份发布制品的滚动升级 E2E。 | 格式兼容还包括默认算法、构建选项和数据布局。保留已知分歧向量，用恢复节点实际接流量证明数据来源。 [兼容问题 #368](https://github.com/deepseek-ai/3FS/issues/368) · [Shuffle 测试](https://github.com/deepseek-ai/3FS/blob/22fca04564c7cc230fd8b9523b8b92864e1dad47/tests/common/utils/TestShuffle.cc) · [恢复设计](https://github.com/deepseek-ai/3FS/blob/main/docs/design_notes.md#data-recovery) · [链恢复测试](https://github.com/deepseek-ai/3FS/blob/main/tests/mgmtd/TestUpdateChain.cc) |
| Curvine | 发布版本、线协议范围、连接能力分别表示。Master 用 worker 心跳和 client 握手判断准入，Worker 在数据连接首次打开时判定，双方声明后才启用对应能力。默认诊断、可选拒绝，另有强制拒绝版本清单。合并记录包含策略单测和 Worker 连接集成对照。上游问题 #1599 报告了严格模式首次握手通过、后续 `statfs` 请求失败。 | 对不同角色和连接明确支持范围，测试首次连接、后续请求、重连和切换。只有已证明语义等价的优化可以降级，数据正确性和权限相关的不兼容在副作用前拒绝。公开证据支撑握手与集成合同，完整有状态滚动/回退仍需独立证据。 [版本手册](https://curvineio.github.io/docs/User-Manuals/Operations/version-management/) · [数据面集成对照](https://github.com/CurvineIO/curvine/pull/1588) · [上游 #1599](https://github.com/CurvineIO/curvine/issues/1599) · [发布说明模板](https://curvineio.github.io/docs/User-Manuals/Operations/release-notes/) |

## 设计、实现和验收

| 规则 | 设计与实现 | 要提交的测试证据 |
| --- | --- | --- |
| Protobuf 保持稳定字段身份 | 保留已发布字段号和类型、原有 tag 的语义与默认行为。新字段用新 tag。需要区分未传与空值的标量用 `optional`。删除字段或枚举项后 `reserved` 其编号及名称。零值枚举留给未指定语义。明确旧端收到新值的处理条件。 | 用已发布 schema 编解码固定样本，双向断言行为。含旧端读后重写时，再测 `新写→旧读改写→新读`。覆盖真实 SDK、映射、网关或存储链路，特别是 JSON 转换和按字段重建可能丢未知字段。 [Proto3 演进](https://protobuf.dev/programming-guides/proto3/#updating) · [Proto 最佳实践](https://protobuf.dev/best-practices/dos-donts/) · [AFS 同协议行为对照 #123](https://buildkite.com/agent-dx/agent-dx-full-test/builds/123) |
| 用类型表达核心业务语义 | 当前语义可容许缺失的可选优化或诊断提示，才适合预留有界的扩展 KV。每个键约定命名域、值类型、上限及未知键处理。权限、写入一致性、持久化格式、`recursive=false` 等有明确后果的输入用类型化字段或能力。 | 两端支持时实跑新行为。旧端缺能力时验证继续走等价旧路径，或返回明确错误且无副作用。SDK 校验返回值和实际目录状态。 [rc12→旧服务端 #172](https://buildkite.com/agent-dx/agent-dx-full-test/builds/172) · [新服务端对照 #173](https://buildkite.com/agent-dx/agent-dx-full-test/builds/173) |
| 准入、功能启用与格式写入分开 | 在受信握手里核对身份、协议和能力，验证通过后才写会话等状态。新增行为由配置及实际会读写相关状态的角色能力共同限制。借鉴先铺设兼容读取、再开写入的顺序。新格式一旦落盘，必须记录旧写入者下线或被拒绝的条件。保留 AFS 现有异协议硬拒绝结果作为回归基线。 | 混合矩阵分开验旧 A/新 B、新 A/旧 B，覆盖注册、持续心跳、后续请求及重连。拒绝时验证精确错误、无新增会话/挂载/副本指派，健康节点仍可 `fsync`。同协议替换后验证数据路径恢复。 [同协议 #123](https://buildkite.com/agent-dx/agent-dx-full-test/builds/123) · [跨协议 #163](https://buildkite.com/agent-dx/agent-dx-full-test/builds/163) · [反向 #164](https://buildkite.com/agent-dx/agent-dx-full-test/builds/164) |
| 落盘演进保留可读性和可判定性 | 为持久状态定义格式标识和读写支持范围。新增字段只在缺省值能正确代表旧数据时默认回填。目录位置、哈希布局、归属、压缩、加密及迁移进度的变化同样审查。无法正确解释的格式要在修改状态前明确拒绝。 | 旧制品先落盘，再换新版并核对内容和元数据，随后继续写、重启再读。必要时让旧版读改写新状态并再次从新版读回。用定点故障证明断言会因格式或内容变化报红。 [OwnerFs 真实 catalog 失败 #158](https://buildkite.com/agent-dx/agent-dx-full-test/builds/158) · [catalog 故障检出 #138](https://buildkite.com/agent-dx/agent-dx-full-test/builds/138) · [换回原制品 #143](https://buildkite.com/agent-dx/agent-dx-full-test/builds/143) |
| 业务存活、恢复副本和回退分别证明 | 固定旧、混跑、全新、回退四阶段，以及每一步应继续提供的服务。用 `fsync` 和应用确认记录定义应保留的写入。回退支持两个明确出口。保留当前状态换回二进制，或恢复一致快照。快照出口标明 RPO 和快照后已确认写入的去向。 | 滚动和中途取消时都有持续写入，记录每笔 SHA 和确认点。读前停原源并证实从恢复 B 走数据路径。如果旧版不能打开新落盘，验证拒绝未改变原状态，再从一致快照恢复并核验应出现和消失的文件。 [DFS 滚动 #157](https://buildkite.com/agent-dx/agent-dx-full-test/builds/157) · [DFS 中途回退 #160](https://buildkite.com/agent-dx/agent-dx-full-test/builds/160) · [OwnerFs 中途回退 #159](https://buildkite.com/agent-dx/agent-dx-full-test/builds/159) |
| 检出能力与执行结果分开 | 每条验收记录测试 SHA、服务和客户端制品 SHA、角色、环境、关键配置、阶段证据及清理回执。测试准入、环境阻塞、未运行、断言失败各有独立状态。特性兼容失败单列，共有能力能继续出结论。 | 使用同版绿基线、双向混跑及一个可控反例。反例必须命中原断言的预期阶段，撤销后应复绿。保留版本和具体失败证据。本地与 Buildkite 核对这些输出，而非只看任务退出码。 [故障正反对照 #152](https://buildkite.com/agent-dx/agent-dx-full-test/builds/152) · [恢复 #153](https://buildkite.com/agent-dx/agent-dx-full-test/builds/153) · [真实跨版本 #172](https://buildkite.com/agent-dx/agent-dx-full-test/builds/172) |

[落盘、滚动和回退](afs-case-prompts.md#version-storage)

[Node–Meta 与 Node–Node](afs-case-prompts.md#version-protocol)

[POSIX 与独立 Python SDK](afs-case-prompts.md#version-posix-sdk)

## MR 和发布评审用的检查项

- MR 声明受影响的接口、角色、落盘域和支持版本，列出默认行为是否保持、新能力何时开启、缺能力如何处理、预期错误和是否有副作用。
- 发布说明写清受支持的起点和替换顺序、何时开放新写入、如何检查会话和副本恢复、哪个阶段还能只退二进制，以及何时需要快照和对应 RPO。
- 对改动直接受影响的混跑方向提交真实产物证据。至少区分同协议的发布版本混跑和不同协议的准入保护。后端和拓扑差异由证据单独支撑，local-file Meta、gRPC 或进程更换结果不能外推到 Redis/etcd、RDMA 或 Pod 替换。

## 提请集中评审

- **兼容承诺和版本矩阵**。建议发布时只承诺明确实测的版本对及升级顺序。待确认要长期守住上一正式版，还是再纳入仍服役的更早版本。
- **新能力启用策略**。建议沿用严格的安全拒绝边界，新增能力先通过类型化声明和实测启用条件。若引入诊断模式，待明确可安全放行的范围及观测责任。
- **门禁和回退承诺**。待确定哪些是必需发布门禁，已知不兼容如何保留可见失败，以及哪些部署形态需要支持仅回退二进制。快照回退的可接受 RPO 由业务确认。
