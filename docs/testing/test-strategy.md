<a id="top"></a>
# ADX／AFS 测试规范：五步验证路径

状态：2026-11-01。默认AFS ON候选已在Base #227完成编译、新包/镜像及公共＋AFS冒烟，显式OFF的#226也通过，改动尚未合入。AFS专属串行冒烟本次2分45秒；完整CI时差受到公共Redis首败与重试干扰，不能当成稳定AFS增量。共享默认分支、外部触发与合入规则未改。

[OwnerFs MR !126](https://gitcode.com/openJiuwen/agent-dx/merge_requests/126)、[CI0/CI1 MR !127](https://gitcode.com/openJiuwen/agent-dx/merge_requests/127)、[SDK/冒烟 MR !128](https://gitcode.com/openJiuwen/agent-dx/merge_requests/128)均已合入。早期Base #223固定包验证保留原身份；当前正式集成验证采用master `a76a97c6`和Base #224新制品。本轮仅运行验证和更新规范，未修改生产/测试/CI实现、共享默认分支或合入保护。当前结果、待补项与历史失败同页记录，组件、产品和门禁分别举证。

## 先看五步的逻辑

开发时先把基础问题拦住，提交后由公共CI守住集成底线，拿到固定包后再做更充分的版本验收。

1. **基础UT／LLT：先取得代码级保证。** 开发者在Linux运行 `make afs-test` 或选定包的 `cargo test`；命令会编译并运行普通测试，`cargo build`本身不运行测试。复用现有入口，新功能随代码补UT。
2. **单VM集成：有条件就验证真实组件协作。** 默认在隔离Linux VM内，经AFS统一入口启动Meta/Node和真实挂载，验证读写、权限、可见性、正常恢复与清理。
3. **多namespace集成：一台VM验证隔离节点的协作。** 使用公共环境的M/A/B/C网络、PID和挂载namespace，复用同一入口验证真实TCP/mTLS、恢复和多读者；不要求另备三／四台VM，也不外推为跨宿主故障隔离。
4. **Buildkite门禁：用已有CI环境保护共享集成。** 在ADX既有目标K8s部署AFS，选核心用例进冒烟、更完整功能进Full，并验证ADX实际使用AFS；不调用开发者VM。
5. **固定包验收：开发者和测试人员都能自主闭环。** 拿固定包及配套制品，在合格环境中复用公共E2E跑全量、专项；小时／天级长稳和受控性能另补公共能力与AFS负载。

第2/3步按开发者条件选择，没条件可以不跑并注明未验证；实际失败不能伪装成环境跳过。当前已通过AFS组件定向CI；本分支已补SDK产品路径与必需包装，真实SDK与固定包组合门禁均已通过；接线已合入，正式新包#224通过；本轮默认ON候选#227与显式OFF#226均通过，候选待合入。它决定已触发构建的AFS模式，不新增事件触发；合入pipeline-success要求仍为false，不能当作已生效的自动合入保护。两步都默认在一台Linux VM内：第二步同guest多进程，第三步隔离namespace。可选SSH多VM适配仍保留，未纳入本轮实测；这不是AFS不能容器化的声明。

## 五步总表

先用这张表看清原则、现状和工作顺序；目录、用例与命令在下方同页展开。

| 步骤 | 原则／要拦截的问题 | 当前现状（事实） | 环境、目录与入口 | 待补项（未实施） | 优先级 |
| --- | --- | --- | --- | --- | --- |
| ① 基础UT／LLT | 尽早发现模块规则、错误传播、状态机及接口合同回归 | 已有8个AFS crate测试入口；普通非ignored测试已接ON组件构建 | Linux；UT在源文件，LLT在域内tests；`make afs-test`／选包 `cargo test` | 固定推荐范围、说明feature/ignored边界；新功能按代码补UT，不另造runner | 维护基线；P0明确范围 |
| ② 单VM集成 | 验证真实Meta/Node、FUSE/bind、数据/权限/缓存和正常恢复/清理 | 已实现统一入口和11个基础case；本轮冻结产物真实运行11/11通过 | 一台Linux VM；`afs/tests/run.py`＋`suites/`＋local/exec适配 | 此前冻结输入的新构建阻塞保留为历史；后续新版本持续跑对应本地集合 | P0基础已完成 |
| ③ 多namespace集成 | 验证隔离节点的真实TCP/mTLS、远端数据路径、恢复和多读者 | 用户确认一VM多namespace；三角色基础＋C读者共9case，本轮9/9通过 | 一VM的M/A/B/C隔离角色；标准Linux环境说明；`cluster-basic`／`cluster-manyread`；同run.py | 可选SSH真实多VM适配未实跑；不宣称VM断电/宿主故障隔离 | P1基础已完成 |
| ④ Buildkite流水线验证 | 防止共享候选包破坏AFS/ADX基本功能；流水线内必测、事件触发、合入保护分别验收 | MR126/127/128已合入；Base224正式新包通过。本轮Base默认ON的227与显式OFF的226均通过，候选待合入；Full缺省模式已对齐并通过合同检查。合入pipeline-success仍false | 既有CI目标K8s；`run-e2e.sh` → `afs_gate.py`；Base本次固定22c7f9b3，未传旧制品或诊断选择器 | 默认ON候选评审合入；外部触发仍单独核实；CI4 Full／PK0专项 | P0默认ON合入；P1 Full |
| ⑤ 固定包验收 | 人或AI可自主部署、验收和复现，扩大版本质量验证 | 公共K8s Full、Docker单宿主11组和专项已有；只有5分钟soak/90秒采样，完整长稳未闭环 | 先备固定包/SDK/backend/bundle及合格环境；直接公共E2E或独立Full代跑 | PK0–PK4：拿包复现→产品Full→专项→公共长稳→受控性能；复用第四步适配，补说明/负载/判据 | P1可复现全量/专项；P2长稳/性能 |

## 统一口径与优先级

- **已定原则／决策**：组件UT/集成归AFS，真实组件测试统一目标 `afs/tests/`；ADX公开SDK及完整产品场景归 `build/e2e/`。本地、CI、拿包验收复用断言和结果语义，按环境适配，不复制一套Buildkite专用业务用例。
- **事实**是已有代码/调用；**已有规划**是原仓内目标但未闭环；**待补项**是后续实现范围；**待验证**是尚未实测的环境/触发条件。总体规范已确认，不表示具体CLI文件、CI接线或所有场景已存在。
- **当前**第二/三步基础及CI0/CI1组件定向闭环已通过；**P0**正式候选#224已通过；自动触发和合入强制策略尚未闭合（本次只读核实，不改共享设置）；SDK与流水线内冒烟已通过，**P1**扩产品Full、拿包复现和专项，**P2**建设完整长稳及受控性能。实现顺序不把本地VM变成所有开发者的提交前强制条件。
- **通过标准**：选中的必需项必须实际执行，0匹配、ignored、未执行、BLOCKED不能记PASS；定向通过不冒称完整Full，含AFS二进制不证明产品使用了AFS；正常清理也必须举证。
- **环境与结果范围**：VM、Docker、K8s分别记录；Pod跨worker不能冒充多VM/断电验证；功能、正常恢复、故障、长稳及性能分别报告。CI缺必需环境不能静默跳过为成功。

<a id="step-1"></a>
## 第一步：基础UT／LLT

**原则**：复用自然的编译/测试工具，在开发期快速定位基础问题，不新增测试平台。

**现状／事实**：`make afs-test`已选择8个AFS crate及all-targets；单独 `cargo test -p afs`只选择主crate，不能当作整个AFS域都跑过。Cargo测试可自行编译；单独编译不会自动测试。普通LLT与UT均可能参与Cargo，不能仅按命令名称统称纯UT。

**待补与优先级**：P0固定推荐命令和范围说明；后续行为变更补相应UT/LLT。真实挂载、外部依赖、ignored及特殊feature明确转入第2/3步或定向扩展，不能从普通Cargo通过推导它们也通过。

### 现有入口、目录和边界

```sh
# 现有命令，须在Linux执行；本轮未运行
make afs-test JOBS=4
# 只选主crate的定向开发验证
cargo test --locked -p afs
```

8个crate为 `afs`、`afs-client`、`afs-error`、`afs-logging`、`afs-metrics`、`afs-protocol`、`afs-tracing`、`afs-transport`。
UT在 `afs/src/**`、`afs/client/src/**`、`afs/common/*/src/**`；LLT在 `afs/tests/*_contract.rs`、`local_sdk.rs`、`storage_localfs.rs`及域内crate的tests。普通测试在Linux构建环境直接执行；受影响真实环境集合另选。
CI的 `component-tests.sh afs`也选择这8个crate，但与Makefile的target/参数并非逐字完全相同；具体以各自脚本为准。CI仅ON选择AFS构建，默认组件测试可配置关闭。

依据：[Makefile](../../Makefile)、[组件测试](../../.buildkite/component-tests.sh)、[AFS逐项盘点](afs-test-inventory.md)。


<a id="step-2"></a>
## 第二步：单VM基础集成

**原则**：在一台Linux VM里跑真实组件，先确认基础读写、权限、可见性、恢复和清理。开发者有条件就跑；条件不足可以不跑并注明未验证。

**现状／事实（2026-10-31）**：`afs/tests/run.py`、实际 `suites/`、local/namespace/SSH执行适配、环境模板、身份与结果/清理协议已实现。单机11case、第三步9case合计20基础case在公共VM真实通过；Linux工具合同24项通过。

**输入边界**：产品复用冻结ARM64产物，源码输入摘要 `7b76ba244970509625c8fad2f45e8fc51ba577de24c7e8ba54cbc161f5e6f0e0`；受控内核测试ELF为已有 `cdb104a8d013f490cd3e72abc827935b514e9bf6cc58b3f9316fe0befff9dfc6`。该次未改生产Rust；当时新构建的网络/离线缺依赖失败保留，不能用那次冻结结果声称新生产代码编译通过。该次尚未接入第四/五步；本次amd64 CI进展见第四步，二者输入和成绩独立。

### 第二、三步实施对照与验收出口

通用VM从依赖安装到namespace创建/运行/回收的操作说明及标准命令模板见 [AFS组件集成README](../../afs/tests/README.md#从普通linux-vm准备环境)，不依赖个人路径或外部环境管理工具。通用setup/PYENV与预检、20项基础用例已在Ubuntu 24.04 ARM64上重新通过；测试shell及namespace模板清除继承代理，首轮代理导致的失败原样保留。该验证从已有VM开始，不声称空白镜像安装或所有平台均已验证。

两步共同设计并已按下表闭环。用户在实施时明确第三步默认改成一VM内的namespace，不再要求独立三/四VM。角色执行/业务断言不依赖具体接入方式；本地、公共环境exec和可选SSH共用同一套逻辑。

| 批次 | 已实现内容 | 本轮出口与边界 |
| --- | --- | --- |
| B0／P0 | 发现/选择、能力检查、稳定ID/标签、matrix环境映射、身份与退出/结果协议 | Linux24项工具合同通过，含namespace消歧、混合输入拒绝、失败合并、迟到回复与清理 |
| B1／P0 | OwnerFs单机真实bind/远端FUSE、基本操作、非root/setid、正常恢复 | 4case通过；A使用独立egress bind，不能把OwnerFs元数据FUSE目录当成本地bind |
| B2／P1 | 单VM内M/A/B独立network/PID/mount namespace、OwnerFs跨节点同断言 | 4个OwnerFs case＋拓扑清理case通过；不是独立VM或宿主隔离验收 |
| B3／P1 | DFS同guest与隔离节点部署、发布/版本/权限、正常恢复与独立C读者 | 单机DFS5case、namespace DFS4case通过；R1不承诺Home停机时仍可读未缓存数据 |
| B4／P1 | 精确Rust内核顺序/busy调用、Python真实DFS H1/H2/H3、完整组计数与正常清理 | 内核2case含3个实际Rust test通过；与其它case合计20/20、LB01–08/MV01–08齐全，清理错误0 |

当前是基础组件集成出口，不是完整ADX产品/全部后端/长稳/性能通过。旧失败与调试记录保留；最终20例使用同一冻结测试输入。可选真实DFS Rust交叉验证因旧ELF无该selector而保持未验收，不计入基础集合，也不以Python结果冒充该Rust通过。

### 单机基础集合：8组对应11case

| 组 | 解决的问题 | 当前实现／证据 |
| --- | --- | --- |
| LB01 启动与拓扑 | 真实Meta/Node、OwnerFs A egress/ext4 bind与B FUSE、DFS A/B/C挂载 | fixture每例验证真实挂载；OwnerFs核对物理inode；不以health/PID替代 |
| LB02 文件与目录 | mkdir/list/stat、读写/覆盖/truncate、rename/unlink/rmdir、ENOENT | `ownerfs.basic.namespace`、`dfs.basic.namespace`，跨Node读回和错误检查 |
| LB03 OwnerFs互通 | 三轮64KiB不同内容、A bind↔B FUSE、fsync后重开及删除 | `ownerfs.basic.read_write`，双向长度/完整SHA核对 |
| LB04 权限与错误 | owner成功、other拒绝、mode000新OPEN拒绝/恢复；OwnerFs setid/CAP_FSETID合同 | `ownerfs.permissions.deny_other_uid`、`dfs.visibility.new_open_permissions`；独立降权进程 |
| LB05 DFS一写多读 | R1下A写、B/C并发读，三版本和删除 | `dfs.basic.one_writer_many_readers`；内容校验与实际peer_read_ranges调用增量分别记录 |
| LB06 内核顺序/缓存 | 同fd write/truncate顺序、增长/截短；真实同inodeV2/H1尾部/H2/H3和权限 | `fuse.contracts.ordering`显式调用两个Rust选择器；`dfs.visibility.real_cross_handle_cache`用非root Python跑16MiB/完整SHA/EOF和READ计数 |
| LB07 正常恢复 | 成功确认数据在正常停启/同目录重启后保持，身份/权限/内容一致 | OwnerFs/DFS各自 `recovery.normal_restart`；不替代SIGKILL/断电验收 |
| LB08 正常清理/busy | 正常退出、先Node后Meta、无新增挂载/自有监听；busy拒绝不偷偷detach | 每例强制清理检查；`fuse.contracts.busy_unmount`运行精确Rust内核合同 |

Rust受控backend选择器仍为 `fuse_ftruncate_on_same_handle_waits_for_prior_write`、`dfs_buffered_reopen_refreshes_growth_and_truncation_with_new_heads`、`fuse_normal_join_rejects_busy_mount_without_detaching`。目标先 `--list --exact` 核对，再 `--ignored --exact` 实际执行，拒绝0测试PASS。受控backend与真实DFS证据分开。

真实DFS缓存probe在同一非root进程可见的A/B两挂载下执行；B自有FUSE BDI的readahead临时置0并恢复，校验16MiB、V2不换inode、H1未缓存尾部、H2新页、H3同版本缓存复用、mode000新OPEN拒绝及恢复。原Rust同语义test留作可选 `local-extended` 交叉验证；跨namespace不能把两条路径当作同进程可见，改用第三步的角色协作断言。

### 环境与输入标准

- VM/root、`/dev/fuse`、FUSE3、原生ext4、私有mount namespace、已有工具与明确UID/GID由环境维护者准备；目录至少1GiB空闲，编译空间另计。不将Mac共享路径当Linux数据盘，不自动安装包/创建用户或修改全局配置。
- `base`是已有专用Linux目录；每case使用run-ID＋短摘要新目录，避免UDS超过108字节；不覆盖历史目录、不终止未知进程。默认串行，独占自有资源。
- 同候选native ELF、产品source身份与实际SHA由已有构建流程提供；同目录 `artifact-identity.json` 核对产品及测试ELF，传输前后再验SHA。只有SHA一致而没有有效构建来源，也不能自称新版本已构建通过。
- 默认local-file Meta、真实mTLS、OwnerFs bind/远端FUSE、DFS R1；不需要完整ADX、S3、etcd、Redis、RDMA。相关后端/设备变更仍要另选扩展。
- 单机可在guest内用local transport；Mac编排可用exec prefix进入公共VM的mgmt节点，所有文件系统/产品操作仍在Linux内执行。不把控制端的Path/open用于访问远端挂载。
- VM是参考工程环境；未来容器须另有FUSE/权限/ext4准入，不能从“能启动容器”推导本集合已通过。

### 唯一入口与目录（已落地）

```text
afs/tests/
├── README.md
├── run.py                         # list/check-env/run，选择、调度与结果
├── suites/                        # 实际用例，没有scenarios层
│   ├── ownerfs/                   # 基本/权限/恢复/跨节点
│   ├── dfs/                       # 基本/权限/恢复/缓存与多读者
│   │   └── real_cache.py          # 真实DFS非root H1/H2/H3 probe
│   ├── fuse/test_contracts.py     # 精确调用已有Rust内核合同
│   ├── cluster/test_multivm.py    # 拓扑与清理，历史文件名保留
│   └── operations.py             # 按角色复用业务断言
├── fixtures/
│   ├── common.py                 # case发现/选择、身份、带request ID的通道
│   ├── worker.py                 # Linux父进程真实wait、私有挂载与角色操作
│   ├── singlevm.py               # guest本机接入
│   ├── namespaces.py             # 公共受管角色的literal argv接入
│   ├── ssh.py                    # 可选SSH入口，保持已有主机信任
│   ├── multivm.py                # 共用Cluster fixture，支持三种环境
│   ├── singlevm.example.json
│   ├── namespaces-3.example.json
│   ├── namespaces-4.example.json
│   ├── matrix.example.json       # 引用local/cluster两份环境
│   └── multivm-3/4.example.json   # 可选真实VM模板，未实跑
├── runner_tests/                 # 工具合同测试，不冒称产品通过
├── support/                      # 已有Rust探针，原位保留
└── *_contract.rs 等              # 已有Rust合同，原位保留
```

现有单机driver的配置/业务/正常退出断言用于迁移对照，Rust合同原位调用；旧跨机/cohort的etcd/粗kill/lazy卸载/R3测量及固定身份不照搬。旧driver保留已有辅助接口和历史用途，新基础验收只维护上述统一入口。精确CLI、产物清单、模板及[添加用例规则](../../afs/tests/README.md#用例组织与后续添加规则)见组件README；当前20项是基础集合，不代表全量覆盖。

### 选择、执行与结果

```sh
python3 afs/tests/run.py list
python3 afs/tests/run.py check-env --suite all-basic --env /absolute/matrix.json --binary-dir /absolute/bin
python3 afs/tests/run.py run --suite all-basic --env /absolute/matrix.json --binary-dir /absolute/bin --work-root /absolute/new-results
# 定向：--case ownerfs.basic.read_write，或 --case 'ownerfs.basic.*'
# 多个--case取并集、按ID去重，任一0匹配拒绝
```

`local-basic`为11case；`cluster-basic`为三角色8case；`cluster-manyread`为基础＋C读者共9case；`all-basic`通过matrix合并20基础case。`run`无选择器默认local-basic，`list`无选择器列全注册。`--all`还包含可选诊断/交叉验证，不能静默跳过缺输入项；需要全部基础时明确选all-basic。

每次新work-root保存身份、实际环境/配置、每case/组结果、原始错误、日志、真实wait和清理；退出0仅表示声明范围通过，1为失败/超时/清理错误，2为环境或输入BLOCKED。case/glob是partial，不冒称整套。每例先停所有Node、确认实际退出，再停Meta；非零但已确认退出不遗留Meta，停止未知则保留并报告。失败且清理确认后可继续独立例，清理不明/环境阻塞后其余NOT_RUN。控制端中断、迟到RPC响应不得伪装清理成功。

当前默认每例预算600秒（可设置60–3600），仅超时保护；Meta/Home正常恢复允许有界30秒会话/缓存收敛，保留暂态错误，内容损坏立即失败。全部配置与日志在清理后保留，控制端临时私钥正常删除；运行数据验后导出核对再按本任务归属清理。

<a id="step-3"></a>
## 第三步：单VM多namespace集成

**原则／用户决定**：使用一台VM内的namespace即可，不要求另搭三或四台独立VM。第二步同guest直接跑进程，第三步M/A/B/C分别进入独立network/PID/mount namespace，验证真实TCP/mTLS与隔离节点行为；两步共用同一run.py、角色操作、业务断言和结果协议。

**现状／事实**：通用模板通过标准`ip netns`/`unshare`进入M/A/B/C；环境由执行者按README预先准备。首次实测使用已有受管VM提供的mgmt和worker入口，属于可替换的exec接入示例。fixture只部署本轮产品、分发输入/配置、取证与清理，不安装公共环境或创建VM。实测namespace基础9case通过，包括额外C读者；每case核对同一boot内各角色net/PID/mount身份不同。SSH真实多VM适配仍是可选，未作本轮通过声明。

### 角色、环境与责任

| 角色 | 默认公共节点 | 产品职责 | 隔离/输入要求 |
| --- | --- | --- | --- |
| M | mgmt | local-file Meta、测试证书下的管理/RPC | 与A/B/C独立namespace；自己的state/监听/日志 |
| A | worker-1 | OwnerFs真实egress bind/Home；DFS R1 writer | 原生ext4，自有挂载/UID、真实mTLS与固定候选 |
| B | worker-2 | OwnerFs远端FUSE；DFS新鲜读者 | 独立进程/网络/挂载，内容与实际peer调用证据 |
| C | worker-3，可选扩展 | 第二个独立DFS reader | 三角色基础不需要；四角色集合必须有且实际读回 |

VM和公共节点namespace由环境维护者/公共工具准备；使用者先登记、open并保存session。项目提供模板、检查所选能力、分发/部署本轮AFS并回收；结束后使用者导出核对、close、清理本任务临时盘、release。控制端可为Mac或Linux，不增加一台“控制VM”，不在运行节点重编译产品。

### 跨节点基础：7组＋1扩展

MV是保留的验收组编号；本轮实际拓扑是单VM多namespace，**不是独立VM通过**。

| 组 | 必须解决的问题 | 当前用例 |
| --- | --- | --- |
| MV01 | 候选一致、角色隔离、真实挂载与服务路径 | `cluster.topology_cleanup`＋每例fixture |
| MV02 | A bind↔B FUSE双向同步读写、版本/长度/SHA/删除 | `ownerfs.cluster.read_write` |
| MV03 | 命名空间、旧FD在rename/unlink/名称复用后仍正确、单一Home、EXDEV | `ownerfs.cluster.namespace`，含同步FD握手而非猜测sleep |
| MV04 | DFS R1发布、新鲜B读回三版本、删除、实际远端取数 | `dfs.cluster.read_write`；RPC调用计数与成功内容分别举证 |
| MV05 | 新OPEN得到新版本和长度；非root权限拒绝/恢复 | `dfs.cluster.visibility`、`ownerfs.cluster.permissions` |
| MV06 | 正常Meta/Home进程重启与同目录恢复；不可达时有错误、恢复后可读 | OwnerFs/DFS各自 `cluster.restart`；R1不承诺A停机时未缓存内容可用 |
| MV07 | 各节点真实退出、无自有挂载/监听，失败/中断仍取证 | 每例统一清理与拓扑case；未知不记PASS |
| MV08 | A发布后独立B/C并发读回同版本与删除，各自数据路径正确 | `dfs.cluster.many_readers`，四角色集合同时包含前7组 |

根创建竞争要求一个成功且实际Home唯一，保留竞争失败者errno，不将此结果冒充完整POSIX错误码兼容。普通进程重启不是VM断电；同一宿主不能证明宿主故障隔离、跨物理机HA或精确性能。

### 复用同一入口

```sh
python3 afs/tests/run.py run --suite cluster-basic --env /absolute/namespaces-3.json --binary-dir /absolute/bin --work-root /absolute/new-three-role-results
python3 afs/tests/run.py run --suite cluster-manyread --env /absolute/namespaces-4.json --binary-dir /absolute/bin --work-root /absolute/new-four-role-results
# 与第二步一起：--suite all-basic --env /absolute/matrix.json
```

具体角色地址/command-prefix均在仓外环境文件；不把个人session、凭据或当前机器状态写死进产品用例。`namespaces-3/4.example.json`为模板，须填实际公共工具和session；`matrix.example.json`引用local/cluster配置。只有真实不同net/PID/mount身份才准入，不能给同一进程取三个host别名冒充隔离。可选SSH配置按已有信任接入，不创建用户/密钥或修改机器identity。

**当前边界与后续**：第二/三步基础闭环已通过此前冻结输入；第四步组件CI已通过，SDK产品与流水线内必需冒烟本分支已真实通过。独立VM、VM断电、复杂后端/设备矩阵、长稳和性能按需求另验收，不从本轮结果外推。

<a id="step-4"></a>
## 第四步：Buildkite共享门禁

**本步原则、现状与优先级**：原则：复用既有CI目标K8s，部署AFS并选集；不调用个人VM。现状：普通Cargo、CI0资产/环境和CI1真实组件定向已通过，SDK产品路径本分支Full #81已通过；必需包装在#85以固定包通过。接线已合入且正式候选#224全流程通过；当前缺口是自动触发/合入必需状态和CI4完整Full选集。优先级：P0冒烟、P1 Full。

**状态**：五步总体逻辑已确认；以下目录/命令中标注目标或待补者尚未实施。

### Base／Full默认AFS冒烟与时延验证（2026-11-01，候选待合入）

**默认覆盖所有进入现有流水线的改动，只跑AFS既定冒烟。** 仓内没有按改动范围选择AFS组件的机制，因此候选在`.buildkite/pipeline-package.yml`顶层用`${ADX_WITH_AFS:-1}`统一模式：不传参数即ON，显式`0`仍可关闭。历史master #221确有省略AFS参数的调用；其它专项构建中显式OFF的调用仍有效。本候选尚未合入，不能称线上默认已生效。

**复用入口，不复制用例。** Base保持原依赖链：`build-afs`普通UT/编译、source-gate AFS lint→同次组包/镜像→`.buildkite/run-e2e.sh`→`build/e2e/kubernetes/afs_gate.py`，依次运行公共L0/auth、原组件5项、OwnerFs/DFS SDK两模式各6项断言。组件/SDK子驱动的`full --case`是精确选中冒烟目标，不表示运行AFS全量或公共11组Full。ARM是否构建/测试继续由原开关控制。

**Full同步匹配候选模式。** Base默认ON包会被原Full默认OFF的模式校验拒绝，因此`.buildkite/pipeline-full.yml`也设置同一缺省值；消费旧OFF包/镜像需显式`ADX_WITH_AFS=0`。Full原公共profile/steps不变，只在原公共集合后追加同一组AFS冒烟；不运行AFS功能Full、专项或长稳。SDK/admin/maintenance、独立Makefile/release默认不改。

**已完成的验证。** [Base #226 OFF](https://buildkite.com/agent-dx/agent-dx/builds/226)和[Base #227 默认ON](https://buildkite.com/agent-dx/agent-dx/builds/227)固定同一提交`22c7f9b36f424c2f9db3ece8beb1481fcffd8c5e`，顺序执行，均新编译/组包/镜像；无旧候选或target/profile覆盖。ON请求故意省略AFS参数，服务器与关键job实际均为1；OFF为0且AFS job未启动，包/镜像排除AFS。ON完成AFS普通测试2058 PASS/58 ignored、公共2项、组件5项和SDK两模式12子断言，根结果/JUnit/summary及各层清理通过。最终候选的Base YAML解析结果与实跑22c完全相同，仅改注释；Full仅增加顶层模式缺省，通过54项合同检查，**本轮未触发Full，也未验收公共11组或ARM**。

| 观察项 | 本次实测 | 如何理解 |
| --- | --- | --- |
| AFS源码check/clippy | 74.867秒，约1分15秒 | 并行source-gate内的AFS追加段，不直接累加到总CI |
| AFS组件作业 | 202.661秒，约3分23秒 | 含测试编译/UT156.111秒、release编译29.024秒，以及环境、打包/传输等；不要再把子段重复相加 |
| AFS组件冒烟 | 57.911秒，约58秒 | 5项真实用例及该子驱动的部署/正常清理 |
| AFS SDK冒烟 | 107.194秒，约1分47秒 | OwnerFs/DFS两模式、warmup、12断言及回收/清理 |
| AFS专属串行冒烟合计 | **165.105秒，约2分45秒** | 本次最明确的额外串行等待；不含公共L0/auth，不代表稳定总增量 |
| 组包前直接等待AFS编译 | 0秒 | AFS比其它前置更早结束，本次最后前置是Platform retry；不能由此断言AFS总影响为0 |
| 完整Base墙钟 | OFF792.193秒（13:12）；ON1226.674秒（20:27）；观察差+434.481秒 | ON包含公共Redis首败与完整作业重试，**不是干净的AFS因果对照** |

**保留失败，不用重试掩盖成本。** 首次默认ON [#225](https://buildkite.com/agent-dx/agent-dx/builds/225)在公共Execd固定envd Git fetch连续两次超时300秒，组包/冒烟未执行，单独保留而不作为干净样本。#227在公共Coordinator real-Redis的`node_retirement_new_boot_readiness_closes_the_previous_departure_durably`返回Unavailable/timeout，测试连接/操作超时为300ms；未修改产品、超时、并发或断言，原条件重试该Platform作业后7项通过。首败结束至retry创建170.633秒、至启动176.171秒，retry作业267.200秒；不能将这些随意相加减推算稳定AFS增量。Redis小日志未见明确服务退出/持久化错误，根因未确定，不把它预先归为AFS或环境问题。

**时延结论的边界。** 当前可以确认AFS专属冒烟本次增加约2分45秒串行工作，编译和lint在并行路径。完整总增量尚不能由这一对构建精确归因：公共失败/人工重试间隔、包和镜像传输、排队、共享缓存/资源均可能影响墙钟。共享缓存没有清空，此处不是冷编译、稳定分位数或性能SLA；若后续需要总时延预算，应使用无重试的正常构建继续积累同口径数据。

**外部触发与合入规则保持独立。** 缺省ON只作用于已经读取本配置的Base/Full构建，不创建webhook，不覆盖显式OFF，不改变共享默认分支或合入规则。旧的外部relay未知、GitCode合入强制false结论仍保留，不用这次Buildkite成功替代触发/保护配置。

### 合入后正式候选与线上门禁核对（2026-11-01）

**先分清三个问题**：明确启动AFS ON后必须跑哪些用例，是仓内流水线规则；合入后是否自动启动它，是触发规则；未通过是否阻止合入，是仓库保护规则。一次手动API构建通过，只能证明第一条实际运行，不能代替后两条。

本次固定master `a76a97c6a7bef93d0f8df47e7b615c434de228c8`，三份MR的原head均在祖先链中。[Base #224](https://buildkite.com/agent-dx/agent-dx/builds/224)使用`ADX_WITH_AFS=1`、`ADX_COMPONENT_TESTS=1`、`ADX_ARM_BUILD=0`，不传旧package/SDK/image build ID，不传targetcase/profile覆盖，尊重branch filter。复用共享缓存，但构建/制品身份必须属于本次build；ARM、外部OBS/PyPI发布及公共11组Full不在本轮范围。

| 层次 | 当前可核实的事实 | 能得出的结论 |
| --- | --- | --- |
| 流水线内部 | AFS默认OFF；明确ON且没有诊断选择器时，正常Base L0包装串公共L0/auth、组件5项、两个SDK模式；原source/普通组件测试、新组包/镜像也在依赖链中 | 已接通AFS ON本次运行内部必测；不能据此声称所有提交都会自动选ON |
| 合入后自动回归 | GitCode默认分支为master；Buildkite基础和Full共享默认仍refactor、provider为private。现有3个hook指向CodeArts/OpenLibing，部分监听push/MR，最近HTTP200；未见直接Buildkite hook，未知是否存在间接relay。近期各30条记录里新master只有本次明确API请求的224 | 尚无master合入后自动选择AFS ON并触发Buildkite的可核验闭环。API source不等于人工来源，不能只靠该字段判断；未知外部relay不能写成不存在 |
| 合入前强制 | master受保护；官方GET PR设置返回`only_allow_merge_if_pipeline_succeeds=false` | 当前未启用流水线成功才允许合入；分支保护本身不等于AFS Buildkite硬门禁 |

这些共享设置本轮只读，没有修改默认分支、hook、合入规则或全局AFS OFF默认值。后续缺口是触发策略明确branch/ref/commit与何时传AFS ON，并把正确的非诊断AFS结果绑定到合入必需状态；泛化的“CI成功”或一次API运行不能替代这条链。

**本次真实结果**：AFS普通测试2058通过、58 ignored，source-gate及其它amd64构建通过。第一次Execd在固定envd源commit `92197909` 的Git fetch超时300秒，组包/冒烟当时未执行；原失败日志与制品收齐后，仅原条件重试该job，第2次已通过，原失败保留。最终同build组件/SDK/统一包、索引、新镜像及正常AFS ON L0全流程通过；实际选择公共L0/auth、组件5项、OwnerFs/DFS SDK两模式各6项，根报告/JUnit/CI summary与最终作业通过。它不是旧包重放，也不代表ARM或公共11组Full、长稳性能已验收。

本次release archive SHA为`529aabb89fdeca2d3d0ee5c366cd25824389940de493a83e41636465c2641602`，SDK wheel SHA为`c1521bd5e1aebdabc186463a471cab1d6e6a5b7d8ed2697f22604694a40f87fc`；build manifest声明source为a76、with_afs=true，release manifest声明source为a76、dirty=false；SDK candidate声明同一source及当前build224。AFS ELF已由真实运行角色核对，三组测试的正常清理及namespace删除也通过，不以清单声明代替实物校验。

接口来源：[GitCode PR设置GET](https://docs.gitcode.com/docs/apis/get-api-v-5-repos-owner-repo-pull-request-settings/)、[保护分支GET](https://docs.gitcode.com/docs/apis/get-api-v-5-repos-owner-repo-protect-branches/)。实际仓库设置、脱敏hooks、分支与近期构建原件分别保留在本轮证据中；只保留事实和读取范围，不公开hook凭据，也不把不适用接口的非JSON错误当成无配置。

### CI2／CI3当前实现与验证

CI2/CI3实现仅补测试、部署配置与CI包装，不修改ADX、AFS或SDK生产源码。依赖[CI0/CI1 MR !127](https://gitcode.com/openJiuwen/agent-dx/merge_requests/127)，受影响内核另需[OwnerFs MR !126](https://gitcode.com/openJiuwen/agent-dx/merge_requests/126)的产品修复；早期验证复用Base #223固定包，下表保留这些历史记录，合入后正式新包结果见上节Base #224。实现复用已安装SDK的`Mount(host_path=...)`和`node_id`，没有增加AFS专属API；A/B在现有ADX `node1/node2` Pod中，与本节点sandboxd共用容器挂载namespace，M独立Pod。公共组件目标继续保留私有worker挂载namespace。

| 内容 | 当前入口与合同 | 验证状态／边界 |
| --- | --- | --- |
| SDK产品目标 | `ADX_E2E_TARGET_CASE=afs-sdk` → `.buildkite/run-e2e.sh` → K8s `run.py --profile full --case afs-sdk` → `afs_product.py` / `afs_sdk.py` | Full #81真实两模式/12子项通过；只运行两种AFS产品模式，不是公共11组Full |
| 两种模式的必需断言 | OwnerFs、DFS各要求挂载device/inode/mountinfo、三版本双向write/fsync/新OPEN全内容、权限EACCES、只读EROFS、删除、Environment回收六个结果；DFS另需B端peer计数增长 | 不以READY、普通目录、零退出码或缺失结果作为通过；AFS正常卸载、ADX backend为空及K8s删除分别验收 |
| 现有后端能力与环境 | sandboxd不支持tool user/group覆盖；测试不请求该能力。只有SDK目标通过已有`basic_spec`配置`afs_runc_spec.json`：默认三项capability加SETUID、SETGID、DAC_OVERRIDE，后者供root SDK Files接口访问普通用户私有工作区 | 普通POSIX工作负载先降到10000或10001，再要求CapEff/CapPrm/CapAmb全0；权限拒绝因此仍由普通用户实际验证。该配置是测试部署前提，不宣称共享默认环境支持身份切换 |
| 正常AFS ON门禁 | `.buildkite/run-e2e.sh`未选定向目标时 → `afs_gate.py`：公共原profile → 原五项AFS组件 → 两个SDK模式；缺结果/缺项/失败/清理不明即失败，三子harness整体必须一致 | #84/#85以固定包重放L0组合并通过原CI summary身份检查；OFF沿用公共路径。公共Full嵌套时保留stop的Collector/Gateway指标/trace收据要求；该Full分支只有工具回归，未实跑公共11组 |
| 固定包重放冒烟 | `ADX_E2E_TARGET_CASE=afs-required-smoke`，同一wrapper以`l0`执行上述三部分 | 已在#85用已有Full管道重放Base等价冒烟；不重编或替换产品，不因此声称新head已跑完Base构建、公共11组Full或线上合入保护 |

当前固定产品/SDK：Base #223，`00f1695bd25bac01e44c8dd1f452cf42c47b5c77`；固定镜像bundle来自Full #77。harness单独按当前commit同步、哈希验证。工具测试和Python静态检查只证明测试工具合同，不代替真实SDK结果。

| 真实运行 | 结果与未执行范围 | 清理边界 |
| --- | --- | --- |
| [Full #78](https://buildkite.com/agent-dx/agent-dx-full-test/builds/78) | 63工具通过；warmup被后端拒绝tool user/group参数；两SDK组未执行 | 无Environment成功创建、无AFS准备；ADX/K8s清理通过，不算真实AFS卸载 |
| [Full #79](https://buildkite.com/agent-dx/agent-dx-full-test/builds/79) | 63工具通过；首个Environment创建后身份切换失败，未进入AFS；不能推断未输出的具体UID | 已创建Environment回收、ADX/K8s清理通过；无AFS准备 |
| [Full #80](https://buildkite.com/agent-dx/agent-dx-full-test/builds/80) | 63工具和两侧warmup通过，实际UID10000；AFS传输入口只接受afs-a/b，拒绝复用的node1/node2；产品正文未执行 | 沿原结果保留失败，后续补显式共享namespace、opt-in标签及冻结UID检查 |
| [Full #81](https://buildkite.com/agent-dx/agent-dx-full-test/builds/81) | 测试commit `bd73b190`，64工具、两个模式各6项全部通过；各3代×16KiB双向内容检查；DFS peer计数0→4 | 每模式4个Environment回收；AFS正常退出/卸载无残留、ADX backend为空、K8s删除均通过 |
| [Full #82](https://buildkite.com/agent-dx/agent-dx-full-test/builds/82) | 同一commit、固定产品和镜像；公共L0/auth、组件5、SDK两模式均通过，但gate成功报告缺error字段，JUnit与CI summary抛KeyError，最终作业failed | 不把子项通过当门禁通过；修复后#84/#85通过，原失败保留 |
| [Full #83](https://buildkite.com/agent-dx/agent-dx-full-test/builds/83) | 测试commit `ee421f51`，三部分/各层清理通过，root JUnit四项零失败；root缺harness被原CI summary身份检查拒绝，CI仍failed | 三子harness整体及336文件SHA完全一致；修复根身份承接/一致性核对并通过原summary回归后，#84/#85整条通过，原失败保留 |
| [Full #84](https://buildkite.com/agent-dx/agent-dx-full-test/builds/84) | 测试commit `ce61c0f1`，三部分及各层清理通过；根报告承接三子相同harness，JUnit四项零失败，CI summary JSON/Markdown与最终作业通过 | 三个独立namespace均已删除；保留本次独立输入和证据 |
| [Full #85](https://buildkite.com/agent-dx/agent-dx-full-test/builds/85) | 最终runtime commit `6660ffc1b5447e7d8b6aef2771b1d3767ebbb6e7`，同一L0组合再次通过；公共L0/auth、组件5、OwnerFs/DFS各6项、根报告/JUnit/CI summary全部通过 | 严格正常AFS、Environment、ADX backend和K8s清理；新补Full stop观测分支未因L0重放而获得实跑证明 |

本批本地公共工具150项、K8s传输mock6项、固定Python预检（master差异19文件／父提交差异8文件）通过。回归包含成功gate由既有CI summary.collect/render消费、缺结果/错身份/清理失败判红及公共Full stop收据不丢失。早期#78–80、#82–83失败均保留；不修改既有CI guard来放过缺失身份。最终文档提交与上述实跑runtime提交分开记录。

**本批出口与下一项**：AFS ON流水线内必需冒烟在固定候选上已接通；尚未改共享默认分支、webhook或合入保护。三份MR现已合入，正式Base/包路径已在本轮#224全流程通过；CI4完整公共11组＋已登记AFS Full集合、第五步PK0–PK4、取消实跑、长稳与性能仍另行验收。不能用#85外推新head已编译打包或公共Full已通过。

### CI0／CI1历史运行：旧包故障、修复与新候选复测

以下保留CI0/CI1交付时的历史记录，其中SDK暂缓、未接默认门禁是当时范围；本分支进展以上一节为准。

本次CI0/CI1独立交付保留最新master的读写专项。测试资产另带`tools/`和`runner_tests/`的Python文件，全部进身份清单；工具合同在同步后的M Pod（Python3.12）执行，控制端仍兼容Python3.8。不复制OwnerFs生产修复，受影响内核须使用已包含[MR !126](https://gitcode.com/openJiuwen/agent-dx/merge_requests/126)的候选，或此前已验证的#223固定产品。SDK/CI2暂缓，本次不接默认必需冒烟或扩功能Full。


**这次要证明什么**：在ADX现有目标K8s中真正启动AFS Meta/Node并挂载，跑通跨Pod读写、权限/版本和正常清理。不是再跑一遍mock，也不是SDK使用AFS的产品验收；CI2/CI3仍后置。

入口为现有Full流水线的单项定向机制：`ADX_WITH_AFS=1`、明确base/SDK候选UUID与产品commit，加`ADX_E2E_TARGET_CASE=afs-component`。`.buildkite/run-e2e.sh` → `build/e2e/kubernetes/run.py --profile full --case afs-component` → `afs_component.py`准备M/A/B Pod → 同一`afs/tests/run.py`和已有5项断言。默认L0/Full选集不扩大，也不依赖开发者VM。

| 用例 | 复用的断言 |
| --- | --- |
| `ownerfs.cluster.read_write` | A本地egress bind与B远端FUSE双向读写、长度/SHA、发布和删除 |
| `ownerfs.cluster.permissions` | 非root授权/拒绝、mode变化与原setid合同 |
| `dfs.cluster.read_write` | A写入/fsync，B新OPEN读回、真实peer路径计数与删除 |
| `dfs.cluster.visibility` | 多版本新OPEN和权限变化 |
| `cluster.topology_cleanup` | 实际挂载路径/源身份；每例另核验真实退出、卸载和监听/挂载残留 |

环境前提：控制端兼容现有Linux Python3.8+部署镜像，Pod worker使用ON镜像的Python3.12；需要openssl、kubectl和现有kubeconfig/registry权限；目标worker已有`/dev/fuse`、ext4-backed emptyDir及每角色至少1GiB空闲。ON测试镜像补fuse3/util-linux，OFF不引入；不修改宿主模块/存储。M/A/B独立Pod，A/B在Full目标要求不同worker；不冒称多VM。冻结namespace/Pod UID，并由worker核对实际Pod UID和隔离身份，防止角色被替换。

**第一次真实运行已完成**：[Full #72](https://buildkite.com/agent-dx/agent-dx-full-test/builds/72)，测试commit`23057ad905809d171bb4fcb990317fd225f54799`，产品为#208的`18acb0fe12152628dc0568c2943cc02edccd2c0b`。真实release/SDK校验、ON工具安装、AFS ELF/测试资产materialize及镜像发布通过；部署端因Python低于3.10被新增guard拦截，5项全部NOT_RUN、无Pod或挂载，不能称AFS正常清理通过。已修复控制端兼容性：去除Python3.9专有路径API、保留路径/符号链接隔离检查，并让Linux工具合同在真实部署镜像里运行；已用#73复用#72镜像并同步当前harness重跑。共享部署镜像及默认分支不改。

**首次业务实跑**：[Full #73](https://buildkite.com/agent-dx/agent-dx-full-test/builds/73)，测试commit`409c4a0b`，产品仍为#208/`18acb0fe`；镜像来自#72，Pod内有效harness/AFS清单已核对为本次测试commit。Linux工具合同 **29/29通过**；M/A/B Pod实际运行，A/B在不同worker，实测ext4、FUSE和隔离身份合格。

| 选中的组件项 | #73真实结果 | 准确含义 |
| --- | --- | --- |
| `dfs.cluster.read_write` | PASS | 真实AFS进程/FUSE、fsync、远端读回/peer计数与删除完成 |
| `dfs.cluster.visibility` | PASS | 真实新OPEN版本与权限行为完成 |
| `cluster.topology_cleanup` | FAIL，准备阶段 | OwnerFs A本地egress bind未建立；业务函数尚未执行 |
| `ownerfs.cluster.read_write` | FAIL，准备阶段 | 同一bind激活失败；不能称OwnerFs读写断言执行后失败 |
| `ownerfs.cluster.permissions` | FAIL，准备阶段 | 同一bind激活失败；权限业务正文尚未执行 |
| 每例AFS正常清理＋K8s收尾 | PASS | 5例均正常回收，无残留进程/挂载，退出回执为0；namespace删除另行确认成功 |

这次旧包结果是**5项尝试、2项业务完成通过、3项准备失败**。日志为`activate failed: Invalid argument (os error 22)`后退到`fuse_only`，精确`findmnt --mountpoint`失败；不能把FUSE健康当作native bind成功。该轮尚未改生产Rust。

**根因已确认**：[Full #74](https://buildkite.com/agent-dx/agent-dx-full-test/builds/74)/[#75](https://buildkite.com/agent-dx/agent-dx-full-test/builds/75)保持旧产品和原5项，在自有worker追加逐步诊断。目标Linux `5.10.0-182.0.0.95.r3090_252.hce2.x86_64`支持`open_tree`与`move_mount`，不支持`mount_setattr`（ENOSYS）。回退补策略使用挂载前打开的target fd，指向被覆盖的旧目录，因而EINVAL；给旧fd加MS_BIND仍失败，改用实际attached tree fd的private/bind-remount对照成功。探针不改变原失败判定；#75工具36项通过，业务仍2通过/3准备失败，正常清理通过。

**最小修复与构建**：`d159a710`让旧接口策略及失败回滚使用保留的attached tree fd，remount加MS_BIND限定本挂载；测试进一步核验nosuid、nodev和private。Base [#222](https://buildkite.com/agent-dx/agent-dx/builds/222)首次构建因5项mTLS测试证书和2项Platform Redis测试失败而未组包；`00f1695b`将测试CA改用独立OpenSSL配置并校验扩展唯一，保留严格mTLS。Base [#223](https://buildkite.com/agent-dx/agent-dx/builds/223)的AFS 2058项通过、58项ignored，原mTLS五项和原Redis两项均通过；格式、公共workspace Clippy及AFS默认特性检查通过，AFS全特性/RDMA未运行。Execd拉取GitHub外部源码TLS断连后仅重试该步骤，未改Platform断言/超时，也未关闭组件测试。新包、镜像与Base公共L0 2/2通过；同候选原5项在Full #76通过，见下表。

**新候选真实复测**：[Full #76](https://buildkite.com/agent-dx/agent-dx-full-test/builds/76)使用Base #223的产品、SDK和镜像；产品与测试commit均为`00f1695bd25bac01e44c8dd1f452cf42c47b5c77`，不再使用旧#208。Linux工具合同 **37/37通过**。这是Full流水线的定向组件目标，不是公共11组或完整AFS Full。

| 本轮原集 | #76结果 | 验收含义 |
| --- | --- | --- |
| `ownerfs.cluster.read_write` | PASS | 真实A bind↔B FUSE读写、长度/SHA与删除 |
| `ownerfs.cluster.permissions` | PASS | 原非root授权/拒绝、权限变化及setid合同 |
| `cluster.topology_cleanup` | PASS | 原拓扑断言；OwnerFs A额外核验ext4、nosuid、nodev、private |
| `dfs.cluster.read_write` | PASS | fsync、新OPEN完整内容与真实peer路径 |
| `dfs.cluster.visibility` | PASS | 新版本与权限可见性 |
| AFS正常清理／K8s删除 | PASS／PASS | 每例正常退出/卸载、无自有进程或挂载残留；namespace删除单独确认 |

**当前出口**：修复后的原5项及两层清理均通过，CI1定向组件闭环完成。SDK/CI2暂缓；下一项先交付CI0/CI1接线并接组件必需冒烟，再扩CI4。完整产品门禁仍依赖后续SDK实际AFS路径；本轮不实施这些扩展。#223首次两次Execd外部下载失败、#222失败均保留，不用重跑成绩抹去。AFS全特性/RDMA、ignored内核测试、取消流程、长稳和性能未因此获得通过。

**独立发布分支复测（最新master）**：[Full #77](https://buildkite.com/agent-dx/agent-dx-full-test/builds/77)为本次CI0/CI1交付重新构建测试镜像，测试代码commit `817078f489459604b1eb108990a6290b929ca448`，固定产品/SDK仍为Base #223/`00f1695b`，不把两种commit混写。M Pod Python3.12工具合同 **62/62通过**，原5项组件用例 **5/5通过**，正常AFS清理和K8s删除通过。最新master的22项基础、20项读写专项、2项性能资产注册全部保留；这些集合没有因资产发现或工具合同运行而自动进入本次五项功能选集。

本交付不含`afs/src`生产改动，OwnerFs兼容修复仍由MR !126独立交付；在受影响内核上换用未修复的包，真实bind断言仍会失败。`tools/`和`runner_tests/`作为已哈希的测试文件交付，不进入产品包。独立打包目录的入口回归已覆盖工具遗漏；M Pod执行合同避免把tomllib依赖加到Python3.8控制端。当前公共工具回归139项、固定Python预检规则通过；make包装的离线工具缓存失败单列，不冒称该命令通过。Rust测试CA文件与#223已测文件相同，未为本发布分支重新编译Rust；AFS全特性/RDMA仍未验证。对应[Issue #67](https://gitcode.com/openJiuwen/agent-dx/issues/67)，下一项为接线MR交付及组件必需冒烟，SDK/完整产品门禁继续后置。

原#72/#73证据保留在工作区`adx/evidence/afs-ci1-buildkite-20261031/`；本次诊断、修复和失败构建保留在`adx/evidence/afs-ownerfs-bind-ci-20261031/`。不另建规范页面，不用探针成功代替产品运行。

工具检查通过只说明适配代码和失败传播合同可用。只有真实运行中5项均执行通过、AFS正常停进程/卸载与namespace删除分别成功，才记CI1定向组件通过；缺环境、缺用例、超时或清理不明都不能变绿。本轮只认CI1定向组件通过；CI2/CI3/CI4未完成。

### CI0当前实现：交清测试资产和身份，不提前部署

**本次完成的事情**：AFS ON时，把已有测试入口、用例和worker交给既有测试镜像/harness通道；同时交清“测哪个产品、用哪版测试、实际文件是否一致”。复用`ADX_WITH_AFS`，不改共享默认分支，也不创建AFS独家流水线。OFF候选不携带这些AFS测试资产。

| 交接位置 | 已实现的调用和产物 | 校验与边界 |
| --- | --- | --- |
| `platform-images`镜像准备 | `package-smoke.sh` → `package-e2e.sh` → `prepare.py`，仅ON复制`afs/tests/`运行文件至node测试镜像`/opt/adx/e2e/afs-tests/`；输出`out/buildkite/bundle/afs-tests.json` | `bundle.json`绑定sidecar SHA；沿用现有`bundle/*.json`制品规则，无pipeline YAML改动；测试资产不进入正式产品包 |
| 产品与测试身份 | `afs_handoff.py`记录产品commit/target、独立harness commit和测试文件SHA；从已校验release manifest生成`inputs/artifact-identity.json` | `afs-meta/afs-node`实际ELF架构、SHA和可执行位全部核对后，才链接进`inputs/`；不复制或修改正式产品二进制 |
| `platform-e2e`部署前 | `run-e2e.sh`下载JSON后，先调用`afs_handoff.py verify-bundle --bundle ... --with-afs "${ADX_WITH_AFS:-0}"` | ON/OFF错配、缺sidecar、SHA不符立即失败；旧ON bundle需用当前工具和明确候选重新准备测试镜像，不伪装为可用 |
| 复用旧产品镜像＋当前测试 | `kubernetes/run.py: sync_harness()`同步当前`afs/tests/`，只替换测试Pod的`/opt/adx/e2e/afs-tests/`，再核对实际产品输入 | 不要求测试commit等于产品commit；两者分别核对、保留有效`afs-tests.json`和`harness.json`供结果追溯；未调用AFS业务用例 |

本地工具验证覆盖ON/OFF、身份错配、文件篡改、错架构、镜像准备和harness同步；Docker/K8s调用为mock或禁止访问，ELF为不执行的测试数据。**不是镜像构建、K8s部署、真实FUSE或AFS冒烟通过。** 完整记录见工作区`adx/evidence/afs-ci0-handoff-20261031/`。

**当前出口与剩余缺口**：新候选的CI0输入/组件环境准入和CI1定向5项已通过。还缺SDK-on-AFS、默认必需冒烟与完整功能Full的选择/汇总。Full若选入Rust内核合同，需独立交付同架构`fuse_contract`测试ELF；不把尚未选入的ELF算作本轮已验收。

### CI0此前只读盘点快照（2026-10-31，交接编码前）

**盘点时结论**：已完成仓内调用与线上既有构建/JSON制品的只读盘点；该次未部署、启动测试、触发流水线或修改CI。下表保留当时事实，资产/身份缺口的后续实现以上节为准；目标K8s仍缺直接准入证据，不能据此开始CI1部署。

线上查询范围为基础流水线#121–220的100条、独立Full #1–71的71条；时间以本次API读取为准，不声称覆盖全部历史或之后的新构建。以下区分**已有事实、确定缺失、当前访问阻塞、待验证能力**。

| 核对项 | 盘点时已有事实与可追溯入口 | 盘点时缺失／阻塞与影响 | 盘点时状态 |
| --- | --- | --- | --- |
| 候选包与交接通道 | `package-components.sh` / `package-e2e.sh`已支持明确AFS ON、同架构component/release/build manifest、SDK、backend及bundle/registry清单 | 发布通道具备不等于当前要测的候选已选定；不得用latest自动替代版本锁定 | 通道已有 |
| 可用AFS ON审计样本 | [基础#208](https://buildkite.com/agent-dx/agent-dx/builds/208)：commit `18acb0fe12152628dc0568c2943cc02edccd2c0b`、build UUID `01a182de-a7ec-4792-b2ce-2d8f529ad1c9`、`x86_64-unknown-linux-gnu`、`with_afs=true`；包内声明afs-meta/afs-node SHA，SDK为同commit的0.1.0rc3，镜像/公共L0均有通过记录 | 作为已知ON历史样本，不自动指定为后续正式候选；与当前测试代码 `02d1a06f`不同。只读取小型JSON并核对元数据，未下载/重验release、backend、wheel本体或拉镜像 | 元数据链一致；实物未重验 |
| 当前版本选择 | 本次读取时最新[基础#220](https://buildkite.com/agent-dx/agent-dx/builds/220)通过，但 `ADX_WITH_AFS=0`，release无AFS文件，不能用来运行AFS组件 | 本次扫描范围未发现commit `02d1a06f`的构建；不能写成全历史没有该构建。后续须显式确定产品候选与harness身份，旧ON包和新测试代码的兼容性尚未运行验证 | 需锁定候选；不触发补构建 |
| 新AFS测试代码与执行适配 | `afs/tests/run.py`、suites、worker、local/namespace/SSH适配已在当前检出；#208有旧 `build/e2e/afs/`资产 | 当前 `stage_harness()`只携带 `build/e2e/`，未带新 `afs/tests/`；prepare镜像也未新增该目录。K8s角色适配尚不存在，不能把namespace模板直接填Pod地址使用 | **CI1确定缺口** |
| 产品二进制到runner的身份交接 | #208 release/build manifest有AFS二进制SHA；现有runner要求显式binary-dir内的 `artifact-identity.json`，校验实际ELF架构/二进制SHA | 现有CI组包/分发没有生成或传递该runner输入。需要从可信候选清单映射身份，并在交付二进制后重验SHA；不能手填一个PASS或只改文件名 | **CI1确定缺口** |
| Rust合同测试资产 | AFS组件产物合同只交付afs-meta/afs-node；普通Cargo测试不会自动把测试ELF留作下游制品 | 选中Rust合同还需同架构 `fuse_contract`、SHA和测试来源/依赖；当前无下游交接。不阻塞仅Python断言的最小冒烟；Full选择相应合同前必须补，不能把ARM64冻结ELF用于amd64 | 条件阻塞Full，不一概阻塞冒烟 |
| 目标K8s历史运行证据 | #208公共L0、[独立Full#71](https://buildkite.com/agent-dx/agent-dx-full-test/builds/71)完整11组，以及当天#220公共L0均有result/placement/cleanup证据；两个节点名为 `10.244.128.124`、`10.244.128.160`。公共manifest已有root＋privileged | 只能说明这些历史运行的ADX部署链/配置/清理成功；未包含新AFS runner或真实AFS产品场景，也不证明当前资源余量、RBAC/准入策略或FUSE能力 | 公共部署已有实绩；AFS未验 |
| 本轮直接K8s读取 | 本机既有管理kubeconfig仅有internal context，server为 `https://10.10.100.37:5443`；已尝试一次有10秒上限的 `GET /version` | 请求超时，无法经此配置取得/核对目标kubeconfig、当前worker/权限/资源。本轮未直接探测目标集群，不能推断目标集群宕机；既有CI成功也不能代替本机当前访问可用。未启VPN/隧道、未改网络或凭据 | **阻塞本轮直接环境盘点** |
| AFS环境准入 | worker已有对工具/文件系统/角色的检查；当前公共preflight面向runtime/bridge，Pod状态卷为emptyDir | 尚无目标Pod `/dev/fuse`、实际ext4数据路径、mount/UID/CAP_FSETID、角色连通和正常卸载的当前证据。runtime镜像配方未显式安装fuse3；是否实际已有fusermount3等工具未进容器检查，不能仅据配方判为必然缺失 | **CI0未放行；需后续受控验证** |
| 在线流水线默认值 | 本次API读取基础与Full流水线均指向官方仓，默认分支仍为 `refactor`；本机yr-bk默认配置是其它YuanRong项目，查询时已显式指定ADX | 不显式固定分支/commit可能取错版本；这属于配置风险，使用正确固定候选可避免，不能因此说整套CI不可用。本轮未改默认分支或触发/合入保护配置 | 已识别风险，非部署结论 |

**#208身份核对的限度**：release manifest的摘要与build manifest引用一致，AFS两ELF声明SHA在component/release清单中一致；SDK candidate与bundle同commit，bundle内package/SDK与对应清单一致，registry清单的bundle SHA一致，backend revision与当前固定sandboxd来源相同，三份镜像引用均为digest。原始制品索引仍在，但本轮不下载约97.6MB release和57.0MB backend，不把这些元数据一致写成二进制已重新验证或镜像仍可拉取。

**分支与开关边界（2026-10-31讨论确认）**：已通过上游 `git ls-remote --symref upstream HEAD` 实时确认ADX仓库默认分支为 `master`。AFS接入复用共享流水线，不修改其默认分支、也不新增AFS专属默认分支；线上默认值是否清理由ADX公共CI统一维护，不是本任务CI0/CI1的前置条件。构建branch/commit负责选择版本，现有 `ADX_WITH_AFS=0|1`负责AFS组件编译/包内容；AFS冒烟/Full还需复用公共profile补用例选择与结果接线，不能只打开编译开关就当测试已接通。

**后续顺序（交接已补）**：先明确用于CI1的ON产品候选＋测试代码身份，补K8s角色接入；环境维护侧先解决本轮只读访问路径，再读当前目标节点/权限/资源。设备与真实挂载能力只能由后续另行安排的隔离准入验证确认，本轮不创建探针Pod或挂载。Rust ELF可随Full接入补齐，不为它拖住纯Python冒烟范围。

依据：[候选组包/校验](../../.buildkite/package-e2e.sh)、[AFS交付文件集](../../build/release/component.py)、[runner输入合同](../../afs/tests/fixtures/multivm.py)、[harness范围](../../build/e2e/kubernetes/run.py)、[运行镜像配方](../../build/images/Dockerfile.e2e-runtime)、[现有runtime准入](../../build/e2e/preflight.py)。本节是编码前盘点快照；下方计划除上节已实现的交接子项外仍待实施。

### 第四步实施计划：组件、产品与门禁分别验收（CI0–CI3流水线内已通过，CI4及线上策略待完成或核验）

**先看顺序**：固定候选与CI环境 → 真实AFS组件读写/清理 → SDK实际使用AFS → 接入必需冒烟 → 扩展功能Full。沿用现有两条公共流水线；CI组件结果、ADX产品结果和最终门禁状态分别可追溯，前一批通过不冒充后一批已经完成。

本计划按 `02d1a06f` 检出核对。第二/三步的runner、worker、业务断言和结果协议已经存在，不再从旧脚本另做一套；此前ARM64冻结产物的20项通过只保留原身份，**不能代替正式amd64 CI候选、K8s能力或产品路径验收**。CI0/CI1已用Base #223新候选在Full #76/#77定向通过；CI2已在Full #81验证两个SDK模式；CI3已在#84/#85完成固定包组合门禁，根报告/JUnit/CI summary和最终作业均通过；CI4功能Full与线上触发/保护策略仍待完成或核验。

#### 最小闭环分批实施

| 批次／优先级 | 复用什么 | 这一批要补什么、放哪里 | 如何验收／通过能说明什么 |
| --- | --- | --- | --- |
| CI0／P0：候选和环境准入 | 统一包/SDK/backend/bundle身份检查、现有K8s manifest/preflight、AFS身份与worker准入 | 已通过：`build/e2e/prepare.py`、`afs_handoff.py`及K8s harness交接；#223/#76实际核验FUSE、ext4、UID/GID、网络/mTLS、真实bind/FUSE及正常清理。不自动改集群全局设置 | 在目标K8s用自有临时资源实际验证必需能力并清理；缺设备/错误架构/产物缺失明确阻塞。只能声称环境与输入可用，不能声称AFS功能通过；此前ARM64结果不能为amd64放行 |
| CI1／P0：AFS组件纵向闭环 | `afs/tests/run.py`、`suites/operations.py`、现有读写/权限/版本/清理断言及worker协议 | 已通过：`afs/tests/fixtures/kubernetes.py`角色接入和环境描述；#76原5项与加强的bind策略断言通过；公共K8s工具提供Pod/exec/copy与资源生命周期，复用同一worker执行M/A/B角色。复用本次已补的 `afs/tests/` 分发和已校验候选输入；不在目标Pod编译产品 | 先分别通过OwnerFs A本地egress bind↔B FUSE、DFS A发布/B远端读回与删除；证明真实挂载、peer路径、实际退出和无自有残留。旧#75已真实验证失败被保留、作业判红且正常清理；取消尚未实测。此时只报K8s组件定向通过，尚未接入必需门禁 |
| CI2／P0：最小ADX产品路径（已通过） | 已有两ADX node Pod、独立sandboxd、SDK `Mount(host_path=...)`、共享 `scenarios.py` / `sdk_smoke.py` | 已由 `build/e2e/kubernetes/afs_product.py` / `afs_sdk.py` 管理M/A/B，复用原SDK Local Mount及节点放置；OwnerFs/DFS分开运行，源身份与runtime挂载对应。仅SDK测试Pod使用已有basic_spec配置；普通UID实际降权且零capability，SDK Files接口root身份单独说明 | Full #81已通过两个SDK模式、各六项断言，三版本双向完整内容；实际EACCES/EROFS、删除/Environment回收及正常AFS/ADX/K8s清理通过。用mountinfo/device/inode及DFS peer 0→4证明路径；组件PASS不替代产品PASS |
| CI3／P0：必需冒烟接线（流水线内已通过） | 基础 `pipeline-package.yml: platform-e2e`、`.buildkite/run-e2e.sh`、公共L0/auth、JUnit及artifact上报 | 本分支已实现 `.buildkite/run-e2e.sh` → `afs_gate.py`，AFS ON正常入口串公共原profile、五项组件与两种SDK；OFF沿用公共入口。#82/#83汇总失败保留，#84/#85修复后整条通过；未因包装丢失原Full stop观测收据要求。仍须另核验ON触发范围、不可关闭/必需状态等线上策略，不改共享默认分支 | 已用明确ON候选在#85真实跑通必需冒烟；失败/缺项/错身份/清理失败判红由工具回归和真实失败运行共同举证，未执行不能变绿；OFF分支结果不被标为AFS通过。若webhook/必需状态未落实，只能称流水线内接通，不能称自动合入保护已完成 |
| CI4／P1：功能Full接线 | 独立 `pipeline-full.yml`、同一验收包装、公共11组及AFS现有基础/扩展断言 | 增加明确登记的CI Full选集、单Pod内核合同环境和跨Pod角色环境；按需加入C读者，补同目录恢复、权限/缓存、命名空间/busy合同。公共11组继续复用，AFS产品变体扩展相关行为；不机械复制每组、不直接用 `--all` 代替合同 | Full包含已登记冒烟必需断言；对应单Pod/跨Pod环境、选中Rust ELF和每个case都有实际证据。公共Full保持两worker检查，AFS跨worker声明另验角色实际放置。缺必需资产/拓扑不得删项后报Full；不外推Pod删除持久恢复、VM故障、长稳或性能 |

CI0/CI1的环境适配与第五步拿包输入可同时设计，做到后可先直接调用验证，不必等线上触发策略就绪；但CI3的真实调用、失败传播和线上条件分别验收，不能用本地运行补齐它们的结果。

#### 首批冒烟先复用哪些用例

| 选择范围 | 当前可复用的精确case／断言 | CI需要补的联动 |
| --- | --- | --- |
| 真实启动、跨角色数据与删除 | `ownerfs.cluster.read_write`、`dfs.cluster.read_write` | M/A/B的Pod身份/地址、OwnerFs真实egress、DFS新鲜读者与peer计数；沿用现有三版本小文件校验，不新增测量协议 |
| 权限与新版本 | `ownerfs.cluster.permissions`、`dfs.cluster.visibility` | 在Pod中保留owner/other数字身份、权限和版本断言；不得因root/privileged而省略拒绝例 |
| 拓扑与回收 | `cluster.topology_cleanup`，以及每case现有wait/cleanup结果 | K8s namespace UID、Pod UID、node名与角色映射；正常停止/卸载证据先于删Pod，不能用namespace消失掩盖产品清理失败 |
| 完整ADX最小用户链路 | 现有L0/auth调度、SDK命令/文件接口 | 已实现 `afs-sdk-ownerfs`、`afs-sdk-dfs` 两个必需产品模式，报告真实存储身份；Full #81各六项通过，组合入口继续核验 |

上述组件五项和SDK两个模式已登记并真实运行，入口分别为 `afs-component` 与 `afs-sdk`，没有复制到CI专属用例目录；整体必需包装的最终通过另验收。现有权限函数可能同时验证setid等行为，先完整保留，不为缩短冒烟私自删除原断言；首次实跑后再按稳定耗时和保护范围调整集合，任何删减均更新合同。Full再加入已有的namespace/restart/many_readers、单Pod内核顺序/busy以及适用的真实缓存合同。

#### 部署、目录和调用边界一次讲清

- **组件部署**：参考布局为本次隔离namespace中的M/A/B角色Pod，Full按需增C；本机双挂载/内核例另在一个受控Pod里运行。具体资源配额和放置在CI0准入时校准，不承诺现有worker容量自动足够。
- **产品部署**：复用原两ADX节点Pod，各自提供本节点AFS访问路径，Meta由fixture独立管理；实际AFS挂载必须出现在对应sandboxd的运行视图中。不会依赖不同Pod之间未经验证的FUSE挂载传播；组件环境与产品环境独立起停、结果隔离。
- **K8s身份**：当前 `kind=namespaces` 和 `kind=multivm` 有各自身份判据，不能把Pod地址填入旧模板冒充验证。新增K8s接入应登记实际Pod/worker身份和语义；可以复用exec/worker传输，不复制业务断言。
- **产品接入**：现有SDK已支持Local Mount和Managed Volume，后者仍要求后端先挂载；首批用受控Local挂载贯通路径，不新造AFS专属SDK接口。若当前产品无法按支持合同准备/消费该路径，记录明确的产品适配依赖再单独安排，不能假挂到普通目录让测试成功。Managed Volume生命周期组合属于后续Full/专项，按现有支持边界补。
- **资产分发**：当前 `stage_harness()` 在AFS ON时已显式携带`afs/tests/`运行代码/worker及身份清单，分别校验产品与harness；测试资产不进入正式产品包。Rust合同仍需编译阶段生成并冻结的同架构测试ELF及来源/SHA，不复用未知来源ELF、不让目标Pod临时补编译。
- **目录归属**：AFS断言/角色适配仍在 `afs/tests/`；公共产品fixture/场景、环境回收、结果编排在 `build/e2e/`；Buildkite仅在 `.buildkite/` 选集/调用。K8s共用工具不放AFS业务断言，也不为CI复制一套产品E2E。
- **准确阶段**：组件普通Cargo链不变；拟在 `platform-images` 后的 `platform-e2e` 接AFS真实组件＋公共产品验收，基础选择冒烟、独立Full选择功能全集。当前OBS上传早于L0的事实不因计划改变；上传、流水线验收及版本发布资格分别判断。

#### 第四步的最终出口与交付

交付应包含：K8s环境准入说明/模板、候选与测试资产清单、AFS角色接入、公共AFS产品变体、明确冒烟/Full注册、现有包装的调用与失败聚合、可直接复现的命令及真实运行报告。CI3通过后才可称“AFS必需冒烟已接”；CI4完整通过后才可称“支持范围内的AFS功能Full已接”。线上触发策略、amd64/ARM、组件/产品、基础/专项分别记录，不互相继承PASS。

事实入口：[AFS现有case](../../afs/tests/README.md)、[AFS角色编排](../../afs/tests/fixtures/multivm.py)、[worker](../../afs/tests/fixtures/worker.py)、[K8s harness分发](../../build/e2e/kubernetes/run.py)、[SDK挂载合同](../../platform/sdk/environment/python/README.md)、[Volume前提](../../platform/api/http/volumes.md)。这些支撑复用方案，不表示本轮验证了K8s或产品AFS。

### 先讲清五条原则

1. **第二步同guest多进程、第三步同VM多namespace；第四步使用CI自己的环境。** 当前x86构建作业在CI K8s集群的Pod中运行，产品E2E由部署作业向已有的目标K8s集群部署ADX。没有调用开发者VM的链路。
2. **用例可以复用，环境准备要适配。** AFS组件断言仍归 `afs/tests/`，CI也可以调用同一个 `run.py`；需要补K8s适配，不能直接把singlevm或SSH多VM配置交给CI。Buildkite负责调度、选集和结果门禁。
3. **冒烟保护集成底线，全量扩大功能覆盖。** 冒烟选真实启动、核心读写、基本拒绝和清理；全量再选缓存/顺序、命名空间、正常恢复、多读者等。两者均不自动代表长稳、精确性能或所有后端/硬件矩阵。
4. **AFS组件通过与ADX产品通过分别举证。** 前者直接验证AFS；后者必须通过公开SDK，证明真实产品已经使用AFS并正确完成用户行为。仅编译AFS、带上二进制或普通文件读写通过，都不能证明产品接入了AFS。
5. **选入门禁的必需项必须有明确结果。** CI环境由门禁维护者准备；缺FUSE/权限/拓扑时应明确阻塞或失败，不能静默跳过后报成功。本地因条件不足不跑，与CI必需项的结果语义不同。

总体环境与融合方向已对齐；下列具体选集和接线仍为**待补方案**。下面的现状仅指仓内已经检入的调用关系，不冒充线上最新一次执行结果。

### 当前ADX究竟在哪里、怎么跑

**事实：有两层K8s环境，测试节点是目标集群中的Pod。** 基础设施中的worker可能本来就是VM，但它们不使用开发者第二、三步的个人VM清单。

```text
Buildkite
  └─ CI执行K8s集群
       ├─ 构建Pod：Cargo测试、编译、组包
       ├─ 镜像准备Pod：固定包/SDK/backend → 镜像 → registry
       └─ 部署Pod：run-e2e.sh，持有目标集群kubeconfig
            └─ 已有目标K8s集群
                 └─ 本次独立 adx-e2e-* namespace
                      ├─ node1 Pod：ADX进程、sandboxd、真实runc/Execd
                      ├─ node2 Pod：ADX进程、sandboxd、真实runc/Execd
                      └─ 公开SDK场景 → 结果/日志 → 停服务并删除namespace
```

部署器负责创建本次namespace/Pod、配置和启动服务、执行用例、取证与清理；**不负责购买机器或新建K8s集群**。目标集群、可调度worker、registry与kubeconfig权限须事先具备。

公共产品的两个platform容器原本已是root＋privileged，不能把“新增privileged”写成通用前提。公共ADX preflight仍面向OCI runtime/bridge；本次用三个AFS组件Pod在#76实测了 `/dev/fuse`、ext4、OwnerFs真实bind/远端FUSE、DFS FUSE、非root权限及正常卸载。组件准入通过仍不能提升为SDK产品使用AFS已通过。

当前L0不要求两个Pod分处不同worker；Full在调度后强制检查两个不同K8s worker。代码核验的是不同worker节点名，不能据此断言云基础设施底层绝无共宿物理机。它也不能替代多VM独立guest内核、VM重启/断电等证据。

依据：[基础流水线](../../.buildkite/pipeline-package.yml)、[Full流水线](../../.buildkite/pipeline-full.yml)、[K8s部署器](../../build/e2e/kubernetes/run.py)、[Pod定义](../../build/e2e/kubernetes/manifest.py)。

### 当前阶段、入口与AFS覆盖（事实）

| 阶段 | 当前调用链与执行内容 | 当前AFS覆盖／缺口 |
| --- | --- | --- |
| 源码/工具检查，与组件构建并行 | `pipeline-package.yml` → `source-gate.sh` → API合同、`unittest discover -s build/e2e/tests`、`build/release/tests`、`make rust-check` | 有AFS模式的工程/发布工具合同；ON时加入AFS lint。不是实际挂载、组件集群或产品AFS功能测试 |
| AFS组件测试与编译 | `build-afs` 仅在 `ADX_WITH_AFS=1` 时选择 → `build-component.sh afs` → `component-tests.sh afs` → Cargo测试 → release编译 | 普通Rust UT及适用非ignored LLT已接；测试默认开启，可用 `ADX_COMPONENT_TESTS=0` 关闭。没有自动运行所有feature、ignored真实FUSE或外置Python集成驱动 |
| 组包与安装检查 | `platform-build` → `package-components.sh`，等待对应组件、source-gate、SDK/admin候选 | ON/OFF清单、AFS组件身份/二进制和安装合同检查已有；不启动AFS文件系统 |
| 包上传 | `publish-amd64` → `publish-amd-package.sh`，等待组包 | **当前OBS上传早于产品L0**；不能说“上传的包都已过E2E”。整体流水线成功才包括后续必需作业 |
| E2E镜像准备 | `platform-images` → `package-smoke.sh` → `package-e2e.sh` → `prepare.py` / `kubernetes/publish_images.py` | 固定base/SDK并校验包模式；本次新增ON测试资产及`afs-tests.json`身份交接，#72真实镜像构建/校验/发布已通过。`package-smoke.sh`名字不表示在此执行用户冒烟 |
| 基础产品冒烟 | `platform-e2e` → `run-e2e.sh` → `build/e2e/kubernetes/run.py --profile l0` → 共享场景 | 当前选择 `l0`＋`auth`：创建/查询Environment、命令stdout/stderr/退出码、二进制文件往返、删除及鉴权隔离。没有明确配置并验证真实AFS存储的场景 |
| 独立功能Full | `agent-dx-full-test` → `pipeline-full.yml` → 准备或复用固定镜像 → 同一 `run-e2e.sh` → K8s `run.py --profile full` | 当前默认11组公共功能；与基础L0是不同流水线。没有AFS组件全量/产品存储变体的完整接线 |
| 定向专项 | `ADX_E2E_TARGET_CASE` → 同一K8s入口；多选 `ADX_E2E_TARGET_CASES` → `targeted_suite.py` → 每例独立namespace | 已有公共定向机制；不是“有AFS脚本就会自动被选中”。长稳与受控性能仍有公共能力缺口 |

当前AFS Cargo选择是：`afs`、`afs-client`、`afs-error`、`afs-logging`、`afs-metrics`、`afs-protocol`、`afs-tracing`、`afs-transport`，用 `cargo test --locked -j ... -p ...` 执行。没有 `--ignored` / `--all-features`；不能从静态定义数量推导本次运行数量。普通合同文件内的非ignored测试会按其cfg/feature参与；真实挂载等显式ignored部分不自然执行。

公开产品冒烟的具体业务入口：`build/e2e/run.py` 的profile/调度 → Pod内 `build/e2e/scenarios.py l0` → `sdk_smoke.py`；鉴权为 `scenarios.py auth`。已有文件测试使用SDK读写 `/tmp/adx-e2e.bin`，该事实本身不证明文件落在AFS上。

Full默认11组为：`sdk`、`data-plane`、`lifecycle`、`auth`、`capacity`、`placement`、`local-first`、`node-failure`、`sandboxd-restart`、`restart`、`stop`。其它可选故障、`mixed-soak`、`load-performance`等不是默认全部执行。现有短soak默认300秒，不能称为全天长稳。

**范围限制**：上述主要是x86路径。ARM构建由macOS agent上的Linux ARM64 Docker builder承载，测试默认关闭，需 `ADX_ARM_TESTS=1` 开启；当前正式K8s E2E路径为amd64。upstream合入/PR/webhook、分支过滤、nightly及必需状态的线上设置本轮未查询；仓内只能证明流水线启动后运行什么，不能保证每次合入都已选中AFS ON。

依据：[组件测试](../../.buildkite/component-tests.sh)、[组件构建](../../.buildkite/build-component.sh)、[源码门禁](../../.buildkite/source-gate.sh)、[镜像准备包装](../../.buildkite/package-smoke.sh)、[产品执行包装](../../.buildkite/run-e2e.sh)、[共享profile](../../build/e2e/run.py)、[业务场景](../../build/e2e/scenarios.py)、[SDK冒烟](../../build/e2e/sdk_smoke.py)。

### 第二、三步哪些断言进入冒烟／全量（待补方案）

原则是复用**可追溯的行为断言**，用CI环境执行；不是把VM整套部署原封不动搬进K8s。下表的LB/MV编号仅标注来源；Pod运行必须记录自己的环境和拓扑，不能报成“多VM通过”。同一代表行为不因来自两个集合重复运行两遍。

| 来源与要拦截的问题 | 冒烟：最小代表 | 功能全量：扩展覆盖 | 现状与需要补的部分 |
| --- | --- | --- | --- |
| LB01、MV01：真实AFS进程/挂载/路径是否存在 | OwnerFs bind＋远端FUSE、DFS真实挂载；同候选、真实网络寻址 | 保留准入并记录跨worker放置、完整角色和节点身份 | M/A/B跨worker、FUSE/ext4/mTLS与OwnerFs native bind已在#76通过；SDK产品两模式及流水线内必需冒烟已在#81/#85通过 |
| LB02/03、MV02：基本操作与OwnerFs互通 | 小文件目录创建、读写/长度/哈希、fsync后重开、rename/删除；OwnerFs双向读写 | 覆盖/截短、属性/目录和边界错误；多个版本变化 | 现有runner已有核心读写和最低操作链；资产/角色接入及OwnerFs读写在#76通过；SDK挂载身份、双向fsync/重开、EACCES/EROFS/删除及回收已在#81/#85通过 |
| LB05、MV04：DFS发布与远端可读 | R1下一写一远端读，成功fsync、新OPEN版本/长度/哈希和删除；证明确实发生远端取数 | 扩充读者/版本组合，保留真实数据路径证据 | DFS A/B跨Pod读写/远端计数在#76新候选复测通过；默认冒烟/Full登记待补 |
| LB04、MV05部分：权限拒绝与新鲜度 | 一个授权非root读写、一个越权新OPEN拒绝、ENOENT；短新版本重开校验 | setid/CAP_FSETID、权限修改/恢复、缓存拒绝/刷新等完整合同 | OwnerFs UID/setid断言、真实DFS权限断言可复用；OwnerFs/DFS的CI权限均在#76通过；产品挂载适配未接 |
| LB06、MV05部分：内核顺序与缓存 | 只保留上面短重开代表；不默认执行全部16MiB缓存探针 | 显式选择真实内核顺序、增长/截短、真实DFS keep-cache及计数断言 | 已有Python真实DFS缓存合同；选入Full的Rust合同须显式提供ELF/依赖并执行；同机双挂载例在受控单Pod运行 |
| MV03：命名空间和对象身份 | 上面基本rename/unlink已选；不追加整组 | 旧FD经rename/unlink/名称复用仍正确、并发单一Home、跨root EXDEV | 现有OwnerFs cluster命名空间断言/worker已统一；K8s身份/拓扑适配已接；完整命名空间case未入本次定向 |
| LB07、MV06：正常停启与恢复 | 暂不纳入最小产品冒烟 | 正常全停/同目录恢复、Meta/Home正常进程重启，已确认数据/权限可恢复 | 组件OwnerFs/DFS正常恢复已实现并有限实测；K8s恢复联动待接；emptyDir下进程重启保留目录，不等同于Pod删除或VM断电恢复 |
| LB08、MV07：回收与挂载残留 | **所有执行均必须正常排空、卸载、退出且无自有残留** | 再加入busy拒绝、异常退出/中断后的结果与清理核验 | 单机退出/mountinfo断言可复用；CI须在删除namespace前保留AFS正常卸载证据，不能用删Pod掩盖失败 |
| LB05完整、MV08：一写两独立读者 | 单远端读者代表即可 | 一写两读者并发、同版本/删除及各读者路径证明 | 当前公共只有两ADX node Pod；需显式定义额外AFS Node/读者角色和放置，不把VM数等同于Pod数；未支持前须列缺口 |
| 公共ADX产品行为：上述组件通过是否真的惠及用户 | 在已有L0/鉴权场景加入AFS支持的存储变体，证明SDK实际写入/读回/删除经过AFS、隔离/清理正确 | 在既有11组中按支持边界扩展AFS相关生命周期、放置、故障/恢复和资源回收 | 产品挂载/配置/路径断言缺失；不能把AFS组件用例替代SDK场景，也不能要求与AFS无关的每组都重写 |

Full包括已登记的冒烟必需断言；扩大覆盖不等于 `run.py --all` 无条件执行所有环境/专项。RDMA、S3/etcd矩阵、VM断电、HA、POSIX大套件、小时/天级长稳和精确性能继续按能力与判据登记专项；不自动塞进本轮冒烟/功能全量。R1正常恢复也不承诺Home停机时未缓存数据仍可读。

### 接线落在什么阶段、用哪个入口（待补方案，不是当前实现）

**保留现有两条公共流水线，AFS真实集成放在固定包和镜像准备完成后的验收阶段。** 构建阶段继续跑现有Cargo测试；不让普通构建容器默认承担真实挂载。

```text
基础 pipeline-package.yml / platform-e2e（已有）
  run-e2e.sh（已有，待扩展AFS分支）
    ├─ AFS ON：afs/tests/run.py 选择冒烟断言＋CI K8s环境适配（待补）
    └─ 现有K8s run.py --profile l0 ＋真实AFS产品存储变体（变体待补）

独立 pipeline-full.yml / platform-e2e（已有）
  run-e2e.sh（同一包装，待扩展AFS分支）
    ├─ AFS ON：同一AFS runner选择功能全量＋CI K8s环境适配（待补）
    └─ 现有K8s run.py --profile full ＋真实AFS产品存储变体（变体待补）
```

这定义了拟接入的阶段和入口，不新增一套AFS产品流水线。AFS组件环境与产品环境应隔离运行/清理，复用固定候选包及镜像；任一必需项失败须使该验收作业不通过。当前定向组件布局为M/A/B独立Pod、A/B跨worker；是否拆并行job及默认冒烟/Full标签与选集仍待后续批次明确。

| 优先级 | 接线缺口／归属 | 完成后才能声称什么 |
| --- | --- | --- |
| 已完成／CI1 | AFS：同一 `afs/tests/run.py`、suites、worker和结果协议，已增加K8s角色适配 | #76验证同一断言在目标K8s运行；与本地VM分别记证据 |
| 已完成／CI0 | 公共K8s＋AFS适配：目标worker上的FUSE/ext4/UID权限/mTLS、OwnerFs bind及正常清理已实测 | 当前组件拓扑准入通过；不据此认定完整产品拓扑也已使用AFS |
| 已完成／CI0；P1补ELF | 制品/harness：新包、SDK、镜像、测试文件与身份清单交接已真实通过；选中Rust内核合同时另交付ELF | #223/#76确认候选与实际AFS二进制/测试资产身份；目标节点不重编译产品 |
| P0 | 公共验收包装：扩展现有 `run-e2e.sh`/验收编排，注册AFS冒烟与必需结果；公共E2E补支持的AFS存储配置及路径断言 | AFS ON冒烟既测到AFS本身，也测到ADX实际使用AFS |
| P0 | CI策略/线上配置：明确哪些变更必须覆盖ON候选、测试禁止关闭的门禁配置、触发与必需状态；保留OFF包边界检查 | 不会因默认OFF或关闭测试，使AFS/共享改动长期没有AFS门禁；当前自动触发规则待核实 |
| P1 | 复用Full入口扩大选集；将已有正常恢复、权限/缓存/命名空间、多读者断言接入CI，并补产品侧缺口 | 已登记支持范围内的AFS功能Full；未接项须明示，不以公共11组通过代替 |
| P1/P2 | 复用已有定向机制，补AFS专项负载/故障/兼容判据；公共长稳/精确性能能力不足另登记 | 对应专项结论；不因为基础流水线成功就自动获得这些保证 |

**已有规划与本轮待补的区别**：仓内既有门禁文档提出nightly/release、Full和1–24小时soak等目标；独立Full和定向调用机制确实已实现，但定时触发未核实，完整全天长稳仍无闭环。AFS runner及本地基础集合已在第二/三步实现；CI0/CI1组件适配是本次新增并实测；产品变体、必需冒烟/Full和长期能力仍待补，不能标成ADX原有能力。

总体方向已确认，CI0交接与CI1组件适配/定向入口已实现；具体选集、环境适配和其余接线按本页优先级推进。拿固定包直接验收的现状及待补见 [第五步规范](#step-5)。#73/#75旧包故障保留；修复后#223新包和公共L0、#76组件定向通过，下一P0为CI2 SDK产品路径。准确计数与清理结果见本节开头。


<a id="step-5"></a>
## 第五步：固定包的全量、专项与长稳

**本步原则、现状与优先级**：原则：固定包＋合格环境，开发者/测试/AI可复用公共E2E自主闭环。现状：功能Full/专项已有，长稳/受控性能有缺口。待补：拿包指引、AFS全量/专项、兼容测量工具收口、公共长稳与AFS负载。优先级：P1全量/专项、P2长稳/性能。

**状态**：五步总体逻辑已确认；以下目录/命令中标注目标或待补者尚未实施。

### 第五步实施计划：先能拿包复现，再扩专项、长稳和性能（2026-10-31，未实施）

**先看顺序**：固定输入与使用说明 → 自主跑功能Full → 定向专项 → 连续长稳 → 受控性能。复用第四步的候选清单、AFS部署/产品变体与选集，不为开发者或测试人员再建另一套部署工具。CI调用与直接调用共享代码，权限/凭据从执行者环境显式提供；后者不依赖Buildkite私有Secret、个人VM地址或在线构建会话。

第五步可以和第四步并行准备输入、文档及直接复现；其AFS组件部署依赖CI1、产品路径依赖CI2、完整功能集依赖CI4的选集/适配，但不要求先完成线上webhook设置。直接运行通过不反向宣称CI门禁、自动触发或发布资格已完成。

#### 最小闭环分批实施

| 批次／优先级 | 复用什么 | 这一批要补什么、放哪里 | 如何验收／通过能说明什么 |
| --- | --- | --- | --- |
| PK0／P1：拿包与环境操作闭环 | 现有 `prepare.py`、`kubernetes/publish_images.py`、bundle与镜像复用机制、K8s/本地Docker说明及CI0输入合同 | 在公共 `build/e2e/README.md`、`kubernetes/README.md` 补固定包＋SDK/backend/测试代码/ELF/镜像的配套清单，给出不带秘密的AFS配置示例、准入命令、全量/专项命令与结果阅读/回收说明；产品包与harness分别标识，缺项明确拒绝 | 在一套已有合格环境上，由直接CLI按文档检查输入→准备或复用镜像→部署→一组AFS读写→取证→清理；错误SHA/模式/架构/缺依赖拒绝，不临时编译产品。此时只算可自主复现一个case，不算完整Full |
| PK1／P1：固定版本功能Full | CI1/CI2的AFS环境与产品变体、CI4的Full选集、现有K8s `run.py --profile full` 和单宿主 `run.py --profile standalone` | 将AFS模式/候选/环境选择通过公共CLI显式传入；复用同一注册和结果协议，补支持范围内的SDK挂载读写、权限、新版本、正常恢复/清理。K8s和Docker声明各自准入，不强行要求一套环境兼具所有能力 | 选定固定包，在其实际支持的目标拓扑跑完整必需集；公共基础＋AFS所选变体逐case及cleanup通过，才能报对应Full。另按文档独立重放一个固定case，身份与结果可追溯；不要求失败后反复重跑来取得通过。Docker若纳入支持，再独立验收，不能继承K8s PASS |
| PK2／P1：定向故障/压力与兼容专项 | 现有 `--case`、`targeted_suite.py`、进程重启/压力/短soak，AFS现有恢复/权限/命名空间及标准套件adapter | 先登记三类代表：AFS Meta/Home正常进程重启后的确认数据恢复；ADX进程重启期间的AFS文件访问/回收；有界并发/容量压力下的内容/错误/资源一致性。产品类在公共E2E，纯AFS合同仍由组件runner执行；兼容adapter补当前候选/挂载/工具版本与完整计数 | 每类一次事先选定的目标运行，故障前/期间/后断言和恢复/清理证据完整；不同case独立结果。R1不可达允许明确错误，不承诺未支持的HA。标准套件必须有实际执行计数；旧ARM64/Full ENV限制不自动解除，若未补相应能力就明示缺口 |
| PK3／P2：公共连续长稳 | `mixed_soak.py`的命令/文件/创建删除负载基元、现有SDK调用与结果/清理方法 | 在公共 `build/e2e/` 增加长稳profile/配置，贯通持续时长、父子进程超时/保活与取消、持续资源趋势、错误/故障策略和证据输出；AFS接入版本/内容/持久确认及FD、内存、磁盘、挂载/积压等观测和预定判据。不是另建AFS长稳平台 | 先以短资格运行验证定时、计数、采样、取消/失败清理，再做一次预先冻结时长的真实持续验收；24小时结论须实际完成24小时及预定判据。短资格/多次重新部署/三小时suite总预算均不能代替。没有冻结资源趋势/错误判据时不进入正式PASS验收 |
| PK4／P2：受控性能验收 | 公共 `load-performance`诊断负载、AFS `perf/dfs_compare.py`已定义的计时/缓存/校验/配对方法 | 将共享集群短诊断与正式基准分开；固定版本、资源、后端、负载、缓存条件、测量边界、观察器与比较基线/阈值；纯文件系统复用AFS工具，ADX完整用户链复用公共专项。配置与结果记录全部条件，不复制一套对照工具 | 先验证内容/长度/EOF及实际路径，再按冻结合同的轮数/顺序一次完成全部原始样本；输出失败、波动、绝对数和配对结果。功能正确、测量完成、稳定性及性能达标分别判定；不从90秒采样、短Full或挑出的快样本推导性能通过 |

PK0/PK1先解决“拿到某个包就能按说明完成验收/复现”的使用闭环；PK2扩问题类型；PK3/PK4的公共能力与判据先具备，再开展正式运行。各批完成条件是未来验收计划，当前未执行其中任何步骤。

#### 最小交付物与复用位置

| 交付物 | 放在哪里／通过哪个入口使用 | 与第四步复用的关系 |
| --- | --- | --- |
| 一份拿包和环境说明、无秘密配置示例 | 现有公共E2E README / K8s README；实际包、env、kubeconfig及证据在执行者仓外目录 | 使用CI0同一输入/能力合同；现有bundle可复用，不要求每次重建镜像 |
| 产品功能Full和定向case | 现有 `build/e2e/run.py`、`kubernetes/run.py`、`scenarios.py`及产品fixture；AFS角色/断言继续复用 `afs/tests/` | 与CI1–CI4共用；直接CLI与Buildkite只更换调度/凭据来源，不复制业务测试 |
| 多专项编排与case结果 | 现有 `kubernetes/targeted_suite.py`；组件专项直接用AFS runner，各自汇入可追溯结果 | 支持的产品case沿用现有选择/namespace清理；不能把组件结果命名成SDK产品通过 |
| 长稳/受控性能扩展 | 公共 `build/e2e/` 负载与运行编排、AFS适用负载/指标和已有测量工具 | 是公共能力扩展，AFS提供实际存储变体；不强行纳入默认L0/功能Full |

#### 环境、预算、结果和磁盘验收

- **环境责任**：K8s、Linux Docker、真实SSH多VM、条件硬件分别选择并准入；先交付已支持路径，未验证路径明确标记。第三步已有namespace结果不提升为K8s、真实SSH多VM或故障隔离验收；公共三VM完整自动部署仍缺，不列作首批一键能力。
- **时长链路**：当前mixed-soak是300秒、函数上限900秒，调用方也有超时；direct targeted suite总预算最多3小时、CI还有自己的作业预算。PK3必须明确采用公共长稳入口或贯通调整整条预算链，不能只加一个时长参数就声称可跑24小时。实际持续负载时长与部署/准备/清理时间分别记录。
- **结果合同**：固定产品/SDK/backend/镜像/测试commit与SHA，记录选集和环境；保留逐case原始结果、首个错误、资源趋势及清理结论。基准和候选分开，完整与定向分开；BLOCKED/缺计数/采样缺失/清理未知不得通过，不能将不同运行的绿色子项拼成一次完整PASS。
- **性能观察**：复用既有测量合同，记录观察器与缓存/预读状态；避免用同数据盘高频进度写污染计时。阈值和样本数在正式运行前冻结，失败不自动补样或更换负载；不把共享集群的吞吐统计当固定硬件SLA。
- **磁盘闭环**：每批开始前估算输入、测试正文/副本、日志/趋势和清理余量；明确数据复用或验后回收。结束前逐目录核对归属，保留一份必要身份/输入摘要、原始结果/日志及失败记录，清理可确认重复传输、解包、成功用例可再生数据；记录清理目录、前后占用和保留项。唯一冻结二进制、必要失败现场/用户指定保留数据不自动删除，不全局prune或清空共享缓存。

#### 第五步的最终出口

PK1通过才能声称“该固定版本、该拓扑的AFS产品功能Full已验收”；PK2按专项分别出结果。PK3只认实际完成时长及全部持续性判据，PK4只认其冻结基准与性能合同；这两项不因功能Full通过而自动完成。已发布的说明应让人或AI在不依赖私有CI状态的前提下重复部署和选定case，定位问题并正常回收。

事实入口：[公共E2E](../../build/e2e/README.md)、[K8s部署](../../build/e2e/kubernetes/README.md)、[现有负载](../../build/e2e/load/README.md)、[短soak源码](../../build/e2e/mixed_soak.py)、[现有suite预算](../../build/e2e/kubernetes/targeted_suite.py)、[AFS测量合同](../../build/e2e/afs/perf/README.md)、[标准兼容工具边界](../../build/e2e/afs/acceptance/README.md)。完整长稳和受控性能仍是待补，不是本轮新增的现有能力。

### 先讲清使用逻辑

**第五步是拿固定版本做进一步验收：开发者、AI协助的开发者和测试人员都能调用仓内入口，不必等待测试人员代跑。** 第四步关注候选版本的共享门禁；第五步关注某个版本、更大范围或某类问题。两者可以使用同一套公共用例和K8s部署器，也可以由同一条Full流水线执行，不按执行者身份另造一套工具。

1. **先选定版本和要验证的范围。** 固定产品包、独立SDK候选、backend与镜像，选择功能Full、定向故障/压力或其它专项；结果不能越过本次实际选集和环境范围。
2. **先有合格环境，再交给脚本部署。** 有现成目标K8s可直接复用；没有时需要环境维护者另行提供。现有脚本不创建云机器/K8s集群。只有一台合格Linux Docker主机时，也可做单宿主完整产品功能复现。
3. **公共E2E自动部署、运行、取证和清理。** 在提供的宿主/K8s中创建本次隔离环境，使用包内二进制和已安装SDK，启动ADX及真实runtime，执行用户行为；不是只探测进程是否存活。
4. **Full、专项、长稳分别出结论。** Full是已登记功能集合；专项检查选择的故障/负载/设备条件。连续小时/天级长稳需要持续负载与资源趋势合同，不能用一批短测或重复建环境代替。
5. **AFS融入公共产品验收，保留组件专项归属。** ADX产品场景仍归 `build/e2e/`，增加真实AFS存储变体及相关断言；纯文件系统兼容、恢复等组件合同归AFS测试体系。复用现有工具，具体缺口单独列出。

以上为已确认的使用逻辑。下面的“事实”表示已有代码和调用，不表示本轮已经完成运行验收；也不推断仓外测试团队一定有或没有另一套系统。

### 环境怎么准备、脚本负责到哪里（事实）

| 现有路径 | 使用者/环境维护者先准备 | 脚本自动完成 | 当前能力边界 |
| --- | --- | --- | --- |
| 公共K8s功能Full/专项，优先复用已有目标环境 | 至少两个可调度Linux worker、匹配架构、跨节点网络和registry访问、允许privileged/nested runc的能力、可创建namespace及exec/copy/log/删除的kubeconfig；固定bundle与镜像引用 | 发布器可推送镜像；runner建独立namespace、两个ADX节点Pod及Service/Secret，同步harness、启动ADX和独立sandboxd、运行SDK、取证、停服务并确认清理 | Full检查两个不同K8s worker节点名；不证明云底层物理宿主分离。普通emptyDir不证明Pod删除后持久恢复。AFS专门FUSE/ext4/权限准入尚缺 |
| 单机Linux Docker产品复现 | 专用Linux主机或Linux VM、同架构Docker、privileged/cgroup/网络能力、固定bundle；按OCI/EROFS模式准备对应内核能力 | runner创建两个privileged节点容器和网络，启动真实ADX/runtime，运行共享11组并回收 | `standalone`是单宿主两个逻辑节点，不是跨VM/跨worker Full；不是在macOS宿主直接运行内核验收 |
| 公共ADX三VM产品拓扑 | 一control＋两worker VM、SSH、固定安装包/SDK、配置/TLS、已部署ADX和两个sandboxd | 已有inventory、若干用例driver、suite/汇总/verify；可对已部署环境执行子集 | **完整自动部署adapter仍缺**；不能宣称提供VM后已有统一一键完整部署验收。不同于第三步AFS专有Meta/Node组件拓扑 |
| 条件专项环境 | 按例增加动态StorageClass/PVC、runsc、KVM/Firecracker kit、设备与驱动或可控网络故障能力 | 已有对应条件driver和部分接线 | 条件未满足不等于基础Full覆盖。GPU/NPU有独立SDK验证工具，但普通K8s manifest未提供设备部署fixture |

目标K8s文档已有每worker 4vCPU/8GiB、至少30GiB可用磁盘等**建议值**；这是当前普通ADX fixture的运行建议，不是实测AFS容量准入。AFS接入后要按角色/数据/缓存重新核验。提供已有合格集群时，开发者无需另建个人VM；选择Docker或真实多VM拓扑时，才需要对应宿主/VM。

当前公共runner会创建并管理自己的测试部署；不是只给任意现网集群一个API地址，就能对它无改动执行默认11组和故障注入。三VM子集及设备等driver对已有部署的使用方式，要遵循各自环境和独占边界。

依据：[K8s前提与资源建议](../../build/e2e/kubernetes/README.md)、[Pod定义](../../build/e2e/kubernetes/manifest.py)、[公共Docker入口](../../build/e2e/README.md)、[三VM现状](../../build/e2e/multivm/README.md)、[设备专项](../../build/e2e/device/README.md)。

### 拿到包还需要哪些输入（事实）

**当前不是只有一个产品压缩包就能直接跑所有E2E。** 使用者需要公共测试harness，并提供匹配的SDK wheel及candidate身份、固定backend、运行时基础镜像和本次所需的条件fixture。已有验证过的bundle/registry镜像时可复用；测试运行节点不重新编译产品。

本地镜像准备入口是 `build/e2e/prepare.py`，必需输入包括 `--package`、`--backend`、`--sdk-wheel`、`--sdk-candidate`、`--runtime-base`、`--execd-base`、`--output`。它验证输入并组装测试镜像/bundle，需要有Docker的准备环境。K8s再通过 `build/e2e/kubernetes/publish_images.py --bundle ... --repository ...` 发布不可变镜像引用；准备/发布和目标部署可以在不同机器完成。

也可复用Buildkite准备产物：独立Full流水线支持精确base/SDK构建ID，或用 `ADX_E2E_ARTIFACT_BUILD`＋`ADX_E2E_ARTIFACT_COMMIT` 复用已有镜像bundle。直接脚本接受已取得的文件/镜像输入，不会自动替用户从任意位置找到包和凭据。产品commit与测试harness commit分别记录，不能把“固定包”误解成测试代码也一定相同。

AFS ON候选须使用明确包含AFS的统一包并校验模式；“包里有AFS”与“产品配置真正使用AFS”仍是两项证据。

依据：[prepare输入](../../build/e2e/prepare.py)、[镜像发布](../../build/e2e/kubernetes/publish_images.py)、[现有Full包装](../../.buildkite/run-e2e.sh)、[Full流水线](../../.buildkite/pipeline-full.yml)。

### 全量、专项、长稳：当前哪些已有（事实）

| 目标 | 当前入口和覆盖 | 已有／没有的边界 |
| --- | --- | --- |
| K8s功能Full | `build/e2e/kubernetes/run.py --profile full`；或独立 `agent-dx-full-test` 流水线代跑 | 已有部署、默认11组、逐case/JUnit/清理结果；不是所有optional case、兼容套件、性能和长稳总和 |
| 单宿主产品功能集 | `build/e2e/run.py --profile standalone` | 已有共享11组和自动部署/清理；可用于固定包复现，不宣称跨worker Full |
| 单个故障/功能专项 | K8s `run.py --profile full --case <name>`；Docker只支持其自身注册的case | 已有Redis/Coordinator/API入口重启、响应丢失、网络分区等定向选择；实际故障范围以driver为准，不等于整VM断电或整节点HA |
| 多个定向专项 | `build/e2e/kubernetes/targeted_suite.py --case ... --case ...`；CI用 `ADX_E2E_TARGET_CASES` | 已有共享固定bundle、每例独立namespace/结果和清理后续跑；direct总预算≤10800秒。预算不是持续负载时长，清理不明不得继续 |
| 短混合稳定性 | `--case mixed-soak` → `mixed_soak.py` | 已有持续300秒命令/文件和创建删除混合负载、错误/计数/延迟及清理；底层函数仅允许1–900秒，公共入口没有任意小时级时长选项，不在默认Full内 |
| 短时性能采样 | K8s `--profile full --case load-performance` → `load/performance.py` | 已有90秒吞吐/延迟样本、P50/P95/P99等；没有对未指定集群强加固定SLA，不等于受控性能回归/AFS专用性能验收 |
| 容量压力 | K8s `--profile full --case load-pressure` | 已有两节点容量占满、8个请求排队后释放、归属和回收断言；是容量压力场景，不是持续高压长稳 |
| 小时/天级连续长稳 | 既有门禁文档 `FD-SOAK-01` 规划1–24小时、反复故障与无资源增长 | **规划已有，当前没有对应完整连续执行/资源趋势判据/中断恢复闭环**。不能用重复运行5分钟、定向suite三小时预算或本地历史记录冒充 |
| 条件专项 | PVC Redis Pod替换、异构runtime、Firecracker、GPU/NPU工具等各自入口 | 部分driver/接线已有；需额外资产与环境，部分目标环境验收仍待完成。普通Full不自动覆盖 |
| AFS组件兼容/测量资产 | 当前 `build/e2e/afs/acceptance/` 标准套件adapter，`build/e2e/afs/perf/dfs_compare.py` 双口径对照等 | 工具已有；旧acceptance runner ARM64/root guard、Full ENV verifier始终BLOCKED；perf依赖外部fixture/挂载与固定基线，不创建机器/后端。不能称为公共产品全量/长稳已融入 |

默认11组是：`sdk`、`data-plane`、`lifecycle`、`auth`、`capacity`、`placement`、`local-first`、`node-failure`、`sandboxd-restart`、`restart`、`stop`。定向结果标记为targeted，不能因使用 `--profile full --case ...` 就汇报“完整Full通过”。

依据：[profile与结果](../../build/e2e/run.py)、[定向suite](../../build/e2e/kubernetes/targeted_suite.py)、[短soak](../../build/e2e/mixed_soak.py)、[负载合同](../../build/e2e/load/README.md)、[性能代码](../../build/e2e/load/performance.py)、[已有长稳规划](test-inventory-and-buildkite-gates.md)、[AFS旧验收限制](../../build/e2e/afs/acceptance/README.md)、[AFS测量工具](../../build/e2e/afs/perf/README.md)。

### 使用者实际调用哪个入口（现有命令形状）

下面仅展示入口。示例路径须替换为事先取得的真实输入；输出目录必须新建，**本轮未执行**。

```sh
# 已有Linux Docker宿主及已准备的bundle：单宿主完整产品功能集
python3 build/e2e/run.py \
  --bundle /path/to/bundle --profile standalone \
  --output /path/to/new-standalone-results

# 已有合格目标K8s及发布镜像：功能Full
python3 build/e2e/kubernetes/run.py \
  --bundle /path/to/bundle/bundle.json \
  --registry-images /path/to/bundle/registry-images.json \
  --kubeconfig /path/to/kubeconfig --profile full \
  --output /path/to/new-full-results
```

K8s单个专项使用同一条命令增加 `--case coordinator-restart` 或 `--case mixed-soak` 等，换新的输出目录。多专项入口为：

```sh
python3 build/e2e/kubernetes/targeted_suite.py \
  --bundle /path/to/bundle/bundle.json \
  --registry-images /path/to/bundle/registry-images.json \
  --kubeconfig /path/to/kubeconfig \
  --case load-performance --case load-pressure --case mixed-soak \
  --budget-seconds 10800 --output /path/to/new-targeted-results
```

私有registry/context/节点限制按现有CLI加参数；`redis-pod-restart`有独立PVC准入且不能直接混入上述多选suite。选择Buildkite代跑则使用现有Full流水线和 `ADX_E2E_TARGET_CASE` / `ADX_E2E_TARGET_CASES`，不用再写一套业务用例。

结果看 `result.json`、`case-results.json`、JUnit、case专项报告、部署身份/放置及清理证据；目录内仅有日志或Pod Ready不能算通过。没执行的必需项、失败、超时、无法确认清理都不能升级为完整通过。拿到失败证据后，开发者可固定同一包和case复现，修改后的新包须另记身份。

### AFS怎么融入、还补什么（待补方案）

| 优先级 | 复用的现有能力 | AFS待补／公共缺口 | 用例归属 |
| --- | --- | --- | --- |
| P0 | 第四步同一包、bundle、K8s部署/清理、SDK场景与结果协议 | 将第四步AFS部署适配和产品存储变体开放给直接调用；明确ON包、OwnerFs/DFS配置、FUSE/ext4/UID、挂载/路径证明及正常清理前提，不绑定CI私有默认值 | 产品fixture/场景继续 `build/e2e/`；AFS组件runner及K8s adapter已实现；产品目标环境适配和直接运行说明待补 |
| P0 | 公共默认11组及定向case选择 | 补实际AFS支持的文件读写/隔离/持久性/正常恢复/清理断言；按支持边界纳入Full，组件通过与产品通过分别记录 | ADX产品行为融入共享场景；直接AFS合同复用第二/三步用例 |
| P1 | 公共故障、压力、短soak、90秒采样 | 接入AFS读写工作负载，保留fsync/版本/内容校验、错误计数和相应资源观测；原ADX故障不自动等于AFS Meta/Home故障矩阵 | 产品专项在公共E2E；纯Meta/Node故障在组件集合 |
| P1 | AFS现有pjdfstest/LTP/FSx adapter与对照测量工具 | 整理当前包/环境/基线身份和完整计数；解除需解除的平台绑定与Full ENV缺口；复用外部fixture，不复制另一套实验室部署体系 | 当前工具原址保留；组件归属沿已对齐AFS测试规范，迁移另行实施 |
| P1/P2 | 已有长稳目标与短负载基元 | **公共先补**可配置持续时长、连续负载、持续资源趋势/阈值、错误与故障策略、取消/清理和中断证据；**AFS再补**相应负载与挂载/FD/内存/磁盘等指标判据。不得宣称已有一键24小时 | 公共长稳能力融入ADX，AFS提供适用负载和观测，不另造AFS独家长稳平台 |
| P1/P2 | 公共负载采样与AFS已有测量方法 | 区分共享集群诊断与受控基准；固定资源/版本/缓存/数据/比较基线、计时边界及门槛，再输出性能结论 | 产品性能复用公共专项；纯文件系统性能保留AFS专用工具与合同 |

第四步已由用户确认总体方向：复用现有CI部署、部署AFS并选集运行，不使用开发者VM；CI0/CI1已实现并实测，SDK产品/流水线内必需冒烟本分支已在#81/#85通过。第五步承接这些适配，让人或AI在已准备环境上直接运行同一能力。**当前最完整的是固定版本的功能Full和定向专项；统一小时/天级长稳与受控性能验收仍有明确缺口。**

五步总体逻辑已确认；后续按本页优先级另行安排实现，不因规范收口启动全量、专项、长稳或环境操作。


<a id="layer-inventory"></a>
## 分层现状、原有规划与源码参考

以下保留CI0/CI1实施前的原13集合盘点快照，供追查当时目录和缺口；其中“未接”“尚未迁移”等是当时状态。当前实现、实测结果和剩余优先级以上方五步总表及第四步为准；本轮CI0/CI1已完成，不从这张历史表反向判定尚未实施。

### 读表口径与归属


- **现状／事实**：已经检入的脚本、场景、配置和调用关系；不代表本轮或某个当前版本实际跑过。
- **已有规划**：本次讨论前仓内文档提出的目标、频率或建议。没有对应实现/触发证据时，不能写成已有能力。
- **AFS待补**：原盘点时的接入缺口与后续范围；CI0/CI1已完成部分以上文更新为准，其他仍待实施；冒烟/全量范围与接入阶段按本页第四步原则执行；具体用例登记、专项判据、CI选集标签和Pod布局尚待落实。
- **决策／已实现范围**：AFS维护自己的UT和组件集成；真实FUSE/bind、单机多进程和namespace节点基础已归 `afs/tests/`，旧外置driver保留辅助/专项用途。完整产品场景复用现有ADX公共E2E。
- 表中“谁来跑”按本次对齐的使用角色表述；仓内脚本不规定团队具体人员。环境前提由执行者与环境维护者
  提供，驱动已自动完成的部署/清理另行注明。Buildkite是执行/调度入口，不是用例的新层级或专属目录。
- UT/LLT/产品E2E说明被测边界；VM/Docker/K8s说明环境；冒烟/Full/专项说明选择集合。
  同一断言可以在多个时机复用。故障、兼容、资源、性能、长稳按被测边界归组件或产品，不能只凭名称决定目录。

#### 分层现状参考（13集合×9列）

目录相对ADX仓根；CI调用省略日志/registry包装器，精确参数及源码依据见表后。

| 时机／测试集合 | 要拦截的问题与边界 | 用例放哪里：当前／已对齐归属 | 谁来跑／何时跑 | 环境如何准备：前提与自动化 | 现有脚本与Buildkite调用（事实） | AFS已接／现状缺口 | ADX已有规划（区别于实现） | AFS待补（未实施） |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **①开发自检：UT／普通LLT** | 在提交前定位模块规则、状态机、错误传播、接口/RPC组合、数据校验及资源释放问题；不证明完整产品已部署 | UT在 `afs/src/**`、`afs/client/src/**`、`afs/common/*/src/**`；LLT在 `afs/tests/*_contract.rs`、`local_sdk.rs`、`storage_localfs.rs` 及域内crate `tests/` | 开发者修改后在Linux运行受影响集合；同一集合也由②CI代跑 | 开发者提供已有Linux VM及Rust工具链；测试按例启动进程/socket、临时目录；需外部后端的例另备fixture；不必每例一个VM | 本地 `cargo test --locked -p <crate> ...`；CI复用 `.buildkite/component-tests.sh afs` 的8个crate选择；不是先执行cargo build才允许测试 | 普通非ignored集合已接；已有预算/校验/取消、Meta/VFS/mTLS/UDS/SHM等断言；部分是替身，feature/ignored例不会自然全跑 | 既有文档将UT/组件测试列为代码门禁；不把此集合当作L0或Full | 按附件补基础断言；明确feature/ignored/后端适用性，避免“有定义即有运行”的遗漏 |
| **①开发自检：真实后端、FUSE/bind及单机子系统集成** | 在提交前拦截真实持久确认、重开/重放、挂载权限/版本缓存、Meta与多个Node读写发布/删除/清理错误；与真实跨宿主故障分开 | 已有 `afs/tests/{meta,redis,fuse}_contract.rs`、源内hook；外置 `build/e2e/afs/scripts/ownerfs/bind-two-node-localfile.py`、`scripts/dfs/three_node_core.py`；已对齐统一归 `afs/tests/`，尚未迁移 | 开发者在受影响的真实语义变更后定向运行；并非每次修改都先部署完整ADX | 已有一台Linux/ext4 VM，按例具备FUSE、root/mount权限及Redis/etcd；现有单机驱动用 `--binary-dir` 和新的 `--work-root`，生成配置/证书并起Meta＋2/3 Node进程，不要求每Node一个容器 | Rust显式选择相应test/ignored；两个现有Python脚本可直接调用；普通Cargo只运行适用非ignored部分；当前没有完整显式特权集成Buildkite链 | 真实挂载/跨句柄/权限及单机访问驱动已有；许多例ignored；后端与挂载矩阵、包身份及CI调度未统一 | 没有已实现的“普通编译自动运行全部真实后端/挂载”机制；旧AFS文档保留复杂可靠性后置范围 | 统一AFS集成归属、输入/环境要求和选集；明确开发自检及CI分别选择哪些现有场景；不在本文预设新runner/step名称 |
| **①开发自检：跨节点AFS集成（默认namespace）** | 在提交前定向发现真实网络/远端源、独立Node视图和节点恢复问题；不能用单机多进程代替 | 当前统一 `afs/tests/run.py`、suites与fixture；旧 `build/e2e/afs/` driver/cohort保留历史及专项用途 | 开发者涉及跨节点合同变更时；也可由版本专项复用 | 执行者先提供多台已有Linux VM/主机、SSH、二进制/配置/后端和挂载；各机直接运行AFS进程；旧工具存在历史环境默认值，尚无统一新部署适配 | 直接调用既有driver/cohort；本地已提供cluster集合，Buildkite仍无跨节点AFS组件接线 | 驱动已有，候选/环境绑定分散；不表示当前包或完整故障矩阵已验收 | 旧acceptance有smoke/full清单，但其环境限制和旧身份不能当作公共ADX设计 | 去除隐含历史身份依赖、明确部署/清理与适用集合；保留跨节点证据；归入同一AFS集成体系 |
| **①开发自检／③版本复现：本地ADX产品E2E** | 发现公共SDK→Environment→真实runtime/Execd的命令、文件、生命周期和清理回归；支持CI失败复现 | 已有 `build/e2e/run.py`、`scenarios.py`、`sdk_smoke.py`、`functional_data_plane.py`，fixture `node.py`；继续作为公共产品用例 | 开发者需要完整产品自检或复现；测试人员也可直接运行 | 先提供匹配架构的Linux Docker主机、固定包/SDK/backend及镜像；`prepare.py` 生成bundle；`run.py` 自动建两privileged节点容器/网络、启动平台与sandboxd、执行并清理；不创建宿主机 | `build/e2e/prepare.py` → `build/e2e/run.py --bundle <目录> --profile l0/standalone`；Buildkite基础线使用K8s driver，业务场景共享 | 原有ADX入口可用；尚缺公共场景明确配置/使用真实AFS并验证其路径；AFS组件PASS不能代替这里的PASS | 已有Standalone完整11组合同与本地复现说明；不代表Multi-VM/跨物理宿主验收 | 让真实AFS存储变体进入共享产品场景及fixture，本地与K8s复用业务断言；不复制一套AFS产品E2E |
| **②主线／候选回归：代码及组件门禁** | 在共享版本构建中发现基础组件行为、工具、API合同及工程检查回归；保护其他组件与AFS相互影响 | 组件自有UT/LLT；工具自测 `build/e2e/tests/`、`build/release/tests/`；门禁脚本 `.buildkite/` | 选中基础流水线后由Buildkite agent运行；开发者负责失败定位；外部合入触发配置尚未核实 | CI使用既有构建agent/Pod和bootstrap工具链/cache；实际测试是Linux进程；AFS编译能力不等于具备特权挂载条件 | `pipeline-package.yml` 并行source-gate与组件作业；`build-afs` → `build-component.sh afs` → `component-tests.sh afs` → Cargo测试后release编译；`source-gate.sh` → 公共工具测试/API/Rust检查 | `ADX_WITH_AFS=1`时已接AFS8crate普通测试；`ADX_COMPONENT_TESTS`默认1、可关；公共Python discovery不覆盖所有外置AFS测试；ignored未自动执行 | 既有门禁文档要求UT/组件失败阻断；触发频率建议不等于外部已配置。ARM用Linux ARM64 Docker builder，`ADX_ARM_TESTS`默认0 | 将组件工具自测纳入正确发现入口；登记ON/OFF、架构、feature和未执行项；明确AFS真实集成如何被选中，尚未修改CI |
| **②主线／候选回归：制品组装与交接检查** | 发现组件遗漏、错架构、manifest/hash、安装和测试制品错配；不证明业务功能可用 | `.buildkite/package-components.sh`、`build/release/`及其自测；产品安装/CLI合同在所属组件 | Buildkite在组件/源码/SDK等前置成功后运行 | 使用同build组件制品，临时prefix安装和CLI help；镜像作业准备Docker/runtime bundle；不在此执行完整用户场景 | `platform-build` → `package-components.sh`；`publish-amd64` → `publish-amd-package.sh`；`platform-images` → `package-smoke.sh` → `package-e2e.sh`；随后才L0 | AFS ON/OFF包内容和交接模式有检查；**包含AFS二进制不等于启动或使用AFS**；旧AFS DEP driver依赖旧包/afs-processctl，不能直接复用验新包 | 已有公共制品/安装门禁；没有已确认的新增AFS发布前step或test-kit方案 | 补齐当前统一包下实际AFS服务/存储行为验证；测试资产与产品身份对应；不复活旧交付体系 |
| **②主线／候选回归：K8s L0基本冒烟** | 尽早发现真实发布包/SDK组合破坏创建、执行、文件、删除及鉴权等最小用户闭环；保护共享主线的基本可用性 | 已有 `build/e2e/kubernetes/{run,manifest}.py`；共享公共 `run.py`、`scenarios.py`、`sdk_smoke.py` | Buildkite自动执行选定的基础流水线；同一用例可本地复现。合入后运行发现的是已进入主线的回归，不是合入前阻断 | 目标K8s、Linux worker、registry/kubeconfig/权限由既有环境提供；driver自动建namespace/两ADX Pod与Service、同步harness、启动/测试/取证/清理；不创建集群 | 基础 `pipeline-package.yml: platform-e2e` → `.buildkite/run-e2e.sh` → `build/e2e/kubernetes/run.py --profile l0`；当前选择 `l0`＋`auth`，不自动跑Full | 公共L0已存在；AFS候选模式/包可交接，但未见明确选中真实AFS挂载/后端的公共闭环；尚不能宣称已守住AFS双向回归 | 旧门禁文档曾建议提交级L0或5组k8s-basic；**实际YAML为l0**；webhook/分支触发和合入保护未核验 | 选定AFS最小产品冒烟场景，证明实际使用AFS并核对核心行为/清理；补fixture与能力选择；选集范围见本页第四步，不为CI复制断言 |
| **③固定版本验收：K8s功能Full** | 在明确版本上系统检查公共功能面、两worker放置、生命周期、基础故障及物理资源清理；不是所有专项的总称 | 公共 `build/e2e/run.py` 的STANDARD11组和 `scenarios.py`；部署 `build/e2e/kubernetes/` | 测试人员或拿版本的开发者主动选择；可触发独立Full，也可直接运行现有K8s入口 | 先提供目标K8s与两个不同Linux K8s worker、权限/registry；选择不可变产品包/SDK，或复用既有镜像bundle；driver自动部署/运行/取证/清理，不在被测节点重编译产品 | `agent-dx-full-test` → `pipeline-full.yml`：`platform-images` → `package-e2e.sh`，后 `platform-e2e` → `run-e2e.sh` → K8s `run.py --profile full`；镜像复用时跳过准备；直接运行也可 | 公共Full已实现；未明确绑定AFS产品存储变体。AFS旧acceptance的Full环境BLOCKED是另一套工具限制，不能当作ADX Full不存在 | 原文档提出主分支/合入候选/发布候选运行Full；基础出包未自动串独立Full，定时/事件触发外部配置未核实 | 将AFS公共用户行为、实际挂载/后端及模式适用性融入既有Full；组件与产品结果分别可追溯，不因出包ON就算覆盖 |
| **③固定版本验收：故障／资源／短时性能及压力专项** | 在版本上定位重启、网络分区、恢复/路由、资源队列/超分、短时吞吐与延迟问题；定向PASS不代替完整Full | 公共 `build/e2e/` 故障场景、`load/performance.py`、`load/pressure.py`、`mixed_soak.py`；AFS纯组件异常仍在源内/`afs/tests/`与外置probe | 测试人员/开发者按问题选择；Buildkite可代跑相同case；不要求每次提交全跑 | 复用Full固定bundle与K8s能力；多case每例独立namespace和清理。`load-performance`90秒采样；pressure验证两节点占满后8请求排队/释放；当前无统一固定时延SLA | 直接K8s `run.py --profile full --case <case>`；CI `ADX_E2E_TARGET_CASE` → 同入口；`ADX_E2E_TARGET_CASES` → `targeted_suite.py`逐例调用；已有，不必新建专项调度体系 | ADX公共故障/负载场景已有；AFS低层重放/损坏/预算断言部分已有，但不代表公共专项已验证AFS数据/挂载/回收 | 既有文档将条件专项放在相关变更/nightly/release；性能/压力文档标有实现、待正式部署运行，不能据脚本存在宣称已验收 | 按组件/产品边界补AFS故障与资源断言；把真实AFS负载绑定既有产品专项；登记固定输入、环境、判据和未执行项 |
| **③固定版本验收：持续长稳／受控性能验收** | 发现小时/天级累积泄漏、资源持续增长、反复故障后的劣化及可重复性能回退；短功能PASS不能覆盖 | 已有公共 `build/e2e/mixed_soak.py`、`load/performance.py` 只是短测；AFS已有测量/probe资产，不等于正式统一长稳/性能套件 | 测试人员或开发者在稳定、受控环境做版本验收；当前没有可直接宣称完成1–24小时合同的公共入口 | 执行者需稳定资源/固定版本、持续负载与观测、清理/故障边界、比较基线及预定判据；现有mixed-soak默认300秒、函数仅允许1–900秒，公共入口未开放任意时长；perf仅90秒 | 现有入口仍是上述定向case；direct targeted_suite总预算≤3小时、CI预算660–7800秒；**不能把suite预算当持续soak时长，也不能把循环重建当连续长稳** | AFS长期/精确性能未因代码合入获得公共保障；既有AFS测量和历史对照保留原版本/环境身份 | `FD-SOAK-01`已有1–24小时、无资源增长目标；nightly/release为既有规划。当前未见对应完整持续执行/判据闭环；统一精确性能SLA尚无已实现合同 | 先分清公共能力缺口与AFS负载/指标缺口，再补AFS适配和判据；不在此另造AFS独家长稳平台，不声称已有“一键全天长稳” |
| **③固定版本验收：AFS标准兼容专项** | 发现POSIX/随机操作/差分中的语义、错误码、完整性与计数问题；按AFS组件边界验收 | 当前 `build/e2e/afs/acceptance/{drivers,probes,suites}/`；pjdfstest/LTP/FSx等adapter；已对齐组件部分目标同归 `afs/tests/` | 测试人员或开发者在版本专项中运行适用集合；必要短例是否入开发/冒烟另行选择 | 先准备真实Linux AFS挂载、套件版本/参考实现/seed及root/非root身份；不要求为了直接文件系统合同部署完整ADX | 现有AFS acceptance runner/adapter；Linux ARM64 guard、Full ENV verifier返回BLOCKED；当前无默认公共Buildkite L0/Full完整运行这些套件的链 | adapter已有，READY不是PASS；旧69 active全标smoke.required不能照搬；环境/完整计数与当前制品绑定未闭合 | 旧AFS文档保留pjdfstest/固定LTP子集/短FSx兜底及复杂矩阵后置；不等于已完成公共接入 | 整理当前有效adapter/环境/完整计数；纳入AFS集成归属与适用专项集合；公共产品兼容行为另进共享SDK场景 |
| **③固定版本验收：Multi-VM拓扑专项** | 发现独立VM身份/放置、真实网络、控制/worker故障与恢复问题；补足单机容器不能证明的边界 | 已有 `build/e2e/multivm/` inventory/contract、SDK/鉴权/故障等driver、`suite.py`/`assemble.py`/`verify.py` | 测试人员/开发者在专用三VM环境执行；没有完整公共Buildkite一键部署验收链 | 先由执行者/环境维护者准备一control＋两worker VM、SSH并安装包/配置ADX和sandboxd；目录不创建机器。可执行子集依赖已部署环境 | 已有 `sdk_accept.py` 等定向driver及suite/结果汇总/verify；当前不在基础K8s L0或Full的隐含链中 | 公共合同与子集存在；README明确完整部署适配仍缺，部分case未实机验证；AFS跨VM组件driver不能充当完整ADX三VM产品验收 | 原README明确下一自动部署adapter应复用adxctl/公开SDK，不能用SSH进程探活代替业务验收 | 有该产品拓扑需求时复用公共三VM合同并补AFS行为；公共部署缺口单独登记，不能写成AFS已有能力 |
| **③固定版本验收：KVM／设备条件专项** | 验证需要特定runtime/device的checkpoint/snapshot/故障行为；不属于通用冒烟或默认Full | 已有 `build/e2e/firecracker/kubernetes.py` 等FC场景；GPU/NPU在当前公共manifest尚不可执行 | 测试人员/开发者选择能力和对应专用worker；CI可代跑已实现FC步骤 | 先具备明确KVM worker、`/dev/kvm`、匹配FC kit/包/镜像及特权Pod；设备专项另需真实硬件/驱动/暴露，普通worker不具备 | Full已有条件 `platform-fc-e2e` → `firecracker-step.sh` → `run-firecracker.sh` → FC K8s driver；需 `ADX_E2E_CHECKPOINT=1` 等条件及 `ADX_FC_K8S_NODE` | FC公共入口已有，不表示AFS与checkpoint/设备组合已覆盖；GPU/NPU不能由普通Full通过替代 | 既有规划将checkpoint/设备相关变更与对应条件环境关联；没有证据证明所有自动触发已配置 | 仅在支持/需求明确时补AFS相关产品组合；不把设备能力设为所有AFS测试的强制前提 |

### 现有调用关系和环境准备细节（事实）

#### ① 开发自检：同一套用例可以本地或CI执行

本地使用Linux VM运行受影响Cargo测试；需要真实语义时显式准备后端/挂载fixture。
真实AFS单机驱动现有参数均包含 `--repo`、`--binary-dir`、`--work-root`；
后者要求新的Linux ext4工作/证据目录。驱动启动真实AFS进程，不负责购买或创建VM。
跨节点驱动须按各自现有参数绑定实际主机，尚无可假定的一套统一新入口。

开发自检优先快速定位；本地VM也能承载专项，但普通UT/LLT没有提供长稳时长、受控负载、
资源测量和性能比较证据。`cargo test`可自行编译测试，`cargo build`不会自动运行UT。

#### ② 主线回归：基础流水线的实际顺序

`agent-dx` 经 `.buildkite/select-pipeline.sh` 选择 `.buildkite/pipeline-package.yml`：

1. source-gate与组件/SDK/admin作业先行。x86 `build-afs` 仅在 `ADX_WITH_AFS=1` 时选择；
   `build-component.sh` 显式执行组件测试（默认开启）后release编译。
2. `platform-build` 等待前置作业，`package-components.sh` 组包、验证、临时安装并检查CLI help。
3. `publish-amd64` → `publish-amd-package.sh` 上传OBS（可关闭）；**当前上传早于L0**。
4. `platform-images` 等待组包/上传 → `package-smoke.sh` → `package-e2e.sh` 准备bundle和镜像。
   `package-smoke.sh`的名称不表示在此运行用户冒烟；artifact-manifest是上传后的并行旁支。
5. `platform-e2e` → `run-e2e.sh` → `build/e2e/kubernetes/run.py --profile l0`，运行 `l0`、`auth`。
   基础线可选SDK/admin PyPI发布等待此作业。

**待验证**：仓内配置证明“流水线启动后做什么”；upstream合入的webhook、分支过滤、nightly调度、
必需状态及合入保护由外部配置决定，本轮没有查询。不能将规划频率写成事实；合入后执行只能发现
已进入主线的回归。ARM组件由macOS agent承载Linux ARM64 Docker builder，测试默认关闭，
不能从x86通过推导ARM运行通过。

#### ③ 固定版本验收：取包、部署、选集、结果和清理已有公共路径

**环境先决条件**：环境维护者/使用者先提供目标K8s、可调度Linux worker、kubeconfig权限及registry访问。
目标K8s与承载Buildkite作业的Agent Stack集群分开。Full需两个ADX Pod实际位于不同K8s worker；
落在同一K8s worker将失败；未核验云底层物理机放置。privileged、cgroup、网络命名空间、nested runc等能力须通过preflight。

**已有环境准备工具**：`.buildkite/pipeline-maintenance.yml` 已有 `platform-node-prepare`，
显式设置 `ADX_K8S_NODE_PREPARE_ONLY=1`、`ADX_K8S_PREPARE_NODE_NAMES` 和
`ADX_E2E_ARTIFACT_BUILD` 后调用 `.buildkite/prepare-k8s-node.sh`。
它只在已有指定worker上用维护Pod配置 `br_netfilter` 与 `net.bridge.bridge-nf-call-iptables=1`，
保存证据并清理维护namespace；**不创建机器/集群，也不自动补全全部内核/权限能力**。
普通构建/E2E不会代为改宿主机设置。本轮没有执行该工具。

**制品与部署**：独立 `agent-dx-full-test` 选择 `pipeline-full.yml`；通常传
`ADX_BASE_PACKAGE_BUILD_ID`、`ADX_SDK_BUILD_ID`，由 `package-e2e.sh` 校验固定包/SDK/backend，
`prepare.py`和`kubernetes/publish_images.py`准备并发布不可变镜像。
AFS ON候选须显式 `ADX_WITH_AFS=1`，校验模式与manifest匹配。
也可设置 `ADX_E2E_ARTIFACT_BUILD`＋`ADX_E2E_ARTIFACT_COMMIT` 复用既有镜像bundle，跳过镜像准备。
`run-e2e.sh` 下载制品/镜像清单并调用K8s runner；runner自动创建namespace/资源、启动平台和runtime、
运行公开SDK场景、保存结果/JUnit/日志、停止服务并确认删除。测试节点不编译或替换产品二进制。
当前测试harness与固定产品制品可有不同commit，结果记录两者身份。

**已有选择器**：

- 默认 `--profile full` 为STANDARD11组：`sdk`、`data-plane`、`lifecycle`、`auth`、`capacity`、
  `placement`、`local-first`、`node-failure`、`sandboxd-restart`、`restart`、`stop`。
- `ADX_E2E_TARGET_CASE=<case>`：`run-e2e.sh` → K8s `run.py --profile full --case ...`；
  直接执行同一K8s脚本也可，不强制经过Buildkite。
- `ADX_E2E_TARGET_CASES=<case1,case2>`：要求Full → 已有 `targeted_suite.py` → 逐例K8s `run.py`；
  每例独立部署/清理，未知清理/超时会停止后续。`redis-pod-restart`带独立StorageClass要求，不能混入此多选suite。
- 可选故障及 `mixed-soak` 不随默认11组全部执行；`load-performance`、`load-pressure`是Full-only定向项。
  CI多选预算默认7200秒、允许660–7800秒；direct suite≤3小时，均预留清理时间；预算不等于负载持续时长。
- FC由独立条件步骤调用 `firecracker/kubernetes.py`；不纳入默认Full，镜像复用模式下当前FC步骤条件不会选中。

**本地复现**：已有Linux Docker主机上以固定package、backend、独立SDK候选运行 `build/e2e/prepare.py`，
再用公共 `build/e2e/run.py --bundle <目录> --profile l0/standalone` 自动部署/执行/清理。
与K8s共用业务场景和结果语义。Multi-VM的完整部署适配仍缺；不把K8s已有能力套用于该目录。

### 已有规划与待补项的使用约束

既有 [公共门禁文档](test-inventory-and-buildkite-gates.md) 的频率和nightly/release段落为规划/建议，
其中 `FD-SOAK-01` 是1–24小时目标；目前 `mixed-soak` 默认300秒、函数允许1–900秒，
性能用例90秒且无通用固定SLA。这些能力差距是公共体系已有欠账，不能当作AFS接入已解决。

**决策**：AFS补例按表中三个时机分别说明保护什么风险；组件断言归自身，产品链路融入公共场景；
CI和本地复用断言，不创建专属于Buildkite的一份业务用例。实际迁移目录、明确冒烟/Full清单、
配置支持后端/权限/设备、实现AFS适配以及核准公共缺口的实施归属，均为后续任务，未在本轮执行。
不预设新的公共流水线、发布依赖、test-kit或脚本名字。

**证据规则**：源码已有断言、工具READY、历史通过、当前版本实测通过分开登记；ignored/未选择/missing/
BLOCKED不能记PASS；定向结果不能冒充完整Full；含AFS二进制不能代替真实使用AFS的证据。
性能、功能、正常重启、故障恢复和长稳分别报告。旧AFS acceptance的Linux ARM64 guard及Full ENV
BLOCKED保留为工具事实，不提升为公共ADX架构要求。旧69项全为smoke.required不能直接成为新冒烟集合。

### 可追溯源码

- 开发/组件：[AFS测试合同](afs.md)、[组件测试](../../.buildkite/component-tests.sh)、
  [组件构建](../../.buildkite/build-component.sh)、[源码门禁](../../.buildkite/source-gate.sh)、
  [OwnerFs单机](../../build/e2e/afs/scripts/ownerfs/bind-two-node-localfile.py)、
  [DFS单机](../../build/e2e/afs/scripts/dfs/three_node_core.py)。
- 主线：[基础pipeline](../../.buildkite/pipeline-package.yml)、[选择器](../../.buildkite/select-pipeline.sh)、
  [统一包](../../.buildkite/package-components.sh)、[上传](../../.buildkite/publish-amd-package.sh)、
  [镜像准备](../../.buildkite/package-e2e.sh)、[E2E调用](../../.buildkite/run-e2e.sh)。
- 版本验收：[Full pipeline](../../.buildkite/pipeline-full.yml)、[Buildkite说明](../../.buildkite/README.md)、
  [K8s部署/清理](../../build/e2e/kubernetes/run.py)、[环境前提](../../build/e2e/kubernetes/README.md)、
  [定向suite](../../build/e2e/kubernetes/targeted_suite.py)、[共享选集](../../build/e2e/run.py)、
  [公共场景](../../build/e2e/scenarios.py)、[负载合同](../../build/e2e/load/README.md)、
  [短soak](../../build/e2e/mixed_soak.py)、[短性能](../../build/e2e/load/performance.py)。
- 环境/拓扑：[维护pipeline](../../.buildkite/pipeline-maintenance.yml)、[worker准备](../../.buildkite/prepare-k8s-node.sh)、
  [本地Docker](../../build/e2e/README.md)、[Multi-VM缺口](../../build/e2e/multivm/README.md)、
  [FC调用](../../.buildkite/run-firecracker.sh)。
- [AFS静态盘点附件](afs-test-inventory.md)：旧70 ID、文件/断言证据及逐项缺口；建议不代表公共设计已实现。

HTML由本Markdown生成：在仓库根目录执行 `python3 build/docs/render_test_strategy.py`；无需本地环境工具或个人路径。

## 组件读写专项的增补原则

读写专项继续遵循第二/三步统一入口，详见[组件测试README](../../afs/tests/README.md#读写专项与性能资产)。
`all-special`只聚合已注册的功能扩展，不自动扩大基础集合；基础集合因独立缓存复用/热缓存权限两项明确增为22项，也不等于完整AFS Full。
普通新增用例复用角色操作、typed profile和正常回收；需要持有FUSE FD的探针必须由fixture接管，
先退休辅助进程再停Node，保留真实退出和清理结果。

性能负载是可维护源码资产，可以登记在同一入口，但必须另行显式选择和声明固定输入；
`--all`不能隐式启动测量。测量完成与性能准入分开，固定基线、资源/缓存资格、统计和长稳仍按第五步处理。
本次迁移不新增性能对照、不接Buildkite或ADX产品流程，也不把历史冻结产物通过当作当前生产Rust重编通过。
