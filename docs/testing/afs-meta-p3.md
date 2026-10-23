# AFS Meta 操作日志：阶段交付与性能摸底

本次交付以增量提交记录替换每批全库快照，保留领域接口、条件事务、原操作结果、
授权和持久屏障。功能版本供 review；完整性能、规模和可靠性验收未完成。
本文保存紧凑结果和解释，环境绑定、完整运行输出与历史失败不进入产品仓。

## 改动导航

| 范围 | 实现／合同 |
| --- | --- |
| 唯一权威事实与编码 | `afs/src/meta/store/codec.rs`、`log.rs`：值池、确定变化及原结果，派生watch；显式schema与拒绝规则 |
| 共享状态与读 | `state.rs`、`store.rs`：私有准备，持久后原子公开／ACK，查询索引从权威重建，读视图有界 |
| 本地持久链 | `journal.rs`、`local_log.rs`：分组追加、CRC/hash/身份与连续性、checkpoint、CURRENT、两恢复点与安全退休 |
| 有界历史 | `history.rs`、`meta/watch.rs`：确定性清理、保留集内幂等、Compacted和响应／引用预算 |
| Root授权生命周期 | `owner_roots.rs`、`state.rs`：epoch水位不复用、小事务停用、有界grant清理、当前Home恢复保护 |
| 配置与启动 | `config.rs`、`meta.rs`：恢复完成后开监听，local-file默认、memory显式易失，旧后端／旧目录明确拒绝 |
| 可复现检查 | `afs/tests/meta_log_contract.rs`、`meta_contract.rs`、`store/crash_tests.rs`及既有AFS验收驱动 |

Node／DFS正文业务没有因本次持久化模型更换而重写。公共构建、CI、部署框架不增加
另一套入口；`ADX_WITH_AFS`开关及普通包边界保持，基准是显式example，不进产品包。

## 优化前后：已有三轮工程摸底

同一本地Linux ARM64环境，4vCPU／8GiB VM、4CPU／6GiB容器、guest ext4、Rust1.95。
S夹具为十万业务文件、700001实体、每chunk一份copy元数据，不创建正文。
并发4、每轮30秒、每类固定三轮、诊断关闭。调用从公开Store API进入到完整返回计时；
请求准备不计入逐次时延，计入吞吐墙钟。p99来自实际请求，不能由吞吐反推。

| 操作 | 旧快照ops/s中位数 | P3 ops/s中位数 | 旧p99中位数 | P3 p99中位数 |
| --- | ---: | ---: | ---: | ---: |
| create | 1.076833 | 5527.672389 | 6539.378942ms | 1.487251ms |
| commit（修改inode时间） | 1.076473 | 6132.397339 | 6572.442520ms | 1.331667ms |
| read（轮换inode/dentry） | 1698270.921510 | 4236965.263605 | 26.250µs | 0.958µs |
| watch空tail轮询 | 443.818140 | 6652301.037965 | 2.919010ms | 0.000417ms |

[结构化三轮基线和汇总](afs-meta-p3-results.json)保留输入／工具SHA、旧逐轮结果及新汇总。
旧代码每批fork／clone／排序／JSON并读写约512MB全库状态；增量实现不再为小修改
重新编码未变实体。工程结果支持“全库成本已显著下降”，但不能精确归因成某一个局部改动的倍数。

### 比较限制与未达标

- 旧seed只有一笔revision／outcome，新seed分391批并在测前checkpoint；业务实体数相同，
  初始历史不完全相同。因此报告观察值，不将其比值宣传为严格单变量收益或文件E2E收益。
- 旧输入manifest `873630aa`，旧基线产品为`1e9ae5f`；P3性能输入`06a6e9ef`。
  它早于最终功能候选，不能自动充当最终候选的正式性能成绩。
- P3 create三轮5180.195／5527.672／5611.000 ops/s，未达到20000目标；
  p99的max/min为1.407323，超过预设1.10。原失败保留，不补样本挑成绩。
- P3 commit p99为1.330584／1.331667／1.455834ms，中位1.331667ms，
  max/min为1.094132；本地数值满足2ms及稳定性，但环境未确认正式NVMe条件，不判正式通过。
- read和watch是Store层调用，watch为空tail；旧watch p99不稳。不能拿上述数值表示
  网络RPC、FUSE、S3、正文吞吐或活跃订阅能力，也不能承诺稳定的watch改善倍数。
- 旧正常重开完整墙钟中位2.650s，P3为13.182s；旧create每轮完成33～35次，
  新版155427～168351次，恢复结束状态不同。缺同结束状态对照，不能判定恢复加速或退化。

## 功能证据和交付边界

整改前候选 `714f814` 的历史证据在Linux通过Meta253、OwnerFs root25及RootCommand9，包含3个WAL和
6个checkpoint／轮转的实际子进程SIGKILL及ACK恢复；新增领域形状后Meta合同60、
namespace LocalLog合同1、fmt和受影响严格Clippy通过。原workspace全targets/allfeatures
严格Clippy按未变范围复用，不称每轮重新执行整套工程门禁。

普通Meta／Node的有限真实运行覆盖：OwnerFs双Node、mTLS、独立egress bind ON、
双向64KiB、权限／错误、全停后原目录恢复；DFS同VM三Node、一份必需持久副本、
三代A写/fsync后B/C并发读、删除与正常排空。它不是覆盖FUSE子树的完整bind集成、
跨VM三同步副本、完整POSIX或性能认证。固定pjdfstest236文件和短FSx按版本／影响复用。

1024 receipt的单／三copy和单layout65537 extent经过真实DfsMetaRpc适配器、
领域校验与LocalLog提交／重开／原结果重试。ACK是可信Node测试夹具，不能替代真实
正文持久化或物理三副本证明。超字节／组失败仍须整笔拒绝，不能靠放大预算取得接纳。
详细测试入口见[AFS测试说明](afs.md#meta-事务资源边界与有限交付回归)。

## 接口变化与剩余事项

- 默认持久化是新格式local-file，memory显式易失；etcd／Redis及其旧参数退出本次交付。
  旧目录不自动导入。保留原二进制和匹配目录用于回退，不让旧程序覆盖新日志。
- requests／events有限保留，窗口外不承诺永久exactly-once；旧watch游标返回Compacted。
  Root停用立即按权威失效，旧grant物理清理由后台完成，不能手工删除水位或恢复链。
- 对 `714f814` 的独立 review 确认 epoch 和恢复选择等阻断，下表记录本轮整改；
  不能沿用整改前“未发现确定阻断”的判断。所有合法拓扑／累计大inode和placement、
  Full FSx／完整FUN/REL、M/L、正式性能／恢复时间及完整RSS仍未验收；24h长稳暂停。
  已有有限检查不能替这些项目签署通过，原失败和未完成结论保留。

本PR的出口是上述功能版本和可审查实现，不是完整P3性能／规模验收。下一轮优化与合入
另行安排；不为本次收尾追加正式性能campaign。

## PR70 有限功能整改

本轮保持原性能数据、失败和41项验收身份，不补性能 campaign。
各项先以命中风险的 Linux 失败例验证，再执行受影响回归；失败原件保留在本地证据索引。

| 项 | 修正及有限证据 |
| --- | --- |
| F1 | epoch仅前水位加一；MAX expected拒绝且水位不变，后续创建／原请求重试正常；grant11 |
| F2 | 已初始化目录缺CURRENT、新非空WAL未列入清单均拒绝；只接受精确空预备段；LocalLog22及checkpoint六点 |
| F3 | 完整核验后回收未被恢复点引用的本库镜像；重复六次安装后崩溃仍可checkpoint；LocalLog23 |
| F4 | 持久INITIALIZING标记及精确前缀恢复；真实初始化SIGKILL后身份保持；初始化3及LocalLog20 |
| F5 | 入队至处理终态的身份保护与退休互斥，不持锁跨持久I/O；确定性排队重试RED→GREEN，retention14／history11／grant11 |
| F6 | 成对回收安全完成的命令／ACK，派生引用索引恢复重建且纳入原计账；lifecycle2／retention14／history11／grant11／RootCommand9 |

F6按用户决定保留旧NodeSession持久拒绝记录，不引入退休协议。未ACK、失败、孤立及仍引用
的命令／ACK保留；现有bind消费者不自动ACK。因此F6只关闭安全完成命令的回收子项，
NodeSession及未完成命令的长期增长仍未关闭，不增加实体cap或宣称所有权威实体有界。
F2不检测没有外部见证的整目录自洽回滚。故障测试是同内核SIGKILL，不是物理掉电。

最终集成候选另执行fmt、workspace全targets／allfeatures严格Clippy、原三点WAL及六点
checkpoint进程故障、meta_log_contract和meta_contract；实际提交、原始结果及未过项以
原PR70正文为准，不将本节的分项通过当成完整P3、真实FUSE或性能验收。
