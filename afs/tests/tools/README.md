# AFS 可复用测量负载

统一入口为`afs/tests/run.py`的`local-performance`，不是另一套测试平台。
`io_workload.py`也可在已准备好的真实挂载上单独使用；调用者负责环境资格、候选身份与清理。
它不配置机器、不关闭CRC/副本/Meta，也不生成一个假后端以取得更好数字。

## 两种模式

- 默认功能核验：只验证I/O计数、正文和EOF，不输出耗时/CPU数字。
- 显式`--measure`：记录声明的测量边界、客户端CPU和本进程峰值RSS；输出并不构成性能准入。

I/O数据量有界为4KiB–1GiB，请求粒度4KiB–1MiB，两者均需页对齐。
写模式使用新文件，已有文件拒绝覆盖；请求和返回字节逐笔核对。进度只在内存计数，结束后输出JSON。

## 计时和账本边界

- 写：计时前准备重复输入块和预期摘要；从open开始，分别记录全部write交付、数据fsync、close及父目录fsync后的完整时间。完整内容读回在计时后执行，额外验证读字节单列。
- 读：fixture先在A准备并fsync文件，B计时从open到全部读取/EOF校验和close；流式SHA验证包含在读计时中。来源明确为writer-hot，不假装远端冷盘。
- CPU只覆盖当前客户端进程的计时区间；峰值RSS是该进程峰值，不是整个AFS或相同瞬时区间的内存。
- Node的前后metrics快照另存，覆盖fixture准备和验证，不能冒充同一exclusive I/O时间段的CPU/字节差。
- `payload_calls`只统计正文调用，不包含EOF哨兵、fsync、close或验证读；不能当RPC/设备请求数。

读含SHA、写另做读回，这些边界不与不同定义的旧cohort直接相减；不把write交付当持久化完成。
Node完整CPU/内存、设备IO和公平性采样、交错基线及统计准入仍属于后续受控性能编排，不能从此工具单独得到。

## 业界benchmark辅助

- `benchlib.py`：`bench`参数校验、fio/elbencho命令构造、fio/elbencho/iperf3自身JSON报告解析与样本汇总；纯函数，由`runner_tests/test_bench.py`固定。
- `bench_probe.py`：在角色主机上执行，`evict`对文件/目录做fdatasync+`POSIX_FADV_DONTNEED`（不进入其他挂载），`resident`以mmap+mincore实测页缓存驻留量（与util-linux `fincore`同原理）。驱逐只是建议，调用者必须以`resident`结果为准；无法映射的文件报错而不是按0计。

- `bench-tools.sh`：在一台Linux角色主机上准备上述工具（锁定版本、核对SHA-256、源码编译fio/iperf3、官方静态elbencho），输出`manifest.json`；不安装进系统。

用法与case见[上级README](../README.md)的"业界通用benchmark"一节。

## OwnerFs catalog 版本切换

在现有 Buildkite 组件版本切换入口指定 `ADX_E2E_TARGET_CASE=afs-component-upgrade-rollback` 和
`AFS_E2E_UPGRADE_CASE=ownerfs.version.catalog_compatibility`。基线和候选各使用执行者提供的制品 UUID 与完整提交 SHA。
专项检查真实原目录升级、只退二进制、停机快照回退，以及 catalog 损坏时的安全拒绝。相同提交和制品摘要可用来跑恢复流程对照。
Linux 可用同一个 `tools.version_switch.exercise`，传入既有 role 配置和两个包含 `artifact-identity.json` 的二进制目录。
旧式目录覆盖现有 workspace 的恢复、读写、无目录的删除残留、以及身份冲突时的单工作区隔离。原生目录还验证候选创建的新 workspace 能被原二进制打开。outlet 的跨节点访问需要单独的真实 NFS 环境。

## 资产去向

| 资产 | 维护位置 |
| --- | --- |
| 长期读写语义、压力/故障恢复断言 | `../suites/dfs/test_{write,read}_special.py`，复用fixtures与统一入口 |
| 无环境绑定的正文/请求/计时负载 | 本目录；通过显式性能case复用 |
| 已有低层C/Rust测量辅助 | `../support/`原位保留，不复制多份 |
| 历史交错矩阵、原始结果、冻结ELF及源码、BPF/临时探针 | 仓外实验/证据归档；不作为当前case通过或性能阈值的来源 |

当前迁移没有新增性能实验。功能核验模式跑过也不能声称测量模式已取得性能成绩。
