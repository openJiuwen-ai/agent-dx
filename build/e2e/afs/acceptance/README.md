# AFS 验收工具

本目录保留维护中的验收 runner、驱动、探针、套件绑定和必要测试。它是工程测试资产，不是历史证据归档。

## 目录

- `cases.json`：正式 case ID、适用范围、smoke/full 边界和 driver 注册。
- `acceptance.lock.example.json`：无机器状态的锁结构示例。实际环境、套件、参考系统、源码、二进制和 runner 身份锁保存在仓外，通过 `--lock` 显式传入；`PREPARING` 状态不能通过完整发布门禁。
- `runner.py`：调度注册 driver，并核对结构化结果和矩阵计数。
- `environment.py`：检查本轮环境输入的路径、SHA 和格式；可移植完整 ENV verifier 尚未实现，full 始终明确 BLOCKED。
- `drivers/`：标准套件、健康、计数、目标身份、挂载隔离和可见性等 driver。
- `probes/`：OwnerFs、DFS、环境和基线辅助探针。

## 使用边界

工具可运行不等于产品通过。smoke 不能替代 full，memory Meta 不能替代 durable backend，OFF/FUSE 结果不能替代 bind ON 结果。

G2 功能完备性优先使用 pjdfstest、固定 LTP filesystem 子集和短 FSx；项目自定义 case 只补充 OwnerFs bind、远端访问、DFS 复制、Meta 恢复和权限语义。G3 再运行长稳、完整故障矩阵和后端轴。

Linux 运行示例：

```sh
python3 -m venv /var/tmp/afs-tools-venv
/var/tmp/afs-tools-venv/bin/python -m pip install -r build/e2e/afs/acceptance/requirements.txt
sudo env PYTHONDONTWRITEBYTECODE=1 /var/tmp/afs-tools-venv/bin/python -m unittest discover -s build/e2e/afs/acceptance -p 'test_*.py' -v
```

STD-04 缩减回放依赖已固定的 Hypothesis，CI 与本地必须使用同一个明确的 Python 环境；不要假定 root 能读取普通用户的 user-site 安装。完整工具回归需要 root，以验证真实权限与内核锁辅助程序。三个 coordinator 子进程测试仅在 Linux ARM64 root 下运行，其它平台明确 skip；通用 CI 不能替代这三项的平台验证。固定 helper 校验和只绑定当前工具，不用于重评历史运行记录。

inventory CLI 只接受专用 Linux ARM64 环境。其真实 PID 文件回归在 ARM64 验证采集成功，在其它架构验证明确拒绝且无成功输出；另有子进程回归验证拒绝发生在采集之前。通用 x86_64 CI 的拒绝路径通过不能冒充 ARM64 环境验收。

网络资源清理、OFD 缺失和输出保护的单元测试仅隔离其平台/root 前置条件；另有真实拒绝哨兵证明不支持架构不会绑定或创建输出。实际锁原语仍只在符合条件的 ARM64 root 下运行。

根据具体任务选择更小的测试集合；不要为文档改动扩大成完整矩阵。

## 环境输入

```sh
python3 build/e2e/afs/acceptance/runner.py --list
cp build/e2e/afs/acceptance/acceptance.lock.example.json /path/to/run/acceptance.lock.json
python3 build/e2e/afs/acceptance/runner.py --case FUN-01 --profile smoke \
  --lock /path/to/run/acceptance.lock.json --results-dir /path/to/run/results
```

`/path/to/run` 应先替换为已创建的仓外运行目录；复制示例不等于环境准入。按实际观测补齐 lock 的身份和环境证明，完整门禁要求 FROZEN、校验和一致及真实语义资格检查；当前通用完整 ENV verifier 未实现，因此即便输入自报 PASS 也不放行 full。旧固定实验室判定器和原始观测已归档仓外，底层参数化探针及合成负例保留。`--list` 只读取 case，不执行 driver；缺少 `--lock` 的执行在创建结果目录前拒绝。

本机 VM 创建配置、网络租约和旧阶段启动脚本不再随工具分发。环境由现有部署层准备，运行条件以本次 lock 记录，不复用个人路径或历史 VM 状态。跨节点锁/RDMA 等后置专题的工具回归不代表当前产品支持这些功能。
