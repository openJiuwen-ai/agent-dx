# AFS 组件集成：单 VM 与多 namespace

`run.py` 是第二、三步的共用入口，Python 用例直接位于 `suites/`；已有 Rust
`*_contract.rs` 和 `support/` 保持原位。本工具不部署完整 ADX、不接 Buildkite、不创建 VM，
不自动安装依赖、修改主机用户或全局挂载/网络配置。

## 运行条件

- Python 3.10+；控制端需要 openssl。通用流程在Linux VM内作为控制端执行；多namespace通过标准`sudo`/`ip netns`/`unshare`接入。已有受管环境可另配exec入口，可选真实多VM使用SSH。
- guest 需要 Linux、root（或已有 `sudo -n`）、`/dev/fuse`、私有 mount namespace、原生 ext4 数据目录，
  以及 python3、openssl、findmnt、fusermount3、mount、unshare。每个数据目录至少 1 GiB 空间；编译空间另计。
- 预先提供同候选、同架构的 `afs-meta`、`afs-node`。Rust 合同用例还需要一个显式提供的 `fuse_contract`
  测试 ELF。测试节点不编译产品。
- 单 VM：M/A/B/C 映射到同一 guest 的不同进程/目录/loopback 地址；OwnerFs 用 A/B，DFS 用 A/B/C。
- 第三步默认一台VM内的多namespace：M为Meta、A为Home/写者、B为读者，第四角色C为第二读者。
  按下文创建角色网络，exec入口为每个worker创建PID/mount namespace；工具核验同一boot/kernel内各角色network/PID/mount namespace不同。
  这是节点隔离/真实TCP验收，不是跨VM、宿主故障隔离或VM断电验收。
- 可选SSH多VM仍支持；独立guest的boot/machine/可用UUID须能消歧，克隆machine-id可相同。
  不修改机器身份来通过测试；本轮真实验收只覆盖单VM/namespace，不声称SSH跨VM已通过。
- 测试shell和namespace入口清除HTTP(S)/ALL_PROXY的大小写变量，避免内网健康检查被代理转发；具体命令见步骤4。
- 显式选择 owner/other 的非 root 数字 UID/GID，不自动创建用户。运行文件操作在独立降权进程中进行。
- 多 VM 预先提供 SSH 信任/认证、guest 地址互通及专用目录；不接受交互认证，不关闭主机密钥校验。

默认使用local-file Meta、真实双向证书认证、OwnerFs本地egress bind/远端FUSE、DFS NodeReplicated R1。
应用数据持续加密以有效`tls_encryption_enabled`配置为准，当前生产默认关闭；证书认证不代表正文持续加密。
实际解析配置保留在每例证据中，不为说明文字调整运行模式。
S3、etcd、Redis、RDMA、VM 断电、HA、长稳和性能不在这些基础集合的验收范围。

## 输入准备

复制 `fixtures/singlevm.example.json`、`namespaces-3.example.json` 或 `namespaces-4.example.json`
到仓外，替换地址、SSH alias/可选 `ssh_config`、已有专用 `base` 目录及 UID/GID。
示例中的 `192.0.2.*` 为文档地址，不能直接执行。`base` 须简短，产品 UDS 路径必须小于108字节。
程序使用新的 run-ID/短 case 摘要子目录，拒绝覆盖已有运行目录。

namespace示例的 `transport=exec` 使用标准Linux字面argv：`sudo -n env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy ip netns exec afst-m unshare --mount --propagation private --pid --fork --mount-proc --`。
M/A/B/C分别使用`afst-m/a/b/c`。`run.py`不创建网络；先按下节准备。已有受管环境也可自定义
`command_prefix`，但必须进入自己的实际角色，不能仅用不同host名称伪造隔离；仓内模板不依赖外部管理工具或本机路径。

单机控制端也可用 `transport=exec` 进入同一mgmt guest；M/A/B/C映射同一个host键和不同loopback地址。
`fixtures/matrix.example.json` 引用一份local和一份cluster环境；相对路径按matrix文件目录解析，
用于在同一公共VM上顺序运行两类集合。可选真实SSH多VM沿用 `multivm-3/4.example.json`。

## 从普通Linux VM准备环境

以下是第二、三步共用的环境操作说明；五步原则仍以[原总规范](../../docs/testing/test-strategy.md)为准。
**在专用Linux VM内执行全部命令**，先进入仓库根目录。宿主机可以是macOS、Windows或Linux；
虚拟化软件不限，VM由使用者自行创建。参考本轮已验证的Ubuntu 24.04 ARM64环境，建议4 vCPU、4 GiB内存，
测试数据盘预留至少8 GiB可用空间；这不是性能基准配置。x86_64须配套同架构产物，本轮未验证该架构。
代码在每个base检查至少1 GiB，但全套累计保留传输副本，不能把该下限当成总磁盘预算。编译空间另外准备。

### 1. 安装依赖并检查VM能力

在新Ubuntu 24.04 VM中安装以下发行版软件包；其它发行版提供等价命令即可，未逐一验证。
安装是执行者明确进行的环境准备，测试入口不会自动修改系统。

```sh
sudo apt-get update
sudo apt-get install -y python3 openssl util-linux fuse3 iproute2 procps kmod ca-certificates sudo coreutils
# 已有设备时不重新加载模块；失败则先处理VM内核/FUSE支持，不能跳过必需例
if ! test -c /dev/fuse; then sudo modprobe fuse; fi
test -c /dev/fuse
sudo -n true
python3 --version
findmnt -n -o FSTYPE -T /var/tmp
df -h /var/tmp
sudo unshare --mount --propagation private --pid --fork --mount-proc --   python3 -c 'import os; print("namespace probe PID", os.getpid())'
```

Python须3.10+；最后应打印PID 1。`sudo -n true`失败时，以root在专用VM内运行，或由VM维护者预先配置合适的免交互授权；
本说明不自动写sudoers。仅执行`sudo -v`不保证后续长时间运行一直有免交互权限。
`/var/tmp`必须是原生ext4；若不是，选择已有ext4数据盘上的短路径，勿把Mac共享目录、OverlayFS或/tmpfs作为数据盘。
没有ext4盘时，先用虚拟化平台给VM添加数据盘，由环境维护者完成分区、格式化和挂载；这里不提供自动选盘/格式化命令。
不要为通过测试全局放宽`/dev/fuse`权限或修改`fuse.conf`。测试进程以root准备挂载，再以数字UID/GID验证文件操作。

### 2. 创建专用目录和四个namespace角色

第二步只需目录；第三步还需下述网络。为避免重复准备，本例一次建齐，两套测试顺序运行。
以下变量在后续步骤继续使用；路径属于当前VM，可替换，不是开发者电脑上的固定路径。

```sh
export AFS_DATA=/var/tmp/afst
export AFS_INPUT="$HOME/afs-test-inputs"
mkdir -p "$AFS_INPUT/env"
# 以下脚本有冲突即退出；失败后保留已建资源供检查，不会删除已有同名资源
sudo env AFS_DATA="$AFS_DATA" bash <<'SH'
set -euo pipefail
case "$AFS_DATA" in /|*[!a-zA-Z0-9_./-]*|*..*) echo '需要安全的专用绝对短路径' >&2; exit 1;; esac
[[ "$AFS_DATA" == /* && ${#AFS_DATA} -le 24 ]]
[[ ! -e "$AFS_DATA" ]]
[[ "$(findmnt -n -o FSTYPE -T "$(dirname "$AFS_DATA")")" == ext4 ]]
for role in m a b c; do
  [[ ! -e "/run/netns/afst-$role" ]]
  if ip link show "afst-${role}0" >/dev/null 2>&1; then echo 'veth名称冲突' >&2; exit 1; fi
done
if ip link show afst-br0 >/dev/null 2>&1; then echo 'bridge名称冲突' >&2; exit 1; fi
mkdir -m 0755 -- "$AFS_DATA"
printf '%s\n' "$AFS_DATA" > "$AFS_DATA/.afs-environment-owner"
for role in local m a b c; do mkdir -m 0755 -- "$AFS_DATA/$role"; done
ip link add afst-br0 type bridge
ip link set afst-br0 alias "afs-tests:$AFS_DATA"
ip link set afst-br0 up
for pair in m:10 a:20 b:21 c:22; do
  role=${pair%:*}; last=${pair#*:}
  ip netns add "afst-$role"
  ip netns exec "afst-$role" readlink /proc/self/ns/net > "$AFS_DATA/.net-$role"
  ip link add "afst-${role}0" type veth peer name eth0 netns "afst-$role"
  ip link set "afst-${role}0" master afst-br0
  ip link set "afst-${role}0" up
  ip -n "afst-$role" address add "10.203.0.$last/24" dev eth0
  ip -n "afst-$role" link set lo up
  ip -n "afst-$role" link set eth0 up
done
SH
```

bridge不配置VM地址、不接物理网卡；角色只在内部二层网络互通，无默认路由、NAT或互联网出口。
因此不改VM现有路由、IP转发、全局防火墙或管理网卡。M=`10.203.0.10`、A=`.20`、B=`.21`、C=`.22`；
该网段只配置在新建的独立netns内，若使用者另接网络则须自己核对冲突。
每次exec通过`unshare --pid --fork --mount-proc`新建PID视图，worker再用私有mount namespace准备FUSE；
不需要预启动长期keeper。角色共享VM内核和根文件系统，base目录各自不同，不能作为安全租户隔离或VM故障隔离结论。
第三步三角色集合不用C；此处统一建四角色，便于运行完整基础集合。

### 3. 生成本机环境配置

仓库示例都可复制编辑；下面生成与刚才目录匹配的配置，并显式选择两组不同的非root数字UID/GID。
无需创建同名Linux用户；已有实际业务目录不得作为base。不要使用自己的登录UID作为权限隔离测试的默认身份。

```sh
python3 - <<'PYENV'
import json, os
from pathlib import Path
src = Path('afs/tests/fixtures')
out = Path(os.environ['AFS_INPUT']) / 'env'
base = Path(os.environ['AFS_DATA'])
for filename in ('singlevm', 'namespaces-3', 'namespaces-4'):
    config = json.loads((src / (filename + '.example.json')).read_text())
    config.update(uid=10001, gid=10001, other_uid=10002, other_gid=10002)
    for host, settings in config['hosts'].items():
        settings['base'] = str(base / ('local' if filename == 'singlevm' else host.removeprefix('ns-')))
    (out / (filename + '.json')).write_text(json.dumps(config, indent=2) + '\n')
(out / 'matrix.json').write_text(json.dumps({
    'kind': 'matrix', 'environments': {'local': 'singlevm.json', 'cluster': 'namespaces-4.json'}
}, indent=2) + '\n')
PYENV
```

`command_prefix`只使用标准系统命令，没有session文件或外部环境工具。模板的`sudo`要求与第1步相同。
从macOS编排属于可选自定义接入；这份通用流程直接在VM内运行Python，不要求电脑能访问角色私网地址。

### 4. 放入产物，预检并运行

将同架构`afs-meta`、`afs-node`和显式选择的`fuse_contract`测试ELF放进`$AFS_INPUT/bin/`，
连同实际构建身份填写的`artifact-identity.json`。构建方式和清单字段见下一节；已拿到包时不要求VM装Rust工具链。
原始包应在仓外保留一份；测试入口会校验SHA并复制到专用数据目录，不直接修改原件。
测试shell先清除以下代理变量（只影响当前shell及其子进程），namespace模板也在sudo后再次清除，
防止内网健康检查被转发到隔离网络外的代理。安装/下载阶段如需代理，在另一终端使用原配置；测试不修改全局代理设置。

```sh
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
python3 afs/tests/run.py check-env --suite all-basic \
  --env "$AFS_INPUT/env/matrix.json" --binary-dir "$AFS_INPUT/bin"
# 每次使用新的结果目录；也可按后文分别运行local-basic或cluster-basic
python3 afs/tests/run.py run --suite all-basic \
  --env "$AFS_INPUT/env/matrix.json" --binary-dir "$AFS_INPUT/bin" \
  --work-root "$AFS_INPUT/results/run-$(date -u +%Y%m%dT%H%M%SZ)"
```

READY只代表预检满足，实际PASS看本次结果；退出2/BLOCKED须补环境或输入，不能当作通过。
完整基础集合为单机13＋namespace9，不包含`--all`的可选扩展合同。

### 5. 导出证据，再回收自己的环境

先查看结果及每case的`cleanup.json`、`waits.json`，处理未确认退出/卸载；不要把网络删除当作进程正常退出的证明。
把`$AFS_INPUT`的结果、输入身份和`$AFS_DATA`内要保留的原始日志/失败现场导出到仓外，按清单/大小/SHA核对完整。
下面只回收本次创建的网络，不自动删数据；若环境搭建中途失败，按保留的`.net-*`逐项核对实际已建资源后手动回收。

```sh
sudo env AFS_DATA="$AFS_DATA" bash <<'SH'
set -euo pipefail
[[ "$(cat "$AFS_DATA/.afs-environment-owner")" == "$AFS_DATA" ]]
for role in m a b c; do
  [[ "$(cat "$AFS_DATA/.net-$role")" == "$(ip netns exec "afst-$role" readlink /proc/self/ns/net)" ]]
  [[ -z "$(ip netns pids "afst-$role")" ]] || { echo "afst-$role仍有进程，先正常停止" >&2; exit 1; }
done
ip -j link show afst-br0 | python3 -c 'import json,os,sys; assert json.load(sys.stdin)[0].get("ifalias") == "afs-tests:" + os.environ["AFS_DATA"]'
for role in m a b c; do ip netns del "afst-$role"; done
ip link del afst-br0
SH
# 确认仅剩自己的已停止数据目录、无子挂载，且证据已导出核验后，再删除$AFS_DATA。
# 保留输入原件和必要原始失败；不删公共缓存或他人的目录。
```

用`ip netns list`、`ip link show afst-br0`检查上述名字已消失；检查所有`$AFS_DATA`路径没有残留挂载后，
再由执行者删除自己的临时数据并记录前后`du -sh`。输入、结果、数据盘、VM生命周期分别管理；
只有本次独占VM且不再需要时才关闭/删除VM。不要在共享环境执行`ip -all netns delete`、全局kill、lazy卸载或磁盘prune。

初版通用setup/PYENV、预检与20项基础用例曾在Ubuntu 24.04 ARM64 Linux环境实测通过；
该历史记录不包含后来新增的缓存复用和热缓存权限两项，当前注册基础集合为22项。
代理继承导致的初次失败已保留，清除代理后完整重跑通过。该验证是在已有VM内按本文准备新namespace，
不表示从空白镜像安装软件包、所有架构/虚拟化平台均已逐项验收。

这套方法使用发行版标准的[unshare](https://man7.org/linux/man-pages/man1/unshare.1.html)与
[ip netns](https://man7.org/linux/man-pages/man8/ip-netns.8.html)能力。本轮通用步骤的Linux验证结果与范围见原总规范，
不把已验证的受管VM接入自动等同于所有发行版和虚拟化平台都已验证。

## 产品和测试ELF来源

产品继续通过已有 Linux 构建流程提供，例如：

```sh
ADX_WITH_AFS=1 cargo build --locked --release -p afs --bins -j 2
cargo test --locked -p afs --test fuse_contract --no-run --message-format=json -j 2
```

从第二条命令的 Cargo `compiler-artifact` JSON 读取 `target.name=fuse_contract` 的
`executable`，把该 ELF 作为输入目录的 `fuse_contract`；不要用不确定的 target 通配符误选旧文件。
同目录放 `artifact-identity.json`，从实际构建记录填写，不以当前 HEAD 冒充旧二进制的来源：

```json
{
  "commit": "0000000000000000000000000000000000000000",
  "source_input_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
  "binaries": {
    "afs-meta": "实际afs-meta的SHA256",
    "afs-node": "实际afs-node的SHA256"
  },
  "test_elf": {"sha256": "实际fuse_contract的SHA256"}
}
```

上述零值/中文均为占位，须替换。也支持已有冻结构建清单的 `compiled_source_logical`
源摘要字段。产品 SHA、测试 ELF SHA 在执行和传输时重新核对；清单用于输入一致性，不能代替真实构建证据。
产品与测试可有独立来源，结果分别记录；不能把旧测试 ELF 的通过升级为当前测试源码已编译通过。

## 选择与执行

```sh
# 列举所有用例，无产品副作用
python3 afs/tests/run.py list
# 仅检查所选能力，不创建正式运行目录、不安装依赖或启动产品
python3 afs/tests/run.py check-env --suite local-basic \
  --env /absolute/singlevm.json --binary-dir /absolute/bin
# 单 VM 全套
python3 afs/tests/run.py run --suite local-basic \
  --env /absolute/singlevm.json --binary-dir /absolute/bin --work-root /absolute/new-results
# 三角色namespace全套；四角色换cluster-manyread与四角色环境文件
python3 afs/tests/run.py run --suite cluster-basic \
  --env /absolute/namespaces-3.json --binary-dir /absolute/bin --work-root /absolute/new-results
# 精确 case / 通配符 / 多个选择器并集（按ID去重）
python3 afs/tests/run.py run --case ownerfs.basic.read_write \
  --env /absolute/singlevm.json --binary-dir /absolute/bin --work-root /absolute/new-results
python3 afs/tests/run.py run --case 'ownerfs.basic.*' --case 'dfs.basic.*' \
  --env /absolute/singlevm.json --binary-dir /absolute/bin --work-root /absolute/new-results
```

`--all`、`--suite`、`--case` 互斥，重复 `--case` 除外。任何零匹配报错，不输出0例PASS。
`run`不写选择器默认 `local-basic`；`list`不写选择器则列出全部。
`--all`表示所有已登记用例（含可选交叉验证），不能静默过滤缺环境/输入项。
`--suite all-basic --env /absolute/matrix.json` 一次运行local-basic和四角色cluster基础集合，
不包含可选旧ELF交叉验证；matrix缺任何已选环境则BLOCKED。也可分别运行两个集合。
`ownerfs.*`同时匹配单机/cluster例，单机定位请用 `ownerfs.basic.*` 等。
默认每例预算600秒，`--case-timeout`允许60–3600秒，是超时保护，不是性能判据。

## 真实断言与集合

- `local-basic`：OwnerFs/DFS 启动和真实挂载，基本文件/目录/错误，OwnerFs双向读写，
  非root/setid，DFS一写两读/版本/权限，正常停启恢复，FUSE顺序/真实DFS缓存及busy拒绝、正常清理。
- `cluster-basic`：M/A/B独立namespace节点，OwnerFs跨节点读写/命名空间/旧FD/单Home/EXDEV/权限，
  DFS R1跨机发布/读回/新鲜度，Meta/Home正常进程重启及逐机清理。
- `cluster-manyread`：上述全部，再验证独立C读者。三角色结果不冒称四角色通过。
- `all-basic`：local-basic＋cluster-manyread，共用matrix环境，完整结果包含LB01–08与MV01–08。
  MV是沿用的验收组编号，实际拓扑以每case的kind/namespace证据为准，不表示多VM通过。
- `local-extended`：单VM执行额外OwnerFs对象身份/根创建竞争断言，供多机逻辑的本机定向诊断；不属于默认基础集合。

DFS远端路径记录新鲜读者的内容/长度/哈希和 `peer_read_ranges` RPC调用增量。
RPC计数本身包含失败，不等于服务端成功；必须结合新鲜读者、内容正确、无外部后端来解释。
受控backend的Rust内核选择器使用 `--exact --ignored`，先核验该test，再要求实际1 passed。
受控backend的内核合同与真实DFS结果分别记录。真实DFS缓存例仅在同VM A/B双挂载下执行，
由Python直接运行非root、16MiB、H1/H2/H3、同inode新版本/旧fd尾部/新OPEN拒绝与恢复、完整SHA/EOF及B READ计数，
自有FUSE BDI readahead=0并在结束后恢复。原Rust同语义版本留在local-extended作可选交叉验证；
缺其精确selector明确BLOCKED，不影响独立Python合同的结果，不能把Python结果记成该Rust ELF通过。

## 结果和安全回收

根目录有 `identity.json`、`environment.json`、`result.json`、`waits.json`、`cleanup.json`；
每case保留各自身份/配置/断言/日志和退出、清理证据。配置文件只有生成私钥路径，不导出私钥内容；
完成回收后删除控制端临时CA/私钥，失败且资源未确认时保留本轮私有目录以便处理。
Home/Meta重启允许有界30秒会话/缓存收敛，记录全部暂态错误，不重试内容损坏。
根创建竞争验证恰有一个成功和唯一实际Home，保留失败竞争者errno；该项不替代POSIX错误码专项。

- 退出0：本次**声明范围**实际PASS；case/glob结果为partial，不能写成完整基础集合通过。
- 退出1：断言失败、超时、中断或清理失败。原始错误与随后清理错误均保留。
- 退出2：环境/输入BLOCKED。未知selector为参数错误，同样不等于通过。
- 每例先正常停止全部Node、确认真实wait，再停Meta；检查私有namespace新增挂载、进程和监听。
  Node非零退出但已确认停止不会导致Meta被遗留；Node/SSH失联、停止未知则保留Meta并报告未确认。
  不用强制kill/lazy卸载伪造正常成功，不终止未知进程。
- SSH worker是本轮父进程，保存真实子进程退出码；连接断开不宣称清理完成。
  超时/中断后的迟到RPC回复不能被误当成cleanup回复。
- 失败后只有确认清理完成才继续下一例；环境阻塞或清理不明后，其余选中项标NOT_RUN。

运行数据和输入传输副本保留在本轮专用guest目录供取证。导出并核对日志/配置/结果及失败记录后，
执行者按工作区规则清理本轮已停进程、无挂载目录，不删除共享缓存、他人任务或唯一故障现场。

## 用例组织与后续添加规则

本README是AFS组件测试的使用和扩展总入口；测试分层、Buildkite和完整ADX产品行为仍遵循
[五步总规范](../../docs/testing/test-strategy.md)。当前22项是第二/三步的**基础集合**，不是AFS全量覆盖。
`--all`仅表示全部已登记case，不能据此宣称功能、兼容性、故障、长稳和性能均已覆盖。
当前列表以`python3 afs/tests/run.py list`为准，覆盖缺口和优先级记入总规范，不另建一份平行清单。

### 目录各负责什么

| 位置 | 放什么 | 添加时的原则 |
| --- | --- | --- |
| `suites/ownerfs/`、`suites/dfs/`、`suites/fuse/`、`suites/cluster/` | 对应能力的用例；`test_*.py`导出`CASES` | 按被测能力归类，环境由标签/配置声明；不要为本机和namespace各复制一份相同断言 |
| `suites/operations.py`或同能力目录中的普通Python模块 | 多个用例共用的业务操作/断言 | 至少有真实复用需求再提取；普通辅助模块不必叫`test_*.py` |
| `fixtures/` | 环境接入、角色、配置/TLS、生命周期、传输、取证与清理 | 普通新case复用现有`Cluster`；新增环境能力才改fixture，不在case内复制部署或强制清理逻辑 |
| `runner_tests/` | 选择、协议、环境拒绝和清理规则的工具合同 | 工具逻辑改变才补对应测试；它们不替代真实产品用例 |
| 原有`*_contract.rs`、`support/` | Rust组件/内核合同及辅助程序 | 原位维护，通过显式测试ELF调用；不拷贝Rust断言到另一套假实现 |
| `build/e2e/` | ADX安装、部署、SDK及产品全链路场景 | 按第四/五步融合公共体系，不放进AFS组件目录冒充完整产品验证 |

### 最小示例：新增一个OwnerFs截断用例

例如新增`suites/ownerfs/test_truncate.py`，内容如下。**这是添加方法示例，尚未登记为本轮已实现/已验证用例**；
真正加入前须在Linux实跑。用例函数只负责业务断言，入口已完成`prepare()`，结束后统一执行`cleanup()`。

```python
from fixtures.common import Case


def truncate_tail(cluster):
    name = 'truncate.bin'
    cluster.write('A', name, b'abcdef')
    cluster.python(
        'A',
        'with open(sys.argv[1], "r+b") as f:\n'
        '    f.truncate(3)\n'
        '    f.flush()\n'
        '    os.fsync(f.fileno())',
        cluster.workspace('A') + '/' + name,
    )
    cluster.read('A', name, b'abc')
    cluster.read('B', name, b'abc')


CASES = (
    Case(
        id='ownerfs.basic.truncate_tail',
        tags=('local-extended',),
        mode='ownerfs',
        groups=(),
        function=truncate_tail,
    ),
)
```

| 注册字段 | 规则 |
| --- | --- |
| `id` | 全仓AFS registry内唯一且稳定，用`能力.主题.行为`；例如`ownerfs.basic.truncate_tail`。目录/文件名不是命令行ID；重复ID会拒绝运行 |
| `tags` | 决定所选集合/环境；`local-*`用于单VM，`cluster-*`用于隔离角色。普通新case先登记适用扩展集合，不自动扩大必需基础集合 |
| `mode` | 当前fixture支持`ownerfs`或`dfs`，决定实际部署。FUSE内核合同也要明确所需产品模式；新增模式需实现和验证fixture |
| `groups` | 对应总规范已定义的LB/MV验收要求，可为空或贡献多个要求；这些是行为覆盖编号，不是case序号，不为凑齐覆盖随意填写 |
| `function` | 接收一个已准备好的`Cluster`，通过角色API执行真实操作、校验内容/权限/错误并留证据；正常返回才表示断言通过 |
| `rust` | 默认false；设为true会要求可追溯的测试ELF输入。函数仍须精确选择测试并核验实际运行计数，不能仅检查ELF存在 |

`run.py`递归发现`suites/**/test_*.py`并读取`CASES`，不会自动发现文件中的任意`test_*`函数；
普通新case不需要修改入口。导入时只定义函数和注册信息，不启动进程、不挂载、不访问测试环境。
单机与cluster集合不能混贴到同一个Case：如需复用，分别注册两个稳定ID，共用同一个业务函数。
`matrix`模式按是否含`local-*`标签分派，没有该标签的case进入cluster环境；因此新增单机集合仍使用`local-`前缀，避免被分到错误环境。

### 添加后怎么验证、怎么进入集合

```sh
# 在仓库根目录检查新ID确实被发现，且只选中预期用例
python3 afs/tests/run.py list --case ownerfs.basic.truncate_tail
# 按前面的通用说明准备环境/输入并清除代理，再显式执行
python3 afs/tests/run.py check-env --case ownerfs.basic.truncate_tail \
  --env /absolute/singlevm.json --binary-dir /absolute/bin
python3 afs/tests/run.py run --case ownerfs.basic.truncate_tail \
  --env /absolute/singlevm.json --binary-dir /absolute/bin --work-root /absolute/new-case-results
```

- 断言必须检查可观察结果：内容/长度/哈希、权限或精确错误，以及必要的实际数据路径；服务Ready、命令退出0或仅有日志不够。
  使用`cluster.record()`保留关键观测；共享操作优先使用`write/read/python/command`等角色API，避免访问控制端本地路径冒充远端文件系统。
- 环境能力不满足应显式`Blocked`并写清原因，真实功能不符用断言/异常失败；不得catch后返回成功。
  新增设备/后端/拓扑等能力要求时，先完善预检和fixture，再登记依赖它的case，不在用例里偷偷安装环境或改全局配置。
- 不自行强杀进程、lazy卸载、删除他人目录或绕开统一清理。新增FD/辅助进程/挂载须纳入生命周期与实际退出证据，
  正常/失败/中断都能收尾；检查`result.json`、`cleanup.json`、`waits.json`，保留失败记录。
- 提交Python改动前按根规范运行`make python-check PYTHON_CHECK_BASE=<已更新的目标分支ref>`，然后运行受影响测试；本地预检不等于远端CodeCheck通过。
- 定向case通过只证明该项，结果为partial；随后在所需单机/namespace环境跑受影响集合。
  修改调度/清理/环境逻辑时再跑Linux工具合同；真实FUSE行为必须在Linux验收。
- 基础集合要求短、确定、可正常回收，拦截常见回归；新case进入`local-basic`、`cluster-basic`、`cluster-manyread`须明确必要性、环境和时间预算并更新总规范。
  `cluster-basic`的必需行为也要登记`cluster-manyread`，后者再包含C读者扩展；`all-basic`由`local-basic`和`cluster-manyread`自动归集。
- 普通扩展case的标签可直接由`--suite`选取；新增正式必需集合/验收组时，须同步`run.py`的required映射、工具合同、文档和实际完整运行证据。
  CI冒烟/Full标签及调用仍属第四步接线，不因添加case自动生效；长稳/精确性能也不因归入`--all`获得验收结论。

## 工具测试与当前验收状态

```sh
# Linux工具合同测试；不产生产品或多VM通过结论
python3 -m unittest discover -s afs/tests/runner_tests -v
```

代码提供local/受管namespace/可选SSH适配、matrix调度和全部基础集合；真实通过、旧冻结输入验证、当前源码构建阻塞以及缺少多VM环境
必须分别登记。当前运行记录见 [五步测试规范](../../docs/testing/test-strategy.md#step-2)，
AFS技术边界见 [测试合同](../../docs/testing/afs.md)。本入口不调用或宣称已接通第四、五步产品/CI。

## 读写专项与性能资产

基础集合为22项（原20项及独立的缓存复用、热缓存权限两项）；以下是可选扩展，不自动进入Buildkite、基础冒烟或产品Full。
用例ID和实际profile以`run.py list`为准；本节只说明归属，不维护第二份独立注册表。

| 集合 | 当前行为范围 | 配置与边界 |
| --- | --- | --- |
| `local-write-special` / `cluster-write-special` | 跨fd覆盖/截断顺序；O_SYNC/O_DSYNC/close-only/并发append；有界接纳下同文件及独立文件fsync推进；必需副本失败恢复；失败close退役 | 各5项。显式64MiB前台额度；R2例只启动A/B两个Node并要求两个不同Node的持久副本。256MiB压力不冒充实测队列峰值、严格公平性或时延SLA |
| `local-read-special` / `cluster-read-special` | off/receiver/both三种CRC模式下的远端非对齐/跨块/EOF正确性；显式direct顺序读触发预取、seek/close后缓冲释放；cached首读/同fd/重开正确性及direct专用预取门控 | 各5项。固定open授权显式开启以覆盖原生句柄路径；不加强旧fd一致性。不把正向CRC测试记成损坏注入/坏副本恢复，也不把cached内容正确当作缓存驻留或零RPC证明 |
| `all-special` | 上述单机和隔离节点专项的并集 | 20项，使用matrix环境；不含性能，不替代`all-basic`或完整AFS验收 |
| `local-performance` | 顺序写交付/同步完成测量资产及远端读测量资产 | 两个测量入口，默认拒绝执行。不是受控基线比较或性能准入，不能用PASS代表性能无退化 |

### 用同一入口选择

```sh
python3 afs/tests/run.py list --suite all-special
python3 afs/tests/run.py check-env --suite all-special \
  --env /absolute/matrix.json --binary-dir /absolute/bin
python3 afs/tests/run.py run --suite all-special \
  --env /absolute/matrix.json --binary-dir /absolute/bin \
  --work-root /absolute/new-special-results
# 只定位一项；定向结果仍为partial。
python3 afs/tests/run.py run --case dfs.write.local.required_replica_recovery \
  --env /absolute/singlevm.json --binary-dir /absolute/bin \
  --work-root /absolute/new-recovery-results
```

`DfsProfile`只接受经过范围检查的副本数/Node数、前台额度、读CRC、原生open授权、
内核预读、B预取距离和keep_cache参数，不接受任意TOML或个人环境路径。
原基础用例不指定profile时仍用原R1/默认参数；有效产品配置保留在checks/config证据中。
单机与namespace用同一业务函数、分别注册稳定ID，仍由角色API执行，不把控制端文件访问当远端证据。

需要持有FD等待副本恢复的辅助进程通过fixture的`spawn_python`接管。
故障控制采用已登记Node的正常停止/恢复；辅助进程先于Node退休，实际wait状态保留。
取消辅助进程明确记`cancelled_helper`，不能冒充其业务成功；断言、超时、异常退出和清理错误均使本例失败。
这不提供SIGKILL、断电、独立宿主或完整多副本修复结论。

### 性能资产须单独显式执行

本次迁移只验证性能资产的发现、拒绝规则和小数据功能正确性，不执行新的性能对照。
源码见[测量工具说明](tools/README.md)。使用者以后执行前，先在专门的singlevm环境JSON中加入：

```json
{"performance": {"bytes": 1073741824, "request_bytes": 1048576}}
```

该片段不是完整环境配置。仅当明确安排性能工作时再显式使用：

```sh
python3 afs/tests/run.py list --suite local-performance
python3 afs/tests/run.py run --suite local-performance --allow-performance \
  --env /absolute/singlevm-performance.json --binary-dir /absolute/bin \
  --case-timeout 1800 --work-root /absolute/new-measurement-results
```

没有`--allow-performance`时，`run`和`check-env`在启动产品前返回BLOCKED，`--all`也不能隐式绕过。
没有固定数据量/请求粒度同样BLOCKED。结果逐项标`purpose=performance`及
`performance_acceptance=NOT_EVALUATED`：通过只表示负载完成且内容/账本有效，不判断加速或退化。
需要固定基线、交错顺序、资源/缓存资格和阈值的正式性能比较，继续按第五步复用公共受控性能能力。

原始cohort、旧源码/ELF、进度写盘/BPF等诊断证据仍留仓外。源码仓不提交实际环境JSON、
证书私钥、二进制、运行正文或大日志。历史批次成绩不转移到当前代码或不同计时口径。


## 缓存用例的前提与证据

三项基础检查独立出结果，不用一次断言失败遮住后面的权限结果：

| ID | 条件与硬断言 |
| --- | --- |
| `dfs.visibility.real_cross_handle_cache` | 旧V1句柄仍存活时发布V2；核对实际open日志，H2/H3应走安全direct回退，必须读到正确V2且确有FUSE读；保留原legacy配置下旧句柄未缓存尾部的检查，不增加通用旧fd一致性承诺 |
| `dfs.cache.same_version_reopen` | 独立fixture，无冲突旧句柄。预热必须获缓存准入；同fd复读零新增READ；关闭后同版本重开必须cached=true、keep_cache=true，仍严格要求零新增FUSE READ |
| `dfs.cache.warm_permission_change` | 确认缓存已热后改mode000，新open必须EACCES；恢复权限后内容/长度/SHA/EOF正确，不要求已有fd即时撤销 |

仅这些case设置`Case.observe_cache=true`，fixture将已有日志级别设为Info，读取原有
`dfs.fuse_cache_open`的ino/fh/versioned_read/cached/keep_cache字段；不改产品代码或缓存策略。
请求显式为O_RDONLY、没有O_DIRECT，versioned_read必须为true；该条件下实际cached结果用于核对缓存准入/回退。
日志缺失、类型不对或意外direct均失败，不推断成已经准入，也不算通过。

驻留用Linux `mincore`对本UID所有的文件做非触碰MAP_PRIVATE映射观测，保存逐页向量、页数和驻留数；
完整预热后、重开但尚未读正文、再次读完后三次均要求全量驻留。OPEN丢页会在正文重新预热前失败。
每项使用新目录与独立正常回收的fixture；旧冲突fixture已清理后才进入另一项，无固定sleep或重试OPEN来取得有利结果。
精确计数期间临时把自有B挂载的BDI预读设为0并finally恢复，不改源盘或全局系统配置；这只是功能合同，不是性能对照。
所有阶段在断言前输出JSON，失败时也保留实际日志、驻留向量和计数；零READ没有改成允许少量READ。

本批实现的冻结功能结果、制品身份与发布边界见[读写专项验证记录](../../docs/testing/afs-io-specials-validation-20261031.md)。
