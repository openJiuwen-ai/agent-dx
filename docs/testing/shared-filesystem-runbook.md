# JuiceFS / AFS 部署与 Firecracker 双节点验证手册

本文供同事在自己的专用 Linux KVM 测试机上复现共享 checkpoint 存储。覆盖存储部署、ADX 接入、直接 sandboxd 恢复、双 Ingress 流量唤醒、故障与回收，以及性能对比。

命令按 PR [GitHub #89](https://github.com/openJiuwen-ai/agent-dx/pull/89) / [GitCode !73](https://gitcode.com/openJiuwen/agent-dx/merge_requests/73) 的源码 `133491130cbdef7d45526d2c2de0d1fe4ef9ce54` 静态核对。后续更换代码版本时，应同时更新发布包、SDK、驱动与运行时身份。本次补充文档未重新执行真实 Firecracker 双节点部署。此前真实测试结果和性能数字见[存储实现与验证记录](snapshot-storage.md)，不能作为新机器的验收结果。

## 1. 先选运行范围

| 层次 | 执行内容 | 能证明什么 |
| --- | --- | --- |
| 文件系统 smoke | AFS 三客户端基础读写；JuiceFS 在启动存储套件时自动做 COW 探针 | 共享 namespace、跨客户端可见性、文件克隆与写隔离 |
| 存储性能集合 | `ADX_STORAGE_BENCHMARK=1` | 捕获发布、双 Ingress 并发流量唤醒、完整 512 MiB 内存校验、最终清理 |
| 完整存储生命周期 | 不设置 `ADX_STORAGE_BENCHMARK` | 上述功能，以及节点失效、恢复中 Coordinator 重启、旧执行引用保护、匿名恢复点和半成品回收、共享目录只读使用 |
| AFS GC 收敛 | 完整生命周期加 `ADX_STORAGE_GC=1` | COW / open-unlink 读者保护、AFS Node session 更换、多轮暂停恢复后 Meta 与物理 chunk 收敛 |
| 直接运行时对照 | 提供 `ADX_STORAGE_PROBE` | 在 ADX 创建 Environment 前，先直接调用 sandboxd 做 Full / Incremental 捕获与恢复 |

建议先跑 AFS smoke，再分别跑一次 AFS / JuiceFS 性能集合，确认链路可用；然后执行完整生命周期，最后各跑三次独立性能试验。

## 2. 拓扑与目录职责

```text
专用 KVM 主机
  控制进程：Redis、Coordinator、API Server、Ingress 1、Ingress 2
  执行节点 1：独立 network / mount namespace，adxlet + Relay + sandboxd + Firecracker
  执行节点 2：独立 network / mount namespace，adxlet + Relay + sandboxd + Firecracker

  AFS：一个 local-file Meta + 三个 DFS Node / FUSE client
       node-a → 执行节点 1；node-b → 执行节点 2；node-c → Coordinator

  JuiceFS：三个独立 FUSE client → 同一 PostgreSQL 元数据库 + Ceph RGW
           node1 → 执行节点 1；node2 → 执行节点 2；coordinator → Coordinator
```

两种文件系统都向 ADX 暴露 `distributed_fs` 路径。adxlet 负责捕获与恢复，Coordinator 负责共享制品回收；PostgreSQL、Ceph 凭据只交给 JuiceFS 部署层。匿名恢复点跟随 Environment；运行时仍在使用的 checkpoint 文件保持引用保护。

本手册的自动化拓扑是一台物理主机上的两个逻辑执行节点。AFS 为单 Meta、一份必需持久副本；下文的最小 Ceph 也是单主机。结果不证明跨物理主机故障域或存储 HA。

| 目录 | 内容与生命周期 |
| --- | --- |
| `adx/<run>/...` | 不可变 checkpoint 和 manifest；由 Coordinator 根据权威引用回收 |
| `runtime/<run>/node1`、`runtime/<run>/node2` | 每个 sandboxd 的私有可写运行文件；由 sandboxd 清理 |
| `shares/...` | 业务共享目录；不随 Environment 删除而删除 |
| 本地 `RUN_ROOT` | 配置、证据、日志、临时服务数据；每次全新目录 |

checkpoint 与私有 runtime filestore 放在同一文件系统中，才能利用文件系统的 `copy_file_range` / COW 路径；不要把不可变 checkpoint 目录直接作为可写运行目录。

## 3. 机器、制品与预检查

### 3.1 测试机

使用专用 Linux 主机，例如 Ubuntu 24.04，root Bash 执行下文部署命令。推荐至少 16 vCPU、32 GiB RAM、50 GiB 可用测试空间；驱动本身最低检查 8 GiB，长时间 GC 与多轮测试需要更多空间。macOS 用于编辑、分析结果；真实 Firecracker / FUSE 测试在 Linux 上运行。

需要 KVM、FUSE、EROFS、loop device、network / mount namespace、cgroup，以及匹配 kit 的内核能力。机器上同时只运行一套该 FC fixture；驱动使用固定服务端口和网络段。

Ubuntu 依赖示例：

```bash
apt-get update
apt-get install -y python3 python3-venv python3-pip ca-certificates curl \
  openssl iproute2 iptables ipset kmod util-linux procps fuse3 libfuse3-3 \
  e2fsprogs erofs-utils xfsprogs strace busybox-static libseccomp2 libapparmor1
modprobe fuse
modprobe loop
modprobe erofs
test -c /dev/kvm
test -c /dev/fuse
python3 - <<'PY'
import fcntl
with open('/dev/kvm', 'rb', buffering=0) as f:
    assert fcntl.ioctl(f.fileno(), 0xAE00, 0) == 12
print('KVM API 12 OK')
PY
```

KVM API 检查成功后，驱动还会实际创建 VM / vCPU。不要在已有 ADX 业务节点直接执行。预先检查以下网络不冲突：`10.240.0.0/24`、`10.231.16.0/20`、`10.231.32.0/20`；端口至少检查 `5000`、`6379`、`8443`、`9443`、`17000`、`19090`、`19091`，以及生成配置中的监听地址。

性能对照使用原生 ext4 / XFS 数据盘；XFS 对照要求显式挂载点和 reflink 支持。不要把 overlay、tmpfs 或 loop / file-backed Ceph OSD 标为原生盘结果。记录 `lsblk -f`、`findmnt`、设备序列号、运行目录与 OSD 的设备映射。本文不提供整盘格式化命令。

### 3.2 同事需要领到的制品

| 输入 | 要求 |
| --- | --- |
| ADX 源码 | 本文版本或后续明确固定的 commit，包含 `afs/` 与 `build/e2e/` |
| ADX 统一 package | Linux release，`ADX_WITH_AFS=1`，带 `manifest.json`、SDK wheel、静态 Execd、EROFS runtime、`afs-meta`、`afs-node` |
| sandboxd backend 制品目录 | `manifest.json`，与 kit 的 sandboxd revision / target 匹配 |
| Firecracker kit | `manifest.json` 与 `kit.py` 要求的完整文件，见下方布局 |
| `execd.tar` | 同包构建的 Execd OCI/Docker 归档，guest 含 Python 3，用于启动 512 MiB HTTP 工作负载 |
| JuiceFS client | 固定的 1.4.1 二进制与 SHA256；不要使用浮动下载脚本 |
| 直接恢复 helper，可选 | 固定 sandboxd revision 的 `test/e2e/checkpoint-restore` 构建；直接 HTTP 模式还需支持 `-response-file` |

统一发布包的源码构建入口为 `make ADX_WITH_AFS=1 JOBS=4 platform-release`，所需 Redis 固定二进制、native target、独立 `CARGO_TARGET_DIR` 和输出参数见[发布构建脚本](../../build/release/build.sh)。构建环境与运行环境分开；运行机不临时编译 VMM、guest 或 sandboxd。

kit 必需布局：

```text
fc-kit/
  manifest.json
  bin/{sandboxd,sbox,checkpoint-restore,firecracker}
  artifacts/{Image,initrd.img}
  tools/{virtiofsd,distill_fs,minio,docker-registry,redis-cli}
```

当前锁定的 sandboxd revision 以 `third_party/sandboxd/source.json` 为准，本文版本为 `efc201531d7e2e9d69505da151eb66084b61eebf`。不能用旧 initrd 或不同 sandboxd 混拼 kit。

已有 E2E bundle 可直接提供 `execd.tar`。需要重建 bundle 时，使用 `build/e2e/prepare.py --help` 所列的 `--package`、`--backend`、`--sdk-wheel`、`--sdk-candidate`、`--runtime-base`、`--execd-base`、`--firecracker-kit`、`--output`；SDK candidate commit 必须与 package 匹配。guest base 必须包含 Python 3。

### 3.3 安装测试目录

以下路径为示例输入，请先放好实际制品。`ADX_FC_BASE` 和 `/opt/adx-fc` 必须是本轮独占的新目录。环境变量要在后续命令所在的同一个 root Bash 中设置。

```bash
export ADX_SRC=/srv/agent-dx
export ADX_PACKAGE=/srv/adx-input/package
export ADX_BACKEND=/srv/adx-input/backend
export ADX_KIT=/srv/adx-input/fc-kit
export ADX_EXECD_TAR=/srv/adx-input/bundle/execd.tar
export ADX_FC_BASE=/opt/adx-storage
export ADX_DATA=/mnt/adx-test
export ADX_RUNS="$ADX_DATA/runs"
cd "$ADX_SRC"
python3 build/release/package.py verify "$ADX_PACKAGE"
python3 - "$ADX_KIT" "$ADX_BACKEND/manifest.json" <<'PY'
import json, sys
from pathlib import Path
sys.path.insert(0, 'build/e2e')
from firecracker.kit import verify
verify(Path(sys.argv[1]), json.loads(Path(sys.argv[2]).read_text()))
print('FC kit verified')
PY
test ! -e "$ADX_FC_BASE"
test ! -e /opt/adx-fc
install -d "$ADX_FC_BASE/tools" "$ADX_RUNS"
cp -a "$ADX_PACKAGE" "$ADX_FC_BASE/package"
cp -a "$ADX_PACKAGE/sdk" "$ADX_FC_BASE/sdk"
cp -a "$ADX_KIT" /opt/adx-fc
cp -a "$ADX_KIT/tools/." "$ADX_FC_BASE/tools/"
cp -a build/e2e "$ADX_FC_BASE/e2e"
cp build/ci/rpc_certificates.py "$ADX_FC_BASE/e2e/"
cp build/release/package.py "$ADX_FC_BASE/e2e/package.py"
cp "$ADX_PACKAGE/lib/package_manifest.py" "$ADX_FC_BASE/e2e/package_manifest.py"
cp "$ADX_EXECD_TAR" "$ADX_FC_BASE/execd.tar"
python3 -m venv "$ADX_FC_BASE/client"
"$ADX_FC_BASE/client/bin/pip" install "$ADX_FC_BASE"/sdk/adx_environment-*.whl
"$ADX_FC_BASE/client/bin/pip" freeze >"$ADX_FC_BASE/sdk-installed.txt"
test -x "$ADX_FC_BASE/package/bin/afs-meta"
test -x "$ADX_FC_BASE/package/bin/afs-node"
python3 "$ADX_FC_BASE/e2e/package.py" verify "$ADX_FC_BASE/package"
```

`package_manifest.py` 必须与 package 一起带过去，否则脱离源码目录运行验证器时会导入失败。保留 `ADX_PACKAGE/manifest.json` 和 kit manifest；性能汇总会核对制品身份。

## 4. AFS：部署与运行

### 4.1 独立文件系统 smoke

```bash
cd "$ADX_SRC"
python3 build/e2e/afs/scripts/dfs/three_node_core.py \
  --repo "$ADX_SRC" --binary-dir "$ADX_FC_BASE/package/bin" \
  --work-root "$ADX_DATA/afs-smoke-001"
```

脚本启动 local-file Meta 与三个 DFS Node，连续写三个 64 KiB 版本，让另外两个客户端读回并检查删除可见性；最后正常卸载与停止。成功看该目录的 `result.json`，同时检查进程退出和卸载记录。失败时保留目录，下次换新名字，不覆盖原始失败证据。

### 4.2 AFS + Firecracker 初次跑通

AFS 存储套件会自动启动专用 Meta / Node、生成 mTLS 配置、挂载三个客户端，并为两个 sandboxd 放置独立 filestore。无需预先手工启动 AFS 服务。AFS 数据根目录的父目录必须已存在。

```bash
install -d "$ADX_DATA/afs"
env -u ADX_FC_JUICEFS_CONFIG -u ADX_FC_OBS_CONFIG -u ADX_STORAGE_NATIVE_XFS_ROOT \
  -u ADX_STORAGE_GC -u ADX_STORAGE_TRACE_RESTORE -u ADX_STORAGE_FILESYSTEM_TRACE -u AFS_META_PROFILE \
  ADX_FC_BASE="$ADX_FC_BASE" ADX_FC_SOURCE="$ADX_SRC" \
  ADX_FC_AFS_ROOT="$ADX_DATA/afs/services-smoke-001" \
  ADX_STORAGE_IO_ENGINE=Async ADX_STORAGE_BENCHMARK=1 ADX_STORAGE_REPETITIONS=1 \
  "$ADX_FC_BASE/client/bin/python" -u "$ADX_FC_BASE/e2e/firecracker/storage.py" \
  "$ADX_RUNS/afs-smoke-001" >"$ADX_RUNS/afs-smoke-001.log" 2>&1
```

此模式验证两组流量唤醒和完整工作集校验；不运行完整故障集合。`storage.py` 结束后会停止自己的 AFS 服务并卸载自己的挂载；它不是常驻存储集群安装器。

### 4.3 完整生命周期与 GC

使用新的 FC / AFS 根目录，去掉 benchmark，打开 AFS 回收测试：

```bash
env -u ADX_FC_JUICEFS_CONFIG -u ADX_FC_OBS_CONFIG -u ADX_STORAGE_NATIVE_XFS_ROOT \
  -u ADX_STORAGE_BENCHMARK -u ADX_STORAGE_TRACE_RESTORE -u ADX_STORAGE_FILESYSTEM_TRACE -u AFS_META_PROFILE \
  ADX_FC_BASE="$ADX_FC_BASE" ADX_FC_SOURCE="$ADX_SRC" \
  ADX_FC_AFS_ROOT="$ADX_DATA/afs/services-lifecycle-001" \
  ADX_STORAGE_IO_ENGINE=Async ADX_STORAGE_REPETITIONS=3 ADX_STORAGE_GC=1 \
  "$ADX_FC_BASE/client/bin/python" -u "$ADX_FC_BASE/e2e/firecracker/storage.py" \
  "$ADX_RUNS/afs-lifecycle-001" >"$ADX_RUNS/afs-lifecycle-001.log" 2>&1
```

GC 模式把专用测试 Meta 的幂等结果窗口改为 30 秒、Watch 保留上限改为 16 个事件（按完整 revision 截断），保留 60 秒引用保护。保护期等待、节点 session 更换和两批各三次暂停恢复会增加耗时；不以整个套件运行时间衡量恢复性能。

## 5. JuiceFS：PostgreSQL + Ceph RGW + 三个 client

### 5.1 存储服务由谁部署

`storage.py` 只负责三套 JuiceFS client 的启动与卸载，**不会创建 PostgreSQL、Ceph 集群或格式化已有卷**。可复用同事已部署的测试服务；从零部署可参考本节。元数据库、对象 bucket、客户端版本和数据盘位置必须明确记录。

对齐本次性能配置：JuiceFS 1.4.1、PostgreSQL 17、原生 Ceph OSD、`fsync=on`、`synchronous_commit=on`、JuiceFS 块大小 1 MiB、最大预读 4 MiB、`cache-size=0`、`cache-dir=memory`、不启用 `--writeback`。这是本轮试验参数，不代表所有工作负载的最优配置。

### 5.2 PostgreSQL 17 最小测试服务

先安装 Docker，并选择具体 digest 的 PostgreSQL 17 镜像；以下密码文件和数据目录放在专用原生盘。若 PostgreSQL 已有服务，跳过创建，提供独立数据库与账号并检查持久化设置。

```bash
export JFS_DIR="$ADX_DATA/juicefs"
export PG_IMAGE='postgres:17@sha256:REPLACE_WITH_APPROVED_DIGEST'
install -d -m 700 "$JFS_DIR/private" "$JFS_DIR/postgres"
umask 077
openssl rand -hex 24 >"$JFS_DIR/private/pg-password"
docker run -d --name adx-jfs-pg --network host \
  --mount "type=bind,src=$JFS_DIR/postgres,dst=/var/lib/postgresql/data" \
  --mount "type=bind,src=$JFS_DIR/private/pg-password,dst=/run/secrets/pg-password,readonly" \
  -e POSTGRES_USER=juicefs -e POSTGRES_DB=juicefs \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/pg-password "$PG_IMAGE" \
  -p 15432 -c listen_addresses=127.0.0.1 \
  -c fsync=on -c synchronous_commit=on -c full_page_writes=on
```

等待数据库 ready，然后检查设置：

```bash
docker exec adx-jfs-pg pg_isready -U juicefs -d juicefs -p 15432
docker exec adx-jfs-pg psql -U juicefs -d juicefs -p 15432 -Atc \
  'SHOW fsync; SHOW synchronous_commit; SHOW full_page_writes;'
```

三项都应为 `on`。如果使用远端数据库，改用对应主机地址和 TLS 配置；本例的 `sslmode=disable` 仅用于同机隔离测试。JuiceFS 密码传递方式见[官方 PostgreSQL 指南](https://juicefs.com/docs/community/postgresql_best_practices/)。

### 5.3 Ceph RGW

已有 Ceph RGW 时，只需专用测试用户、bucket / endpoint 和版本记录。新部署需先安装与所选 Ceph 版本匹配的 `cephadm`、容器运行时、LVM 和时间同步服务。以下为**专用空主机、单 OSD 测试集群**的 cephadm 示例；已有 Ceph 集群跳过 bootstrap。

```bash
export CEPH_IMAGE='quay.io/ceph/ceph@sha256:REPLACE_WITH_APPROVED_DIGEST'
export CEPH_HOST="$(hostname -s)"
export CEPH_MON_IP='REPLACE_WITH_TEST_HOST_PRIVATE_IP'
cephadm --image "$CEPH_IMAGE" bootstrap --mon-ip "$CEPH_MON_IP" \
  --single-host-defaults --skip-monitoring-stack --skip-dashboard
cephadm shell -- ceph orch device ls --refresh
```

添加 OSD 的下一条命令会初始化指定设备。先由机器维护人确认它是独占空盘，核对 `lsblk`、挂载、序列号；`CEPH_HOST` 以 `ceph orch host ls` 实际登记名为准。仅填写这一个设备，不使用“所有空闲盘”自动发现策略。

```bash
export OSD_DEVICE='/dev/disk/by-id/REPLACE_WITH_DEDICATED_EMPTY_DISK'
test -b "$OSD_DEVICE"
lsblk -f "$OSD_DEVICE"
cephadm shell -- ceph orch daemon add osd "$CEPH_HOST:$OSD_DEVICE"
cephadm shell -- ceph orch apply rgw adx-jfs --placement="1 $CEPH_HOST" --port=7480
cephadm shell -- ceph -s
```

确认 OSD `up/in`、RGW 可用且 PG 能正常提供读写。单副本只用于本节测试，不能作为 HA 结果。Ceph 服务与测试机其他进程共用资源时，记录 OSD 内存配置与限额。

创建专用对象用户，输出文件包含凭据，只保存在私有目录：

```bash
umask 077
cephadm shell -- radosgw-admin user create \
  --uid=adx-jfs-test --display-name=adx-jfs-test \
  >"$JFS_DIR/private/rgw-user.private.json"
export JFS_BUCKET_URL="http://$CEPH_MON_IP:7480/adx-juicefs-test"
```

`format` 使用此用户访问专用 bucket；应具备需要的建桶与对象读写权限，或提前创建该 bucket。不得把凭据写进公共日志。Ceph 创建流程参考[cephadm 部署](https://docs.ceph.com/en/latest/cephadm/install/)、[指定 OSD 设备](https://docs.ceph.com/en/latest/cephadm/services/osd/)、[RGW 服务](https://docs.ceph.com/en/latest/cephadm/services/rgw/)。

### 5.4 初始化一个全新 JuiceFS 卷

安装已固定版本和摘要的 JuiceFS 二进制，例如 `/usr/local/bin/juicefs`。先核对本机帮助和版本。下面只对**新数据库 / 新测试卷**执行一次，已有卷使用 `juicefs status` 核对配置，不重复 format。

```bash
export JFS_BINARY=/usr/local/bin/juicefs
export JFS_META_URL='postgres://juicefs@127.0.0.1:15432/juicefs?sslmode=disable'
export JFS_VOLUME=adx-jfs-test
"$JFS_BINARY" version
"$JFS_BINARY" format --help
python3 - <<'PY'
import json, os, subprocess
from pathlib import Path
private = Path(os.environ['JFS_DIR']) / 'private'
keys = json.loads((private / 'rgw-user.private.json').read_text())['keys'][0]
env = {**os.environ, 'META_PASSWORD': (private / 'pg-password').read_text().strip(),
       'ACCESS_KEY': keys['access_key'], 'SECRET_KEY': keys['secret_key']}
subprocess.run([os.environ['JFS_BINARY'], 'format', '--storage', 's3',
    '--bucket', os.environ['JFS_BUCKET_URL'], '--block-size', '1024', '--trash-days', '0',
    os.environ['JFS_META_URL'], os.environ['JFS_VOLUME']], env=env, check=True)
PY
```

`trash-days=0` 用于专用回收试验，避免回收站保留干扰观测；若使用其他策略，必须记录。块大小属于卷格式，预读属于 mount 参数，更换块大小应使用独立卷。S3 bucket URL、format 与参数语义见[JuiceFS 对象存储配置](https://juicefs.com/docs/community/reference/how_to_set_up_object_storage/)和[命令参考](https://juicefs.com/docs/community/command_reference/)。

### 5.5 写入 fixture 配置

身份文件填写实测版本、镜像 digest 和数据库设置。`postgres_synchronous_commit` 不是用来开启数据库选项的开关：驱动只核对记录值，操作者仍需执行上面的数据库检查。

```bash
export JFS_CONFIG="$JFS_DIR/private/fixture.json"
python3 - <<'PY'
import hashlib, json, os
from pathlib import Path
root = Path(os.environ['JFS_DIR'])
binary = Path(os.environ['JFS_BINARY'])
identity = root / 'private/identity.json'
identity.write_text(json.dumps({
    'juicefs_version': '1.4.1', 'juicefs_block_size_kib': 1024,
    'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
    'postgres': os.environ['PG_IMAGE'], 'postgres_synchronous_commit': 'on',
    'ceph': os.environ['CEPH_IMAGE'], 'object_endpoint': os.environ['JFS_BUCKET_URL']
}, indent=2) + '\n')
config = {
    'binary': str(binary), 'metadata_url': os.environ['JFS_META_URL'],
    'metadata_password_file': str(root / 'private/pg-password'),
    'volume': os.environ['JFS_VOLUME'], 'alias': 'juicefs-checkpoints',
    'mount_root': str(root / 'mounts'), 'identity_file': str(identity),
    'max_readahead_mib': 4
}
path = Path(os.environ['JFS_CONFIG'])
path.write_text(json.dumps(config, indent=2) + '\n')
path.chmod(0o600)
PY
```

`fixture.json` 必须为非符号链接、权限 `0600` 的普通文件。`metadata_url` 中不能带密码。已有外部 PostgreSQL / Ceph 时，替换对应字段与身份记录，密码文件仍单独提供。

不要手工预挂载 `mounts/node1`、`mounts/node2`、`mounts/coordinator`：驱动要求这些挂载点未挂载且为空。驱动分别使用 `--cache-size 0 --cache-dir memory --max-readahead 4`，不开 `--writeback`；客户端挂载来源必须为 `JuiceFS:<volume>`。

### 5.6 JuiceFS + Firecracker

先跑一轮性能 smoke：

```bash
env -u ADX_FC_AFS_ROOT -u ADX_FC_OBS_CONFIG -u ADX_STORAGE_NATIVE_XFS_ROOT \
  -u ADX_STORAGE_GC -u ADX_STORAGE_TRACE_RESTORE -u ADX_STORAGE_FILESYSTEM_TRACE -u AFS_META_PROFILE \
  ADX_FC_BASE="$ADX_FC_BASE" ADX_FC_SOURCE="$ADX_SRC" \
  ADX_FC_JUICEFS_CONFIG="$JFS_CONFIG" ADX_STORAGE_IO_ENGINE=Async \
  ADX_STORAGE_BENCHMARK=1 ADX_STORAGE_REPETITIONS=1 \
  "$ADX_FC_BASE/client/bin/python" -u "$ADX_FC_BASE/e2e/firecracker/storage.py" \
  "$ADX_RUNS/juicefs-smoke-001" >"$ADX_RUNS/juicefs-smoke-001.log" 2>&1
```

完整生命周期使用新运行名，取消 benchmark：

```bash
env -u ADX_FC_AFS_ROOT -u ADX_FC_OBS_CONFIG -u ADX_STORAGE_NATIVE_XFS_ROOT \
  -u ADX_STORAGE_GC -u ADX_STORAGE_BENCHMARK -u ADX_STORAGE_TRACE_RESTORE \
  -u ADX_STORAGE_FILESYSTEM_TRACE -u AFS_META_PROFILE \
  ADX_FC_BASE="$ADX_FC_BASE" ADX_FC_SOURCE="$ADX_SRC" \
  ADX_FC_JUICEFS_CONFIG="$JFS_CONFIG" ADX_STORAGE_IO_ENGINE=Async ADX_STORAGE_REPETITIONS=3 \
  "$ADX_FC_BASE/client/bin/python" -u "$ADX_FC_BASE/e2e/firecracker/storage.py" \
  "$ADX_RUNS/juicefs-lifecycle-001" >"$ADX_RUNS/juicefs-lifecycle-001.log" 2>&1
```

启动时还会自动做 16 MiB 文件系统探针：跨客户端 `copy_file_range` 不下载源对象数据、克隆写入不修改源、源删除后仍有目录项的克隆保持完整。结果在 `evidence/juicefs/filesystem-proof.json`。

退出时三个 JuiceFS client 会卸载，PostgreSQL / Ceph 服务继续由部署人管理。Coordinator 删除文件成功不等于 RGW 对象空间即时释放，JuiceFS 的物理对象 GC / trash 必须单独检查；不能拿 AFS Meta 的 chunk 收敛断言替代它。

## 6. 接入已有 ADX 部署时配置什么

上面的 fixture 会自动生成配置。接入自己的常驻 ADX 部署时，需要先由部署系统启动存储与挂载，再启动 sandboxd / adxlet / Coordinator。三个客户端的绝对路径可不同，但必须指向同一 namespace，且 `alias`、`prefix` 相同。

两个 adxlet 的配置示意，各自填写自己的挂载根：

```json
{
  "checkpoint_storage": {
    "kind": "distributed_fs",
    "alias": "shared-checkpoints",
    "mount_root": "/mnt/shared",
    "prefix": "adx/checkpoints"
  }
}
```

Coordinator 配置：

```json
{
  "checkpoint_reclaimers": [{
    "kind": "distributed_fs",
    "alias": "shared-checkpoints",
    "mount_root": "/mnt/shared",
    "prefix": "adx/checkpoints"
  }]
}
```

同时移除原 `checkpoint_dir`，两者不能一起配置。默认部署仍是本地 checkpoint，本节是显式切换共享模式。

sandboxd 必须在其 mount namespace 里看见所配置绝对路径。将各节点的 `filestore_dir` 指向同一文件系统的独立 `runtime/<node>` 目录，并选取支持的磁盘模式，例如：

```toml
[plugin.runtime.firecracker]
writable_io_engine = "Async"
writable_cache_type = "Writeback"
```

AFS 当前不能直接沿用运行时默认 `AsyncDirect`。ADX 启动会固定挂载身份，挂载丢失 / 被覆盖 / 重挂载后访问返回错误；确认挂载正确后重启使用它的 ADX 进程。不要只创建同名本地空目录代替真实挂载。

## 7. 用例、断言与证据

完整存储集合的六项名称由 `storage_contract.py` 固定，不能只看到 SDK 返回成功就宣布验收通过。

| 用例 | 关键动作与通过条件 | 主要证据 |
| --- | --- | --- |
| 持久捕获、目录直写 | 运行时直接写共享路径，完成发布屏障后提交恢复点；恢复使用共享目录 | `sdk-storage.json`、`storage.json`、实际 mount 记录 |
| 并发流量唤醒 | 每组 12 个真实 HTTP 请求交替经过两个 Ingress；只恢复一次；guest PID / 状态 / 文件保持 | `sdk-storage.json` 样本、两个 Ingress 日志、sandboxd 物理恢复日志 |
| 节点故障与恢复中 Coordinator 重启 | 原节点失效后跨节点恢复；旧执行输入保护跨 Coordinator 重启仍存在 | `fault-progress.json`、`recovery-runtime.trace*`、执行输入保护记录 |
| Coordinator 回收 | 删除 Environment 后，未确认停止的旧执行仍阻止共享回收；原节点清理对账后才允许删除 | `coordinator-gc.trace*`、故障记录、最终目录 / inventory |
| 匿名恢复点归属 | 匿名 checkpoint 随 Environment 删除而回收，活跃执行输入仍受保护 | `sdk-storage.json`、最终 `result.json` |
| 未完成捕获回收 | 已停止写入、`size_bytes=0`、未发布 manifest 的预留由 Coordinator 回收 | 半成品捕获与 GC 记录、最终文件检查 |

额外覆盖：

- `shared-mounts.json`：外部文件系统写者、两个只读消费者；暂停恢复后共享数据保持最新值、私有状态恢复，Environment 删除后共享目录仍保留。当前锁定 Firecracker 构建拒绝 RW host-path；本用例不能当作 RW 挂载支持证据。
- `direct-storage.json`：提供 helper 时，先做直接 Full 恢复，再改写 64 MiB、做 Incremental 恢复，每次对全部 512 MiB 校验 SHA-256。
- AFS `gc-file-lifecycle.json`、`gc-observations.json`：GC 实际运行期间 COW / open-unlink 可读，AFS Node session 更换后读者仍受保护，关闭读者并经过保护期后 Meta 与物理 chunk 回落。
- Snapshot 使用中删除应返回 `SNAPSHOT_IN_USE` 和使用者 Sandbox 列表；该契约的定向测试见 `platform/coordinator/tests/storage/artifact_lifecycle.rs`，完整 FC Snapshot 场景见[FC 验收说明](../../build/e2e/firecracker/README.md)。不要把本存储套件六项扩写为已经覆盖所有 Snapshot API / TTL / 应答丢失场景。

### 7.1 最终通过条件

```bash
export CHECK_RUN="$ADX_RUNS/afs-lifecycle-001"
python3 - "$CHECK_RUN/evidence" "$ADX_SRC" <<'PY'
import json, sys
from pathlib import Path
evidence = Path(sys.argv[1])
sys.path.insert(0, str(Path(sys.argv[2]) / 'build/e2e/firecracker'))
from storage_contract import verify
result = json.loads((evidence / 'result.json').read_text())
verify(result)
cleanup = json.loads((evidence / 'storage-cleanup.json').read_text())
assert cleanup['exit_code'] == 0 and cleanup['errors'] == [], cleanup
assert result['inventories'] == {'node1': 0, 'node2': 0}
print('PASS:', result['storage']['kind'], len(result['cases']), 'cases')
PY
```

AFS 同时检查 `evidence/afs/afs-cleanup.json` 的 `errors=[]`。AFS GC 模式还需检查 GC 观察数据，而不只是空目录。JuiceFS 还需 `filesystem-proof.json` 通过，且三个 mount 已卸载；对象物理回收另记。

失败时保留顶层 `.log`、`evidence/result.json`、`sdk-storage.json`、组件日志和清理结果；重新运行使用新目录。`result.status=passed` 与 `storage-cleanup.exit_code=0/errors=[]` 必须同时满足。

## 8. 性能对比怎么跑

### 8.1 固定可比条件

同一 package / SDK / sandboxd / Firecracker / Image / initrd / guest 镜像 / 工作负载、同一物理盘布局与 CPU 内存限额，统一 `ADX_STORAGE_IO_ENGINE=Async`。正式计时关闭 `AFS_META_PROFILE`、restore trace 和 filesystem trace。

每个后端三次独立运行，每次 `ADX_STORAGE_REPETITIONS=1`，便于 fresh-client；每轮中 cold / warm 各一组。不要并行跑后端对比。AFS 每次新服务目录；JuiceFS 每次新客户端且卸载完成后再起下一轮。服务端缓存不清空，因此 cold 表示跨客户端 / 无该制品缓存，并非全链路 cold cache。

各后端若要同时带直接 sandboxd 对照，先提供相同 helper：

```bash
export ADX_STORAGE_PROBE=/opt/adx-fc/bin/checkpoint-restore
unset ADX_STORAGE_DIRECT_HTTP
export ADX_STORAGE_CHECKPOINT_MODE=incremental
test -x "$ADX_STORAGE_PROBE"
```

这组环境需在启动命令中保留。上面使用 kit 的普通 helper，结果包含 `sbox exec` 耗时。若制品提供者另外交付了支持 `-response-file` 的 `checkpoint-probe-http`，可将 `ADX_STORAGE_PROBE` 指向它，并设置 `ADX_STORAGE_DIRECT_HTTP=1`，得到直接 HTTP 边界；两种探针不能混合汇总。未提供 helper 仍可跑完整 ADX 存储集合，但没有直接运行时对照。

顺序运行三轮 AFS、三轮 JuiceFS；任一轮失败立即停止，保留日志：

```bash
(
  set -e
  for backend in afs juicefs; do
    for trial in 01 02 03; do
      name="$backend-perf-$trial"
      if [ "$backend" = afs ]; then
        storage_env=("ADX_FC_AFS_ROOT=$ADX_DATA/afs/services-$name")
      else
        storage_env=("ADX_FC_JUICEFS_CONFIG=$JFS_CONFIG")
      fi
      env -u ADX_FC_AFS_ROOT -u ADX_FC_JUICEFS_CONFIG -u ADX_FC_OBS_CONFIG \
        -u ADX_STORAGE_NATIVE_XFS_ROOT -u ADX_STORAGE_GC -u AFS_META_PROFILE \
        -u ADX_STORAGE_TRACE_RESTORE -u ADX_STORAGE_FILESYSTEM_TRACE \
        ADX_FC_BASE="$ADX_FC_BASE" ADX_FC_SOURCE="$ADX_SRC" \
        ADX_STORAGE_IO_ENGINE=Async ADX_STORAGE_BENCHMARK=1 ADX_STORAGE_REPETITIONS=1 \
        "${storage_env[@]}" \
        "$ADX_FC_BASE/client/bin/python" -u "$ADX_FC_BASE/e2e/firecracker/storage.py" \
        "$ADX_RUNS/$name" >"$ADX_RUNS/$name.log" 2>&1
    done
  done
)
```

### 8.2 原生 XFS 对照

使用已有的专用 XFS 挂载点，不在这里新建文件系统。保留上面的 helper 与 incremental 设置：

```bash
export ADX_XFS_ROOT=/mnt/dedicated-native-xfs
findmnt --mountpoint "$ADX_XFS_ROOT" -o TARGET,SOURCE,FSTYPE
env -u ADX_FC_AFS_ROOT -u ADX_FC_JUICEFS_CONFIG -u ADX_FC_OBS_CONFIG \
  -u ADX_STORAGE_GC -u ADX_STORAGE_BENCHMARK -u ADX_STORAGE_TRACE_RESTORE \
  -u ADX_STORAGE_FILESYSTEM_TRACE -u AFS_META_PROFILE \
  ADX_FC_BASE="$ADX_FC_BASE" ADX_FC_SOURCE="$ADX_SRC" \
  ADX_STORAGE_NATIVE_XFS_ROOT="$ADX_XFS_ROOT" ADX_STORAGE_IO_ENGINE=Async \
  "$ADX_FC_BASE/client/bin/python" -u "$ADX_FC_BASE/e2e/firecracker/storage.py" \
  "$ADX_RUNS/xfs-direct-001" >"$ADX_RUNS/xfs-direct-001.log" 2>&1
```

XFS 只跑直接 sandboxd 对照，结果标记 `native_control_only`，不跑 ADX SDK / 流量唤醒，不能当作 XFS 的 ADX 端到端数据。XFS 结果用 `native_xfs.verify_result()` 校验，不用第 7.1 节的共享存储 verifier。

### 8.3 汇总和指标含义

三轮 AFS / JuiceFS 都成功且清理通过后：

```bash
python3 "$ADX_SRC/build/e2e/firecracker/compare_storage.py" \
  "$ADX_RUNS/afs-perf-01/evidence" "$ADX_RUNS/afs-perf-02/evidence" "$ADX_RUNS/afs-perf-03/evidence" \
  "$ADX_RUNS/juicefs-perf-01/evidence" "$ADX_RUNS/juicefs-perf-02/evidence" "$ADX_RUNS/juicefs-perf-03/evidence" \
  --output "$ADX_RUNS/comparison.json" --markdown "$ADX_RUNS/comparison.md"
```

汇总会拒绝运行时 / 发布包 / 负载身份不同、带诊断开关、清理未完成的样本。同一 JuiceFS 后端块大小与预读等参数也不能混合。XFS 对照单独分析 `direct-storage.json`；当前通用汇总器不直接接收它作为共享后端组。

| 指标 | 含义 |
| --- | --- |
| `pause_seconds` | 暂停到恢复点发布完成；共享后端包含持久化屏障 |
| `first_http_seconds` | 12 个唤醒请求中最短的请求耗时 |
| `touch_seconds` / checksum | 首次响应后再读取全部 512 MiB、计算并核对独立 SHA-256 的耗时 |
| direct `rpc_seconds` | 直接 sandboxd 恢复 RPC 耗时，不代表业务已可服务 |
| direct `first_response_seconds` | 直接恢复后首个 HTTP；具体是否包含 exec 控制路径由 probe 身份决定 |
| `remote_read_bytes.first` | 整组 12 个响应完成后、checksum 前采样，不能作为精确首响应瞬间的读量 |

`Full` / `Incremental` 表示捕获内存的方式，两者恢复都可通过 mmap 按需缺页；`Full` 不等于恢复前预加载全部内存。直接探针以 Python 服务为初始进程，ADX 路径由 Execd 启动服务，两条路径的总耗时不能直接相减当作控制面开销。三轮报告中位数和范围，不以三个样本计算可信 P95。

需要 S3 对照时，清除 AFS / JuiceFS / XFS 选择变量，`storage.py` 默认启动本机 MinIO；真实 OBS 则提供私有 `ADX_FC_OBS_CONFIG`。MinIO、Ceph RGW 与远端 OBS 的网络和持久化成本不同，结果分别命名。S3 模式不支持本手册的直接文件系统 helper，需取消 `ADX_STORAGE_PROBE` 等直接探针变量；不要与带 helper 的结果强行混合汇总。

## 9. 故障定位与收尾

| 现象 | 先检查什么 |
| --- | --- |
| 缺 package、kit、SDK 或 Python 依赖 | 第 3 节制品清单、manifest 校验、实际运行的 venv；不要混用系统 SDK |
| `run directory must be new` | 使用新运行名；同时给 AFS 选择新的服务根目录 |
| FUSE 挂载失败 / startup 挂载身份不匹配 | `/dev/fuse`、权限、真实 `findmnt`、挂载是否在 sandboxd namespace 可见、是否被本地目录覆盖 |
| JuiceFS client 提前退出 | PG ready、密码文件、URL、TLS 模式、Ceph 凭据与 endpoint；看脱敏的 client 日志 |
| `AsyncDirect` / 对齐错误 | 使用 `ADX_STORAGE_IO_ENGINE=Async`；核对最终生成的两份 sandboxd 配置 |
| clone 产生对象下载、`EXDEV` 或恢复前全量复制 | checkpoint 与 runtime filestore 是否同一文件系统，clone 目标是否归本客户端，COW 探针是否通过 |
| 首 HTTP 慢，但 VMM load 很短 | 拆开 sandboxd prepare / COW / load / agent，ADX Execd readiness、路由等待与 checksum；不要把总耗时全部归给存储 |
| JuiceFS GET 读放大 | 核对卷 block size、mount readahead、重复范围、运行期 memory.live 写入；另开诊断轮，不混进性能样本 |
| `EBUSY` / unlink / 卸载失败 | 活跃 VMM、memory.live、FUSE 读者与写回、停止顺序；保留首次错误与重试证据，不直接删除底层 chunk |
| Environment 删除后空间没有立刻下降 | Coordinator 是否仍有运行 / 旧执行输入保护；AFS 是否过保护期；JuiceFS 是否仍有 trash / 待物理 GC 对象 |
| 节点失联后文件一直保留 | 心跳失效不证明旧进程已停止；先隔离 / 清理旧执行并恢复对账，不只按 TTL 强制删除 |

分段汇总：

```bash
python3 "$ADX_SRC/build/e2e/firecracker/profile_storage.py" \
  "$CHECK_RUN/evidence" --output "$ADX_RUNS/stages.json"
```

AFS 按需恢复独立诊断：

```bash
env -u ADX_FC_JUICEFS_CONFIG -u ADX_FC_OBS_CONFIG -u ADX_STORAGE_NATIVE_XFS_ROOT \
  -u ADX_STORAGE_GC -u ADX_STORAGE_BENCHMARK -u ADX_STORAGE_FILESYSTEM_TRACE -u AFS_META_PROFILE \
  ADX_FC_BASE="$ADX_FC_BASE" ADX_FC_SOURCE="$ADX_SRC" \
  ADX_FC_AFS_ROOT="$ADX_DATA/afs/services-trace-001" \
  ADX_STORAGE_IO_ENGINE=Async ADX_STORAGE_REPETITIONS=1 ADX_STORAGE_TRACE_RESTORE=1 \
  "$ADX_FC_BASE/client/bin/python" -u "$ADX_FC_BASE/e2e/firecracker/storage.py" \
  "$ADX_RUNS/afs-trace-001" >"$ADX_RUNS/afs-trace-001.log" 2>&1
```

它要求 COW 事件、首次触页前未全量读取源 memory、延迟触页带来新增远端读取；结果不计入性能样本。Meta profile 使用 `AFS_META_PROFILE=1`，也单独保存和分析。

每次只分享 `RUN_ROOT/export/` 脱敏导出、必要的顶层日志及结果汇总。原始 `secrets/`、PostgreSQL 密码、RGW user JSON、JuiceFS 私有配置与数据库备份不作为公开测试附件；导出前仍应核对自定义凭据是否进入日志。

驱动正常退出会回收自己的 ADX / sandboxd 进程、网络 namespace、挂载和 AFS 服务。若进程被强制杀死，先按 `topology.json` 与运行目录核对残留，只清理本轮拥有的资源。JuiceFS 的 PG / Ceph、OSD 和卷不会随测试退出销毁，交由部署人管理；不要为了清理一轮测试格式化数据库或删除共享数据盘。

## 10. 交付结果模板

每位同事附一份以下信息，便于比较不同机器的结果：

```text
源码 commit / package commit + dirty：
package / SDK / sandboxd / Firecracker / kernel / initrd / guest 镜像摘要：
机器 CPU、RAM、内核；KVM；单机双节点或其他拓扑：
运行目录与设备映射；AFS 数据 / PG 数据 / Ceph OSD 所在设备：
后端及参数：AFS 副本数 / Meta 持久化；或 JFS 版本 / block / readahead / cache / writeback / PG / Ceph：
运行范围：smoke / benchmark / lifecycle / AFS-GC / direct：
result.status、cases 数、inventories、storage-cleanup：
内存完整校验、物理恢复次数、引用保护与回收结果：
pause、direct RPC、首 HTTP、完整触页中位数与范围：
失败与未覆盖项：
脱敏 export / 日志 / comparison 文件位置：
```

本手册编写时核对了仓内驱动、配置与 verifier；具体部署参数还参考上述官方文档。本文新增的从零 PostgreSQL / cephadm 安装步骤未在本轮重新部署验证。此前真实 FC 数据来自固定版本试验；同事需在目标机以本轮 `result.json` 和清理证据形成自己的验收结论。
