//! OwnerFs 的根目录身份、授权缓存与创建合同。
//!
//! `mkdir /ownerfs/job-42` 与根内的 `open job-42/a.txt` 是两条不同的路径：
//! 前者先向 Meta 预留根，再准备本地目录并激活；后者应凭已缓存的有效
//! 根授权直接访问本地普通文件。这里不保存逐文件的 inode 或数据。
//!
//! 启动必须先核对 Meta 的 pending/active 状态、本机 catalog 与物理目录，
//! 才能把根放进热路径。目录存在或预留成功都不构成访问授权。

use std::{
    collections::{HashMap, HashSet},
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStrExt,
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use afs_error::{Error, Result};

use super::catalog::{
    LocalFsRootCatalog, LocalRootCatalog, LocalRootRecord, LockedLocalRootCatalog,
};
use crate::node::storage::{FileStore, LocalFs, StoragePath};

/// Meta 使用的稳定根身份；与 FUSE inode、磁盘目录名均不同。
///
/// 一级目录名可以包含非 UTF-8 字节。未来从 `OsStr` 生成这个字符串时
/// 必须采用无损、无碰撞编码，不能使用 `to_string_lossy`。
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RootId(pub String);

/// 本次操作需要的粗粒度根权限；根内仍需执行 POSIX 文件权限检查。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootRight {
    Lookup,
    Read,
    Write,
    Admin,
}

/// 查询根位置的结果。位置不携带权利或 fencing token，不是读写授权。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootLocation {
    pub id: RootId,
    pub epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
}

/// Meta 的创建预留：只允许 Node 准备目录，不允许对外访问该根。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootReservation {
    pub id: RootId,
    pub epoch: u64,
    pub home_node_id: String,
    pub session_id: String,
    pub create_intent_id: String,
    pub prepare_token: String,
}

/// One linearizable Meta view for startup. Reading active and pending in two
/// separate RPCs could observe an Activate between them and misclassify a root.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwnerRootInventory {
    pub active: Vec<RootLocation>,
    pub pending: Vec<RootReservation>,
}

/// 本地目录及 Node 私有身份记录已准备完成的证据。
///
/// 首次创建应先写私有身份记录、同步目录及其父目录，再向 Meta 激活。
/// `data_dir` 必须区分 root epoch，防止同名根删除重建后误认旧目录。
/// 这些持久准备动作还未实现，不能仅构造本结构就声称已经完成。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRoot {
    reservation: RootReservation,
    data_dir: StoragePath,
    local_prepare_id: String,
    parent_fsync_generation: u64,
}

impl PreparedRoot {
    /// Build the evidence passed from the local prepare phase to Meta activate.
    ///
    /// This constructor does not perform I/O. Callers must already have created
    /// and synced the data directory plus the parent directory before using it.
    pub fn new(
        reservation: RootReservation,
        data_dir: StoragePath,
        local_prepare_id: String,
        parent_fsync_generation: u64,
    ) -> Self {
        Self {
            reservation,
            data_dir,
            local_prepare_id,
            parent_fsync_generation,
        }
    }

    pub fn reservation(&self) -> &RootReservation {
        &self.reservation
    }

    pub fn data_dir(&self) -> &StoragePath {
        &self.data_dir
    }

    pub fn local_prepare_id(&self) -> &str {
        &self.local_prepare_id
    }

    pub fn parent_fsync_generation(&self) -> u64 {
        self.parent_fsync_generation
    }
}

/// Meta 签发的访问权。每个根内操作在本机检查一次缓存授权；
/// 已授权本地热路径不逐文件调用 Meta。
///
/// Grant 不携带“独享/共享模式”。Home 始终是同一份普通文件的事实源；
/// 远端节点加入时，Meta 可以同时给 Home 与远端签发 grant，不要求 Home
/// 先撤销本地授权、推进 access_generation 或发送 ACK。`access_generation`
/// 是同一根的围栏代，A/B 授权必须同代；仅删除、显式撤权或 Home 会话恢复等
/// 真正会淘汰旧授权的事件。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootGrant {
    pub id: RootId,
    pub epoch: u64,
    pub home_node_id: String,
    /// 当前 Home 进程会话；旧会话返回的句柄与访问权不能跨重启复用。
    pub home_session_id: String,
    pub holder_node_id: String,
    pub session_id: String,
    pub access_generation: u64,
    pub rights: Vec<RootRight>,
    pub fencing_token: String,
}

/// P2P 请求里远端节点出示的根访问事实。
///
/// 这不是完整 `RootGrant`：P2P data/control 消息不携带 rights，Home 也不
/// 信任 peer 自报的权限集合。Home 首次见到这组字段时必须带上连接层
/// 认证出的 peer node id 请求 Meta 校验；Meta 返回的 `RootGrant` 才能
/// 决定本次操作是否有 Read/Write/Admin 等权利。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PresentedRootAccess {
    pub id: RootId,
    pub epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
    pub holder_node_id: String,
    pub session_id: String,
    pub access_generation: u64,
    pub fencing_token: String,
}

/// Authoritative Meta request to stop admitting new work on the current Home grant.
///
/// This is intentionally narrower than a complete RootCommand receipt. It fences
/// this process' cached authority only when every identity field matches the
/// currently active Home grant. It does not durably advance a command cursor,
/// claim ACK completion, or wait for already admitted `RootUse` instances.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HomeRootRevocation {
    pub command_id: String,
    pub root_id: RootId,
    pub root_epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
    pub access_generation: u64,
}

/// Node→Meta 的业务边界。gRPC/Proto 转换归 `node/rpc/meta.rs`，这里
/// 不直接使用生成的 wire 类型。与当前同步 VFS 回调一致，控制调用
/// 只发生于创建、未命中和恢复等慢路径；适配器不得在 Tokio worker
/// 上直接阻塞，也不得让调用方持有根状态锁等待网络。
pub trait RootMeta: Send + Sync {
    fn reserve_root(&self, id: &RootId, create_intent_id: &str) -> Result<RootReservation>;
    fn activate_root(&self, prepared: &PreparedRoot) -> Result<RootGrant>;
    fn abort_root(&self, reservation: &RootReservation) -> Result<()>;
    fn lookup_root(&self, id: &RootId) -> Result<Option<RootLocation>>;
    /// Linearizable Home inventory, including prior process sessions. An
    /// active root without catalog must prevent mount; a pending root must be
    /// canceled or reconciled before access is enabled.
    fn list_owner_roots(&self, home_node_id: &str) -> Result<OwnerRootInventory>;
    fn acquire_root(&self, id: &RootId, right: RootRight) -> Result<RootGrant>;
    /// Resolve a node id from a RootGrant into the current node-to-node data endpoint.
    ///
    /// Remote OwnerFs uses this only on connection/cache miss. It is not part
    /// of the local file hot path and must not be called per file write.
    /// Implementations backed by older Meta services may return UNIMPLEMENTED.
    fn lookup_node_endpoint(&self, node_id: &str) -> Result<String> {
        let _ = node_id;
        Err(Error::coded(
            afs_error::META_STORE_UNIMPLEMENTED,
            "RootMeta node endpoint lookup is not wired",
        ))
    }
    /// Authoritative current process session for a node. `None` means the
    /// prior session is absent or its Meta lease has expired. An RPC failure
    /// must be returned as an error, never interpreted as peer death.
    fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
        let _ = node_id;
        Err(Error::coded(
            afs_error::META_STORE_UNIMPLEMENTED,
            "RootMeta node session lookup is not wired",
        ))
    }
    /// Home 首次看到远端节点出示的访问事实时调用 Meta 校验一次。
    ///
    /// AFS Node 的认证层必须把连接身份转换为 `authenticated_peer_node_id`。
    /// Meta 返回当前权威 grant 后，Home 可以在本根运行态缓存它；后续该
    /// peer 在同一 grant 上的根内文件操作不得逐个调用 Meta。这个调用
    /// 不是“B 加入要求 A ACK”，只是 Home 防止伪造或过期 grant 的慢路径。
    fn validate_root_access(
        &self,
        presented: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
    ) -> Result<RootGrant>;
    /// A 重启时以已验证的本机根记录重新绑定 Home 会话；失败时不准入热路径。
    fn recover_root(
        &self,
        record: &super::catalog::LocalRootRecord,
        new_session_id: &str,
    ) -> Result<RootGrant>;
}

/// 缓存中的一个本机根；只在 Meta 已激活且身份校验通过后发布。
/// `data_dir` 是 LocalFs 根下的私有路径，不由调用者凭文件名自行拼接。
pub struct LocalRoot {
    pub id: RootId,
    pub name: OsString,
    pub data_dir: StoragePath,
    state: Mutex<RootRuntime>,
    drained: Condvar,
}

/// 授权状态和在途计数在同一把短锁下改变；磁盘 I/O 不持有它。
struct RootRuntime {
    phase: GrantPhase,
    #[cfg(test)]
    pf1_refusal: Option<PrivateRootBinding>,
    in_flight: usize,
    /// Home 已经向 Meta 校验过的远端 grant。缓存键带 grant 代与 token，
    /// 使真正撤权、删除重建或 Home 重启后的旧 grant 无法误命中新授权。
    validated_peer_grants: HashMap<PeerGrantKey, RootGrant>,
    /// A lease-expired process session may not resurrect its cached grant.
    fenced_peer_sessions: HashSet<(String, String)>,
}

enum GrantPhase {
    Active(RootGrant),
    Invalid,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PeerGrantKey {
    holder_node_id: String,
    session_id: String,
    access_generation: u64,
    fencing_token: String,
}

impl PeerGrantKey {
    fn from_presented(access: &PresentedRootAccess) -> Self {
        Self {
            holder_node_id: access.holder_node_id.clone(),
            session_id: access.session_id.clone(),
            access_generation: access.access_generation,
            fencing_token: access.fencing_token.clone(),
        }
    }
}

/// Private observation identity captured from the grant admitted for an actual open.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PrivateRootBinding {
    pub root_id: RootId,
    pub root_epoch: u64,
    pub access_generation: u64,
    pub home_node_id: String,
    pub home_session_id: String,
}

#[cfg(test)]
impl PrivateRootBinding {
    pub(super) fn from_grant(grant: &RootGrant) -> Self {
        Self {
            root_id: grant.id.clone(),
            root_epoch: grant.epoch,
            access_generation: grant.access_generation,
            home_node_id: grant.home_node_id.clone(),
            home_session_id: grant.home_session_id.clone(),
        }
    }
}

#[cfg(test)]
pub(super) struct PrivatePf1Fence {
    root: Arc<LocalRoot>,
    pub binding: PrivateRootBinding,
    pub command_id: String,
}

#[cfg(test)]
impl RootManager {
    pub(super) fn private_pf1_fence(
        &self,
        binding: &PrivateRootBinding,
        command_id: &str,
    ) -> Result<PrivatePf1Fence> {
        if command_id.is_empty() {
            return Err(invalid_grant("empty PF1 command identity"));
        }
        let root = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache poisoned"))?
            .get(&binding.root_id)
            .cloned()
            .ok_or_else(|| invalid_grant("PF1 root missing"))?;
        {
            let mut state = root
                .state
                .lock()
                .map_err(|_| invalid_grant("PF1 root poisoned"))?;
            let GrantPhase::Active(grant) = &state.phase else {
                return Err(invalid_grant("PF1 root inactive"));
            };
            if PrivateRootBinding::from_grant(grant) != *binding
                || !self.control_valid.load(Ordering::Acquire)
            {
                return Err(invalid_grant("PF1 generation changed"));
            }
            if state
                .pf1_refusal
                .as_ref()
                .is_some_and(|prior| prior != binding)
            {
                return Err(invalid_grant("PF1 refusal differs"));
            }
            // Refuse before interpreting zero. Exact peer grants cannot be repopulated.
            state.pf1_refusal = Some(binding.clone());
            let peers: Vec<_> = state
                .validated_peer_grants
                .keys()
                .map(|key| (key.holder_node_id.clone(), key.session_id.clone()))
                .collect();
            state.fenced_peer_sessions.extend(peers);
            state.validated_peer_grants.clear();
        }
        Ok(PrivatePf1Fence {
            root,
            binding: binding.clone(),
            command_id: command_id.into(),
        })
    }

    pub(super) fn private_pf1_root_counts(
        &self,
        fence: &PrivatePf1Fence,
    ) -> Result<(usize, usize)> {
        // Root-only lock order: map read -> root runtime. Never called under an Owner lock.
        let roots = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache poisoned"))?;
        if roots
            .get(&fence.binding.root_id)
            .is_none_or(|current| !Arc::ptr_eq(current, &fence.root))
        {
            return Err(invalid_grant("PF1 root instance replaced"));
        }
        let state = fence
            .root
            .state
            .lock()
            .map_err(|_| invalid_grant("PF1 root poisoned"))?;
        let GrantPhase::Active(grant) = &state.phase else {
            return Err(invalid_grant("PF1 root inactive"));
        };
        if PrivateRootBinding::from_grant(grant) != fence.binding
            || state.pf1_refusal.as_ref() != Some(&fence.binding)
            || !self.control_valid.load(Ordering::Acquire)
        {
            return Err(invalid_grant("PF1 root or refusal changed"));
        }
        Ok((state.in_flight, state.validated_peer_grants.len()))
    }
}

/// 一次操作的准入凭证。创建时增加在途计数，Drop 时减少；旧句柄
/// 后续再次发起操作也必须重新准入，不能无限沿用旧授权。
pub struct RootUse {
    root: Arc<LocalRoot>,
    grant: RootGrant,
}

impl RootUse {
    pub fn root_id(&self) -> &RootId {
        &self.root.id
    }

    pub fn data_dir(&self) -> &StoragePath {
        &self.root.data_dir
    }

    pub fn grant(&self) -> &RootGrant {
        &self.grant
    }
}

impl Drop for RootUse {
    fn drop(&mut self) {
        // 即使调用者 panic，也必须归还计数；锁中毒时仍不允许伪造 ACK。
        let mut state = self
            .root
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.in_flight -= 1;
        self.root.drained.notify_all();
    }
}

/// OwnerFs 内部的根管理器。缓存只含已激活根；预留中的根不得进入。
/// 本机目录存在本身不构成授权。Meta 不可达且无有效授权时须拒绝访问。
pub struct RootManager {
    local_node_id: String,
    session_id: String,
    meta: Arc<dyn RootMeta>,
    disk: Arc<LocalFs>,
    catalog: Box<dyn LockedLocalRootCatalog>,
    control_valid: AtomicBool,
    roots: RwLock<HashMap<RootId, Arc<LocalRoot>>>,
}

impl RootManager {
    /// Open the production root manager, hold the local catalog lock for the
    /// process lifetime, and recover all durable local roots through Meta before
    /// the VFS is mounted. Any invalid durable root fails closed by returning an
    /// error instead of silently publishing a partial namespace.
    pub fn open(
        local_node_id: String,
        session_id: String,
        meta: Arc<dyn RootMeta>,
        disk: Arc<LocalFs>,
    ) -> Result<Self> {
        let catalog = LocalFsRootCatalog::new(disk.clone()).lock_and_open()?;
        Self::open_with_catalog(local_node_id, session_id, meta, disk, catalog)
    }

    /// Test/advanced constructor that accepts an already locked catalog.
    pub fn open_with_catalog(
        local_node_id: String,
        session_id: String,
        meta: Arc<dyn RootMeta>,
        disk: Arc<LocalFs>,
        catalog: Box<dyn LockedLocalRootCatalog>,
    ) -> Result<Self> {
        let manager = Self {
            local_node_id,
            session_id,
            meta,
            disk,
            catalog,
            control_valid: AtomicBool::new(true),
            roots: RwLock::new(HashMap::new()),
        };
        manager.reconcile_on_startup()?;
        Ok(manager)
    }

    /// Compatibility constructor used by existing tests and temporary Node wiring.
    /// It still opens the default LocalFs catalog and panics if the process lock
    /// cannot be acquired; production startup should call `open` and surface the
    /// error instead.
    pub fn new(
        local_node_id: String,
        session_id: String,
        meta: Arc<dyn RootMeta>,
        disk: Arc<LocalFs>,
    ) -> Self {
        Self::open(local_node_id, session_id, meta, disk)
            .expect("OwnerFs root catalog must open before mounting")
    }

    /// Create a first-level OwnerFs root on the local node.
    ///
    /// This is the slow path behind `mkdir /ownerfs/<root>`. It reserves coarse
    /// authority in Meta, creates a local ordinary directory, syncs the parent,
    /// activates the root, then returns a normal hot-path grant. File operations
    /// below the root do not come back here or touch Meta per write.
    pub fn create_root(&self, name: &OsStr, mode: u32) -> Result<RootUse> {
        let id = root_id_from_name(name)?;
        let create_intent_id = format!("{}:{}:{}", self.local_node_id, self.session_id, id.0);
        let reservation = self.meta.reserve_root(&id, &create_intent_id)?;
        if reservation.id != id
            || reservation.home_node_id != self.local_node_id
            || reservation.session_id != self.session_id
            || reservation.epoch == 0
            || reservation.prepare_token.is_empty()
        {
            return Err(invalid_grant("reserved root does not match this Node"));
        }

        let data_dir = root_data_dir(name, reservation.epoch)?;
        let local_prepare_id = format!("{}:{}", reservation.prepare_token, self.session_id);
        match self.disk.mkdir(&data_dir, mode) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !self.disk.metadata(&data_dir).map_err(Error::from)?.is_dir() {
                    let _ = self.meta.abort_root(&reservation);
                    return Err(invalid_grant(
                        "reserved root path exists but is not a directory",
                    ));
                }
                let old = self
                    .catalog
                    .scan_roots()?
                    .into_iter()
                    .find(|record| record.id == id);
                if old.is_none() {
                    // A crash may leave an empty directory after the Meta
                    // reservation was canceled. Never silently reuse its
                    // contents as a new root with the same epoch.
                    self.disk.remove_dir(&data_dir).map_err(Error::from)?;
                    self.disk.sync_root().map_err(Error::from)?;
                    self.disk.mkdir(&data_dir, mode).map_err(Error::from)?;
                }
            }
            Err(error) => {
                let _ = self.meta.abort_root(&reservation);
                return Err(Error::from(error));
            }
        }
        if let Err(error) = self.disk.sync_root() {
            let _ = self.meta.abort_root(&reservation);
            return Err(Error::from(error));
        }
        let record = LocalRootRecord {
            id: reservation.id.clone(),
            name: name.to_os_string(),
            epoch: reservation.epoch,
            data_dir: data_dir.clone(),
            local_prepare_id: local_prepare_id.clone(),
        };
        if let Err(error) = self.catalog.persist_prepared_root(&record) {
            let _ = self.meta.abort_root(&reservation);
            return Err(error);
        }
        let prepared = PreparedRoot::new(reservation, data_dir, local_prepare_id, 1);
        // Activate may have committed even if the RPC reply was lost. Keep the
        // synced catalog record on *any* error; startup can then distinguish an
        // active root from a pending reservation and recover or abort safely.
        self.activate_prepared(name.to_os_string(), &prepared)?;
        self.enter_root(prepared.reservation().id(), RootRight::Write)
    }

    /// B-side slow path: locate a root without granting file access.
    pub fn lookup_root_location(&self, id: &RootId) -> Result<Option<RootLocation>> {
        self.meta.lookup_root(id)
    }

    /// B-side slow path: acquire a remote grant for a root. This does not place
    /// the root into the local Home cache because data remains owned by Home.
    pub fn acquire_remote_root(&self, id: &RootId, right: RootRight) -> Result<RootGrant> {
        self.meta.acquire_root(id, right)
    }

    /// 本地准备完成后调用 Meta 激活；只有返回的授权与预留、当前 Node
    /// 会话完全吻合，且目标确实是目录，才放进热路径缓存。
    /// 调用方必须先完成 PreparedRoot 所声明的落盘步骤；本函数不能替代它。
    pub fn activate_prepared(&self, name: OsString, prepared: &PreparedRoot) -> Result<()> {
        if !self.control_valid.load(Ordering::Acquire) {
            return Err(unavailable_grant("Meta control session is invalid"));
        }
        if prepared.reservation.home_node_id != self.local_node_id
            || prepared.reservation.session_id != self.session_id
        {
            return Err(invalid_grant(
                "root reservation belongs to another Node session",
            ));
        }
        if self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?
            .contains_key(&prepared.reservation.id)
        {
            return Err(invalid_grant(
                "root is already cached; reconcile before reactivation",
            ));
        }
        if !self
            .disk
            .metadata(&prepared.data_dir)
            .map_err(Error::from)?
            .is_dir()
        {
            return Err(invalid_grant("prepared root is not a directory"));
        }
        let grant = self.meta.activate_root(prepared)?;
        if grant.id != prepared.reservation.id
            || grant.epoch != prepared.reservation.epoch
            || grant.home_node_id != self.local_node_id
            || grant.home_session_id != self.session_id
            || grant.holder_node_id != self.local_node_id
            || grant.session_id != self.session_id
            || grant.access_generation == 0
            || grant.fencing_token.is_empty()
        {
            return Err(invalid_grant(
                "activated root grant does not match local reservation",
            ));
        }
        let root = Arc::new(LocalRoot {
            id: grant.id.clone(),
            name,
            data_dir: prepared.data_dir.clone(),
            state: Mutex::new(RootRuntime {
                phase: GrantPhase::Active(grant),
                #[cfg(test)]
                pf1_refusal: None,
                in_flight: 0,
                validated_peer_grants: HashMap::new(),
                fenced_peer_sessions: HashSet::new(),
            }),
            drained: Condvar::new(),
        });
        let mut roots = self
            .roots
            .write()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?;
        if !self.control_valid.load(Ordering::Acquire) {
            return Err(unavailable_grant(
                "Meta control session changed during activation",
            ));
        }
        if roots.contains_key(&root.id) {
            return Err(invalid_grant(
                "root is already cached; reconcile before reactivation",
            ));
        }
        roots.insert(root.id.clone(), root);
        Ok(())
    }

    /// 根内热路径：按已解析 RootId 查缓存并短锁准入，不访问 Meta。
    /// 缓存未命中的查询/授权获取由下方生命周期接口负责，不能把未命中
    /// 当成“本地数据目录不存在”。
    pub fn enter_root(&self, id: &RootId, right: RootRight) -> Result<RootUse> {
        let root = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?
            .get(id)
            .cloned()
            .ok_or_else(|| {
                Error::coded(
                    afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
                    "root grant not cached",
                )
            })?;
        let mut state = root
            .state
            .lock()
            .map_err(|_| invalid_grant("root grant lock poisoned"))?;
        let GrantPhase::Active(grant) = &state.phase else {
            return Err(Error::coded(
                afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
                "root grant is not active",
            ));
        };
        #[cfg(test)]
        if state.pf1_refusal.is_some() {
            return Err(unavailable_grant("private PF1 admission is fenced"));
        }
        if !self.control_valid.load(Ordering::Acquire) {
            return Err(unavailable_grant("Meta control session is invalid"));
        }
        if !grant.rights.contains(&right) {
            return Err(Error::coded(
                afs_error::NODE_OWNER_RIGHT_DENIED,
                "root right not granted",
            ));
        }
        let grant = grant.clone();
        state.in_flight += 1;
        drop(state);
        Ok(RootUse { root, grant })
    }

    /// Home 首次接收某个远端 peer 的根操作前调用；成功后同一 peer grant
    /// 被缓存，根内文件操作不再逐个访问 Meta。
    ///
    /// 这里校验的是 peer 身份与授权事实，不改变 Home 自己的本地授权。
    /// 远端加入不是模式切换，也不是 access_generation 推进点。`right`
    /// 来自本次 P2P 文件命令，必须由 Meta 返回的权威 grant 覆盖。
    pub fn validate_peer_root_access(
        &self,
        presented: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
        right: RootRight,
    ) -> Result<RootGrant> {
        self.validate_peer_root_access_with_right(
            presented,
            authenticated_peer_node_id,
            Some(right),
        )
    }

    pub fn validate_peer_root_transport(
        &self,
        presented: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
    ) -> Result<RootGrant> {
        self.validate_peer_root_access_with_right(presented, authenticated_peer_node_id, None)
    }

    fn validate_peer_root_access_with_right(
        &self,
        presented: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
        right: Option<RootRight>,
    ) -> Result<RootGrant> {
        self.check_presented_shape(presented, authenticated_peer_node_id)?;
        if !self.control_valid.load(Ordering::Acquire) {
            return Err(unavailable_grant("Meta control session is invalid"));
        }
        let root = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?
            .get(&presented.id)
            .cloned()
            .ok_or_else(|| {
                Error::coded(
                    afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
                    "home root grant not cached",
                )
            })?;
        let key = PeerGrantKey::from_presented(presented);
        {
            let state = root
                .state
                .lock()
                .map_err(|_| invalid_grant("root grant lock poisoned"))?;
            self.check_presented_against_home(&state, presented)?;
            self.check_peer_not_fenced(&state, presented)?;
            if let Some(cached) = state.validated_peer_grants.get(&key) {
                self.check_authoritative_matches_presented(cached, presented)?;
                if let Some(right) = right {
                    self.check_grant_right(cached, right)?;
                }
                return Ok(cached.clone());
            }
        }

        let authoritative = self
            .meta
            .validate_root_access(presented, authenticated_peer_node_id)?;
        self.check_authoritative_matches_presented(&authoritative, presented)?;
        if let Some(right) = right {
            self.check_grant_right(&authoritative, right)?;
        }

        let mut state = root
            .state
            .lock()
            .map_err(|_| invalid_grant("root grant lock poisoned"))?;
        if !self.control_valid.load(Ordering::Acquire) {
            return Err(unavailable_grant(
                "Meta control session changed during peer grant validation",
            ));
        }
        self.check_presented_against_home(&state, presented)?;
        self.check_peer_not_fenced(&state, presented)?;
        state
            .validated_peer_grants
            .insert(key, authoritative.clone());
        Ok(authoritative)
    }

    fn check_peer_not_fenced(
        &self,
        state: &RootRuntime,
        access: &PresentedRootAccess,
    ) -> Result<()> {
        if state
            .fenced_peer_sessions
            .contains(&(access.holder_node_id.clone(), access.session_id.clone()))
        {
            return Err(invalid_grant("peer process session has expired"));
        }
        Ok(())
    }

    pub fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
        self.meta.current_node_session(node_id)
    }

    /// Include validated grants even when the peer currently owns no open
    /// handle. Otherwise an expired session can keep using a cached grant
    /// indefinitely after its last file has been closed.
    pub fn cached_peer_sessions(&self) -> Result<HashSet<(String, String)>> {
        let roots = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?;
        let mut sessions = HashSet::new();
        for root in roots.values() {
            let state = root
                .state
                .lock()
                .map_err(|_| invalid_grant("root grant lock poisoned"))?;
            sessions.extend(
                state
                    .validated_peer_grants
                    .keys()
                    .map(|key| (key.holder_node_id.clone(), key.session_id.clone())),
            );
        }
        Ok(sessions)
    }

    /// Fence an expired peer process session before its Home handles are
    /// reclaimed. A concurrent Meta validation cannot repopulate the cache:
    /// both its first and final cache checks consult this tombstone.
    pub fn fence_peer_session(&self, node_id: &str, session_id: &str) -> Result<()> {
        let roots = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?;
        for root in roots.values() {
            let mut state = root
                .state
                .lock()
                .map_err(|_| invalid_grant("root grant lock poisoned"))?;
            state
                .fenced_peer_sessions
                .insert((node_id.to_owned(), session_id.to_owned()));
            state
                .validated_peer_grants
                .retain(|key, _| key.holder_node_id != node_id || key.session_id != session_id);
        }
        Ok(())
    }

    /// Apply a precise Home revocation received from Meta's control plane.
    ///
    /// `Ok(true)` means this process held exactly that active Home grant and
    /// now rejects new local/peer admissions for it. `Ok(false)` means the tuple
    /// is well-formed but stale, unrelated, already inactive, or absent here.
    /// Existing `RootUse` holders drain through their normal lifecycle.
    pub(crate) fn revoke_matching_home(&self, revocation: &HomeRootRevocation) -> Result<bool> {
        self.check_home_revocation_shape(revocation)?;
        if revocation.home_node_id != self.local_node_id
            || revocation.home_session_id != self.session_id
        {
            return Ok(false);
        }
        if !self.control_valid.load(Ordering::Acquire) {
            return Err(unavailable_grant("Meta control session is invalid"));
        }
        let Some(root) = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?
            .get(&revocation.root_id)
            .cloned()
        else {
            return Ok(false);
        };
        let mut state = root
            .state
            .lock()
            .map_err(|_| invalid_grant("root grant lock poisoned"))?;
        Ok(self.invalidate_matching_home_locked(&mut state, revocation))
    }

    fn check_presented_shape(
        &self,
        access: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
    ) -> Result<()> {
        if access.home_node_id != self.local_node_id
            || access.home_session_id != self.session_id
            || access.holder_node_id != authenticated_peer_node_id
            || access.holder_node_id == self.local_node_id
            || access.session_id.is_empty()
            || access.access_generation == 0
            || access.fencing_token.is_empty()
        {
            return Err(invalid_grant(
                "presented peer root access does not match this Home",
            ));
        }
        Ok(())
    }

    fn check_presented_against_home(
        &self,
        state: &RootRuntime,
        access: &PresentedRootAccess,
    ) -> Result<()> {
        #[cfg(test)]
        if state.pf1_refusal.is_some() {
            return Err(unavailable_grant("private PF1 peer admission is fenced"));
        }
        let GrantPhase::Active(home_grant) = &state.phase else {
            return Err(unavailable_grant("home root grant is not active"));
        };
        if home_grant.id != access.id
            || home_grant.epoch != access.epoch
            || home_grant.access_generation != access.access_generation
            || home_grant.home_node_id != self.local_node_id
            || home_grant.home_session_id != self.session_id
            || home_grant.holder_node_id != self.local_node_id
            || home_grant.session_id != self.session_id
        {
            return Err(invalid_grant(
                "presented peer root access does not match active Home",
            ));
        }
        Ok(())
    }

    fn check_home_revocation_shape(&self, revocation: &HomeRootRevocation) -> Result<()> {
        if revocation.command_id.is_empty()
            || revocation.root_id.0.is_empty()
            || revocation.root_epoch == 0
            || revocation.home_node_id.is_empty()
            || revocation.home_session_id.is_empty()
            || revocation.access_generation == 0
        {
            return Err(invalid_grant("home root revocation identity is incomplete"));
        }
        Ok(())
    }

    fn invalidate_matching_home_locked(
        &self,
        state: &mut RootRuntime,
        revocation: &HomeRootRevocation,
    ) -> bool {
        let GrantPhase::Active(grant) = &state.phase else {
            return false;
        };
        if grant.id != revocation.root_id
            || grant.epoch != revocation.root_epoch
            || grant.home_node_id != revocation.home_node_id
            || grant.home_session_id != revocation.home_session_id
            || grant.holder_node_id != self.local_node_id
            || grant.session_id != self.session_id
            || grant.access_generation != revocation.access_generation
        {
            return false;
        }
        let fenced: Vec<_> = state
            .validated_peer_grants
            .keys()
            .map(|key| (key.holder_node_id.clone(), key.session_id.clone()))
            .collect();
        state.phase = GrantPhase::Invalid;
        state.validated_peer_grants.clear();
        state.fenced_peer_sessions.extend(fenced);
        true
    }

    fn check_authoritative_matches_presented(
        &self,
        grant: &RootGrant,
        access: &PresentedRootAccess,
    ) -> Result<()> {
        if grant.id != access.id
            || grant.epoch != access.epoch
            || grant.home_node_id != access.home_node_id
            || grant.home_session_id != access.home_session_id
            || grant.holder_node_id != access.holder_node_id
            || grant.session_id != access.session_id
            || grant.access_generation != access.access_generation
            || grant.fencing_token != access.fencing_token
        {
            return Err(invalid_grant(
                "Meta returned root grant that differs from presented access",
            ));
        }
        Ok(())
    }

    fn check_grant_right(&self, grant: &RootGrant, right: RootRight) -> Result<()> {
        if !grant.rights.contains(&right) {
            return Err(Error::coded(
                afs_error::NODE_OWNER_RIGHT_DENIED,
                "peer root right not granted",
            ));
        }
        Ok(())
    }

    /// Meta watch 断线时先 fail-closed：拒绝新的本地/远端准入，直到上层
    /// 重新建立可证明的控制会话并完成对账。本方法不把缓存改写成撤销态，
    /// 也不宣称旧 grant 已经被持久撤销或 peer 已 ACK。
    pub fn on_watch_disconnected(&self) {
        self.control_valid.store(false, Ordering::Release);
    }

    /// 真正收到 Meta 撤销或本地恢复判定旧授权失效时，封闭单个根。
    /// 已准入操作仍由 RootUse 计数；撤销完成条件由后续持久协议定义。
    pub fn revoke_root(&self, id: &RootId) {
        if let Ok(roots) = self.roots.read()
            && let Some(root) = roots.get(id)
        {
            let mut state = root.state.lock().unwrap_or_else(|error| error.into_inner());
            state.phase = GrantPhase::Invalid;
            state.validated_peer_grants.clear();
        }
    }

    /// 本进程会话确认失效时封闭全部根，不能继续用旧授权准入。
    /// 已准入操作仍由 RootUse 计数；本方法不代表远端撤销已经完成或已 ACK。
    pub fn invalidate_all(&self) {
        self.control_valid.store(false, Ordering::Release);
        // 连缓存锁中毒时也要尽力封闭所有根，不能提前返回留下活动授权。
        let roots = self.roots.read().unwrap_or_else(|error| error.into_inner());
        for root in roots.values() {
            let mut state = root.state.lock().unwrap_or_else(|error| error.into_inner());
            state.phase = GrantPhase::Invalid;
            state.validated_peer_grants.clear();
        }
    }

    /// Snapshot of already recovered/activated local roots for OwnerFs inode table hydration.
    pub fn cached_local_roots(&self) -> Result<Vec<LocalRootSnapshot>> {
        let roots = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?;
        Ok(roots
            .values()
            .map(|root| LocalRootSnapshot {
                id: root.id.clone(),
                name: root.name.clone(),
                data_dir: root.data_dir.clone(),
            })
            .collect())
    }

    /// Fast, conservative answer for the FUSE private-cache policy. A root
    /// without an active local grant must never receive the private TTL.
    pub fn has_active_local_root(&self, id: &RootId) -> bool {
        if !self.control_valid.load(Ordering::Acquire) {
            return false;
        }
        self.roots.read().is_ok_and(|roots| {
            roots.get(id).is_some_and(|root| {
                root.state
                    .lock()
                    .is_ok_and(|state| matches!(state.phase, GrantPhase::Active(_)))
            })
        })
    }

    /// Rebuild the active root cache from durable local records and Meta grants.
    pub fn reconcile_on_startup(&self) -> Result<()> {
        let mut records = HashMap::new();
        for record in self.catalog.scan_roots()? {
            if record.epoch == 0
                || root_id_from_name(&record.name)? != record.id
                || root_data_dir(&record.name, record.epoch)? != record.data_dir
            {
                return Err(invalid_grant(
                    "local root catalog identity is not canonical",
                ));
            }
            if records.insert(record.id.clone(), record).is_some() {
                return Err(invalid_grant("duplicate local root catalog identity"));
            }
        }
        let inventory = self.meta.list_owner_roots(&self.local_node_id)?;
        let mut active = HashMap::new();
        for location in inventory.active {
            if location.home_node_id != self.local_node_id || location.epoch == 0 {
                return Err(invalid_grant("Meta returned a foreign active root"));
            }
            if active.insert(location.id.clone(), location).is_some() {
                return Err(invalid_grant("Meta returned a duplicate active root"));
            }
        }
        if active.keys().any(|id| !records.contains_key(id)) {
            return Err(invalid_grant(
                "Meta has an active Home root without local catalog evidence",
            ));
        }

        // The catalog alone cannot see a crash between Reserve and catalog
        // fsync. A pending reservation is never a grant. Cancel it with a
        // reservation CAS before deleting any local evidence; if the process
        // crashes mid-cleanup, the next startup repeats the remaining steps.
        for reservation in inventory.pending {
            if reservation.home_node_id != self.local_node_id || reservation.epoch == 0 {
                return Err(invalid_grant("Meta returned a foreign pending root"));
            }
            if active.contains_key(&reservation.id) {
                return Err(invalid_grant(
                    "Meta reported the same root as active and pending",
                ));
            }
            if let Some(record) = records.get(&reservation.id) {
                let expected_dir = root_data_dir(&record.name, reservation.epoch)?;
                let expected_prepare_id =
                    format!("{}:{}", reservation.prepare_token, reservation.session_id);
                if record.epoch != reservation.epoch
                    || record.data_dir != expected_dir
                    || record.local_prepare_id != expected_prepare_id
                {
                    return Err(invalid_grant(
                        "pending reservation does not match local durable root",
                    ));
                }
            }
            let data_dir = pending_data_dir(&reservation)?;
            self.meta.abort_root(&reservation)?;
            self.remove_abandoned_dir(&data_dir)?;
            if let Some(record) = records.remove(&reservation.id) {
                self.catalog.remove_prepared_root(&record)?;
            }
        }

        for record in records.into_values() {
            let Some(location) = active.get(&record.id) else {
                // Crash after Abort CAS but before local cleanup. A prepared
                // catalog entry never granted access by itself, and the empty
                // directory cannot contain acknowledged root writes.
                self.remove_abandoned_dir(&record.data_dir)?;
                self.catalog.remove_prepared_root(&record)?;
                continue;
            };
            if location.epoch != record.epoch || location.home_node_id != self.local_node_id {
                return Err(invalid_grant(
                    "active Meta root does not match local durable root",
                ));
            }
            if !self
                .disk
                .metadata(&record.data_dir)
                .map_err(Error::from)?
                .is_dir()
            {
                return Err(invalid_grant("active local root directory is missing"));
            }
            let grant = self.meta.recover_root(&record, &self.session_id)?;
            if grant.id != record.id
                || grant.epoch != record.epoch
                || grant.home_node_id != self.local_node_id
                || grant.home_session_id != self.session_id
                || grant.holder_node_id != self.local_node_id
                || grant.session_id != self.session_id
                || grant.access_generation == 0
                || grant.fencing_token.is_empty()
            {
                return Err(invalid_grant(
                    "recovered root grant does not match local durable record",
                ));
            }
            let root = Arc::new(LocalRoot {
                id: record.id.clone(),
                name: record.name.clone(),
                data_dir: record.data_dir.clone(),
                state: Mutex::new(RootRuntime {
                    phase: GrantPhase::Active(grant),
                    #[cfg(test)]
                    pf1_refusal: None,
                    in_flight: 0,
                    validated_peer_grants: HashMap::new(),
                    fenced_peer_sessions: HashSet::new(),
                }),
                drained: Condvar::new(),
            });
            self.roots
                .write()
                .map_err(|_| invalid_grant("root cache lock poisoned"))?
                .insert(record.id, root);
        }
        Ok(())
    }

    fn remove_abandoned_dir(&self, data_dir: &StoragePath) -> Result<()> {
        match self.disk.remove_dir(data_dir) {
            Ok(()) => self.disk.sync_root().map_err(Error::from),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::from(error)),
        }
    }
}

fn pending_data_dir(reservation: &RootReservation) -> Result<StoragePath> {
    let Some(encoded_name) = reservation.id.0.strip_prefix("root-") else {
        return Err(invalid_grant("pending root id is not a canonical name"));
    };
    if encoded_name.is_empty()
        || encoded_name.len() % 2 != 0
        || !encoded_name.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid_grant("pending root id is not a canonical name"));
    }
    StoragePath::new(format!("{}-e{}", reservation.id.0, reservation.epoch)).map_err(Error::from)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRootSnapshot {
    pub id: RootId,
    pub name: OsString,
    pub data_dir: StoragePath,
}

impl RootReservation {
    pub fn id(&self) -> &RootId {
        &self.id
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrivateRootCommandType {
    RevokeAccess,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct PrivateRootCommand {
    command_id: String,
    home_node_id: String,
    home_session_id: String,
    root_id: RootId,
    root_epoch: u64,
    access_generation: u64,
    command_type: PrivateRootCommandType,
}

#[cfg(test)]
impl PrivateRootCommand {
    fn exact_bytes(&self) -> Vec<u8> {
        let mut bytes = b"AFS-private-root-command-v1\0".to_vec();
        for value in [
            &self.command_id,
            &self.home_node_id,
            &self.home_session_id,
            &self.root_id.0,
        ] {
            bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        bytes.extend_from_slice(&self.root_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.access_generation.to_le_bytes());
        bytes.push(match self.command_type {
            PrivateRootCommandType::RevokeAccess => 1,
        });
        bytes
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct PrivateFoundationReceipt {
    command_id: String,
    root_id: RootId,
    root_epoch: u64,
    access_generation: u64,
    ordinary_root_use_drained: bool,
    opened_fd_native_physical_namespace_drained: bool,
}

#[cfg(test)]
struct PrivateRootCommandJournal {
    dir: std::path::PathBuf,
}

#[cfg(test)]
impl PrivateRootCommandJournal {
    fn open(dir: impl Into<std::path::PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(Error::from)?;
        Ok(Self { dir })
    }

    fn record_exact(&self, command: &PrivateRootCommand) -> Result<()> {
        let path = self.command_path(&command.command_id);
        let bytes = command.exact_bytes();
        if path.exists() {
            let existing = std::fs::read(&path).map_err(Error::from)?;
            if existing == bytes {
                return std::fs::File::open(&self.dir)
                    .map_err(Error::from)?
                    .sync_all()
                    .map_err(Error::from);
            }
            return Err(invalid_grant("journal command id maps to different bytes"));
        }
        let tmp = path.with_extension("tmp");
        {
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp)
                .map_err(Error::from)?;
            std::io::Write::write_all(&mut file, &bytes).map_err(Error::from)?;
            file.sync_all().map_err(Error::from)?;
        }
        // Never replace a command published by a concurrent journal instance.
        std::fs::hard_link(&tmp, &path).map_err(Error::from)?;
        std::fs::remove_file(&tmp).map_err(Error::from)?;
        std::fs::File::open(&self.dir)
            .map_err(Error::from)?
            .sync_all()
            .map_err(Error::from)
    }

    fn command_path(&self, command_id: &str) -> std::path::PathBuf {
        self.dir.join(hex(command_id.as_bytes()))
    }
}

#[cfg(test)]
impl RootManager {
    fn apply_private_foundation_command(
        &self,
        command: &PrivateRootCommand,
        journal: &PrivateRootCommandJournal,
        timeout: std::time::Duration,
    ) -> Result<PrivateFoundationReceipt> {
        journal.record_exact(command)?;
        if command.command_type != PrivateRootCommandType::RevokeAccess
            || command.home_node_id != self.local_node_id
            || command.home_session_id != self.session_id
            || command.root_epoch == 0
            || command.access_generation == 0
        {
            return Err(invalid_grant(
                "private root command does not match this Home",
            ));
        }
        let revocation = HomeRootRevocation {
            command_id: command.command_id.clone(),
            root_id: command.root_id.clone(),
            root_epoch: command.root_epoch,
            home_node_id: command.home_node_id.clone(),
            home_session_id: command.home_session_id.clone(),
            access_generation: command.access_generation,
        };
        self.check_home_revocation_shape(&revocation)?;
        let root = self
            .roots
            .read()
            .map_err(|_| invalid_grant("root cache lock poisoned"))?
            .get(&command.root_id)
            .cloned()
            .ok_or_else(|| unavailable_grant("private root command target is not cached"))?;
        let deadline = std::time::Instant::now() + timeout;
        let mut state = root
            .state
            .lock()
            .map_err(|_| invalid_grant("root grant lock poisoned"))?;
        if !self.invalidate_matching_home_locked(&mut state, &revocation) {
            return Err(invalid_grant(
                "private root command does not match active grant",
            ));
        }
        while state.in_flight != 0 {
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(unavailable_grant("ordinary RootUse drain timed out"));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, wait) = root
                .drained
                .wait_timeout(state, remaining)
                .unwrap_or_else(|error| error.into_inner());
            state = next_state;
            if wait.timed_out() && state.in_flight != 0 {
                return Err(unavailable_grant("ordinary RootUse drain timed out"));
            }
        }
        Ok(PrivateFoundationReceipt {
            command_id: command.command_id.clone(),
            root_id: command.root_id.clone(),
            root_epoch: command.root_epoch,
            access_generation: command.access_generation,
            ordinary_root_use_drained: true,
            opened_fd_native_physical_namespace_drained: false,
        })
    }
}

/// 首次创建、按名字查找、恢复的业务接口。它们尚无实现：必须等 Meta
/// 持久状态机、本地准备记录和 FUSE 根 inode 接线完成后才能提供成功结果。
pub trait RootLifecycle: Send + Sync {
    fn create_root(&self, name: &OsStr, mode: u32) -> Result<RootUse>;
    fn lookup_root(&self, name: &OsStr) -> Result<Option<RootUse>>;
    fn reconcile_on_startup(&self) -> Result<()>;
}

fn invalid_grant(message: &'static str) -> Error {
    Error::coded(afs_error::NODE_OWNER_INVALID_GRANT, message)
}

fn unavailable_grant(message: &'static str) -> Error {
    Error::coded(afs_error::NODE_OWNER_GRANT_UNAVAILABLE, message)
}

/// Encode arbitrary Linux root names without lossy UTF-8 conversion.
pub fn root_id_from_name(name: &OsStr) -> Result<RootId> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.contains(&b'/')
        || bytes.contains(&0)
    {
        return Err(Error::coded(
            afs_error::NODE_VFS_INVALID,
            "root name must be one non-empty path component",
        ));
    }
    Ok(RootId(format!("root-{}", hex(bytes))))
}

fn root_data_dir(name: &OsStr, epoch: u64) -> Result<StoragePath> {
    let bytes = name.as_bytes();
    StoragePath::new(format!("root-{}-e{epoch}", hex(bytes))).map_err(Error::from)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::storage::FileStore;

    struct FixedMeta {
        activate: RootGrant,
        peer: Mutex<Option<RootGrant>>,
        validate_calls: std::sync::atomic::AtomicUsize,
    }

    impl RootMeta for FixedMeta {
        fn reserve_root(&self, _: &RootId, _: &str) -> Result<RootReservation> {
            unreachable!("not used by activation test")
        }

        fn activate_root(&self, _: &PreparedRoot) -> Result<RootGrant> {
            Ok(self.activate.clone())
        }

        fn abort_root(&self, _: &RootReservation) -> Result<()> {
            unreachable!("not used by activation test")
        }

        fn lookup_root(&self, _: &RootId) -> Result<Option<RootLocation>> {
            unreachable!("not used by activation test")
        }

        fn list_owner_roots(&self, _: &str) -> Result<OwnerRootInventory> {
            Ok(OwnerRootInventory::default())
        }

        fn acquire_root(&self, _: &RootId, _: RootRight) -> Result<RootGrant> {
            unreachable!("not used by activation test")
        }

        fn validate_root_access(&self, _: &PresentedRootAccess, _: &str) -> Result<RootGrant> {
            self.validate_calls.fetch_add(1, Ordering::SeqCst);
            self.peer
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| unavailable_grant("test peer grant not configured"))
        }

        fn recover_root(
            &self,
            _: &super::super::catalog::LocalRootRecord,
            _: &str,
        ) -> Result<RootGrant> {
            unreachable!("not used by activation test")
        }
    }

    fn fixed_meta(activate: RootGrant, peer: Option<RootGrant>) -> Arc<FixedMeta> {
        Arc::new(FixedMeta {
            activate,
            peer: Mutex::new(peer),
            validate_calls: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn fixture(
        grant_session: &str,
        home_session: &str,
    ) -> (tempfile::TempDir, RootManager, PreparedRoot) {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let data_dir = StoragePath::new("job-42-e1").unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        let id = RootId("job-42".into());
        let prepared = PreparedRoot {
            reservation: RootReservation {
                id: id.clone(),
                epoch: 1,
                home_node_id: "node-a".into(),
                session_id: "session-a".into(),
                create_intent_id: "intent-a".into(),
                prepare_token: "prepare-a".into(),
            },
            data_dir,
            local_prepare_id: "local-a".into(),
            parent_fsync_generation: 1,
        };
        let grant = RootGrant {
            id,
            epoch: 1,
            home_node_id: "node-a".into(),
            holder_node_id: "node-a".into(),
            home_session_id: home_session.into(),
            session_id: grant_session.into(),
            access_generation: 7,
            rights: vec![RootRight::Lookup],
            fencing_token: "fence-a".into(),
        };
        let manager = RootManager::new(
            "node-a".into(),
            "session-a".into(),
            fixed_meta(grant, None),
            disk,
        );
        (temp, manager, prepared)
    }

    #[test]
    fn only_activated_root_with_requested_right_can_enter() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        let id = &prepared.reservation.id;
        assert!(manager.enter_root(id, RootRight::Lookup).is_err());
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let use_guard = manager.enter_root(id, RootRight::Lookup).unwrap();
        assert_eq!(use_guard.grant().access_generation, 7);
        assert_eq!(
            manager
                .enter_root(id, RootRight::Write)
                .err()
                .unwrap()
                .code(),
            afs_error::NODE_OWNER_RIGHT_DENIED
        );
        manager.invalidate_all();
        assert!(manager.enter_root(id, RootRight::Lookup).is_err());
        drop(use_guard);
    }

    #[test]
    fn peer_access_is_validated_once_then_cached() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let data_dir = StoragePath::new("job-42-e1").unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        let id = RootId("job-42".into());
        let prepared = PreparedRoot {
            reservation: RootReservation {
                id: id.clone(),
                epoch: 1,
                home_node_id: "node-a".into(),
                session_id: "session-a".into(),
                create_intent_id: "intent-a".into(),
                prepare_token: "prepare-a".into(),
            },
            data_dir,
            local_prepare_id: "local-a".into(),
            parent_fsync_generation: 1,
        };
        let local_grant = RootGrant {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-a".into(),
            session_id: "session-a".into(),
            access_generation: 7,
            rights: vec![RootRight::Lookup],
            fencing_token: "fence-a".into(),
        };
        let peer_grant = RootGrant {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "session-b".into(),
            access_generation: 7,
            rights: vec![RootRight::Read],
            fencing_token: "fence-b".into(),
        };
        let meta = fixed_meta(local_grant, Some(peer_grant.clone()));
        let manager = RootManager::new("node-a".into(), "session-a".into(), meta.clone(), disk);
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let presented = PresentedRootAccess {
            id,
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "session-b".into(),
            access_generation: 7,
            fencing_token: "fence-b".into(),
        };

        let validated = manager
            .validate_peer_root_access(&presented, "node-b", RootRight::Read)
            .unwrap();
        assert_eq!(validated, peer_grant);
        assert_eq!(meta.validate_calls.load(Ordering::SeqCst), 1);
        manager
            .validate_peer_root_access(&presented, "node-b", RootRight::Read)
            .unwrap();
        assert_eq!(meta.validate_calls.load(Ordering::SeqCst), 1);
        assert!(
            manager
                .cached_peer_sessions()
                .unwrap()
                .contains(&("node-b".to_owned(), "session-b".to_owned()))
        );
        let mut old_generation = presented.clone();
        old_generation.access_generation -= 1;
        assert_eq!(
            manager
                .validate_peer_root_access(&old_generation, "node-b", RootRight::Read)
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        assert_eq!(meta.validate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            manager
                .validate_peer_root_access(&presented, "node-x", RootRight::Read)
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        assert_eq!(
            manager
                .validate_peer_root_access(&presented, "node-b", RootRight::Write)
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_RIGHT_DENIED
        );
        manager.fence_peer_session("node-b", "session-b").unwrap();
        assert!(
            !manager
                .cached_peer_sessions()
                .unwrap()
                .contains(&("node-b".to_owned(), "session-b".to_owned()))
        );
        assert_eq!(
            manager
                .validate_peer_root_access(&presented, "node-b", RootRight::Read)
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_INVALID_GRANT,
        );
        assert_eq!(meta.validate_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mismatched_node_session_never_enters_cache() {
        let (_temp, manager, prepared) = fixture("session-other", "session-a");
        assert_eq!(
            manager
                .activate_prepared(OsString::from("job-42"), &prepared)
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        assert!(
            manager
                .enter_root(&prepared.reservation.id, RootRight::Lookup)
                .is_err()
        );
    }

    #[test]
    fn old_home_session_never_enters_new_process_cache() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a-old");
        assert_eq!(
            manager
                .activate_prepared(OsString::from("job-42"), &prepared)
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        assert!(
            manager
                .enter_root(&prepared.reservation.id, RootRight::Lookup)
                .is_err()
        );
    }

    #[test]
    fn invalidation_prevents_new_local_operations() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        manager.invalidate_all();
        assert!(
            manager
                .enter_root(&prepared.reservation.id, RootRight::Lookup)
                .is_err()
        );
        assert_eq!(
            manager
                .activate_prepared(OsString::from("job-42"), &prepared)
                .err()
                .unwrap()
                .code(),
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE
        );
    }

    #[test]
    fn watch_disconnect_blocks_new_admission_without_marking_revoke_ack_done() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        manager.on_watch_disconnected();
        assert_eq!(
            manager
                .enter_root(&prepared.reservation.id, RootRight::Lookup)
                .err()
                .unwrap()
                .code(),
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE
        );
    }

    fn home_revocation(id: &RootId, command_id: &str) -> HomeRootRevocation {
        HomeRootRevocation {
            command_id: command_id.to_owned(),
            root_id: id.clone(),
            root_epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            access_generation: 7,
        }
    }

    #[test]
    fn home_revocation_refuses_matching_active_home_without_waiting_for_rootuse() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let use_guard = manager
            .enter_root(&prepared.reservation.id, RootRight::Lookup)
            .unwrap();
        assert!(
            manager
                .revoke_matching_home(&home_revocation(&prepared.reservation.id, "cmd-a"))
                .unwrap()
        );
        assert_eq!(
            manager
                .enter_root(&prepared.reservation.id, RootRight::Lookup)
                .err()
                .expect("revoked Home must refuse new RootUse")
                .code(),
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE
        );
        drop(use_guard);
    }

    #[test]
    fn home_revocation_wrong_tuple_and_missing_root_do_not_revoke_current_home() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let mut stale = home_revocation(&prepared.reservation.id, "cmd-stale");
        stale.access_generation += 1;
        assert!(!manager.revoke_matching_home(&stale).unwrap());
        let _still_active = manager
            .enter_root(&prepared.reservation.id, RootRight::Lookup)
            .unwrap();
        let missing = home_revocation(&RootId("missing-root".into()), "cmd-missing");
        assert!(!manager.revoke_matching_home(&missing).unwrap());
    }

    #[test]
    fn foreign_home_revocation_is_false_and_does_not_touch_current_home() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let mut foreign = home_revocation(&prepared.reservation.id, "cmd-foreign-home");
        foreign.home_node_id = "node-z".into();
        assert!(!manager.revoke_matching_home(&foreign).unwrap());
        let mut wrong_session = home_revocation(&prepared.reservation.id, "cmd-foreign-session");
        wrong_session.home_session_id = "session-z".into();
        assert!(!manager.revoke_matching_home(&wrong_session).unwrap());
        let _still_active = manager
            .enter_root(&prepared.reservation.id, RootRight::Lookup)
            .unwrap();
    }

    #[test]
    fn home_revocation_is_idempotent_after_match() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let revocation = home_revocation(&prepared.reservation.id, "cmd-idempotent");
        assert!(manager.revoke_matching_home(&revocation).unwrap());
        assert!(!manager.revoke_matching_home(&revocation).unwrap());
    }

    #[test]
    fn malformed_home_revocation_is_rejected_before_touching_current_home() {
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let mut malformed = home_revocation(&prepared.reservation.id, "");
        assert_eq!(
            manager.revoke_matching_home(&malformed).unwrap_err().code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        malformed.command_id = "cmd-zero".into();
        malformed.access_generation = 0;
        assert_eq!(
            manager.revoke_matching_home(&malformed).unwrap_err().code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        let _still_active = manager
            .enter_root(&prepared.reservation.id, RootRight::Lookup)
            .unwrap();
    }

    #[test]
    fn home_revocation_fences_cached_peer_grants() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let data_dir = StoragePath::new("job-42-e1").unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        let id = RootId("job-42".into());
        let prepared = PreparedRoot {
            reservation: RootReservation {
                id: id.clone(),
                epoch: 1,
                home_node_id: "node-a".into(),
                session_id: "session-a".into(),
                create_intent_id: "intent-a".into(),
                prepare_token: "prepare-a".into(),
            },
            data_dir,
            local_prepare_id: "local-a".into(),
            parent_fsync_generation: 1,
        };
        let local_grant = RootGrant {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-a".into(),
            session_id: "session-a".into(),
            access_generation: 7,
            rights: vec![RootRight::Lookup],
            fencing_token: "fence-a".into(),
        };
        let peer_grant = RootGrant {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "session-b".into(),
            access_generation: 7,
            rights: vec![RootRight::Read],
            fencing_token: "fence-b".into(),
        };
        let meta = fixed_meta(local_grant, Some(peer_grant));
        let manager = RootManager::new("node-a".into(), "session-a".into(), meta.clone(), disk);
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let presented = PresentedRootAccess {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "session-b".into(),
            access_generation: 7,
            fencing_token: "fence-b".into(),
        };
        manager
            .validate_peer_root_access(&presented, "node-b", RootRight::Read)
            .unwrap();
        assert_eq!(meta.validate_calls.load(Ordering::SeqCst), 1);
        assert!(
            manager
                .revoke_matching_home(&home_revocation(&id, "cmd-peer-fence"))
                .unwrap()
        );
        assert!(manager.cached_peer_sessions().unwrap().is_empty());
        assert_eq!(
            manager
                .validate_peer_root_access(&presented, "node-b", RootRight::Read)
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE
        );
        assert_eq!(meta.validate_calls.load(Ordering::SeqCst), 1);
    }

    fn private_command(id: &RootId, command_id: &str) -> PrivateRootCommand {
        PrivateRootCommand {
            command_id: command_id.to_owned(),
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            root_id: id.clone(),
            root_epoch: 1,
            access_generation: 7,
            command_type: PrivateRootCommandType::RevokeAccess,
        }
    }

    #[test]
    fn private_foundation_journal_records_exact_command_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let journal = PrivateRootCommandJournal::open(temp.path()).unwrap();
        let id = RootId("job-42".into());
        let command = private_command(&id, "cmd-a");
        journal.record_exact(&command).unwrap();
        journal.record_exact(&command).unwrap();
        drop(journal);
        let journal = PrivateRootCommandJournal::open(temp.path()).unwrap();
        journal.record_exact(&command).unwrap();
        let mut changed = command.clone();
        changed.home_session_id = "session-other".into();
        assert_eq!(
            journal.record_exact(&changed).unwrap_err().code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
    }

    #[test]
    fn private_foundation_journal_rejects_same_id_field_boundary_injection() {
        let temp = tempfile::tempdir().unwrap();
        let journal = PrivateRootCommandJournal::open(temp.path()).unwrap();
        let mut first = private_command(&RootId("job-42".into()), "cmd-a");
        first.home_node_id = "node-a\nhome_session_id=session-extra".into();
        first.home_session_id = "session-a".into();
        let mut second = first.clone();
        second.home_node_id = "node-a".into();
        second.home_session_id = "session-extra\nhome_session_id=session-a".into();
        assert_ne!(first.exact_bytes(), second.exact_bytes());
        journal.record_exact(&first).unwrap();
        drop(journal);
        let reopened = PrivateRootCommandJournal::open(temp.path()).unwrap();
        assert_eq!(
            reopened.record_exact(&second).unwrap_err().code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        reopened.record_exact(&first).unwrap();
    }

    #[test]
    fn private_foundation_wrong_tuple_does_not_refuse_current_generation() {
        let journal_temp = tempfile::tempdir().unwrap();
        let journal = PrivateRootCommandJournal::open(journal_temp.path()).unwrap();
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let mut command = private_command(&prepared.reservation.id, "cmd-wrong-generation");
        command.access_generation += 1;
        assert_eq!(
            manager
                .apply_private_foundation_command(
                    &command,
                    &journal,
                    std::time::Duration::from_millis(10),
                )
                .unwrap_err()
                .code(),
            afs_error::NODE_OWNER_INVALID_GRANT
        );
        let _still_active = manager
            .enter_root(&prepared.reservation.id, RootRight::Lookup)
            .unwrap();
    }

    #[test]
    fn private_foundation_refuses_new_admission_and_waits_for_ordinary_rootuse_drop() {
        let journal_temp = tempfile::tempdir().unwrap();
        let journal = PrivateRootCommandJournal::open(journal_temp.path()).unwrap();
        let (_temp, manager, prepared) = fixture("session-a", "session-a");
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let manager = Arc::new(manager);
        let use_guard = manager
            .enter_root(&prepared.reservation.id, RootRight::Lookup)
            .unwrap();
        let command = private_command(&prepared.reservation.id, "cmd-drain");
        let worker = {
            let manager = manager.clone();
            std::thread::spawn(move || {
                manager.apply_private_foundation_command(
                    &command,
                    &journal,
                    std::time::Duration::from_secs(5),
                )
            })
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match manager.enter_root(&prepared.reservation.id, RootRight::Lookup) {
                Ok(guard) => drop(guard),
                Err(error) => {
                    assert_eq!(error.code(), afs_error::NODE_OWNER_GRANT_UNAVAILABLE);
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "refusal was not installed"
            );
            std::thread::yield_now();
        }
        assert!(!worker.is_finished());
        assert_eq!(
            manager
                .enter_root(&prepared.reservation.id, RootRight::Lookup)
                .err()
                .unwrap()
                .code(),
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE
        );
        drop(use_guard);
        let receipt = worker.join().unwrap().unwrap();
        assert!(receipt.ordinary_root_use_drained);
        assert!(!receipt.opened_fd_native_physical_namespace_drained);
        assert_eq!(receipt.command_id, "cmd-drain");
    }

    #[test]
    fn private_foundation_fences_cached_peer_grants_without_native_authority() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let data_dir = StoragePath::new("job-42-e1").unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        let id = RootId("job-42".into());
        let prepared = PreparedRoot {
            reservation: RootReservation {
                id: id.clone(),
                epoch: 1,
                home_node_id: "node-a".into(),
                session_id: "session-a".into(),
                create_intent_id: "intent-a".into(),
                prepare_token: "prepare-a".into(),
            },
            data_dir,
            local_prepare_id: "local-a".into(),
            parent_fsync_generation: 1,
        };
        let local_grant = RootGrant {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-a".into(),
            session_id: "session-a".into(),
            access_generation: 7,
            rights: vec![RootRight::Lookup],
            fencing_token: "fence-a".into(),
        };
        let peer_grant = RootGrant {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "session-b".into(),
            access_generation: 7,
            rights: vec![RootRight::Read],
            fencing_token: "fence-b".into(),
        };
        let manager = RootManager::new(
            "node-a".into(),
            "session-a".into(),
            fixed_meta(local_grant, Some(peer_grant)),
            disk,
        );
        manager
            .activate_prepared(OsString::from("job-42"), &prepared)
            .unwrap();
        let presented = PresentedRootAccess {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "session-b".into(),
            access_generation: 7,
            fencing_token: "fence-b".into(),
        };
        manager
            .validate_peer_root_access(&presented, "node-b", RootRight::Read)
            .unwrap();
        assert!(
            manager
                .cached_peer_sessions()
                .unwrap()
                .contains(&("node-b".to_owned(), "session-b".to_owned()))
        );

        let journal_temp = tempfile::tempdir().unwrap();
        let journal = PrivateRootCommandJournal::open(journal_temp.path()).unwrap();
        let receipt = manager
            .apply_private_foundation_command(
                &private_command(&id, "cmd-peer-fence"),
                &journal,
                std::time::Duration::from_millis(10),
            )
            .unwrap();
        assert!(manager.cached_peer_sessions().unwrap().is_empty());
        assert!(receipt.ordinary_root_use_drained);
        assert!(!receipt.opened_fd_native_physical_namespace_drained);
    }

    #[derive(Default)]
    struct RecoveryMetaState {
        pending: Option<RootReservation>,
        active: Option<RootLocation>,
        active_prepare_id: Option<String>,
        lose_activate_reply: bool,
    }

    #[derive(Default)]
    struct RecoveryMeta(Mutex<RecoveryMetaState>);

    impl RootMeta for RecoveryMeta {
        fn reserve_root(&self, id: &RootId, intent: &str) -> Result<RootReservation> {
            let mut state = self.0.lock().unwrap();
            let reservation = RootReservation {
                id: id.clone(),
                epoch: 1,
                home_node_id: "node-a".into(),
                session_id: "old-session".into(),
                create_intent_id: intent.into(),
                prepare_token: "prepare-1".into(),
            };
            state.pending = Some(reservation.clone());
            Ok(reservation)
        }

        fn activate_root(&self, prepared: &PreparedRoot) -> Result<RootGrant> {
            let mut state = self.0.lock().unwrap();
            let reservation = prepared.reservation();
            assert_eq!(state.pending.as_ref(), Some(reservation));
            state.pending = None;
            state.active = Some(RootLocation {
                id: reservation.id.clone(),
                epoch: reservation.epoch,
                home_node_id: reservation.home_node_id.clone(),
                home_session_id: reservation.session_id.clone(),
            });
            state.active_prepare_id = Some(prepared.local_prepare_id().into());
            if state.lose_activate_reply {
                return Err(unavailable_grant("injected lost Activate reply"));
            }
            Ok(recovery_grant(reservation.id.clone(), "old-session"))
        }

        fn abort_root(&self, reservation: &RootReservation) -> Result<()> {
            let mut state = self.0.lock().unwrap();
            if state.pending.as_ref() != Some(reservation) {
                return Err(invalid_grant("stale abort"));
            }
            state.pending = None;
            Ok(())
        }

        fn lookup_root(&self, id: &RootId) -> Result<Option<RootLocation>> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .active
                .clone()
                .filter(|root| &root.id == id))
        }

        fn list_owner_roots(&self, home: &str) -> Result<OwnerRootInventory> {
            let state = self.0.lock().unwrap();
            Ok(OwnerRootInventory {
                pending: state
                    .pending
                    .clone()
                    .filter(|root| root.home_node_id == home)
                    .into_iter()
                    .collect(),
                active: state
                    .active
                    .clone()
                    .filter(|root| root.home_node_id == home)
                    .into_iter()
                    .collect(),
            })
        }

        fn acquire_root(&self, _: &RootId, _: RootRight) -> Result<RootGrant> {
            unreachable!()
        }

        fn validate_root_access(&self, _: &PresentedRootAccess, _: &str) -> Result<RootGrant> {
            unreachable!()
        }

        fn recover_root(&self, record: &LocalRootRecord, session: &str) -> Result<RootGrant> {
            let mut state = self.0.lock().unwrap();
            if state.active_prepare_id.as_deref() != Some(record.local_prepare_id.as_str()) {
                return Err(invalid_grant("local prepare identity mismatch"));
            }
            let active = state
                .active
                .as_mut()
                .ok_or_else(|| invalid_grant("no active root"))?;
            if active.id != record.id || active.epoch != record.epoch {
                return Err(invalid_grant("local prepare identity mismatch"));
            }
            active.home_session_id = session.into();
            Ok(recovery_grant(record.id.clone(), session))
        }
    }

    fn recovery_grant(id: RootId, session: &str) -> RootGrant {
        RootGrant {
            id,
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: session.into(),
            holder_node_id: "node-a".into(),
            session_id: session.into(),
            access_generation: 1,
            rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write],
            fencing_token: "fence-1".into(),
        }
    }

    #[test]
    fn startup_cancels_each_pending_create_cut_without_exposing_a_root() {
        for cut in 0..4 {
            let temp = tempfile::tempdir().unwrap();
            let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
            let meta = Arc::new(RecoveryMeta::default());
            let name = OsStr::new("job");
            let id = root_id_from_name(name).unwrap();
            let reservation = meta.reserve_root(&id, "old-intent").unwrap();
            let data_dir = root_data_dir(name, reservation.epoch).unwrap();
            if cut >= 1 {
                disk.mkdir(&data_dir, 0o700).unwrap();
            }
            if cut >= 2 {
                disk.sync_root().unwrap();
            }
            if cut >= 3 {
                let catalog = LocalFsRootCatalog::new(disk.clone());
                let locked = catalog.lock_and_open().unwrap();
                locked
                    .persist_prepared_root(&LocalRootRecord {
                        id: id.clone(),
                        name: name.into(),
                        epoch: 1,
                        data_dir: data_dir.clone(),
                        local_prepare_id: "prepare-1:old-session".into(),
                    })
                    .unwrap();
            }
            let manager = RootManager::open(
                "node-a".into(),
                "new-session".into(),
                meta.clone(),
                disk.clone(),
            )
            .unwrap();
            assert!(
                manager.cached_local_roots().unwrap().is_empty(),
                "cut={cut}"
            );
            assert!(meta.0.lock().unwrap().pending.is_none(), "cut={cut}");
            assert!(disk.metadata(&data_dir).is_err(), "cut={cut}");
            drop(manager);
        }
    }

    #[test]
    fn activate_reply_loss_keeps_catalog_for_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        meta.0.lock().unwrap().lose_activate_reply = true;
        let manager = RootManager::open(
            "node-a".into(),
            "old-session".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap();
        assert!(manager.create_root(OsStr::new("job"), 0o700).is_err());
        assert!(
            manager
                .enter_root(
                    &root_id_from_name(OsStr::new("job")).unwrap(),
                    RootRight::Read
                )
                .is_err()
        );
        drop(manager);

        let recovered =
            RootManager::open("node-a".into(), "new-session".into(), meta, disk).unwrap();
        let root = recovered
            .enter_root(
                &root_id_from_name(OsStr::new("job")).unwrap(),
                RootRight::Read,
            )
            .unwrap();
        assert_eq!(root.grant().home_session_id, "new-session");
    }

    #[test]
    fn startup_finishes_cleanup_after_abort_commit() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        let name = OsStr::new("job");
        let id = root_id_from_name(name).unwrap();
        let data_dir = root_data_dir(name, 1).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id,
                name: name.into(),
                epoch: 1,
                data_dir: data_dir.clone(),
                local_prepare_id: "prepare-1:old-session".into(),
            })
            .unwrap();
        drop(locked);
        let manager =
            RootManager::open("node-a".into(), "new-session".into(), meta, disk.clone()).unwrap();
        assert!(manager.cached_local_roots().unwrap().is_empty());
        assert!(disk.metadata(&data_dir).is_err());
    }

    #[test]
    fn active_meta_root_without_catalog_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        meta.0.lock().unwrap().active = Some(RootLocation {
            id: root_id_from_name(OsStr::new("job")).unwrap(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "old-session".into(),
        });
        assert!(RootManager::open("node-a".into(), "new-session".into(), meta, disk).is_err());
    }

    #[test]
    fn pending_root_with_unexpected_file_fails_without_deleting_data() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        let name = OsStr::new("job");
        let reservation = meta
            .reserve_root(&root_id_from_name(name).unwrap(), "old-intent")
            .unwrap();
        let dir = pending_data_dir(&reservation).unwrap();
        disk.mkdir(&dir, 0o700).unwrap();
        let file = dir.join_component(OsStr::new("unexpected.txt")).unwrap();
        std::fs::write(temp.path().join(file.as_path()), b"keep me").unwrap();
        assert!(RootManager::open("node-a".into(), "new-session".into(), meta, disk).is_err());
        assert_eq!(
            std::fs::read(temp.path().join(file.as_path())).unwrap(),
            b"keep me"
        );
    }
}
