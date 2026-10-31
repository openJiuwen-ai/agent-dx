//! OwnerFs 的根目录身份、授权缓存与创建合同。
//!
//! `mkdir /ownerfs/job-42` 与根内的 `open job-42/a.txt` 是两条不同的路径：
//! 前者先向 Meta 预留根，再由选定的 Home（本机或远端）准备目录并激活；
//! 后者应凭已缓存的有效根授权直接访问本地普通文件。这里不保存逐文件的
//! inode 或数据。
//!
//! 启动必须先核对 Meta 的 pending/active 状态、本机 catalog 与物理目录，
//! 才能把根放进热路径。目录存在或预留成功都不构成访问授权。

use std::{
    collections::{HashMap, HashSet},
    ffi::{OsStr, OsString},
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::MetadataExt,
    },
    sync::{
        Arc, Condvar, Mutex, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use afs_error::{Error, Result};

use super::catalog::{
    LocalFsRootCatalog, LocalRootCatalog, LocalRootRecord, LockedLocalRootCatalog,
};
use crate::node::storage::{DirectoryHandle, FileStore, LocalFs, StoragePath};

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
    pub requester_node_id: String,
    pub requester_session_id: String,
}

/// Transient parameters supplied by the mkdir caller, never persisted in Meta.
#[derive(Clone, Debug)]
pub struct RootCreateParams {
    pub name: Vec<u8>,
    pub uid: u32,
    pub gid: u32,
    /// Directory permissions after applying the caller's umask (0o7777 bits).
    pub mode: u32,
}

/// One consistent Meta observation of an active root or a pending creation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootLookupState {
    pub location: Option<RootLocation>,
    pub reservation: Option<RootReservation>,
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
    /// Reserve a root for this entry session; Meta determines its Home.
    fn reserve_root(&self, id: &RootId, create_intent_id: &str) -> Result<RootReservation>;
    fn activate_root(&self, prepared: &PreparedRoot) -> Result<RootGrant>;
    fn abort_root(&self, reservation: &RootReservation) -> Result<()>;
    /// D4: 权威删除一个已 activate 的 active root。
    ///
    /// Meta 移除 Root/全部 holder grant，并通过 REVOKE_ACCESS RootCommand 传播。
    /// 幂等：root 已不在 Meta active 列表时返回 Ok(())。
    fn deactivate_root(
        &self,
        root_id: &RootId,
        epoch: u64,
        home_node_id: &str,
        session_id: &str,
    ) -> Result<()>;
    fn lookup_root(&self, id: &RootId) -> Result<Option<RootLocation>>;
    /// Return active and pending state in one linearizable observation.
    fn lookup_root_state(&self, id: &RootId) -> Result<RootLookupState> {
        let _ = id;
        Err(Error::coded(
            afs_error::META_STORE_UNIMPLEMENTED,
            "RootMeta root state lookup is not wired",
        ))
    }
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
    /// Global snapshot of Active roots, for the cross-node outlet mirror.
    ///
    /// `known_digest` is the digest the caller already holds; `Some` lets Meta
    /// answer `unchanged` without a payload. This is a read-only projection:
    /// it issues no grant and confers no authority.
    ///
    /// Default is UNIMPLEMENTED so existing in-process implementations keep
    /// compiling; the gRPC client overrides it.
    fn list_active_outlet_roots(&self, known_digest: Option<&str>) -> Result<ActiveOutletRoots> {
        let _ = known_digest;
        Err(Error::coded(
            afs_error::META_STORE_UNIMPLEMENTED,
            "RootMeta outlet snapshot lookup is not wired",
        ))
    }
}

/// Global snapshot of Active roots as seen by one outlet mirror.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ActiveOutletRoots {
    /// Ordered by `root_id`. Empty when `unchanged` is true — callers must
    /// keep their previous snapshot rather than interpreting this as "no roots".
    pub roots: Vec<OutletRoot>,
    /// Digest of the returned set; a pure function of the triples below.
    pub digest: String,
    /// True when the caller's `known_digest` matched and `roots` was omitted.
    pub unchanged: bool,
}

impl ActiveOutletRoots {
    /// Interpret a reply against a cached snapshot.
    ///
    /// `unchanged` replies carry no payload by design, so a caller that needs
    /// the roots must keep them from the previous round. Folding that rule into
    /// one function keeps callers from mistaking an empty `roots` for "Meta has
    /// no active roots", which would wipe the public directory (spec H14).
    pub fn merge_into(&self, cached: Vec<OutletRoot>) -> Vec<OutletRoot> {
        if self.unchanged {
            cached
        } else {
            self.roots.clone()
        }
    }
}

/// One Active root as the outlet mirror needs it.
///
/// Deliberately excludes `home_session_id`: the mirror never reads it, and it
/// is roughly a third of the payload. Consumers that need session liveness use
/// `list_owner_roots` instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutletRoot {
    pub root_id: RootId,
    pub root_epoch: u64,
    pub home_node_id: String,
}

impl OutletRoot {
    /// Stable ordering key. `RootId` does not implement `Ord`, and the mirror
    /// only needs a deterministic order for reproducible diffs, so compare the
    /// raw identifier rather than widening the id type's contract.
    pub fn sort_key(&self) -> &str {
        &self.root_id.0
    }
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
    workspace_locks: Mutex<HashMap<RootId, Weak<Mutex<()>>>>,
    /// H11: how long a superseded data directory must stay resolvable before
    /// it may be deleted. Defaults to the spec value; `with_retirement_grace`
    /// overrides it from configuration.
    retirement_grace_ms: u64,
}

impl RootManager {
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.local_node_id
    }

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
            workspace_locks: Mutex::new(HashMap::new()),
            retirement_grace_ms: DEFAULT_RETIREMENT_GRACE_MS,
        };
        manager.reconcile_on_startup()?;
        Ok(manager)
    }

    /// Override the H11 retirement grace window.
    ///
    /// Kept as a builder rather than a constructor parameter because thirteen
    /// call sites (mostly tests) have no opinion about it, and a default that
    /// matches the spec is the safe choice for all of them.
    #[must_use]
    pub fn with_retirement_grace(mut self, grace_ms: u64) -> Self {
        self.retirement_grace_ms = grace_ms;
        self
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

    fn workspace_lock(&self, id: &RootId) -> Result<Arc<Mutex<()>>> {
        let mut locks = self
            .workspace_locks
            .lock()
            .map_err(|_| invalid_grant("workspace lock map poisoned"))?;
        locks.retain(|_, lock| lock.upgrade().is_some());
        if let Some(lock) = locks.get(id).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(id.clone(), Arc::downgrade(&lock));
        Ok(lock)
    }

    /// Reserve a first-level workspace for this entry session.
    /// Same-session retries reuse a pending Home reservation.
    pub fn reserve_workspace(&self, name: &OsStr) -> Result<RootReservation> {
        let id = root_id_from_name(name)?;
        let create_intent_id = format!("{}:{}:{}", self.local_node_id, self.session_id, id.0);
        let reservation = self.meta.reserve_root(&id, &create_intent_id)?;
        if reservation.id != id
            || reservation.create_intent_id != create_intent_id
            || reservation.requester_node_id != self.local_node_id
            || reservation.requester_session_id != self.session_id
            || reservation.epoch == 0
            || reservation.home_node_id.is_empty()
            || reservation.session_id.is_empty()
            || reservation.prepare_token.is_empty()
        {
            return Err(invalid_grant(
                "reserved workspace does not match this entry session",
            ));
        }
        Ok(reservation)
    }

    /// Prepare a reserved workspace on this node when it is the selected Home.
    /// Reusing a prepared directory with different uid, gid or mode returns
    /// IO_INVALID without cancelling the pending reservation.
    /// The workspace lock covers the Meta identity check, local directory
    /// preparation, catalog publication and activation, preventing duplicate notifications from interleaving with the create.
    pub fn prepare_owner_root(
        &self,
        requester_node_id: &str,
        reservation: &RootReservation,
        params: &RootCreateParams,
    ) -> Result<()> {
        let lock = self.workspace_lock(&reservation.id)?;
        let _guard = lock
            .lock()
            .map_err(|_| invalid_grant("workspace lock poisoned"))?;
        self.validate_home_reservation(requester_node_id, reservation, params)?;
        let state = self.meta.lookup_root_state(&reservation.id)?;
        if state.location.is_some() {
            return Err(Error::coded(
                afs_error::IO_ALREADY_EXISTS,
                "workspace is already active",
            ));
        }
        if state.reservation.as_ref() != Some(reservation) {
            return Err(invalid_grant(
                "workspace reservation changed before Home preparation",
            ));
        }
        let prepared = match self.prepare_local_root(reservation, params) {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Err(abort_error) = self.meta.abort_root(reservation) {
                    afs_logging::warn!("ownerfs.prepare_abort_unconfirmed";
                        "root_id" => reservation.id.0.clone(),
                        "home_node_id" => self.local_node_id.clone(),
                        "create_intent_id" => reservation.create_intent_id.clone(),
                        "prepare_error" => error.to_string(),
                        "abort_error" => abort_error.to_string());
                }
                return Err(error);
            }
        };
        let metadata = self.disk.metadata(prepared.data_dir())?;
        if metadata.uid() != params.uid
            || metadata.gid() != params.gid
            || metadata.mode() & 0o7777 != params.mode
        {
            return Err(Error::coded(
                afs_error::IO_INVALID,
                "workspace creation parameters differ from the prepared directory",
            ));
        }
        // Activation may commit before a reply is lost. Keep the durable catalog
        // on any activation error; startup recovery resolves the current state.
        self.activate_prepared(OsString::from_vec(params.name.clone()), &prepared)
    }

    fn validate_home_reservation(
        &self,
        requester_node_id: &str,
        reservation: &RootReservation,
        params: &RootCreateParams,
    ) -> Result<()> {
        if !self.control_valid.load(Ordering::Acquire) {
            return Err(unavailable_grant("Meta control session is invalid"));
        }
        if requester_node_id.is_empty()
            || reservation.epoch == 0
            || reservation.home_node_id != self.local_node_id
            || reservation.session_id != self.session_id
            || reservation.prepare_token.is_empty()
            || reservation.requester_node_id != requester_node_id
            || reservation.requester_session_id.is_empty()
        {
            return Err(invalid_grant(
                "workspace reservation identity is incomplete",
            ));
        }
        if root_id_from_name(OsStr::from_bytes(&params.name))? != reservation.id
            || params.mode & !0o7777 != 0
        {
            return Err(invalid_grant("workspace name or permissions are invalid"));
        }
        let expected = format!(
            "{}:{}:{}",
            reservation.requester_node_id, reservation.requester_session_id, reservation.id.0
        );
        if reservation.create_intent_id != expected {
            return Err(invalid_grant("workspace create intent id is not canonical"));
        }
        Ok(())
    }

    /// Prepare durable local evidence while the caller holds the workspace lock.
    /// Errors here precede activation and can safely cancel this reservation.
    fn prepare_local_root(
        &self,
        reservation: &RootReservation,
        params: &RootCreateParams,
    ) -> Result<PreparedRoot> {
        let name = OsString::from_vec(params.name.clone());
        let data_dir = root_data_dir(&name, reservation.epoch)?;
        let local_prepare_id = format!("{}:{}", reservation.prepare_token, self.session_id);
        let mut local_record = self.catalog.lookup_root(&reservation.id)?;
        if let Some(record) = &local_record
            && record.epoch < reservation.epoch
        {
            // The current reservation was verified under the workspace lock.
            // Remove only an obsolete, canonical directory without a local runtime.
            if record.epoch == 0
                || record.name != name
                || root_id_from_name(&record.name)? != record.id
                || root_data_dir(&record.name, record.epoch)? != record.data_dir
                || record.local_prepare_id.is_empty()
                || self
                    .roots
                    .read()
                    .map_err(|_| invalid_grant("root cache lock poisoned"))?
                    .contains_key(&record.id)
            {
                return Err(invalid_grant(
                    "conflicting local catalog record already exists",
                ));
            }
            self.remove_abandoned_dir(&record.data_dir)?;
            self.catalog.remove_prepared_root(record)?;
            local_record = None;
        }
        if let Some(record) = local_record {
            if record.name != name
                || record.epoch != reservation.epoch
                || record.data_dir != data_dir
                || record.local_prepare_id != local_prepare_id
            {
                return Err(invalid_grant(
                    "conflicting local catalog record already exists",
                ));
            }
            if !self.disk.metadata(&data_dir)?.is_dir() {
                return Err(invalid_grant(
                    "local catalog points to a missing workspace directory",
                ));
            }
        } else {
            // Keep the directory owner-readable while opening the durability
            // handle. The requested final mode may be 000, and LocalFs opens
            // directories with O_RDONLY.
            let temporary_mode = params.mode | 0o700;
            match self.disk.mkdir(&data_dir, temporary_mode) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if !self.disk.metadata(&data_dir)?.is_dir() {
                        return Err(invalid_grant(
                            "reserved workspace path exists but is not a directory",
                        ));
                    }
                    self.disk.remove_dir(&data_dir)?;
                    self.disk.sync_root()?;
                    self.disk.mkdir(&data_dir, temporary_mode)?;
                }
                Err(error) => return Err(Error::from(error)),
            }
            // A daemon umask may have removed the temporary owner bits from
            // mkdir; chmod before open makes the handle acquisition reliable.
            self.disk.chmod(&data_dir, temporary_mode)?;
            let directory = self.disk.open_dir(&data_dir)?;
            self.disk
                .chown(&data_dir, Some(params.uid), Some(params.gid))?;
            self.disk.chmod(&data_dir, params.mode)?;
            directory.sync_all()?;
            self.disk.sync_root()?;
            let record = LocalRootRecord {
                id: reservation.id.clone(),
                name,
                epoch: reservation.epoch,
                data_dir: data_dir.clone(),
                local_prepare_id: local_prepare_id.clone(),
            };
            self.catalog.persist_prepared_root(&record)?;
        }
        Ok(PreparedRoot::new(
            reservation.clone(),
            data_dir,
            local_prepare_id,
            1,
        ))
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

    /// D4: 完整删除一个 active root。
    ///
    /// 调用顺序：revoke → 缓存移除 → 物理目录 → Meta deactivate → catalog。
    /// 每步幂等，部分失败可安全重试。并发 rmdir 中，缓存移除是线性化点：
    /// 第二个调用在缓存中找不到 root，返回 invalid_grant（FUSE 层转 ENOENT）。
    ///
    /// 关键点是 **ENOTEMPTY 时保留 catalog 记录**（H19），不是调整步骤顺序。
    /// AFS 的 `revoke_root` 能撤掉 AFS peer 的授权，但撤不到内核 NFS 客户端
    /// ——outlet 让"目录仍被打开"成为常态，`remove_dir` 返回 ENOTEMPTY 是
    /// 预期而非异常。此前实现在 ENOTEMPTY 时保留 catalog，但
    /// `reconcile_on_startup` 会在下次启动时把它丢弃，导致目录变成无主孤儿。
    /// 现在 catalog 记录贯穿保存，由 [`RootManager::reclaim_deferred`] 回收。
    ///
    /// Meta deactivate 保持在物理删除之前：若反过来，Meta 不可达时目录已删，
    /// 会留下"Meta 仍 Active、数据已不存在"的僵尸 root。
    ///
    /// **ENOTEMPTY 不算失败**（H20）：目录暂不可删时仍完成控制面删除（名字
    /// 应从所有节点消失），并保留 catalog 记录作为"该目录仍需回收"的持久证据，
    /// 由 [`RootManager::reclaim_deferred`] 异步重试。对调用者返回成功。
    ///
    /// Meta 不可达时在 deactivate 处失败，此时物理目录可能已删；catalog 记录
    /// 保留，reconcile_on_startup 的 zombie 检测会在下次启动时收敛。
    pub fn delete_root(&self, id: &RootId) -> Result<()> {
        afs_logging::info!("root.delete_root.start"; "root_id" => ?id);
        let result = self.delete_root_inner(id);
        match &result {
            Ok(()) => afs_logging::info!("root.delete_root.completed"; "root_id" => ?id),
            Err(error) => {
                afs_logging::info!("root.delete_root.failed"; "root_id" => ?id, "error" => ?error)
            }
        }
        result
    }

    /// 执行步骤见 [`RootManager::delete_root`] 文档；每步成功后记录 info，
    /// 任一步失败时提前返回，统一由外层 `delete_root` 记录失败日志。
    fn delete_root_inner(&self, id: &RootId) -> Result<()> {
        let lock = self.workspace_lock(id)?;
        let _guard = lock
            .lock()
            .map_err(|_| invalid_grant("workspace lock poisoned"))?;
        // 1. revoke — 阻止新的 enter_root。
        self.revoke_root(id);

        // 2. 从缓存移除（并发 rmdir 的线性化点）。
        {
            let mut roots = self
                .roots
                .write()
                .map_err(|_| invalid_grant("root cache lock poisoned"))?;
            let removed = roots.remove(id);
            if removed.is_none() {
                return Err(invalid_grant("root not found for deletion"));
            }
        }
        afs_logging::info!("root.delete_root.cache_removed"; "root_id" => ?id);

        let records = self.catalog.scan_roots()?;
        let record = records
            .into_iter()
            .find(|record| &record.id == id)
            .ok_or_else(|| invalid_grant("root catalog record not found"))?;

        // 3. Meta deactivate（epoch 来自 catalog）。名字由此从所有节点消失。
        //
        // 必须先于物理删除：Meta 不可达时这里失败并提前返回，此时目录与
        // catalog 均完好、状态一致可重试。若反过来先删目录，Meta 失败就会
        // 留下"Meta 仍 Active、数据已不存在"的僵尸 root——名字继续发布，
        // 消费者 bind 到空路径，比保留一个待回收目录糟得多。
        self.meta
            .deactivate_root(id, record.epoch, &self.local_node_id, &self.session_id)?;
        afs_logging::info!("root.delete_root.meta_deactivated"; "root_id" => ?id, "epoch" => record.epoch);

        // 4. 尝试物理删除。
        let dir_removed = match self.remove_abandoned_dir(&record.data_dir) {
            Ok(()) => {
                afs_logging::info!("root.delete_root.dir_removed"; "root_id" => ?id, "data_dir" => ?record.data_dir);
                true
            }
            Err(error) if error.code() == afs_error::IO_DIRECTORY_NOT_EMPTY => {
                // H20：不是失败，是"现在不能删"。留下 catalog 记录供回收循环重试。
                //
                // 同时写入弃用标记（H11）：宽限期从这里开始计时。不标记的话
                // 回收循环只能靠"目录非空"反复试探，无法区分"刚被弃用"与
                // "已存在很久"，也就无法保证 spec 要求的 `generation_grace_ms`
                // 最小存活时间。
                if let Err(mark_error) = self.catalog.mark_retired(&record.data_dir, now_unix_ms())
                {
                    // 标记失败不改变删除结果：目录本来就删不掉，回收退化为
                    // 每轮试探（等价于旧行为），不应让 rmdir 报错。
                    afs_logging::warn!(
                        "root.delete_root.retire_mark_failed";
                        "root_id" => ?id,
                        "error" => mark_error.to_string(),
                    );
                }
                afs_logging::warn!(
                    "root.delete_root.dir_busy";
                    "root_id" => ?id,
                    "data_dir" => ?record.data_dir,
                );
                false
            }
            Err(error) => return Err(error),
        };

        // 5. 仅在物理目录确实删掉时移除 catalog 条目（H19）。
        //    未删掉时保留记录：它是"该目录仍待回收"的唯一持久证据，
        //    由 reclaim_deferred 周期性重试。
        if dir_removed {
            self.catalog.remove_prepared_root(&record)?;
            afs_logging::info!("root.delete_root.catalog_removed"; "root_id" => ?id);
        } else {
            afs_logging::info!(
                "root.delete_root.catalog_deferred";
                "root_id" => ?id,
                "data_dir" => ?record.data_dir,
            );
        }

        Ok(())
    }

    /// 回收一次延迟删除的数据目录（H11/H21）。
    ///
    /// **两条独立的回收路径**：
    ///
    /// 1. **catalog 记录的目录**：说明它曾被本节点激活、现已不活跃。
    ///    `delete_root` 删不掉它时（ENOTEMPTY）会写入弃用标记，本方法据
    ///    `retired_at + grace` 判断何时可以真正删除。
    /// 2. **无 catalog 证据的旧代目录**：同名换代后旧代既不在缓存也不在
    ///    catalog。这类目录**只有带弃用标记才回收**；没有标记一律不动——
    ///    我们无法判断它是否仍被 NFS 客户端 bind（H23）。
    ///
    /// 宽限期（H11）解决的是一个与时序有关的竞态：`/public` 的 symlink 换成
    /// 新代是即时的，但（a）其他节点的 symlink 更新是最终一致的，（b）NFS
    /// 目录缓存让新代可见性滞后至多 `actimeo`。窗口内新启动的容器可能仍读到
    /// 旧代 target，若旧代目录已被删除就会 ENOENT。
    ///
    /// 返回 `(removed, pending)`。不持 `roots` 锁跨越磁盘操作。
    pub fn reclaim_deferred(&self) -> Result<(usize, usize)> {
        let cached: std::collections::HashSet<String> = {
            let roots = self
                .roots
                .read()
                .map_err(|_| invalid_grant("root cache lock poisoned"))?;
            roots
                .values()
                .map(|root| root.data_dir.as_path().to_string_lossy().into_owned())
                .collect()
        };
        let now = now_unix_ms();
        let grace = self.retirement_grace_ms;

        // 明确被弃用的目录：这是唯一允许"无 catalog 证据也删除"的入口。
        let retired = self.catalog.scan_retired()?;
        let mut retired_by_dir: HashMap<String, u64> = HashMap::new();
        for (data_dir, at) in retired {
            retired_by_dir.insert(data_dir.as_path().to_string_lossy().into_owned(), at);
        }

        let mut removed = 0;
        let mut pending = 0;
        let mut attempted: std::collections::HashSet<String> = std::collections::HashSet::new();

        // 路径 1：catalog 记录。
        for record in self.catalog.scan_roots()? {
            let dir = record.data_dir.as_path().to_string_lossy().into_owned();
            if cached.contains(&dir) {
                continue;
            }
            attempted.insert(dir.clone());
            match retired_by_dir.get(&dir) {
                // 尚未过宽限期：等。
                Some(at) if now.saturating_sub(*at) < grace => {
                    pending += 1;
                    continue;
                }
                // 已过宽限期，或从未标记（旧版本遗留的记录）：直接尝试。
                _ => {}
            }
            match self.remove_abandoned_dir(&record.data_dir) {
                Ok(()) => {
                    self.catalog.remove_prepared_root(&record)?;
                    if retired_by_dir.contains_key(&dir) {
                        self.catalog.clear_retired(&record.data_dir)?;
                    }
                    removed += 1;
                    afs_logging::info!("root.reclaim.completed"; "data_dir" => dir);
                }
                Err(error) if error.code() == afs_error::IO_DIRECTORY_NOT_EMPTY => {
                    // 对端仍持有句柄。首次遇到时标记弃用，让宽限期开始计时。
                    if !retired_by_dir.contains_key(&dir) {
                        self.catalog.mark_retired(&record.data_dir, now)?;
                    }
                    pending += 1;
                }
                Err(error) if error.code() == afs_error::IO_NOT_FOUND => {
                    self.catalog.remove_prepared_root(&record)?;
                    if retired_by_dir.contains_key(&dir) {
                        self.catalog.clear_retired(&record.data_dir)?;
                    }
                }
                Err(error) => {
                    afs_logging::warn!(
                        "root.reclaim.failed";
                        "data_dir" => dir,
                        "error" => error.to_string(),
                    );
                    return Err(error);
                }
            }
        }

        // 路径 2：只有弃用标记、没有 catalog 记录的旧代（同名换代的产物）。
        for (dir, at) in &retired_by_dir {
            if attempted.contains(dir) || cached.contains(dir) {
                continue;
            }
            if now.saturating_sub(*at) < grace {
                pending += 1;
                continue;
            }
            let Ok(data_dir) = StoragePath::new(dir) else {
                continue;
            };
            match self.remove_abandoned_dir(&data_dir) {
                Ok(()) => {
                    self.catalog.clear_retired(&data_dir)?;
                    removed += 1;
                    afs_logging::info!("root.reclaim.superseded_removed"; "data_dir" => dir);
                }
                Err(error) if error.code() == afs_error::IO_DIRECTORY_NOT_EMPTY => {
                    // 宽限期过后仍被占用：保持标记，下一轮再试。
                    pending += 1;
                }
                Err(error) if error.code() == afs_error::IO_NOT_FOUND => {
                    self.catalog.clear_retired(&data_dir)?;
                }
                Err(error) => {
                    afs_logging::warn!(
                        "root.reclaim.failed";
                        "data_dir" => dir,
                        "error" => error.to_string(),
                    );
                    return Err(error);
                }
            }
        }
        Ok((removed, pending))
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
                // A newer reservation fences older local preparation. The
                // same epoch still requires its exact durable prepare identity.
                if record.epoch > reservation.epoch
                    || record.local_prepare_id.is_empty()
                    || (record.epoch == reservation.epoch
                        && (record.data_dir != expected_dir
                            || record.local_prepare_id != expected_prepare_id))
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
                if record.epoch < reservation.epoch {
                    self.remove_abandoned_dir(&record.data_dir)?;
                }
                self.catalog.remove_prepared_root(&record)?;
            }
        }

        for record in records.into_values() {
            let Some(location) = active.get(&record.id) else {
                // 两种来源：(1) Abort CAS 后崩溃、本地清理前的 pending reservation；
                // (2) delete_root 已提交 Meta deactivate 但物理目录/catalog 清理被
                // 中断（含非空目录删除失败）。两者都表示该 root 已逻辑删除：
                // 收敛为移除 catalog 条目；物理目录非空（ENOTEMPTY）时记录警告
                // 并保留，等待人工清理，不阻塞启动（review: 非空 root 删除失败
                // 会丢失控制面记录）。
                //
                // 保留目录由 `reclaim_deferred` 按目录名发现并回收，不依赖
                // catalog 条目——catalog 以 root_id 为主键，同名重建会覆盖旧代，
                // 不能兼作删除队列。
                // 保留目录时**保留 catalog 条目**（H19）：回收循环以它是
                // 唯一证据来判断"这个目录曾被本节点激活、现在需要清理"。
                // 丢弃条目会让目录失去归属，而回收若改从目录名推断，就会
                // 误删同名换代后的旧代（仍可能被 NFS 客户端 bind）。
                match self.remove_abandoned_dir(&record.data_dir) {
                    Ok(()) => {
                        self.catalog.remove_prepared_root(&record)?;
                    }
                    Err(error) if error.code() == afs_error::IO_DIRECTORY_NOT_EMPTY => {
                        afs_logging::warn!(
                            "root.reconcile.deferred_data_dir";
                            "root_id" => ?record.id,
                            "data_dir" => ?record.data_dir,
                            "error" => error.to_string(),
                        );
                    }
                    Err(error) => return Err(error),
                }
                continue;
            };
            if location.epoch != record.epoch || location.home_node_id != self.local_node_id {
                return Err(invalid_grant(
                    "active Meta root does not match local durable root",
                ));
            }
            // Zombie root detection (spec §4.2): catalog 和 Meta 都有记录，
            // 但物理目录已不存在时，执行完整清理而不是启动失败。
            if !self
                .disk
                .metadata(&record.data_dir)
                .map(|metadata| metadata.is_dir())
                .unwrap_or(false)
            {
                afs_logging::warn!(
                    "root.reconcile.zombie_detected";
                    "root_id" => ?record.id,
                    "data_dir" => ?record.data_dir,
                );
                self.meta.deactivate_root(
                    &record.id,
                    record.epoch,
                    &self.local_node_id,
                    &self.session_id,
                )?;
                self.catalog.remove_prepared_root(&record)?;
                continue;
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

    /// The active roots recovered by `reconcile_on_startup`, as `(id, name)`.
    ///
    /// Node startup calls this after wiring the MountManager to reconcile
    /// residual egress binds (D4 §4.4) and to re-trigger mounts for recovered
    /// roots whose claims were lost with the previous process (v3 §5.6).
    pub(crate) fn recovered_roots(&self) -> Vec<(RootId, OsString)> {
        self.roots
            .read()
            .map(|roots| {
                roots
                    .iter()
                    .map(|(id, root)| (id.clone(), root.name.clone()))
                    .collect()
            })
            .unwrap_or_default()
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

// H11 default: 120000ms = max(interval_ms x 10, actimeo x 2) for the default
// 250ms interval and 60s actimeo.
const DEFAULT_RETIREMENT_GRACE_MS: u64 = 120_000;

/// Wall-clock milliseconds for retirement deadlines.
///
/// Wall time rather than a monotonic clock because the deadline must survive a
/// restart: the marker is persisted and compared after the process comes back,
/// where a monotonic baseline is gone.
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
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

/// Physical data directory for a root generation: `root-<hex(name)>-e<epoch>`.
///
/// `pub(crate)` so the outlet mirror can cross-check its path formula against
/// this function instead of duplicating the encoding.
pub(crate) fn root_data_dir(name: &OsStr, epoch: u64) -> Result<StoragePath> {
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
    use std::os::unix::fs::MetadataExt;

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

        fn deactivate_root(&self, _: &RootId, _: u64, _: &str, _: &str) -> Result<()> {
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
                requester_node_id: String::new(),
                requester_session_id: String::new(),
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
                requester_node_id: String::new(),
                requester_session_id: String::new(),
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
                requester_node_id: String::new(),
                requester_session_id: String::new(),
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
                requester_node_id: String::new(),
                requester_session_id: String::new(),
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
                requester_node_id: "node-a".into(),
                requester_session_id: "old-session".into(),
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

        fn deactivate_root(&self, root_id: &RootId, _: u64, _: &str, _: &str) -> Result<()> {
            let mut state = self.0.lock().unwrap();
            if state
                .active
                .as_ref()
                .is_some_and(|location| &location.id == root_id)
            {
                state.active = None;
            }
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

        fn lookup_root_state(&self, id: &RootId) -> Result<RootLookupState> {
            let state = self.0.lock().unwrap();
            Ok(RootLookupState {
                location: state.active.clone().filter(|root| &root.id == id),
                reservation: state.pending.clone().filter(|root| &root.id == id),
            })
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
        let owner = std::fs::metadata(temp.path()).unwrap();
        let params = RootCreateParams {
            name: b"job".to_vec(),
            uid: owner.uid(),
            gid: owner.gid(),
            mode: 0o700,
        };
        let reservation = manager.reserve_workspace(OsStr::new("job")).unwrap();
        let error = manager
            .prepare_owner_root("node-a", &reservation, &params)
            .unwrap_err();
        assert_eq!(error.message(), "injected lost Activate reply");
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

    #[test]
    fn delete_root_removes_cache_meta_catalog_and_physical_dir() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        let name = OsStr::new("job");
        let id = root_id_from_name(name).unwrap();
        let data_dir = root_data_dir(name, 1).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        meta.0.lock().unwrap().active = Some(RootLocation {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
        });
        meta.0.lock().unwrap().active_prepare_id = Some("prepare-1:session-a".into());
        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id: id.clone(),
                name: name.into(),
                epoch: 1,
                data_dir: data_dir.clone(),
                local_prepare_id: "prepare-1:session-a".into(),
            })
            .unwrap();
        drop(locked);

        // 启动恢复后 root 进入缓存。
        let manager = RootManager::open(
            "node-a".into(),
            "session-a".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap();
        assert_eq!(manager.cached_local_roots().unwrap().len(), 1);

        manager.delete_root(&id).unwrap();

        // 1. 缓存已移除。
        assert!(manager.cached_local_roots().unwrap().is_empty());
        // 2. Meta active 已清除。
        assert!(meta.0.lock().unwrap().active.is_none());
        // 3. 物理目录已删除。
        assert!(disk.metadata(&data_dir).is_err());

        // 4. 并发 rmdir 线性化：第二次调用返回 invalid_grant。
        let err = manager.delete_root(&id).unwrap_err();
        assert_eq!(err.code(), afs_error::NODE_OWNER_INVALID_GRANT);

        // 5. catalog 无记录（manager 持有独占锁，先释放再验证）。
        drop(manager);
        let locked = LocalFsRootCatalog::new(disk.clone())
            .lock_and_open()
            .unwrap();
        assert!(locked.scan_roots().unwrap().is_empty());
    }

    #[test]
    fn startup_cleans_zombie_root_with_missing_physical_dir() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        let name = OsStr::new("job");
        let id = root_id_from_name(name).unwrap();
        let data_dir = root_data_dir(name, 1).unwrap();
        // catalog 持久化要求 data_dir 存在：先建目录，持久化后再删除，
        // 构造"目录已被删除但 catalog/Meta 仍记录"的 zombie root。
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        meta.0.lock().unwrap().active = Some(RootLocation {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
        });
        meta.0.lock().unwrap().active_prepare_id = Some("prepare-1:session-a".into());
        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id: id.clone(),
                name: name.into(),
                epoch: 1,
                data_dir: data_dir.clone(),
                local_prepare_id: "prepare-1:session-a".into(),
            })
            .unwrap();
        drop(locked);
        disk.remove_dir(&data_dir).unwrap();

        // 启动不再失败：zombie 被 deactivate + 清 catalog。
        let manager = RootManager::open(
            "node-a".into(),
            "session-a".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap();
        assert!(manager.cached_local_roots().unwrap().is_empty());
        assert!(meta.0.lock().unwrap().active.is_none());
        // catalog 无记录（manager 持有独占锁，先释放再验证）。
        drop(manager);
        let locked = LocalFsRootCatalog::new(disk.clone())
            .lock_and_open()
            .unwrap();
        assert!(locked.scan_roots().unwrap().is_empty());
    }

    #[test]
    fn delete_root_keeps_catalog_when_physical_dir_not_empty() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        let name = OsStr::new("job");
        let id = root_id_from_name(name).unwrap();
        let data_dir = root_data_dir(name, 1).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        meta.0.lock().unwrap().active = Some(RootLocation {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
        });
        meta.0.lock().unwrap().active_prepare_id = Some("prepare-1:session-a".into());
        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id: id.clone(),
                name: name.into(),
                epoch: 1,
                data_dir: data_dir.clone(),
                local_prepare_id: "prepare-1:session-a".into(),
            })
            .unwrap();
        drop(locked);

        let manager = RootManager::open(
            "node-a".into(),
            "session-a".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap();
        assert_eq!(manager.cached_local_roots().unwrap().len(), 1);

        // 制造非空目录：删除必然 ENOTEMPTY。
        std::fs::write(temp.path().join(data_dir.as_path()).join("payload"), b"x").unwrap();

        // ENOTEMPTY 不是失败（H20）：控制面删除仍然完成，调用者得到 Ok。
        manager.delete_root(&id).unwrap();

        // Meta 记录已删 → 名字从所有节点消失。
        assert!(meta.0.lock().unwrap().active.is_none());
        // 缓存已移除 → 本节点不再准入。
        assert!(manager.cached_local_roots().unwrap().is_empty());
        // 物理目录仍在（对端仍持有，删不掉）。
        assert!(temp.path().join(data_dir.as_path()).exists());

        // catalog 记录必须保留（H19）：它是"该目录仍待回收"的唯一持久证据。
        drop(manager);
        let locked = LocalFsRootCatalog::new(disk.clone())
            .lock_and_open()
            .unwrap();
        assert_eq!(
            locked.scan_roots().unwrap().len(),
            1,
            "catalog entry must survive so reclaim can find the directory"
        );
        drop(locked);

        // 重启 reconcile 也必须保留该条目，不能像旧实现那样丢弃。
        let manager = RootManager::open(
            "node-a".into(),
            "session-a".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap();
        assert!(manager.cached_local_roots().unwrap().is_empty());

        // 回收：句柄仍在 → 仍是 ENOTEMPTY，报告 pending 而不是删除条目。
        let (removed, pending) = manager.reclaim_deferred().unwrap();
        assert_eq!((removed, pending), (0, 1));
        assert!(temp.path().join(data_dir.as_path()).exists());

        // 对端释放句柄（目录清空）。此时仍**不能**回收：H11 宽限期从
        // `delete_root` 遇到 ENOTEMPTY 起计时，默认 2 分钟内旧代必须保持可
        // 解析，否则窗口内新启动的容器会 bind 到已消失的目录。
        std::fs::remove_file(temp.path().join(data_dir.as_path()).join("payload")).unwrap();
        let (removed, pending) = manager.reclaim_deferred().unwrap();
        assert_eq!(
            (removed, pending),
            (0, 1),
            "the grace window has not elapsed yet"
        );
        assert!(temp.path().join(data_dir.as_path()).exists());
        drop(manager);

        // 宽限期归零后同一目录可回收（模拟宽限期已过）。
        let manager = RootManager::open(
            "node-a".into(),
            "session-a".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap()
        .with_retirement_grace(0);
        let (removed, _pending) = manager.reclaim_deferred().unwrap();
        assert_eq!(removed, 1, "expired grace must reclaim the directory");
        assert!(!temp.path().join(data_dir.as_path()).exists());

        drop(manager);
        let locked = LocalFsRootCatalog::new(disk.clone())
            .lock_and_open()
            .unwrap();
        assert!(
            locked.scan_roots().unwrap().is_empty(),
            "successful reclaim must drain the catalog entry"
        );
    }

    /// H19：物理删除成功后，catalog 条目应与目录一同消失，不残留待回收项。
    #[test]
    fn delete_root_drains_catalog_when_dir_removed() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(RecoveryMeta::default());
        let name = OsStr::new("clean");
        let id = root_id_from_name(name).unwrap();
        let data_dir = root_data_dir(name, 1).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        meta.0.lock().unwrap().active = Some(RootLocation {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
        });
        meta.0.lock().unwrap().active_prepare_id = Some("prepare-1:session-a".into());
        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id: id.clone(),
                name: name.into(),
                epoch: 1,
                data_dir: data_dir.clone(),
                local_prepare_id: "prepare-1:session-a".into(),
            })
            .unwrap();
        drop(locked);

        let manager = RootManager::open(
            "node-a".into(),
            "session-a".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap();
        manager.delete_root(&id).unwrap();
        assert!(!temp.path().join(data_dir.as_path()).exists());
        assert!(meta.0.lock().unwrap().active.is_none());

        // 无待回收项：回收循环应为空转。
        assert_eq!(manager.reclaim_deferred().unwrap(), (0, 0));

        drop(manager);
        let locked = LocalFsRootCatalog::new(disk.clone())
            .lock_and_open()
            .unwrap();
        assert!(locked.scan_roots().unwrap().is_empty());
    }

    /// H11：宽限期内不得删除已弃用的目录。
    ///
    /// 竞态在于：`/public` 的 symlink 换成新代是即时的，但其他节点的 symlink
    /// 更新是最终一致的，NFS 目录缓存还会滞后至多 `actimeo`。窗口内新启动
    /// 的容器可能仍解析到旧代 target，此时删掉旧代目录就是 ENOENT。
    #[test]
    fn retired_directory_survives_the_grace_window() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let name = OsStr::new("agent1");
        let id = root_id_from_name(name).unwrap();
        let data_dir = root_data_dir(name, 1).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        // 目录非空：否则启动对账会直接删掉它（进程刚启动时不存在"已 bind
        // 到旧代的容器"，空目录没有保留价值）。本测试关心的是运行期回收。
        std::fs::write(temp.path().join(data_dir.as_path()).join("held"), b"x").unwrap();

        // 有 catalog 记录、无 roots 缓存，且已被标记为刚弃用。
        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id: id.clone(),
                name: name.into(),
                epoch: 1,
                data_dir: data_dir.clone(),
                local_prepare_id: "prepare-1".into(),
            })
            .unwrap();
        let now = now_unix_ms();
        locked.mark_retired(&data_dir, now).unwrap();
        drop(locked);

        let meta = Arc::new(RecoveryMeta::default());
        // 宽限期很长：本轮不得删除。
        let manager = RootManager::open(
            "node-a".into(),
            "session-a".into(),
            meta.clone(),
            disk.clone(),
        )
        .unwrap()
        .with_retirement_grace(3_600_000);

        let (removed, pending) = manager.reclaim_deferred().unwrap();
        assert_eq!((removed, pending), (0, 1), "must wait out the grace window");
        assert!(
            temp.path().join(data_dir.as_path()).exists(),
            "a freshly retired directory must survive"
        );
        drop(manager);

        // 宽限期为 0：目录清空后立即可回收。
        std::fs::remove_file(temp.path().join(data_dir.as_path()).join("held")).unwrap();
        let manager = RootManager::open("node-a".into(), "session-a".into(), meta, disk.clone())
            .unwrap()
            .with_retirement_grace(0);
        let (removed, _pending) = manager.reclaim_deferred().unwrap();
        assert_eq!(removed, 1, "expired grace must allow reclaim");
        assert!(!temp.path().join(data_dir.as_path()).exists());
    }

    /// 宽限期过后若目录仍非空，保持标记并继续等待，不放弃。
    #[test]
    fn expired_retirement_still_waits_when_the_directory_is_busy() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let name = OsStr::new("agent1");
        let data_dir = root_data_dir(name, 1).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        std::fs::write(temp.path().join(data_dir.as_path()).join("held"), b"x").unwrap();

        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id: root_id_from_name(name).unwrap(),
                name: name.into(),
                epoch: 1,
                data_dir: data_dir.clone(),
                local_prepare_id: "prepare-1".into(),
            })
            .unwrap();
        locked.mark_retired(&data_dir, 0).unwrap();
        drop(locked);

        let meta = Arc::new(RecoveryMeta::default());
        let manager = RootManager::open("node-a".into(), "session-a".into(), meta, disk.clone())
            .unwrap()
            .with_retirement_grace(0);

        let (removed, pending) = manager.reclaim_deferred().unwrap();
        assert_eq!((removed, pending), (0, 1), "a busy directory stays pending");
        assert!(temp.path().join(data_dir.as_path()).exists());
    }

    /// 弃用标记必须幂等且保留最早时刻：重复标记不得延长目录寿命。
    #[test]
    fn retirement_marks_keep_the_earliest_time() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let data_dir = root_data_dir(OsStr::new("agent1"), 1).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        disk.sync_root().unwrap();

        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        locked.mark_retired(&data_dir, 1_000).unwrap();
        locked.mark_retired(&data_dir, 9_000).unwrap();
        let marks = locked.scan_retired().unwrap();
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[0].1, 1_000, "the earlier time must win");
    }

    /// 回收范围必须保守：只删"有 catalog 证据且已不活跃"的目录。
    ///
    /// 同名换代后，旧代目录既不在 roots 缓存也不在 catalog。若回收靠"不在
    /// 集合内"推断，就会误删仍在被 NFS 客户端使用的旧代数据（spec §7.5）。
    #[test]
    fn reclaim_leaves_superseded_generation_alone() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());

        // 旧代：目录存在，但 catalog 无记录（已被新代覆盖）→ 必须不动。
        let old_dir = root_data_dir(OsStr::new("agent1"), 7).unwrap();
        disk.mkdir(&old_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        std::fs::write(temp.path().join(old_dir.as_path()).join("live"), b"x").unwrap();

        // 活跃代：catalog 有记录且 Meta 认作 active（epoch 1 与 fixture 的
        // recovery_grant 一致），因此会被加载进 roots 缓存。
        let live_dir = root_data_dir(OsStr::new("agent2"), 1).unwrap();
        disk.mkdir(&live_dir, 0o700).unwrap();
        disk.sync_root().unwrap();
        let live_id = root_id_from_name(OsStr::new("agent2")).unwrap();
        let locked = LocalFsRootCatalog::new(disk.clone())
            .lock_and_open()
            .unwrap();
        locked
            .persist_prepared_root(&LocalRootRecord {
                id: live_id.clone(),
                name: OsStr::new("agent2").into(),
                epoch: 1,
                data_dir: live_dir.clone(),
                local_prepare_id: "prepare-1".into(),
            })
            .unwrap();
        drop(locked);

        let meta = Arc::new(RecoveryMeta::default());
        meta.0.lock().unwrap().active = Some(RootLocation {
            id: live_id,
            epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
        });
        meta.0.lock().unwrap().active_prepare_id = Some("prepare-1".into());
        let manager =
            RootManager::open("node-a".into(), "session-a".into(), meta, disk.clone()).unwrap();

        let (removed, pending) = manager.reclaim_deferred().unwrap();
        assert_eq!((removed, pending), (0, 0), "nothing is reclaimable here");
        assert!(
            temp.path().join(old_dir.as_path()).exists(),
            "a superseded generation must not be deleted by inference"
        );
        assert!(
            temp.path().join(live_dir.as_path()).exists(),
            "the active generation must never be reclaimed"
        );
    }
}

#[cfg(test)]
#[path = "root/creation_tests.rs"]
mod creation_tests;
