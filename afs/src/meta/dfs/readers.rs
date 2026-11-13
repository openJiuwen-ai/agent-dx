//! Round 2 C-1: in-memory reader registrations ("soft readers").
//!
//! An open pins the inode and the version it reads without a log record:
//! Meta inserts the entry first and only then takes the view it validates the
//! version against (L1), keeping the entry only when that version is the
//! inode's head in that view and the inode is still linked (L7). Everything
//! else (a version that is no longer the head, an unlinked inode) is refused
//! with `META_DFS_READER_NOT_HEAD` and the Node pins it durably instead.
//!
//! The table lives only in this Meta process. GC plans read it as live
//! references; a GC commit rechecks it (`ReaderGuard`) for entries registered
//! after the plan read it on any inode of the whole plan. After a restart the
//! table is empty: until every session that may still read has reported its
//! open handles to this writer term, GC deletes no inode and no version
//! (the restart gate). Design: `round2-meta-protocol-design.md` §3.3–§3.4.
use super::*;
use crate::dfs::{FileVersionId, GcReference};
use crate::meta::store::ReaderProbe;
use std::collections::{BTreeMap, BTreeSet};

/// `(node, session)`.
type SessionKey = (String, String);

/// One reader the Node reports: its sequence number, inode and version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReaderEntry {
    pub reader: u64,
    pub inode_id: InodeId,
    pub version_id: Option<FileVersionId>,
}

#[derive(Clone, Debug)]
struct Entry {
    inode_id: InodeId,
    version_id: Option<FileVersionId>,
    seq: u64,
}

#[derive(Debug, Default)]
struct SessionState {
    /// Reported its open handles to this Meta process's writer term, or is
    /// a Node without DFS (it has no readers).
    reported: bool,
    /// When GC found the session departed: its entries protect until this
    /// plus the grace, as a durable reference retired then would.
    retired_at_unix_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct Inner {
    seq: u64,
    entries: BTreeMap<(String, String, u64), Entry>,
    /// The inode of every live entry by its insertion sequence, for the GC
    /// recheck of entries registered after a plan.
    by_seq: BTreeMap<u64, InodeId>,
    sessions: BTreeMap<SessionKey, SessionState>,
}

impl Inner {
    fn remove(&mut self, key: &(String, String, u64)) -> Option<Entry> {
        let entry = self.entries.remove(key)?;
        self.by_seq.remove(&entry.seq);
        Some(entry)
    }
    fn session(&mut self, node: &str, session: &str) -> &mut SessionState {
        self.sessions
            .entry((node.to_owned(), session.to_owned()))
            .or_default()
    }
}

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct SoftReaders {
    inner: std::sync::Mutex<Inner>,
    /// The restart gate (§3.3) is closed: an earlier Meta process may have
    /// held registrations this one never saw. Opens for good once every
    /// session that may still read has reported to this term or expired;
    /// a session registered later never had a reader anywhere else.
    gate_closed: std::sync::atomic::AtomicBool,
}

/// What a GC plan reads from the table.
pub(super) struct ReaderSnapshot {
    /// Every entry with a sequence up to this one is in `references`.
    pub since: u64,
    pub references: Vec<GcReference>,
    /// Sessions that reported to this writer term (or have no DFS).
    pub reported: BTreeSet<SessionKey>,
}

impl SoftReaders {
    /// `writer_term` of the store this Meta process writes. Term 1 is the
    /// first writer of the log: no earlier process held registrations.
    /// Stores without terms (0) are volatile: a restart loses every inode.
    pub(super) fn new(writer_term: u64) -> Self {
        Self {
            inner: std::sync::Mutex::default(),
            gate_closed: std::sync::atomic::AtomicBool::new(writer_term > 1),
        }
    }

    /// Whether GC must still keep every inode and version (§3.3 restart
    /// gate). `sessions` is every recorded session with its deadline; one
    /// may still read until `deadline + grace` and must have reported.
    pub(super) fn restart_gate_closed(
        &self,
        sessions: &BTreeMap<SessionKey, u64>,
        reported: &BTreeSet<SessionKey>,
        now: u64,
        grace: u64,
    ) -> bool {
        use std::sync::atomic::Ordering;
        if !self.gate_closed.load(Ordering::Acquire) {
            return false;
        }
        let waiting = sessions
            .iter()
            .any(|(key, deadline)| deadline.saturating_add(grace) > now && !reported.contains(key));
        if !waiting {
            self.gate_closed.store(false, Ordering::Release);
        }
        waiting
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // The table holds no invariant a panicking holder can break halfway:
        // every method leaves it consistent before it can panic.
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Step ② of a registration: insert (or keep) the entry, under the table
    /// lock, and return its sequence. An existing identical entry keeps its
    /// sequence; anything else gets a new one.
    pub(super) fn insert(
        &self,
        node: &str,
        session: &str,
        reader: u64,
        inode_id: &InodeId,
        version_id: Option<&FileVersionId>,
    ) -> u64 {
        let mut inner = self.lock();
        let key = (node.to_owned(), session.to_owned(), reader);
        if let Some(entry) = inner.entries.get(&key)
            && &entry.inode_id == inode_id
            && entry.version_id.as_ref() == version_id
        {
            return entry.seq;
        }
        inner.remove(&key);
        inner.seq += 1;
        let seq = inner.seq;
        inner.by_seq.insert(seq, inode_id.clone());
        inner.entries.insert(
            key,
            Entry {
                inode_id: inode_id.clone(),
                version_id: version_id.cloned(),
                seq,
            },
        );
        seq
    }

    /// Undoes `insert` after a failed validation, unless a later
    /// registration of the same reader replaced the entry.
    pub(super) fn remove_if(&self, node: &str, session: &str, reader: u64, seq: u64) {
        let mut inner = self.lock();
        let key = (node.to_owned(), session.to_owned(), reader);
        if inner
            .entries
            .get(&key)
            .is_some_and(|entry| entry.seq == seq)
        {
            inner.remove(&key);
        }
    }

    pub(super) fn release(&self, node: &str, session: &str, readers: &[u64]) {
        let mut inner = self.lock();
        for reader in readers {
            inner.remove(&(node.to_owned(), session.to_owned(), *reader));
        }
    }

    /// Replaces the session's entries up to `low_watermark` with `entries`
    /// and keeps the later ones (opens whose reply the Node had not processed
    /// when it built the report). Marks the session reported when `term` is
    /// this writer's term.
    pub(super) fn report(
        &self,
        node: &str,
        session: &str,
        low_watermark: u64,
        entries: &[ReaderEntry],
        current_term: bool,
    ) {
        let mut inner = self.lock();
        let stale = inner
            .entries
            .range(
                (node.to_owned(), session.to_owned(), 0)
                    ..=(node.to_owned(), session.to_owned(), low_watermark),
            )
            .filter(|((_, _, reader), _)| !entries.iter().any(|entry| entry.reader == *reader))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in stale {
            inner.remove(&key);
        }
        drop(inner);
        for entry in entries.iter().filter(|entry| entry.reader <= low_watermark) {
            self.insert(
                node,
                session,
                entry.reader,
                &entry.inode_id,
                entry.version_id.as_ref(),
            );
        }
        if current_term {
            self.lock().session(node, session).reported = true;
        }
    }

    /// A Node registered or renewed its session. One without DFS never
    /// reads DFS files, so it counts as reported.
    pub(crate) fn note_registration(&self, node: &str, session: &str, dfs: bool) {
        if !dfs {
            self.lock().session(node, session).reported = true;
        }
    }

    pub(super) fn is_reported(&self, node: &str, session: &str) -> bool {
        self.lock()
            .sessions
            .get(&(node.to_owned(), session.to_owned()))
            .is_some_and(|state| state.reported)
    }

    /// The table as GC plan input. `current` is every Node's current live
    /// session; any other session departed: it is retired once (at
    /// `max(deadline, now)`, the rule of `retire_departed_references`) and
    /// its entries and state are dropped once `retired + grace` has passed.
    pub(super) fn snapshot(
        &self,
        current: &BTreeSet<SessionKey>,
        deadlines: &BTreeMap<SessionKey, u64>,
        now: u64,
        grace: u64,
    ) -> ReaderSnapshot {
        let mut inner = self.lock();
        let known = inner
            .entries
            .keys()
            .map(|(node, session, _)| (node.clone(), session.clone()))
            .collect::<BTreeSet<_>>();
        for key in known {
            inner.sessions.entry(key).or_default();
        }
        let mut expired = Vec::new();
        for (key, state) in &mut inner.sessions {
            if current.contains(key) {
                state.retired_at_unix_ms = None;
                continue;
            }
            let retired = *state
                .retired_at_unix_ms
                .get_or_insert_with(|| deadlines.get(key).copied().unwrap_or(now).max(now));
            if retired.saturating_add(grace) <= now {
                expired.push(key.clone());
            }
        }
        for key in expired {
            inner.sessions.remove(&key);
            let gone = inner
                .entries
                .range((key.0.clone(), key.1.clone(), 0)..=(key.0.clone(), key.1.clone(), u64::MAX))
                .map(|(entry, _)| entry.clone())
                .collect::<Vec<_>>();
            for entry in gone {
                inner.remove(&entry);
            }
        }
        let references = inner
            .entries
            .iter()
            .map(|((node, session, reader), entry)| GcReference {
                node_id: node.clone(),
                session_id: session.clone(),
                reference_id: format!("reader:{reader}"),
                inode_id: Some(entry.inode_id.clone()),
                publication: false,
                chunks: Vec::new(),
                retired_at_unix_ms: inner
                    .sessions
                    .get(&(node.clone(), session.clone()))
                    .and_then(|state| state.retired_at_unix_ms),
                open_read: None,
                pinned_version: entry.version_id.clone(),
            })
            .collect();
        ReaderSnapshot {
            since: inner.seq,
            references,
            reported: inner
                .sessions
                .iter()
                .filter(|(_, state)| state.reported)
                .map(|(key, _)| key.clone())
                .collect(),
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.lock().entries.len()
    }
}

impl ReaderProbe for SoftReaders {
    fn registered_since(&self, since: u64, inodes: &BTreeSet<InodeId>) -> bool {
        self.lock()
            .by_seq
            .range(since.saturating_add(1)..)
            .any(|(_, inode)| inodes.contains(inode))
    }
}

/// E1 switches, compiled only with the `diagnostics` feature: a product
/// build has neither the code nor the variables.
#[cfg(feature = "diagnostics")]
mod diagnostics {
    pub(super) fn register_delay() -> Option<std::time::Duration> {
        std::env::var("AFS_DIAG_READER_REGISTER_DELAY_MS")
            .ok()?
            .parse()
            .ok()
            .map(std::time::Duration::from_millis)
    }
    pub(super) fn register_after_view() -> bool {
        std::env::var_os("AFS_DIAG_READER_REGISTER_AFTER_VIEW").is_some_and(|value| value == "1")
    }
}

/// Registrations an open retries while the head keeps moving.
pub(super) const OPEN_REGISTER_ATTEMPTS: usize = 4;

/// See `DfsService::open_file_read`.
#[derive(Clone, Debug)]
pub struct OpenFileRead {
    pub node_id: String,
    pub session_id: String,
    pub reader: u64,
    pub inode_id: InodeId,
    /// `(namespace, parent, name)` that must still name the inode.
    pub name: Option<(NamespaceId, InodeId, Vec<u8>)>,
}

/// See `DfsService::open_file_read`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenedForRead {
    /// `None`: the name no longer names the inode.
    pub inode: Option<InodeRecord>,
    /// The inode's head is registered as reader `reader`.
    pub registered: bool,
    pub meta_term: u64,
    /// Another session holds a live write lease on the inode.
    pub writer_elsewhere: bool,
}

impl OpenedForRead {
    fn renamed() -> Self {
        Self {
            inode: None,
            registered: false,
            meta_term: 0,
            writer_elsewhere: false,
        }
    }
}

/// Test hook (U1): runs between the insertion and the validation view.
#[cfg(test)]
pub(super) type BetweenInsertAndView =
    std::sync::Arc<dyn Fn() -> futures_util::future::BoxFuture<'static, ()> + Send + Sync>;

impl DfsService {
    /// Registers a reader of `version_id` (the head the Node saw) of
    /// `inode_id` without a log record and returns this writer's term.
    /// Order (design §3.3 ①–④): session checks, insert, then the
    /// validation view; the entry stays only if the version is the head
    /// there and the inode is linked.
    pub async fn retain_reader(
        &self,
        node: &str,
        session: &str,
        reader: u64,
        inode_id: &InodeId,
        version_id: Option<&FileVersionId>,
    ) -> Result<u64> {
        require_id(node, "node_id")?;
        require_id(session, "session_id")?;
        // ①: a session that has not reported to this writer term could hold
        // handles this table does not know; it reports first.
        if !self.readers.is_reported(node, session) {
            return Err(afs_error::Error::coded(
                afs_error::META_DFS_SESSION_NOT_REPORTED,
                "Node session has not reported its readers to this Meta",
            ));
        }
        // ② then ③ and ④
        let (seq, validated) = self
            .register_then_check(
                || {
                    self.readers
                        .insert(node, session, reader, inode_id, version_id)
                },
                || self.validate_reader(node, session, inode_id, version_id),
            )
            .await;
        if validated.is_err() {
            self.readers.remove_if(node, session, reader, seq);
        }
        validated.map(|()| self.store.writer_term())
    }

    /// ② then ③: the insertion, then `check` on a view taken after it
    /// (L1). In a diagnostics build, `AFS_DIAG_READER_REGISTER_DELAY_MS`
    /// widens the gap between the two and `AFS_DIAG_READER_REGISTER_AFTER_VIEW=1`
    /// takes the view first: the unsafe order E1's control arm must catch.
    async fn register_then_check<T, F>(
        &self,
        insert: impl FnOnce() -> u64,
        check: impl FnOnce() -> F,
    ) -> (u64, Result<T>)
    where
        F: std::future::Future<Output = Result<T>>,
    {
        #[cfg(feature = "diagnostics")]
        if diagnostics::register_after_view() {
            let checked = check().await;
            if let Some(delay) = diagnostics::register_delay() {
                tokio::time::sleep(delay).await;
            }
            return (insert(), checked);
        }
        let seq = insert();
        #[cfg(feature = "diagnostics")]
        if let Some(delay) = diagnostics::register_delay() {
            tokio::time::sleep(delay).await;
        }
        #[cfg(test)]
        {
            let hook = self.reader_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook().await;
            }
        }
        (seq, check().await)
    }

    async fn validate_reader(
        &self,
        node: &str,
        session: &str,
        inode_id: &InodeId,
        version_id: Option<&FileVersionId>,
    ) -> Result<()> {
        let view = self.store.read_view().await?;
        let caller = view
            .lookup(&MetaKey::CurrentNodeSession {
                node_id: node.to_owned(),
            })?
            .entity;
        gc::live_caller(caller.as_slice(), node, session)?;
        let inode = match view.lookup(&MetaKey::DfsInode(inode_id.clone()))?.entity {
            Some(MetaEntity::DfsInode(inode)) => inode,
            _ => return Err(not_found("cannot register a reader of a missing inode")),
        };
        if inode.head_version.as_ref() != version_id || inode.attributes.nlink == 0 {
            return Err(afs_error::Error::coded(
                afs_error::META_DFS_READER_NOT_HEAD,
                "reader version is not the linked head; pin it durably",
            ));
        }
        Ok(())
    }

    /// B2 (`OpenFile` for reading): resolves the inode (by id, or by
    /// `name` when the Node must check that a recently changed name still
    /// names it, round 3 A), registers reader `reader` of its head and
    /// reports whether another session holds a live write lease on it
    /// (D-R3-2 ③), in one call and without a log record. The order is the
    /// one of `retain_reader`: insert first, then the validation view, in
    /// which the head must be unchanged and the inode linked. A head that
    /// moved is registered again (bounded); an unlinked inode is returned
    /// unregistered and the Node pins it durably.
    pub async fn open_file_read(&self, request: OpenFileRead) -> Result<OpenedForRead> {
        require_id(&request.node_id, "node_id")?;
        require_id(&request.session_id, "session_id")?;
        require_id(&request.inode_id.0, "inode_id")?;
        if !self
            .readers
            .is_reported(&request.node_id, &request.session_id)
        {
            return Err(afs_error::Error::coded(
                afs_error::META_DFS_SESSION_NOT_REPORTED,
                "Node session has not reported its readers to this Meta",
            ));
        }
        let mut head = match self.resolve_for_open(&self.store.read_view().await?, &request)? {
            Some(inode) => inode.head_version,
            None => return Ok(OpenedForRead::renamed()),
        };
        for _ in 0..OPEN_REGISTER_ATTEMPTS {
            // ② then ③ and ④, the name check included
            let insert = || {
                self.readers.insert(
                    &request.node_id,
                    &request.session_id,
                    request.reader,
                    &request.inode_id,
                    head.as_ref(),
                )
            };
            let check = || async {
                let view = self.store.read_view().await?;
                let caller = view
                    .lookup(&MetaKey::CurrentNodeSession {
                        node_id: request.node_id.clone(),
                    })?
                    .entity;
                gc::live_caller(caller.as_slice(), &request.node_id, &request.session_id)?;
                let inode = self.resolve_for_open(&view, &request)?;
                let writer_elsewhere = match view
                    .lookup(&MetaKey::DfsWriteLease(request.inode_id.clone()))?
                    .entity
                {
                    Some(MetaEntity::DfsWriteLease(lease)) => {
                        lease.expires_at_unix_ms > now_unix_ms()
                            && (lease.owner_node_id != request.node_id
                                || lease.owner_session_id != request.session_id)
                    }
                    _ => false,
                };
                Ok::<_, afs_error::Error>((inode, writer_elsewhere))
            };
            let (seq, checked) = self.register_then_check(insert, check).await;
            let (inode, writer_elsewhere) = match checked {
                Ok(checked) => checked,
                Err(error) => {
                    self.readers.remove_if(
                        &request.node_id,
                        &request.session_id,
                        request.reader,
                        seq,
                    );
                    return Err(error);
                }
            };
            let Some(inode) = inode else {
                self.readers
                    .remove_if(&request.node_id, &request.session_id, request.reader, seq);
                return Ok(OpenedForRead::renamed());
            };
            if inode.head_version == head && inode.attributes.nlink > 0 {
                return Ok(OpenedForRead {
                    inode: Some(inode),
                    registered: true,
                    meta_term: self.store.writer_term(),
                    writer_elsewhere,
                });
            }
            self.readers
                .remove_if(&request.node_id, &request.session_id, request.reader, seq);
            if inode.attributes.nlink == 0 {
                return Ok(OpenedForRead {
                    inode: Some(inode),
                    registered: false,
                    meta_term: self.store.writer_term(),
                    writer_elsewhere,
                });
            }
            head = inode.head_version;
        }
        // The head kept moving: answer without a registration; the Node
        // registers (or pins durably) the head it ends up opening.
        let inode = self.resolve_for_open(&self.store.read_view().await?, &request)?;
        Ok(OpenedForRead {
            registered: false,
            meta_term: self.store.writer_term(),
            writer_elsewhere: false,
            inode,
        })
    }

    /// The inode an open names in `view`: `None` when `name` is given and no
    /// longer names it.
    fn resolve_for_open(
        &self,
        view: &crate::meta::store::MetaReadView,
        request: &OpenFileRead,
    ) -> Result<Option<InodeRecord>> {
        if let Some((namespace_id, parent, name)) = &request.name {
            let dentry = view
                .lookup(&MetaKey::DfsDentry(crate::dfs::DentryKey {
                    namespace_id: namespace_id.clone(),
                    parent_inode_id: parent.clone(),
                    name: name.clone(),
                }))?
                .entity;
            if !matches!(dentry, Some(MetaEntity::DfsDentry(ref dentry)) if dentry.inode_id == request.inode_id)
            {
                return Ok(None);
            }
        }
        match view
            .lookup(&MetaKey::DfsInode(request.inode_id.clone()))?
            .entity
        {
            Some(MetaEntity::DfsInode(inode)) => Ok(Some(inode)),
            None if request.inode_id.0 == "1" => Ok(Some(root_inode(NamespaceId::new("default")))),
            _ => Err(afs_error::Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "DFS inode was not found",
            )),
        }
    }

    /// Drops closed readers; no log record. A lost call only protects longer:
    /// the next report replaces it.
    pub fn release_readers(&self, node: &str, session: &str, readers: &[u64]) -> Result<()> {
        require_id(node, "node_id")?;
        require_id(session, "session_id")?;
        self.readers.release(node, session, readers);
        Ok(())
    }

    /// The Node's full list of readers up to `low_watermark`, made while it
    /// believed `term` is the writer term. Returns this writer's term; the
    /// session counts as reported only when the two match.
    pub async fn report_readers(
        &self,
        node: &str,
        session: &str,
        term: u64,
        low_watermark: u64,
        entries: Vec<ReaderEntry>,
    ) -> Result<u64> {
        require_id(node, "node_id")?;
        require_id(session, "session_id")?;
        let view = self.store.read_view().await?;
        let caller = view
            .lookup(&MetaKey::CurrentNodeSession {
                node_id: node.to_owned(),
            })?
            .entity;
        gc::live_caller(caller.as_slice(), node, session)?;
        let current = self.store.writer_term();
        self.readers
            .report(node, session, low_watermark, &entries, term == current);
        Ok(current)
    }

    /// See `SoftReaders::note_registration`.
    pub fn note_node_registration(&self, node: &str, session: &str, dfs: bool) {
        self.readers.note_registration(node, session, dfs);
    }

    #[cfg(test)]
    pub(super) fn set_reader_hook(&self, hook: Option<BetweenInsertAndView>) {
        *self.reader_hook.lock().unwrap() = hook;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(reader: u64, inode: &str, version: &str) -> ReaderEntry {
        ReaderEntry {
            reader,
            inode_id: InodeId::new(inode),
            version_id: Some(FileVersionId::new(version)),
        }
    }

    /// U3: a report replaces only readers up to its low watermark; a reader
    /// whose open reply the Node had not handled yet (above it) survives.
    #[test]
    fn a_report_replaces_only_readers_up_to_its_low_watermark() {
        let table = SoftReaders::new(1);
        for reader in [1, 2, 5] {
            table.insert("n", "s", reader, &InodeId::new("i"), None);
        }
        table.report("n", "s", 3, &[entry(1, "i", "v")], true);
        let readers = table
            .snapshot(&[("n".into(), "s".into())].into(), &BTreeMap::new(), 0, 0)
            .references
            .into_iter()
            .map(|reference| reference.reference_id)
            .collect::<Vec<_>>();
        assert_eq!(readers, ["reader:1", "reader:5"]);
        assert!(table.is_reported("n", "s"));
        // A report made under another term changes entries but not the gate.
        table.report("n", "t", 0, &[], false);
        assert!(!table.is_reported("n", "t"));
    }

    /// The recheck sees exactly the entries registered after the plan, on
    /// the plan's inodes; a replaced entry counts as new, an identical
    /// re-registration (a report) does not.
    #[test]
    fn the_recheck_sees_registrations_after_the_plan_on_its_inodes() {
        let table = SoftReaders::new(1);
        table.insert("n", "s", 1, &InodeId::new("a"), None);
        let since = table
            .snapshot(&[("n".into(), "s".into())].into(), &BTreeMap::new(), 0, 0)
            .since;
        let planned = BTreeSet::from([InodeId::new("a")]);
        assert!(!table.registered_since(since, &planned));
        table.insert("n", "s", 1, &InodeId::new("a"), None);
        assert!(
            !table.registered_since(since, &planned),
            "same entry, same seq"
        );
        table.insert("n", "s", 2, &InodeId::new("b"), None);
        assert!(!table.registered_since(since, &planned), "another inode");
        let seq = table.insert("n", "s", 3, &InodeId::new("a"), None);
        assert!(table.registered_since(since, &planned));
        table.remove_if("n", "s", 3, seq);
        assert!(!table.registered_since(since, &planned));
    }

    /// U6: a departed session's readers keep protecting, retired at
    /// `max(deadline, now)`, until that plus the grace; then they are gone.
    #[test]
    fn a_departed_session_keeps_its_readers_until_deadline_plus_grace() {
        let table = SoftReaders::new(1);
        table.insert("n", "old", 1, &InodeId::new("i"), None);
        let deadlines = BTreeMap::from([(("n".to_owned(), "old".to_owned()), 1_000)]);
        let none = BTreeSet::new();
        let at = |now| table.snapshot(&none, &deadlines, now, 100);
        let first = at(500);
        assert_eq!(first.references.len(), 1);
        assert_eq!(first.references[0].retired_at_unix_ms, Some(1_000));
        assert_eq!(at(1_099).references.len(), 1, "inside deadline + grace");
        assert!(
            at(1_100).references.is_empty(),
            "gone after deadline + grace"
        );
        assert_eq!(table.len(), 0);
    }

    /// U5 (gate rule): closed after a restart until every session that may
    /// still read reported or passed its deadline plus grace; then open for
    /// good. The first writer of a log never closes it.
    #[test]
    fn the_restart_gate_waits_for_every_session_that_may_still_read() {
        let sessions = BTreeMap::from([
            (("a".to_owned(), "1".to_owned()), 10_000),
            (("b".to_owned(), "1".to_owned()), 1_000),
        ]);
        assert!(!SoftReaders::new(1).restart_gate_closed(&sessions, &BTreeSet::new(), 0, 0));
        let table = SoftReaders::new(2);
        let a = BTreeSet::from([("a".to_owned(), "1".to_owned())]);
        assert!(table.restart_gate_closed(&sessions, &BTreeSet::new(), 500, 100));
        assert!(
            table.restart_gate_closed(&sessions, &a, 950, 100),
            "b may read until 1100"
        );
        assert!(!table.restart_gate_closed(&sessions, &a, 1_100, 100));
        assert!(
            !table.restart_gate_closed(&sessions, &BTreeSet::new(), 1_100, 100),
            "stays open"
        );
    }
}
