//! Open-time name check for positive kernel entry TTLs (performance batch 1, A).
//!
//! FUSE OPEN carries only a node id. With a positive entry TTL the kernel may
//! open through a cached dentry whose name another Node has since unlinked,
//! recreated or renamed over. This table remembers, per inode, the
//! (parent, name) pairs this mount handed to the kernel within the last two
//! entry TTLs: once a dentry is older than one TTL the kernel revalidates it
//! with a LOOKUP before use, which records the pair again. An open whose inode
//! was reached through exactly one remembered name asks Meta for that name
//! instead of the inode alone (one RPC either way) and answers `ESTALE` when
//! the name moved; Linux then repeats the path walk once with
//! `LOOKUP_REVAL`. A dentry looked up within the last `FRESH_LOOKUP` is not
//! compared: the kernel holds what Meta answered a moment ago, and a change
//! between that LOOKUP and the OPEN is the same race as with no kernel cache
//! at all; this also covers the kernel's retry, whose LOOKUP is fresh. In
//! addition, each `ESTALE` answered grants exactly one unchecked open
//! to the retry (several concurrent openers each get their own), and an
//! unused grant expires after one TTL, so the check itself never hands
//! `ESTALE` to an application and no name bypasses it for long. Inodes with no record, an evicted record or several names
//! (hard links) keep the inode-only check.

use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::dfs::InodeId;

type NameKey = (InodeId, Vec<u8>);

/// What an open of an inode must check.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum NameCheck {
    /// Ask Meta whether `name` in `parent` still names the inode.
    Verify { parent: InodeId, name: Vec<u8> },
    /// Keep the inode-only check.
    Fallback(FallbackReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FallbackReason {
    /// Kernel caching is off (TTL 0): today's behavior, not counted.
    Disabled,
    NoRecord,
    Evicted,
    MultipleNames,
    /// The kernel's retry after this mount answered `ESTALE`.
    AfterRevalidation,
    /// The name was looked up within `FRESH_LOOKUP`.
    FreshLookup,
}

impl FallbackReason {
    fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NoRecord => "no_record",
            Self::Evicted => "evicted",
            Self::MultipleNames => "multiple_names",
            Self::AfterRevalidation => "after_revalidation",
            Self::FreshLookup => "fresh_lookup",
        }
    }
}

/// Counters exported as `afs_dfs_open_name_check_total{result}`,
/// `afs_dfs_open_name_records` and `afs_dfs_open_name_evictions_total`.
pub(super) struct NameCheckMetrics {
    results: afs_metrics::IntCounterVec,
    records: afs_metrics::IntGauge,
    evictions: afs_metrics::IntCounter,
}

impl NameCheckMetrics {
    pub(super) fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        let results = afs_metrics::IntCounterVec::new(
            afs_metrics::Opts::new(
                "afs_dfs_open_name_check_total",
                "DFS opens by name-check outcome: verified, stale (answered ESTALE) or the \
                 reason the inode-only check was kept.",
            ),
            &["result"],
        )?;
        let records = afs_metrics::IntGauge::new(
            "afs_dfs_open_name_records",
            "Names this DFS mount handed to the kernel within the last two entry TTLs.",
        )?;
        let evictions = afs_metrics::IntCounter::new(
            "afs_dfs_open_name_evictions_total",
            "Name records evicted before expiry because the table was full.",
        )?;
        afs_metrics::register_collector(registry, &results)?;
        afs_metrics::register_collector(registry, &records)?;
        afs_metrics::register_collector(registry, &evictions)?;
        Ok(Self {
            results,
            records,
            evictions,
        })
    }
}

/// One unchecked open per `ESTALE` answered, usable until `until`.
#[derive(Clone, Copy, Debug, Default)]
struct Grants {
    count: u32,
    until: Option<Instant>,
}

impl Grants {
    fn live(&self, now: Instant) -> u32 {
        if self.until.is_some_and(|until| until > now) {
            self.count
        } else {
            0
        }
    }

    fn add(&mut self, other: Grants, now: Instant) {
        let count = self.live(now).saturating_add(other.live(now));
        let until = self.until.max(other.until);
        *self = Grants { count, until };
    }

    /// Uses one grant; true if there was one.
    fn take(&mut self, now: Instant) -> bool {
        if self.live(now) == 0 {
            return false;
        }
        self.count -= 1;
        true
    }

    fn one(until: Instant) -> Self {
        Self {
            count: 1,
            until: Some(until),
        }
    }
}

struct NameRecord {
    parent: InodeId,
    name: Vec<u8>,
    at: Instant,
    /// Unchecked opens granted to retries: the kernel's LOOKUP of a name this
    /// mount answered `ESTALE` for moves that name's grants here.
    after_revalidation: Grants,
}

#[derive(Default)]
struct Inner {
    by_inode: HashMap<InodeId, Vec<NameRecord>>,
    by_name: HashMap<NameKey, InodeId>,
    /// Insertion order for expiry and eviction; stale entries are skipped.
    order: VecDeque<(Instant, InodeId, NameKey)>,
    /// Names answered `ESTALE`: their next LOOKUP moves the grants to its
    /// record.
    revalidating_names: HashMap<NameKey, Grants>,
    /// Inodes answered `ESTALE`: a retry may reach the same inode without a
    /// LOOKUP (e.g. `/proc/self/fd`).
    revalidating_inodes: HashMap<InodeId, Grants>,
    evicted: HashMap<InodeId, Instant>,
}

pub(super) struct RecentNames {
    /// Kernel entry TTL; zero disables the table.
    entry_ttl: Duration,
    fresh_lookup: Duration,
    capacity: usize,
    inner: Mutex<Inner>,
    metrics: Option<NameCheckMetrics>,
}

/// A record younger than this is not compared (see the module comment).
pub(super) const FRESH_LOOKUP: Duration = Duration::from_millis(10);

/// Inodes remembered at most; the oldest are evicted beyond this.
pub(super) const DEFAULT_NAME_RECORD_CAPACITY: usize = 262_144;

impl Default for RecentNames {
    fn default() -> Self {
        Self::new(Duration::ZERO, DEFAULT_NAME_RECORD_CAPACITY)
    }
}

impl RecentNames {
    pub(super) fn new(entry_ttl: Duration, capacity: usize) -> Self {
        Self {
            entry_ttl,
            fresh_lookup: FRESH_LOOKUP,
            capacity: capacity.max(1),
            inner: Mutex::new(Inner::default()),
            metrics: None,
        }
    }

    #[cfg(test)]
    fn with_fresh_lookup(mut self, fresh_lookup: Duration) -> Self {
        self.fresh_lookup = fresh_lookup;
        self
    }

    pub(super) fn with_metrics(mut self, metrics: NameCheckMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn enabled(&self) -> bool {
        !self.entry_ttl.is_zero()
    }

    fn lifetime(&self) -> Duration {
        self.entry_ttl.saturating_mul(2)
    }

    /// Poisoning only weakens the check back to the inode-only one.
    fn lock(&self) -> Option<std::sync::MutexGuard<'_, Inner>> {
        self.inner.lock().ok()
    }

    /// The kernel now holds a dentry `name` in `parent` for `inode`.
    pub(super) fn record(&self, inode: &InodeId, parent: &InodeId, name: &[u8]) {
        if !self.enabled() {
            return;
        }
        let Some(mut inner) = self.lock() else {
            return;
        };
        let now = Instant::now();
        self.expire(&mut inner, now);
        let key = (parent.clone(), name.to_vec());
        // Grants belong to the name: the kernel's retry may LOOKUP it twice
        // (revalidate, then a fresh dentry) and see a newer inode each time.
        let mut after_revalidation = inner.revalidating_names.remove(&key).unwrap_or_default();
        if let Some(previous) = inner.by_name.get(&key).cloned()
            && let Some(grants) = Self::take_name(&mut inner, &previous, &key)
        {
            after_revalidation.add(grants, now);
        }
        let records = inner.by_inode.entry(inode.clone()).or_default();
        records.retain(|record| record.parent != key.0 || record.name != key.1);
        records.push(NameRecord {
            parent: key.0.clone(),
            name: key.1.clone(),
            at: now,
            after_revalidation,
        });
        inner.by_name.insert(key.clone(), inode.clone());
        inner.evicted.remove(inode);
        inner.order.push_back((now, inode.clone(), key));
        while inner.by_inode.len() > self.capacity {
            let Some((_, oldest, key)) = inner.order.pop_front() else {
                break;
            };
            if Self::remove_name(&mut inner, &oldest, &key) {
                if !inner.by_inode.contains_key(&oldest) {
                    inner.evicted.insert(oldest, now + self.lifetime());
                }
                if let Some(metrics) = &self.metrics {
                    metrics.evictions.inc();
                }
            }
        }
        self.observe(&inner);
    }

    /// The kernel dropped its dentry `name` in `parent` (unlink, rmdir, the
    /// source and the replaced target of a rename).
    pub(super) fn forget(&self, parent: &InodeId, name: &[u8]) {
        if !self.enabled() {
            return;
        }
        let Some(mut inner) = self.lock() else {
            return;
        };
        let key = (parent.clone(), name.to_vec());
        if let Some(inode) = inner.by_name.get(&key).cloned() {
            Self::remove_name(&mut inner, &inode, &key);
        }
        self.observe(&inner);
    }

    /// A rename through this mount: the kernel moved its dentry from the old
    /// name to the new one (no LOOKUP) and dropped the replaced target's.
    pub(super) fn rename(
        &self,
        old_parent: &InodeId,
        old_name: &[u8],
        new_parent: &InodeId,
        new_name: &[u8],
    ) {
        if !self.enabled() {
            return;
        }
        let moved = self.lock().and_then(|inner| {
            inner
                .by_name
                .get(&(old_parent.clone(), old_name.to_vec()))
                .cloned()
        });
        self.forget(old_parent, old_name);
        self.forget(new_parent, new_name);
        if let Some(inode) = moved {
            self.record(&inode, new_parent, new_name);
        }
    }

    /// Decide the check for an open of `inode`; consumes one-shot marks.
    pub(super) fn check(&self, inode: &InodeId) -> NameCheck {
        if !self.enabled() {
            return NameCheck::Fallback(FallbackReason::Disabled);
        }
        let Some(mut inner) = self.lock() else {
            return self.count(NameCheck::Fallback(FallbackReason::NoRecord));
        };
        let now = Instant::now();
        self.expire(&mut inner, now);
        if inner
            .revalidating_inodes
            .get_mut(inode)
            .is_some_and(|grants| grants.take(now))
        {
            return self.count(NameCheck::Fallback(FallbackReason::AfterRevalidation));
        }
        let evicted = inner.evicted.get(inode).is_some_and(|until| *until > now);
        let check = match inner.by_inode.get_mut(inode) {
            None if evicted => NameCheck::Fallback(FallbackReason::Evicted),
            None => NameCheck::Fallback(FallbackReason::NoRecord),
            Some(records) if records.len() > 1 => {
                NameCheck::Fallback(FallbackReason::MultipleNames)
            }
            Some(records) => match records.first_mut() {
                None if evicted => NameCheck::Fallback(FallbackReason::Evicted),
                None => NameCheck::Fallback(FallbackReason::NoRecord),
                Some(record) => {
                    if now.saturating_duration_since(record.at) < self.fresh_lookup {
                        // A retry's grant is used up by this unchecked open.
                        record.after_revalidation.take(now);
                        NameCheck::Fallback(FallbackReason::FreshLookup)
                    } else if record.after_revalidation.take(now) {
                        NameCheck::Fallback(FallbackReason::AfterRevalidation)
                    } else {
                        NameCheck::Verify {
                            parent: record.parent.clone(),
                            name: record.name.clone(),
                        }
                    }
                }
            },
        };
        self.count(check)
    }

    /// Meta says `name` in `parent` no longer names `inode`: this open answers
    /// `ESTALE`, and the kernel's single retry is not checked again.
    pub(super) fn mark_stale(&self, inode: &InodeId, parent: &InodeId, name: &[u8]) {
        if let Some(metrics) = &self.metrics {
            metrics.results.with_label_values(&["stale"]).inc();
        }
        let Some(mut inner) = self.lock() else {
            return;
        };
        let now = Instant::now();
        let grant = Grants::one(now + self.entry_ttl);
        let key = (parent.clone(), name.to_vec());
        Self::remove_name(&mut inner, inode, &key);
        inner
            .revalidating_names
            .entry(key)
            .or_default()
            .add(grant, now);
        inner
            .revalidating_inodes
            .entry(inode.clone())
            .or_default()
            .add(grant, now);
        self.observe(&inner);
    }

    fn count(&self, check: NameCheck) -> NameCheck {
        if let Some(metrics) = &self.metrics {
            let label = match &check {
                NameCheck::Verify { .. } => "verified",
                NameCheck::Fallback(reason) => reason.label(),
            };
            metrics.results.with_label_values(&[label]).inc();
        }
        check
    }

    fn observe(&self, inner: &Inner) {
        if let Some(metrics) = &self.metrics {
            metrics
                .records
                .set(i64::try_from(inner.by_name.len()).unwrap_or(i64::MAX));
        }
    }

    /// Removes one name of `inode` and returns its unused grants.
    fn take_name(inner: &mut Inner, inode: &InodeId, key: &NameKey) -> Option<Grants> {
        let grants = inner.by_inode.get(inode).and_then(|records| {
            records
                .iter()
                .find(|record| record.parent == key.0 && record.name == key.1)
                .map(|record| record.after_revalidation)
        });
        Self::remove_name(inner, inode, key);
        grants
    }

    /// Removes one name of `inode`; true if it was present.
    fn remove_name(inner: &mut Inner, inode: &InodeId, key: &NameKey) -> bool {
        let mut removed = false;
        if let Some(records) = inner.by_inode.get_mut(inode) {
            let before = records.len();
            records.retain(|record| record.parent != key.0 || record.name != key.1);
            removed = records.len() != before;
            if records.is_empty() {
                inner.by_inode.remove(inode);
            }
        }
        if inner.by_name.get(key) == Some(inode) {
            inner.by_name.remove(key);
        }
        removed
    }

    fn expire(&self, inner: &mut Inner, now: Instant) {
        let lifetime = self.lifetime();
        while let Some((at, inode, key)) = inner.order.front().cloned() {
            if at + lifetime > now {
                break;
            }
            inner.order.pop_front();
            // A newer record of the same name keeps its own queue entry.
            let current = inner.by_inode.get(&inode).is_some_and(|records| {
                records
                    .iter()
                    .any(|record| record.parent == key.0 && record.name == key.1 && record.at == at)
            });
            if current {
                Self::remove_name(inner, &inode, &key);
            }
        }
        inner
            .revalidating_names
            .retain(|_, grants| grants.live(now) > 0);
        inner
            .revalidating_inodes
            .retain(|_, grants| grants.live(now) > 0);
        inner.evicted.retain(|_, until| *until > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> InodeId {
        InodeId::new(value)
    }

    fn verify(parent: &str, name: &str) -> NameCheck {
        NameCheck::Verify {
            parent: id(parent),
            name: name.as_bytes().to_vec(),
        }
    }

    #[test]
    fn zero_ttl_keeps_todays_inode_only_check() {
        let names = RecentNames::new(Duration::ZERO, 8).with_fresh_lookup(Duration::ZERO);
        names.record(&id("x"), &id("d"), b"f");
        assert_eq!(
            names.check(&id("x")),
            NameCheck::Fallback(FallbackReason::Disabled)
        );
    }

    #[test]
    fn one_name_is_verified_and_several_names_fall_back() {
        let names = RecentNames::new(Duration::from_secs(1), 8).with_fresh_lookup(Duration::ZERO);
        assert_eq!(
            names.check(&id("x")),
            NameCheck::Fallback(FallbackReason::NoRecord)
        );
        names.record(&id("x"), &id("d"), b"f");
        assert_eq!(names.check(&id("x")), verify("d", "f"));
        assert_eq!(names.check(&id("x")), verify("d", "f"), "not one-shot");
        names.record(&id("x"), &id("e"), b"hard-link");
        assert_eq!(
            names.check(&id("x")),
            NameCheck::Fallback(FallbackReason::MultipleNames)
        );
        names.forget(&id("e"), b"hard-link");
        assert_eq!(names.check(&id("x")), verify("d", "f"));
    }

    #[test]
    fn a_name_moves_to_the_inode_the_kernel_now_holds() {
        let names = RecentNames::new(Duration::from_secs(1), 8).with_fresh_lookup(Duration::ZERO);
        names.record(&id("old"), &id("d"), b"f");
        names.record(&id("new"), &id("d"), b"f");
        assert_eq!(
            names.check(&id("old")),
            NameCheck::Fallback(FallbackReason::NoRecord)
        );
        assert_eq!(names.check(&id("new")), verify("d", "f"));
    }

    #[test]
    fn revalidation_marks_are_one_shot_and_expire_after_one_ttl() {
        let ttl = Duration::from_millis(200);
        let names = RecentNames::new(ttl, 8).with_fresh_lookup(Duration::ZERO);
        names.record(&id("old"), &id("d"), b"f");
        assert_eq!(names.check(&id("old")), verify("d", "f"));
        names.mark_stale(&id("old"), &id("d"), b"f");
        // The kernel's retry: LOOKUP records the new inode, then OPEN.
        names.record(&id("new"), &id("d"), b"f");
        assert_eq!(
            names.check(&id("new")),
            NameCheck::Fallback(FallbackReason::AfterRevalidation)
        );
        assert_eq!(names.check(&id("new")), verify("d", "f"), "consumed");
        // A retry that reaches the stale inode itself is not checked again,
        // once.
        assert_eq!(
            names.check(&id("old")),
            NameCheck::Fallback(FallbackReason::AfterRevalidation)
        );
        assert_eq!(
            names.check(&id("old")),
            NameCheck::Fallback(FallbackReason::NoRecord)
        );
        // An unconsumed mark expires after one entry TTL.
        names.mark_stale(&id("new"), &id("d"), b"f");
        std::thread::sleep(ttl + Duration::from_millis(50));
        names.record(&id("newer"), &id("d"), b"f");
        assert_eq!(names.check(&id("newer")), verify("d", "f"));
        assert_eq!(
            names.check(&id("new")),
            NameCheck::Fallback(FallbackReason::NoRecord)
        );
    }

    /// Concurrent openers of one name each get their own ESTALE and their
    /// own retry; one retry's LOOKUP must not consume the other's grant.
    #[test]
    fn each_estale_grants_exactly_one_unchecked_retry() {
        let names = RecentNames::new(Duration::from_secs(1), 8).with_fresh_lookup(Duration::ZERO);
        names.record(&id("old"), &id("d"), b"f");
        names.mark_stale(&id("old"), &id("d"), b"f");
        names.mark_stale(&id("old"), &id("d"), b"f");
        names.record(&id("new"), &id("d"), b"f");
        names.record(&id("new"), &id("d"), b"f");
        for _ in 0..2 {
            assert_eq!(
                names.check(&id("new")),
                NameCheck::Fallback(FallbackReason::AfterRevalidation)
            );
        }
        assert_eq!(names.check(&id("new")), verify("d", "f"));
    }

    /// The retry LOOKUPs the name twice and another Node replaced it in
    /// between: the grant follows the name to the newest inode.
    #[test]
    fn a_grant_follows_its_name_across_a_double_lookup() {
        let names = RecentNames::new(Duration::from_secs(1), 8).with_fresh_lookup(Duration::ZERO);
        names.record(&id("old"), &id("d"), b"f");
        names.mark_stale(&id("old"), &id("d"), b"f");
        names.record(&id("y1"), &id("d"), b"f");
        names.record(&id("y2"), &id("d"), b"f");
        assert_eq!(
            names.check(&id("y2")),
            NameCheck::Fallback(FallbackReason::AfterRevalidation)
        );
        assert_eq!(names.check(&id("y2")), verify("d", "f"));
    }

    #[test]
    fn a_name_looked_up_a_moment_ago_is_not_compared() {
        let names = RecentNames::new(Duration::from_secs(1), 8)
            .with_fresh_lookup(Duration::from_millis(50));
        names.record(&id("x"), &id("d"), b"f");
        assert_eq!(
            names.check(&id("x")),
            NameCheck::Fallback(FallbackReason::FreshLookup)
        );
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(names.check(&id("x")), verify("d", "f"));
    }

    #[test]
    fn records_expire_after_two_ttls() {
        let ttl = Duration::from_millis(100);
        let names = RecentNames::new(ttl, 8).with_fresh_lookup(Duration::ZERO);
        names.record(&id("x"), &id("d"), b"f");
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(names.check(&id("x")), verify("d", "f"));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            names.check(&id("x")),
            NameCheck::Fallback(FallbackReason::NoRecord)
        );
    }

    #[test]
    fn a_local_rename_moves_the_record_and_drops_the_replaced_target() {
        let names = RecentNames::new(Duration::from_secs(1), 8).with_fresh_lookup(Duration::ZERO);
        names.record(&id("tmp"), &id("d"), b"f.tmp");
        names.record(&id("old"), &id("d"), b"f");
        names.rename(&id("d"), b"f.tmp", &id("d"), b"f");
        assert_eq!(names.check(&id("tmp")), verify("d", "f"));
        assert_eq!(
            names.check(&id("old")),
            NameCheck::Fallback(FallbackReason::NoRecord)
        );
    }

    #[test]
    fn a_full_table_evicts_the_oldest_and_reports_it() {
        let names = RecentNames::new(Duration::from_secs(1), 2).with_fresh_lookup(Duration::ZERO);
        names.record(&id("a"), &id("d"), b"a");
        names.record(&id("b"), &id("d"), b"b");
        names.record(&id("c"), &id("d"), b"c");
        assert_eq!(
            names.check(&id("a")),
            NameCheck::Fallback(FallbackReason::Evicted)
        );
        assert_eq!(names.check(&id("b")), verify("d", "b"));
        assert_eq!(names.check(&id("c")), verify("d", "c"));
    }
}
