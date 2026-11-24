//! H2 step 2: serving another reader's `ReadRanges` as a version holder
//! (plan/15 v3 §1.3, §2.2.2–§2.2.4).
//!
//! The order is normative. Nothing about this Node's Chunks is revealed before
//! the caller is authorized and its ranges are inside the version:
//!
//! 1. authenticate; one kind of grant in every operation, the request ids
//!    equal to every grant's (R4, B2-1);
//! 2. authorize the holder grant with Meta, outside the read slots, with
//!    bounded concurrency, cached at most [`AUTHORIZATION_KEEP`];
//! 3. check each range against the version's layout ([`HolderRegistry`]);
//! 4. wait for a Chunk being fetched or installed here, bounded;
//! 5. re-validate before taking a slot if the authorization ran short (R5);
//! 6. take a read slot with a fresh admission deadline (R2);
//! 7. inside the slot, compare the deadline only (no Meta);
//! 8. serve.
//!
//! Steps 1–3 fail with one and the same denial whatever this Node holds.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use afs_protocol::node_data::{DfsChunkReadOp, DfsReadRangesRequest};
use tonic::Status;

use crate::dfs::{
    ChunkId, DfsAuthorizedHolderRead, DfsHolderGrant, HOLDER_GRANT_TOKEN_LEN,
    HOLDER_GRANT_TOKEN_PREFIX, ValidateDfsHolderGrant,
};
use crate::node::read::holders::{HolderInFlight, HolderRange, HolderRegistry};

/// Meta answers a holder grant validation (the gRPC `ValidateHolderGrant`).
pub trait DfsHolderGrantValidator: Send + Sync + 'static {
    fn validate(
        &self,
        request: ValidateDfsHolderGrant,
    ) -> afs_error::Result<DfsAuthorizedHolderRead>;
}

impl DfsHolderGrantValidator for super::super::meta::GrpcDfsMeta {
    fn validate(
        &self,
        request: ValidateDfsHolderGrant,
    ) -> afs_error::Result<DfsAuthorizedHolderRead> {
        self.validate_holder_grant(request)
    }
}

/// A validated holder authorization is kept at most this long (plan/15 §2.2.4).
pub(crate) const AUTHORIZATION_KEEP: Duration = Duration::from_millis(2_000);
/// Meta validations in flight per holder; more are refused as busy.
pub(crate) const MAX_VALIDATIONS: usize = 8;
/// Requests waiting for the one validation in flight of the same grant
/// (Q1); more are refused as busy.
pub(crate) const MAX_VALIDATION_WAITERS: usize = 16;
/// How long a Meta denial of a grant is remembered (Q1).
pub(crate) const DENIAL_KEEP: Duration = Duration::from_millis(1_000);
/// Requests waiting for an in-flight Chunk, per holder and per caller.
pub(crate) const MAX_WAITERS: usize = 64;
pub(crate) const MAX_WAITERS_PER_PEER: usize = 16;
/// The longest a holder lets a request wait, whatever it asks.
pub(crate) const MAX_WAIT: Duration = Duration::from_millis(500);
/// An authorization with less than this left is re-validated before a slot.
pub(crate) const REVALIDATE_MARGIN: Duration = Duration::from_millis(100);
const MAX_CACHED_AUTHORIZATIONS: usize = 4096;

/// The one denial of steps 1–3: it must not depend on what this Node holds.
pub(crate) fn holder_denied() -> Status {
    Status::permission_denied("DFS holder read is not authorized")
}

pub(crate) fn holder_not_ready() -> Status {
    afs_transport::grpc::error_status::error_to_status(afs_error::Error::coded(
        afs_error::NODE_TRANSFER_NOT_READY,
        "DFS holder has no current copy of the Chunk",
    ))
}

struct Cached {
    grant: DfsHolderGrant,
    until: Instant,
    /// When Meta answered: a re-validation accepts only a newer answer.
    validated: Instant,
    /// Meta denied the grant (kept [`DENIAL_KEEP`], Q1).
    denied: bool,
}

/// The state of one Meta validation, as its waiters see it (Q1).
#[derive(Clone)]
enum Outcome {
    Pending,
    /// Meta's answer, a denial or an error included, for every waiter.
    Done(Result<Instant, Status>),
    /// The validating request was cancelled before an answer: ask again.
    Abandoned,
}

/// One Meta validation of one grant in flight, shared by its waiters.
struct Flight {
    grant: DfsHolderGrant,
    started: Instant,
    waiters: std::sync::atomic::AtomicUsize,
    outcome: tokio::sync::watch::Sender<Outcome>,
}

#[derive(Default)]
struct Waiters {
    total: usize,
    by_peer: HashMap<String, usize>,
}

/// One holder request after steps 1–3.
pub(crate) struct AuthorizedHolderRead {
    pub(crate) until: Instant,
}

/// See the module documentation.
pub struct HolderServe {
    validator: Arc<dyn DfsHolderGrantValidator>,
    pub(crate) registry: Arc<HolderRegistry>,
    pub(crate) in_flight: Arc<HolderInFlight>,
    node_id: String,
    node_epoch: u64,
    cache: Mutex<HashMap<String, Cached>>,
    /// One Meta validation per token at a time (re-review S4): concurrent
    /// misses of a grant wait for that answer, whatever it is (Q1), instead
    /// of each taking a validation permit.
    flights: Mutex<HashMap<String, Arc<Flight>>>,
    validations: Arc<tokio::sync::Semaphore>,
    waiters: Mutex<Waiters>,
}

/// The request validating a grant: publishes the outcome and retires the
/// flight, also when the request is cancelled (its waiters then ask again).
struct Lead<'a> {
    serve: &'a HolderServe,
    flight: Arc<Flight>,
    done: bool,
}

impl Lead<'_> {
    fn finish(mut self, result: &Result<Instant, Status>) {
        self.flight
            .outcome
            .send_replace(Outcome::Done(result.clone()));
        self.done = true;
    }
}

impl Drop for Lead<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.flight.outcome.send_replace(Outcome::Abandoned);
        }
        let mut flights = self.serve.flights.lock().unwrap_or_else(|e| e.into_inner());
        if flights
            .get(&self.flight.grant.token)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            flights.remove(&self.flight.grant.token);
        }
    }
}

/// One waiter of a flight; leaves its place when dropped.
struct Waiting(Arc<Flight>);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0
            .waiters
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Releases one waiter slot when dropped.
pub(crate) struct WaitSlot<'a> {
    serve: &'a HolderServe,
    peer: String,
}

impl Drop for WaitSlot<'_> {
    fn drop(&mut self) {
        let mut waiters = self.serve.waiters.lock().unwrap_or_else(|e| e.into_inner());
        waiters.total = waiters.total.saturating_sub(1);
        if let Some(count) = waiters.by_peer.get_mut(&self.peer) {
            *count -= 1;
            if *count == 0 {
                waiters.by_peer.remove(&self.peer);
            }
        }
    }
}

impl HolderServe {
    pub(crate) fn new(
        validator: Arc<dyn DfsHolderGrantValidator>,
        registry: Arc<HolderRegistry>,
        in_flight: Arc<HolderInFlight>,
        node_id: String,
        node_epoch: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            validator,
            registry,
            in_flight,
            node_id,
            node_epoch,
            cache: Mutex::default(),
            flights: Mutex::default(),
            validations: Arc::new(tokio::sync::Semaphore::new(MAX_VALIDATIONS)),
            waiters: Mutex::default(),
        })
    }

    /// Step 1 for the holder form: every operation carries a holder grant and
    /// no per-copy grant, all the same grant, naming this Node and the
    /// authenticated peer, for the request's version and layout.
    pub(crate) fn check_request(
        &self,
        peer: &str,
        request: &DfsReadRangesRequest,
    ) -> Result<DfsHolderGrant, Status> {
        validate_holder_shape(request)?;
        let mut grant: Option<&afs_protocol::meta::DfsHolderGrant> = None;
        for op in &request.operations {
            let (None, Some(op_grant)) = (&op.grant, &op.holder_grant) else {
                return Err(holder_denied());
            };
            if grant.is_some_and(|first| first != op_grant) {
                return Err(holder_denied());
            }
            grant = Some(op_grant);
        }
        let grant: DfsHolderGrant = grant.ok_or_else(holder_denied)?.clone().into();
        let now = crate::clock::now_unix_ms();
        if grant.file_version_id.0 != request.file_version_id
            || grant.layout_root_id.0 != request.layout_root_id
            || grant.caller_node_id != peer
            || grant.caller_node_epoch == 0
            || grant.holder_node_id != self.node_id
            || grant.holder_node_epoch != self.node_epoch
            || grant.expires_at_unix_ms <= now
            || grant.token.len() != HOLDER_GRANT_TOKEN_LEN
            || !grant.token.starts_with(HOLDER_GRANT_TOKEN_PREFIX)
        {
            return Err(holder_denied());
        }
        Ok(grant)
    }

    /// Step 2: Meta's answer for `grant`, cached at most [`AUTHORIZATION_KEEP`]
    /// (a denial at most [`DENIAL_KEEP`]). `fresh` (the re-validation of step
    /// 5) accepts only an answer Meta gave after it asked.
    ///
    /// One validation per grant is in flight; concurrent requests for the
    /// same grant wait for its answer, a denial or an error included, at most
    /// [`MAX_VALIDATION_WAITERS`] of them (more are refused as busy).
    pub(crate) async fn authorize(
        &self,
        peer: &str,
        grant: &DfsHolderGrant,
        fresh: bool,
    ) -> Result<AuthorizedHolderRead, Status> {
        let asked = Instant::now();
        loop {
            if let Some(answer) = self.cached(grant, fresh.then_some(asked)) {
                return answer;
            }
            let (flight, lead) = {
                let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
                match flights.get(&grant.token) {
                    Some(flight) => (flight.clone(), false),
                    None => {
                        let (outcome, _) = tokio::sync::watch::channel(Outcome::Pending);
                        let flight = Arc::new(Flight {
                            grant: grant.clone(),
                            started: Instant::now(),
                            waiters: Default::default(),
                            outcome,
                        });
                        flights.insert(grant.token.clone(), flight.clone());
                        (flight, true)
                    }
                }
            };
            if lead {
                let lead = Lead {
                    serve: self,
                    flight,
                    done: false,
                };
                let result = self.validate(peer, grant).await;
                lead.finish(&result);
                return result.map(|until| AuthorizedHolderRead { until });
            }
            // Another grant under the same token can only be a forgery of one
            // of the two: validate it on its own, outside the flight.
            if flight.grant != *grant {
                return self
                    .validate(peer, grant)
                    .await
                    .map(|until| AuthorizedHolderRead { until });
            }
            if flight
                .waiters
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                >= MAX_VALIDATION_WAITERS
            {
                flight
                    .waiters
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return Err(Status::resource_exhausted(
                    "DFS holder grant validation has too many waiters",
                ));
            }
            let waiting = Waiting(flight);
            let mut outcome = waiting.0.outcome.subscribe();
            let settled = outcome
                .wait_for(|outcome| !matches!(outcome, Outcome::Pending))
                .await
                .map(|outcome| outcome.clone());
            match settled {
                // A re-validation needs an answer Meta gave after it asked.
                Ok(Outcome::Done(result)) if !fresh || waiting.0.started >= asked => {
                    return result.map(|until| AuthorizedHolderRead { until });
                }
                _ => continue,
            }
        }
    }

    /// The cached answer for `grant` still valid now (validated after
    /// `since` when given): an authorization, or a recent denial.
    fn cached(
        &self,
        grant: &DfsHolderGrant,
        since: Option<Instant>,
    ) -> Option<Result<AuthorizedHolderRead, Status>> {
        let now = Instant::now();
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache
            .get(&grant.token)
            .filter(|cached| {
                cached.grant == *grant
                    && cached.until > now
                    && since.is_none_or(|since| cached.validated >= since)
            })
            .map(|cached| {
                if cached.denied {
                    Err(holder_denied())
                } else {
                    Ok(AuthorizedHolderRead {
                        until: cached.until,
                    })
                }
            })
    }

    fn remember(&self, grant: &DfsHolderGrant, validated: Instant, until: Instant, denied: bool) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.retain(|_, cached| cached.until > validated);
        if cache.len() < MAX_CACHED_AUTHORIZATIONS || cache.contains_key(&grant.token) {
            cache.insert(
                grant.token.clone(),
                Cached {
                    grant: grant.clone(),
                    until,
                    validated,
                    denied,
                },
            );
        }
    }

    /// Ask Meta, under the validation permits, and cache the answer (a
    /// denial for [`DENIAL_KEEP`]; errors are not cached).
    async fn validate(&self, peer: &str, grant: &DfsHolderGrant) -> Result<Instant, Status> {
        let permit = self
            .validations
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("DFS holder grant validations are full"))?;
        let validator = self.validator.clone();
        let request = ValidateDfsHolderGrant {
            receiver_node_id: self.node_id.clone(),
            receiver_node_epoch: self.node_epoch,
            peer_node_id: peer.to_owned(),
            grant: grant.clone(),
        };
        let answer = crate::node::bridge::spawn_blocking(move || {
            let _permit = permit;
            validator.validate(request)
        })
        .await
        .map_err(|_| Status::internal("DFS holder grant validation stopped"))?;
        let validated = Instant::now();
        let authorized = match answer {
            Ok(authorized) if authorized.grant == *grant => authorized,
            Err(error) if !crate::node::fault::classify::read_authority_rejected(&error) => {
                return Err(afs_transport::grpc::error_status::error_to_status(error));
            }
            // Denied, or an answer for another grant.
            _ => {
                self.remember(grant, validated, validated + DENIAL_KEEP, true);
                return Err(holder_denied());
            }
        };
        let left = authorized
            .expires_at_unix_ms
            .saturating_sub(crate::clock::now_unix_ms());
        let until = validated + Duration::from_millis(left).min(AUTHORIZATION_KEEP);
        self.remember(grant, validated, until, false);
        Ok(until)
    }

    /// Step 3 for every operation: denied outright when any range is outside
    /// the version, not ready when the version is not (completely) shared here.
    pub(crate) fn check_ranges(
        &self,
        grant: &DfsHolderGrant,
        operations: &[DfsChunkReadOp],
    ) -> Result<(), Status> {
        let mut not_ready = false;
        for op in operations {
            match self.registry.check(
                &grant.file_version_id,
                &grant.layout_root_id,
                &ChunkId::new(op.chunk_id.clone()),
                op.chunk_offset,
                op.length,
            ) {
                HolderRange::Covered => {}
                HolderRange::NotReady => not_ready = true,
                HolderRange::Denied => return Err(holder_denied()),
            }
        }
        if not_ready {
            Err(holder_not_ready())
        } else {
            Ok(())
        }
    }

    /// A waiter slot for `peer`, or `None` when the holder or the peer
    /// already has as many waiters as it allows.
    pub(crate) fn wait_slot(&self, peer: &str) -> Option<WaitSlot<'_>> {
        let mut waiters = self.waiters.lock().unwrap_or_else(|e| e.into_inner());
        let mine = waiters.by_peer.get(peer).copied().unwrap_or(0);
        if waiters.total >= MAX_WAITERS || mine >= MAX_WAITERS_PER_PEER {
            return None;
        }
        waiters.total += 1;
        *waiters.by_peer.entry(peer.to_owned()).or_default() += 1;
        Some(WaitSlot {
            serve: self,
            peer: peer.to_owned(),
        })
    }
}

/// Shape of a holder request; independent of anything this Node holds.
fn validate_holder_shape(request: &DfsReadRangesRequest) -> Result<(), Status> {
    let invalid = || {
        afs_transport::grpc::error_status::coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "DFS holder read batch is invalid",
        )
    };
    if request.read_id.is_empty()
        || request.attempt_id.is_empty()
        || request.file_version_id.is_empty()
        || request.layout_root_id.is_empty()
        || request.operations.is_empty()
        || request.operations.len() > crate::node::read::engine::MAX_DFS_READ_OPS
        || request.include_blake3_outboard
    {
        return Err(invalid());
    }
    let mut total = 0_u64;
    for (index, op) in request.operations.iter().enumerate() {
        if op.chunk_id.is_empty()
            || op.source_copy_id.is_empty()
            || op.operation_index as usize != index
            || op.length == 0
            || op.chunk_offset.checked_add(op.length).is_none()
        {
            return Err(invalid());
        }
        total = total
            .checked_add(op.length)
            .filter(|total| *total <= crate::node::read::engine::MAX_DFS_READ_BYTES)
            .ok_or_else(invalid)?;
    }
    Ok(())
}

/// How long the request may wait for in-flight Chunks.
pub(crate) fn wait_budget(request: &DfsReadRangesRequest) -> Duration {
    let asked = request
        .operations
        .iter()
        .map(|op| op.wait_ms)
        .max()
        .unwrap_or(0);
    Duration::from_millis(u64::from(asked)).min(MAX_WAIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Never;
    impl DfsHolderGrantValidator for Never {
        fn validate(
            &self,
            _: ValidateDfsHolderGrant,
        ) -> afs_error::Result<DfsAuthorizedHolderRead> {
            Err(afs_error::Error::coded(afs_error::IO_UNAVAILABLE, "unused"))
        }
    }

    /// Meta answers slowly and counts its calls.
    struct Slow(std::sync::atomic::AtomicUsize);
    impl DfsHolderGrantValidator for Slow {
        fn validate(
            &self,
            request: ValidateDfsHolderGrant,
        ) -> afs_error::Result<DfsAuthorizedHolderRead> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(100));
            Ok(DfsAuthorizedHolderRead {
                grant: request.grant,
                expires_at_unix_ms: crate::clock::now_unix_ms() + 30_000,
            })
        }
    }

    fn sample_grant() -> DfsHolderGrant {
        DfsHolderGrant {
            namespace_id: crate::dfs::NamespaceId::new("namespace"),
            file_version_id: crate::dfs::FileVersionId::new("v"),
            layout_root_id: crate::dfs::LayoutRootId::new("l"),
            caller_node_id: "reader".into(),
            caller_node_epoch: 1,
            holder_node_id: "holder".into(),
            holder_node_epoch: 1,
            expires_at_unix_ms: 4_000_000_000_000,
            token: format!("{HOLDER_GRANT_TOKEN_PREFIX}{}", "0".repeat(64)),
        }
    }

    /// S4 (re-review): concurrent misses of one grant ask Meta once; the
    /// others wait for that answer instead of taking validation permits (and
    /// being refused as busy beyond MAX_VALIDATIONS).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_s4_one_validation_per_grant_however_many_requests_miss() {
        let meta = Arc::new(Slow(Default::default()));
        let serve = HolderServe::new(
            meta.clone(),
            HolderRegistry::new(),
            HolderInFlight::new(),
            "holder".into(),
            1,
        );
        let grant = sample_grant();
        let tasks: Vec<_> = (0..2 * MAX_VALIDATIONS)
            .map(|_| {
                let serve = serve.clone();
                let grant = grant.clone();
                tokio::spawn(
                    async move { serve.authorize("reader", &grant, false).await.map(|_| ()) },
                )
            })
            .collect();
        for task in tasks {
            task.await.unwrap().expect("authorized");
        }
        assert_eq!(meta.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        // A re-validation (`fresh`) still asks Meta.
        serve.authorize("reader", &grant, true).await.unwrap();
        assert_eq!(meta.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// Meta answers every validation the same way after `delay`, and counts.
    struct Scripted {
        calls: std::sync::atomic::AtomicUsize,
        delay: Duration,
        deny: bool,
    }
    impl Scripted {
        fn new(delay_ms: u64, deny: bool) -> Arc<Self> {
            Arc::new(Self {
                calls: Default::default(),
                delay: Duration::from_millis(delay_ms),
                deny,
            })
        }
        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl DfsHolderGrantValidator for Scripted {
        fn validate(
            &self,
            _: ValidateDfsHolderGrant,
        ) -> afs_error::Result<DfsAuthorizedHolderRead> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(self.delay);
            Err(if self.deny {
                afs_error::Error::new(
                    afs_error::CLIENT_REMOTE_STATUS,
                    afs_error::ErrorKind::PermissionDenied,
                    "holder grant is not valid",
                )
            } else {
                afs_error::Error::coded(afs_error::IO_UNAVAILABLE, "Meta is unavailable")
            })
        }
    }

    fn serve_with(meta: Arc<dyn DfsHolderGrantValidator>) -> Arc<HolderServe> {
        HolderServe::new(
            meta,
            HolderRegistry::new(),
            HolderInFlight::new(),
            "holder".into(),
            1,
        )
    }

    async fn concurrently(
        serve: &Arc<HolderServe>,
        grant: &DfsHolderGrant,
        n: usize,
    ) -> Vec<Result<(), Status>> {
        let tasks: Vec<_> = (0..n)
            .map(|_| {
                let serve = serve.clone();
                let grant = grant.clone();
                tokio::spawn(
                    async move { serve.authorize("reader", &grant, false).await.map(|_| ()) },
                )
            })
            .collect();
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        results
    }

    /// Q1: a denial is shared with every request waiting for the same grant
    /// (one Meta call, not one after another), and remembered for at most
    /// DENIAL_KEEP, which never exceeds the authorization keep.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_q1_a_denial_is_shared_and_kept_briefly() {
        assert!(DENIAL_KEEP <= AUTHORIZATION_KEEP);
        let meta = Scripted::new(100, true);
        let serve = serve_with(meta.clone());
        let grant = sample_grant();
        let started = Instant::now();
        let results = concurrently(&serve, &grant, MAX_VALIDATION_WAITERS).await;
        for result in &results {
            assert_eq!(
                result.as_ref().unwrap_err().code(),
                tonic::Code::PermissionDenied
            );
        }
        assert_eq!(meta.calls(), 1, "one Meta call for every waiter");
        assert!(
            started.elapsed() < Duration::from_millis(400),
            "{:?}",
            started.elapsed()
        );
        // Remembered: refused again without Meta ...
        let again = serve.authorize("reader", &grant, false).await;
        assert_eq!(again.err().unwrap().code(), tonic::Code::PermissionDenied);
        assert_eq!(meta.calls(), 1);
        // ... but a re-validation, and the same grant after DENIAL_KEEP, ask Meta.
        assert!(serve.authorize("reader", &grant, true).await.is_err());
        assert_eq!(meta.calls(), 2);
        tokio::time::sleep(DENIAL_KEEP + Duration::from_millis(50)).await;
        assert!(serve.authorize("reader", &grant, false).await.is_err());
        assert_eq!(meta.calls(), 3);
    }

    /// Q1: a Meta error is shared with the waiters of that one validation,
    /// but not remembered: the next request asks Meta again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_q1_a_meta_error_is_shared_but_not_kept() {
        let meta = Scripted::new(100, false);
        let serve = serve_with(meta.clone());
        let grant = sample_grant();
        let results = concurrently(&serve, &grant, MAX_VALIDATION_WAITERS).await;
        for result in &results {
            assert_eq!(
                result.as_ref().unwrap_err().code(),
                tonic::Code::Unavailable
            );
        }
        assert_eq!(meta.calls(), 1);
        assert!(serve.authorize("reader", &grant, false).await.is_err());
        assert_eq!(meta.calls(), 2);
    }

    /// Q1: at most MAX_VALIDATION_WAITERS requests wait for one grant's
    /// validation; the next is refused as busy at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_q1_waiters_of_one_grant_are_bounded() {
        let meta = Arc::new(Slow(Default::default()));
        let serve = serve_with(meta.clone());
        let grant = sample_grant();
        let first = {
            let serve = serve.clone();
            let grant = grant.clone();
            tokio::spawn(async move { serve.authorize("reader", &grant, false).await.map(|_| ()) })
        };
        while meta.0.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let waiters: Vec<_> = (0..MAX_VALIDATION_WAITERS)
            .map(|_| {
                let serve = serve.clone();
                let grant = grant.clone();
                tokio::spawn(
                    async move { serve.authorize("reader", &grant, false).await.map(|_| ()) },
                )
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let started = Instant::now();
        let extra = serve.authorize("reader", &grant, false).await;
        assert_eq!(extra.err().unwrap().code(), tonic::Code::ResourceExhausted);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "refused at once"
        );
        first.await.unwrap().unwrap();
        for waiter in waiters {
            waiter.await.unwrap().unwrap();
        }
        assert_eq!(meta.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Q1: the request validating a grant is cancelled before Meta answers:
    /// its waiters do not hang, one of them asks Meta again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h2_q1_waiters_ask_again_when_the_validating_request_is_cancelled() {
        let meta = Arc::new(Slow(Default::default()));
        let serve = serve_with(meta.clone());
        let grant = sample_grant();
        let first = {
            let serve = serve.clone();
            let grant = grant.clone();
            tokio::spawn(async move { serve.authorize("reader", &grant, false).await.map(|_| ()) })
        };
        while meta.0.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let waiter = {
            let serve = serve.clone();
            let grant = grant.clone();
            tokio::spawn(async move { serve.authorize("reader", &grant, false).await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        first.abort();
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("the waiter does not hang")
            .unwrap()
            .expect("authorized by its own validation");
        assert_eq!(meta.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// T3 (global): at most MAX_WAITERS across callers, MAX_WAITERS_PER_PEER
    /// each; a freed slot can be taken again.
    #[test]
    fn h2_t3_waiters_are_bounded_globally_and_per_caller() {
        let serve = HolderServe::new(
            Arc::new(Never),
            HolderRegistry::new(),
            HolderInFlight::new(),
            "holder".into(),
            1,
        );
        let mut held = Vec::new();
        for peer in 0..MAX_WAITERS / MAX_WAITERS_PER_PEER {
            for _ in 0..MAX_WAITERS_PER_PEER {
                held.push(serve.wait_slot(&format!("peer-{peer}")).unwrap());
            }
        }
        assert_eq!(held.len(), MAX_WAITERS);
        assert!(serve.wait_slot("a-new-peer").is_none(), "global limit");
        held.pop();
        assert!(serve.wait_slot("a-new-peer").is_some());
    }
}
