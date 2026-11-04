//! Authorizing inbound DFS peer reads.

use super::*;

/// Verifies grant authenticity, caller epoch, namespace/version/layout membership
/// and the selected local copy/device epoch, with expiry checks on every range.
/// Structural validation alone never grants access.
#[cfg(feature = "dfs")]
pub trait DfsReadAuthorizer: Send + Sync + 'static {
    fn authorize(
        &self,
        authenticated_peer: &str,
        request: &DfsReadRangesRequest,
    ) -> afs_error::Result<()>;
}

#[cfg(feature = "dfs")]
pub trait DfsReadGrantValidator: Send + Sync + 'static {
    fn authorization_metrics(&self) -> Option<super::super::super::read_work::ReadWorkMetrics> {
        None
    }
    fn validate(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>>;
}

#[cfg(feature = "dfs")]
impl DfsReadGrantValidator for super::super::meta::GrpcDfsMeta {
    fn authorization_metrics(&self) -> Option<super::super::super::read_work::ReadWorkMetrics> {
        self.authorization_metrics()
    }
    fn validate(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        self.validate_read_grants(request)
    }
}

/// Bounded capability-validation cache, never a data cache or an authority
/// fallback. Cache hits keep their original expiry and cannot renew a lease.
#[cfg(feature = "dfs")]
pub struct CachedDfsReadAuthorizer {
    validator: Arc<dyn DfsReadGrantValidator>,
    node_id: String,
    node_epoch: u64,
    pub(super) cache: std::sync::Mutex<HashMap<String, CachedReadAuthority>>,
}

#[cfg(feature = "dfs")]
pub(super) struct CachedReadAuthority {
    authorization: crate::dfs::DfsAuthorizedRead,
    pub(super) deadline: std::time::Instant,
}

#[cfg(feature = "dfs")]
impl CachedDfsReadAuthorizer {
    pub fn new(
        validator: Arc<dyn DfsReadGrantValidator>,
        node_id: String,
        node_epoch: u64,
    ) -> Self {
        Self {
            validator,
            node_id,
            node_epoch,
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(feature = "dfs")]
fn read_authority_key(validation: &crate::dfs::DfsReadValidation) -> afs_error::Result<String> {
    let grant = &validation.grant;
    if grant.token.len() != 76
        || [
            &grant.namespace_id.0,
            &grant.file_version_id.0,
            &grant.layout_root_id.0,
            &grant.caller_node_id,
            &validation.chunk_id.0,
            &validation.copy_id.0,
        ]
        .iter()
        .any(|value| value.len() > 512)
    {
        return Err(replica_denied(
            "read authority identity exceeds its bounded budget",
        ));
    }
    serde_json::to_string(&(grant, &validation.chunk_id, &validation.copy_id))
        .map_err(|_| replica_denied("read authority key serialization failed"))
}

#[cfg(feature = "dfs")]
fn authorized_read_range(entry: &crate::dfs::DfsAuthorizedRead, offset: u64, length: u64) -> bool {
    length != 0
        && offset.checked_add(length).is_some_and(|end| {
            entry.allowed_ranges.iter().any(|(start, count)| {
                offset >= *start && start.checked_add(*count).is_some_and(|limit| end <= limit)
            })
        })
}

#[cfg(feature = "dfs")]
impl DfsReadAuthorizer for CachedDfsReadAuthorizer {
    fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
        let metrics = self.validator.authorization_metrics();
        if let Some(metrics) = &metrics {
            metrics.check(crate::node::read_work::ReadCheck::AuthorizeFull);
            metrics.check(crate::node::read_work::ReadCheck::RequestValidate);
        }
        let _authorization_scope = metrics.as_ref().map(|m| m.authorization_scope("total"));
        validate_dfs_read_request(request)
            .map_err(afs_transport::grpc::error_status::status_to_error)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| replica_denied("read authority clock is before epoch"))?
            .as_millis() as u64;
        let mut checks = Vec::with_capacity(request.operations.len());
        for op in &request.operations {
            let grant = op
                .grant
                .as_ref()
                .ok_or_else(|| replica_denied("read operation grant is absent"))?;
            if grant.caller_node_id != peer {
                return Err(replica_denied("read capability belongs to another peer"));
            }
            let check = crate::dfs::DfsReadValidation {
                grant: crate::dfs::DfsReadGrant {
                    namespace_id: crate::dfs::NamespaceId::new(grant.namespace_id.clone()),
                    file_version_id: crate::dfs::FileVersionId::new(grant.file_version_id.clone()),
                    layout_root_id: crate::dfs::LayoutRootId::new(grant.layout_root_id.clone()),
                    caller_node_id: grant.caller_node_id.clone(),
                    caller_node_epoch: grant.caller_node_epoch,
                    expires_at_unix_ms: grant.expires_at_unix_ms,
                    fence: grant.fence,
                    token: grant.token.clone(),
                },
                chunk_id: crate::dfs::ChunkId::new(op.chunk_id.clone()),
                copy_id: crate::dfs::CopyId::new(op.source_copy_id.clone()),
                chunk_offset: op.chunk_offset,
                length: op.length,
            };
            checks.push((read_authority_key(&check)?, check));
        }
        let missing = {
            let mut cache = self
                .cache
                .lock()
                .map_err(|_| replica_denied("read authority cache is poisoned"))?;
            let maintenance = metrics
                .as_ref()
                .map(|m| m.authorization_scope("cache_maintenance"));
            cache.retain(|_, entry| {
                entry.deadline > std::time::Instant::now()
                    && entry.authorization.expires_at_unix_ms > now
            });
            drop(maintenance);
            checks
                .iter()
                .filter(|(key, check)| {
                    cache.get(key).is_none_or(|entry| {
                        !authorized_read_range(
                            &entry.authorization,
                            check.chunk_offset,
                            check.length,
                        )
                    })
                })
                .map(|(_, check)| check.clone())
                .collect::<Vec<_>>()
        };
        if missing.is_empty() {
            return Ok(());
        }
        let meta_scope = metrics.as_ref().map(|m| m.authorization_scope("meta_rpc"));
        let authorized = self.validator.validate(crate::dfs::ValidateDfsReadGrants {
            receiver_node_id: self.node_id.clone(),
            receiver_node_epoch: self.node_epoch,
            peer_node_id: peer.into(),
            validations: missing.clone(),
        })?;
        drop(meta_scope);
        if authorized.len() != missing.len() {
            return Err(replica_denied("read authority reply is incomplete"));
        }
        let completed_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| replica_denied("read authority clock is before epoch"))?
            .as_millis() as u64;
        for (entry, check) in authorized.iter().zip(&missing) {
            if entry.validation != *check
                || entry.allowed_ranges.is_empty()
                || entry.allowed_ranges.len() > 128
                || entry.expires_at_unix_ms <= completed_now
                || entry.expires_at_unix_ms > check.grant.expires_at_unix_ms
                || !authorized_read_range(entry, check.chunk_offset, check.length)
            {
                return Err(replica_denied(
                    "read authority reply is not the precise requested capability",
                ));
            }
        }
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| replica_denied("read authority cache is poisoned"))?;
        for entry in authorized {
            let key = read_authority_key(&entry.validation)?;
            if cache.len() >= 1024
                && !cache.contains_key(&key)
                && let Some(old) = cache.keys().next().cloned()
            {
                cache.remove(&old);
            }
            // Meta and this receiver have different wall clocks. Bound local
            // retention monotonically instead of rejecting a valid Meta reply
            // whose five-second expiry is slightly ahead of our clock.
            let remaining = entry
                .expires_at_unix_ms
                .saturating_sub(completed_now)
                .min(5_000);
            cache.insert(
                key,
                CachedReadAuthority {
                    authorization: entry,
                    deadline: std::time::Instant::now()
                        + std::time::Duration::from_millis(remaining),
                },
            );
        }
        Ok(())
    }
}

#[cfg(feature = "dfs")]
pub struct DenyDfsReadAuthorizer;

#[cfg(feature = "dfs")]
impl DfsReadAuthorizer for DenyDfsReadAuthorizer {
    fn authorize(&self, _peer: &str, _request: &DfsReadRangesRequest) -> afs_error::Result<()> {
        Err(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "DFS read grant authority is not wired; peer reads are denied",
        ))
    }
}
