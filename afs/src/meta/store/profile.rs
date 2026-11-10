//! Opt-in wall-time profiling, without serializing authority values into logs.
//!
//! Compiled only with the `diagnostics` feature (default off). There it is
//! enabled at run time by `AFS_META_PROFILE=1` (store batch timings) and
//! `AFS_META_NAMESPACE_TRACE=1` (namespace conflicts/applications), written to
//! stderr. Without the feature the same entry points are empty and `enabled()`
//! is a constant `false`, so call sites compile to nothing and the variables
//! are ignored.
#[cfg(feature = "diagnostics")]
pub(super) use imp::*;
#[cfg(not(feature = "diagnostics"))]
pub(super) use off::*;

#[cfg(not(feature = "diagnostics"))]
mod off {
    pub(in crate::meta::store) const fn enabled() -> bool {
        false
    }

    pub(in crate::meta::store) struct Timer;

    impl Timer {
        pub(in crate::meta::store) const fn start() -> Self {
            Self
        }

        pub(in crate::meta::store) const fn ms(&self) -> f64 {
            0.0
        }
    }

    pub(in crate::meta::store) fn record(_value: serde_json::Value) {}

    pub(in crate::meta::store) fn namespace_conflict(
        _state: &super::super::State,
        _txn: &super::super::MetaTxn,
    ) {
    }

    pub(in crate::meta::store) fn namespace_applied(
        _txn: &super::super::MetaTxn,
        _revision: super::super::StoreRevision,
    ) {
    }
}

#[cfg(feature = "diagnostics")]
mod imp {
    use std::{
        io::Write,
        sync::OnceLock,
        time::{Instant, SystemTime, UNIX_EPOCH},
    };

    pub(in crate::meta::store) fn enabled() -> bool {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| std::env::var_os("AFS_META_PROFILE").is_some_and(|v| v == "1"))
    }

    pub(in crate::meta::store) struct Timer(Option<Instant>);

    impl Timer {
        pub(in crate::meta::store) fn start() -> Self {
            Self(enabled().then(Instant::now))
        }

        pub(in crate::meta::store) fn ms(&self) -> f64 {
            self.0.map_or(0.0, |v| v.elapsed().as_secs_f64() * 1_000.0)
        }
    }

    pub(in crate::meta::store) fn record(mut value: serde_json::Value) {
        if let Some(fields) = value.as_object_mut() {
            fields.insert(
                "unix_time_seconds".into(),
                serde_json::json!(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .map(|v| v.as_secs_f64())
                ),
            );
        }
        // Diagnostics cannot fail a durable commit when stderr is unavailable.
        let _ = writeln!(std::io::stderr().lock(), "{value}");
    }

    fn namespace_trace_enabled() -> bool {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED
            .get_or_init(|| std::env::var_os("AFS_META_NAMESPACE_TRACE").is_some_and(|v| v == "1"))
    }

    fn inode_identity(inode: &crate::dfs::InodeRecord) -> serde_json::Value {
        serde_json::json!({
            "namespace": inode.namespace_id.0, "inode": inode.inode_id.0,
            "revision": inode.revision, "head": inode.head_version,
            "nlink": inode.attributes.nlink, "mode": inode.attributes.mode,
            "uid": inode.attributes.uid, "gid": inode.attributes.gid,
            "mtime_ms": inode.attributes.mtime_unix_ms,
            "ctime_ms": inode.attributes.ctime_unix_ms,
        })
    }

    pub(in crate::meta::store) fn namespace_conflict(
        state: &super::super::State,
        txn: &super::super::MetaTxn,
    ) {
        if !namespace_trace_enabled() {
            return;
        }
        let inodes: Vec<_> = txn
            .conditions
            .iter()
            .filter_map(|condition| {
                let super::super::TxnCondition::EntityEquals(super::super::MetaEntity::DfsInode(
                    expected,
                )) = condition
                else {
                    return None;
                };
                if super::super::condition_matches(state, condition) {
                    return None;
                }
                let observed = state
                    .entities
                    .get(&super::super::MetaKey::DfsInode(expected.inode_id.clone()))
                    .and_then(|value| match &value.entity {
                        super::super::MetaEntity::DfsInode(inode) => Some(inode_identity(inode)),
                        _ => None,
                    });
                Some(serde_json::json!({"expected":inode_identity(expected),"observed":observed}))
            })
            .collect();
        let reference_conditions: Vec<_> =
            if txn.operation == super::super::StoreOperation::DfsUpdateReference {
                txn.conditions
                    .iter()
                    .filter(|condition| !super::super::condition_matches(state, condition))
                    .filter_map(|condition| match condition {
                        super::super::TxnCondition::RevisionEquals { key, revision } => {
                            Some(serde_json::json!({"condition":"revision", "key":key,
                        "expected_revision":revision.0,
                        "observed_revision":state.entities.get(key).map(|value|value.revision.0)}))
                        }
                        super::super::TxnCondition::Missing(key) => {
                            Some(serde_json::json!({"condition":"missing", "key":key,
                        "observed_revision":state.entities.get(key).map(|value|value.revision.0)}))
                        }
                        super::super::TxnCondition::NodeSessionCurrent {
                            node_id,
                            session_id,
                        } => Some(serde_json::json!({"condition":"current_session",
                        "node":node_id,"session":session_id})),
                        _ => None,
                    })
                    .take(8)
                    .collect()
            } else {
                Vec::new()
            };
        record(serde_json::json!({"event":"meta_namespace_conflict",
        "operation":txn.operation.as_str(), "caller":txn.request.caller_id,
        "request":txn.request.request_id, "store_revision":state.revision.0,
        "changed_inodes":inodes,"reference_conditions":reference_conditions}));
    }

    pub(in crate::meta::store) fn namespace_applied(
        txn: &super::super::MetaTxn,
        revision: super::super::StoreRevision,
    ) {
        if !namespace_trace_enabled() {
            return;
        }
        let inodes: Vec<_> = txn
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                super::super::TxnMutation::Put(super::super::MetaEntity::DfsInode(inode)) => {
                    Some(inode_identity(inode))
                }
                _ => None,
            })
            .collect();
        if !inodes.is_empty() {
            // This describes application to a candidate batch, not durable publication.
            record(serde_json::json!({"event":"meta_namespace_applied",
            "operation":txn.operation.as_str(), "caller":txn.request.caller_id,
            "request":txn.request.request_id, "store_revision":revision.0,
            "inodes":inodes}));
        }
    }
}
