//! Bind Environment placement to the node that owns a referenced Workspace.
//!
//! Workspace ownership is derived lexically from local bind mounts because the
//! API server may not share a filesystem with the node that stores the mount.
use crate::config::{WorkspaceAffinity, WorkspaceAffinityErrorPolicy};
use adx_protocol::control as pb;
use serde::Deserialize;
use std::{
    path::{Component, Path},
    time::Duration,
};
use tonic::Status;

/// Directory name directly below `root` identifies the Workspace owner.
/// Comparison is lexical: mounts are resolved on the node, not on this host.
pub fn derive_owner(root: &Path, mounts: &[pb::MountSpec]) -> Result<Option<String>, Status> {
    let Some(root) = segments(root) else {
        return Ok(None);
    };
    if root.is_empty() {
        return Ok(None);
    }
    let mut owners = std::collections::BTreeSet::new();
    for mount in mounts {
        let Some(pb::mount_spec::MountSource::Source(source)) = &mount.mount_source else {
            continue;
        };
        let Some(pb::storage_source::Source::Local(path)) = &source.source else {
            continue;
        };
        let Some(parts) = segments(Path::new(path)) else {
            continue;
        };
        if parts.len() <= root.len() || parts[..root.len()] != root[..] {
            continue;
        }
        let Some(owner) = parts.get(root.len()).cloned() else {
            continue;
        };
        if owner.is_empty() {
            continue;
        }
        owners.insert(owner);
    }
    match owners.len() {
        0 => Ok(None),
        1 => Ok(owners.into_iter().next()),
        _ => Err(Status::invalid_argument(
            "environment mounts reference multiple Workspace owners",
        )),
    }
}

/// Normalize an absolute path into ordinary components; parent traversal and
/// non-UTF-8 components make the mount ineligible for ownership derivation.
fn segments(path: &Path) -> Option<Vec<String>> {
    if !path.is_absolute() {
        return None;
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::CurDir => {}
            Component::Normal(value) => parts.push(value.to_str()?.to_owned()),
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(parts)
}

/// Confine every alternative node selector to the resolved node.
///
/// `required_node` is a disjunction, so adding a selector would widen instead of
/// narrow placement; the constraint is appended to each existing alternative.
/// Explicit `node_id` affinities arrive as required placement groups, so both
/// representations are checked for a conflict with the owner node.
pub fn enforce_node(policy: &mut pb::SchedulingPolicy, node_id: &str) -> Result<(), Status> {
    let selectors = policy.required_node.iter().chain(
        policy
            .placement_groups
            .iter()
            .filter(|group| {
                group.target == pb::PlacementTarget::Node as i32 && group.required && !group.anti
            })
            .flat_map(|group| group.terms.iter().filter_map(|term| term.selector.as_ref())),
    );
    for selector in selectors {
        for expression in &selector.expressions {
            if expression.key != "NODE_ID" {
                continue;
            }
            let conflict = match pb::SelectorOp::try_from(expression.op) {
                Ok(pb::SelectorOp::In) => !expression.values.iter().any(|value| value == node_id),
                Ok(pb::SelectorOp::NotIn) => expression.values.iter().any(|value| value == node_id),
                Ok(pb::SelectorOp::DoesNotExist) => true,
                _ => false,
            };
            if conflict {
                return Err(Status::invalid_argument(
                    "explicit node constraint conflicts with Workspace ownership",
                ));
            }
        }
    }
    if policy.required_node.is_empty() {
        policy.required_node.push(pb::LabelSelector::default());
    }
    for selector in &mut policy.required_node {
        selector.expressions.push(pb::LabelRequirement {
            key: "NODE_ID".into(),
            op: pb::SelectorOp::In as i32,
            values: vec![node_id.to_owned()],
        });
    }
    Ok(())
}

#[derive(Deserialize)]
struct Record {
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

/// Queries the per-node Workspace HTTP API that owns the ownership record.
pub struct WorkspaceAffinityResolver {
    config: WorkspaceAffinity,
    http: reqwest::Client,
    token: String,
}

impl WorkspaceAffinityResolver {
    pub fn new(mut config: WorkspaceAffinity) -> Result<Self, Box<dyn std::error::Error>> {
        if config.endpoints.is_empty() {
            config.endpoints.push("http://127.0.0.1:18889".into());
        }
        let token = std::fs::read_to_string(&config.token_file)?
            .trim()
            .to_owned();
        if token.is_empty() {
            return Err("Workspace directory token file is empty".into());
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()?;
        Ok(Self {
            config,
            http,
            token,
        })
    }

    pub fn root(&self) -> &Path {
        &self.config.root
    }

    /// Whether this failure may be downgraded to ordinary scheduling.
    ///
    /// `fail_open` never lets ownership resolution block creation: unregistered
    /// owners, unready records, malformed responses and request conflicts are
    /// logged and the Environment is scheduled without affinity.
    pub fn may_fall_back(&self, _error: &Status) -> bool {
        self.config.on_error == WorkspaceAffinityErrorPolicy::FailOpen
    }

    /// Resolve the owning node, failing over only for transport and server errors.
    pub async fn node_id(&self, owner: &str) -> Result<String, Status> {
        let mut retry = Status::unavailable("Workspace directory unavailable");
        for endpoint in &self.config.endpoints {
            match self.lookup(endpoint, owner).await {
                Ok(node_id) => return Ok(node_id),
                Err((error, true)) => retry = error,
                Err((error, false)) => return Err(error),
            }
        }
        Err(retry)
    }

    async fn lookup(&self, endpoint: &str, owner: &str) -> Result<String, (Status, bool)> {
        let mut url = url::Url::parse(endpoint)
            .map_err(|_| (Status::internal("invalid Workspace endpoint"), false))?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| (Status::internal("invalid Workspace endpoint"), false))?;
            path.pop_if_empty();
            path.push("v1").push("workspaces").push(owner);
        }
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|_| (Status::unavailable("Workspace directory unavailable"), true))?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err((
                Status::failed_precondition("Workspace is not registered"),
                false,
            ));
        }
        if matches!(
            status,
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            return Err((
                Status::unavailable("Workspace directory rejected credentials"),
                false,
            ));
        }
        if !status.is_success() {
            return Err((
                Status::unavailable("Workspace directory request failed"),
                status.is_server_error(),
            ));
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| (Status::unavailable("Workspace directory unavailable"), true))?;
        let record: Record = serde_json::from_slice(&body).map_err(|_| {
            (
                Status::internal("invalid Workspace directory response"),
                false,
            )
        })?;
        if record.state.as_deref() != Some("ready") {
            return Err((Status::failed_precondition("Workspace is not ready"), false));
        }
        record
            .node_id
            .filter(|node| !node.trim().is_empty())
            .ok_or((
                Status::internal("Workspace directory returned no node"),
                false,
            ))
    }
}
