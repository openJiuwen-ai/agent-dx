//! Bind Environment placement to the node that owns a referenced Workspace.
//!
//! Workspace ownership is derived lexically from local bind mounts because the
//! API server may not share a filesystem with the node that stores the mount.
use crate::config::{WorkspaceAffinity, WorkspaceAffinityErrorPolicy};
use adx_protocol::control as pb;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    path::{Component, Path},
    time::Duration,
};
use tonic::Status;

const NODE_ID: &str = "NODE_ID";
const CONFLICT: &str = "explicit node constraint conflicts with Workspace ownership";

/// Directory name directly below `root` identifies the Workspace owner.
/// Comparison is lexical: mounts are resolved on the node, not on this host.
pub fn derive_owner(root: &Path, mounts: &[pb::MountSpec]) -> Result<Option<String>, Status> {
    let Some(root) = segments(root) else {
        return Ok(None);
    };
    if root.is_empty() {
        return Ok(None);
    }
    let mut owners = BTreeSet::new();
    for mount in mounts {
        let Some(pb::mount_spec::MountSource::Source(source)) = &mount.mount_source else {
            continue;
        };
        let Some(pb::storage_source::Source::Local(path)) = &source.source else {
            continue;
        };
        match classify(&root, Path::new(path)) {
            MountPath::Outside => {}
            MountPath::Owner(owner) => {
                owners.insert(owner);
            }
            // A traversal or non-UTF-8 path may still resolve below the root
            // on the node. Guessing would silently place the Environment on
            // the wrong node, so the request follows the error policy.
            MountPath::Ambiguous => {
                return Err(Status::invalid_argument(
                    "Workspace mount path cannot be resolved lexically",
                ))
            }
        }
    }
    match owners.len() {
        0 => Ok(None),
        1 => Ok(owners.into_iter().next()),
        _ => Err(Status::invalid_argument(
            "environment mounts reference multiple Workspace owners",
        )),
    }
}

enum MountPath {
    Outside,
    Owner(String),
    Ambiguous,
}

/// Resolve an absolute mount path against the workspace root without touching
/// the filesystem. Parent traversal is applied lexically and marks the path
/// ambiguous because the node may resolve it through symbolic links.
fn classify(root: &[String], path: &Path) -> MountPath {
    if !path.is_absolute() {
        return MountPath::Outside;
    }
    let mut parts = Vec::new();
    let mut ambiguous = false;
    for component in path.components() {
        match component {
            Component::RootDir => parts.clear(),
            Component::CurDir => {}
            Component::Normal(value) => match value.to_str() {
                Some(value) => parts.push(value.to_owned()),
                None => ambiguous = true,
            },
            Component::ParentDir => {
                ambiguous = true;
                parts.pop();
            }
            Component::Prefix(_) => return MountPath::Outside,
        }
    }
    if parts.len() <= root.len() || parts[..root.len()] != root[..] {
        return MountPath::Outside;
    }
    let Some(owner) = parts.get(root.len()).cloned() else {
        return MountPath::Outside;
    };
    if owner.is_empty() {
        return MountPath::Outside;
    }
    if ambiguous {
        return MountPath::Ambiguous;
    }
    MountPath::Owner(owner)
}

/// Normalize the workspace root into ordinary components.
fn segments(path: &Path) -> Option<Vec<String>> {
    if !path.is_absolute() {
        return None;
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(value) => parts.push(value.to_str()?.to_owned()),
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(parts)
}

/// Confine every alternative node selector to the resolved node.
///
/// `required_node` entries and the terms of a required Node placement group are
/// disjunctions: one alternative that excludes the owner does not make the
/// affinity impossible. The constraint is appended only after proving that at
/// least one alternative admits the owner; adding another alternative would
/// widen placement instead of narrowing it.
pub fn enforce_node(policy: &mut pb::SchedulingPolicy, node_id: &str) -> Result<(), Status> {
    if !policy.required_node.is_empty() && !policy.required_node.iter().any(|s| admits(s, node_id))
    {
        return Err(conflict());
    }
    let empty = pb::LabelSelector::default();
    for group in policy
        .placement_groups
        .iter()
        .filter(|group| group.target == pb::PlacementTarget::Node as i32 && group.required)
    {
        if group.anti {
            // An anti group rejects a node that matches any term. Only proven
            // matches conflict; labels this server cannot observe stay unknown.
            if group
                .terms
                .iter()
                .any(|term| definitely_matches(term.selector.as_ref().unwrap_or(&empty), node_id))
            {
                return Err(conflict());
            }
        } else if !group
            .terms
            .iter()
            .any(|term| admits(term.selector.as_ref().unwrap_or(&empty), node_id))
        {
            return Err(conflict());
        }
    }
    if policy.required_node.is_empty() {
        policy.required_node.push(pb::LabelSelector::default());
    }
    for selector in &mut policy.required_node {
        selector.expressions.push(pb::LabelRequirement {
            key: NODE_ID.into(),
            op: pb::SelectorOp::In as i32,
            values: vec![node_id.to_owned()],
        });
    }
    Ok(())
}

fn conflict() -> Status {
    Status::invalid_argument(CONFLICT)
}

/// Whether a selector can match a node whose `NODE_ID` is `node_id`.
///
/// Only `NODE_ID` is known here. Requirements on other labels are treated as
/// satisfiable, so `true` means "not proven to conflict".
fn admits(selector: &pb::LabelSelector, node_id: &str) -> bool {
    if selector
        .match_labels
        .get(NODE_ID)
        .is_some_and(|value| value != node_id)
    {
        return false;
    }
    selector
        .expressions
        .iter()
        .filter(|expression| expression.key == NODE_ID)
        .all(|expression| requirement_matches(expression, node_id))
}

/// Whether the selector is proven to match the owner node. Selectors that
/// constrain any other label cannot be proven and return `false`.
fn definitely_matches(selector: &pb::LabelSelector, node_id: &str) -> bool {
    let node_only = selector.match_labels.keys().all(|key| key == NODE_ID)
        && selector
            .expressions
            .iter()
            .all(|expression| expression.key == NODE_ID);
    node_only && admits(selector, node_id)
}

/// Mirror the scheduler's `NODE_ID` comparison semantics.
fn requirement_matches(expression: &pb::LabelRequirement, node_id: &str) -> bool {
    let contains = expression.values.iter().any(|value| value == node_id);
    match pb::SelectorOp::try_from(expression.op) {
        Ok(pb::SelectorOp::In) => contains,
        Ok(pb::SelectorOp::NotIn) => !contains,
        Ok(pb::SelectorOp::Exists) => true,
        Ok(pb::SelectorOp::DoesNotExist) => false,
        Ok(pb::SelectorOp::Gt) => compare(node_id, expression, |actual, bound| actual > bound),
        Ok(pb::SelectorOp::Lt) => compare(node_id, expression, |actual, bound| actual < bound),
        Ok(pb::SelectorOp::Unspecified) | Err(_) => true,
    }
}

fn compare(
    node_id: &str,
    expression: &pb::LabelRequirement,
    test: impl FnOnce(i64, i64) -> bool,
) -> bool {
    node_id
        .parse::<i64>()
        .ok()
        .zip(
            expression
                .values
                .first()
                .and_then(|value| value.parse::<i64>().ok()),
        )
        .is_some_and(|(actual, bound)| test(actual, bound))
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
    http: Option<reqwest::Client>,
    token: String,
    init_error: Option<String>,
}

impl WorkspaceAffinityResolver {
    /// Build a resolver even when the token cannot be read. The failure is
    /// reported per request so `fail_closed` keeps its guarantee and
    /// `fail_open` records a fallback instead of silently disabling affinity.
    pub fn new(mut config: WorkspaceAffinity) -> Self {
        if config.endpoints.is_empty() {
            config.endpoints.push("http://127.0.0.1:18889".into());
        }
        let mut init_error = None;
        let token = match std::fs::read_to_string(&config.token_file) {
            Ok(token) => {
                let token = token.trim().to_owned();
                if token.is_empty() {
                    init_error = Some(format!(
                        "Workspace directory token file {} is empty",
                        config.token_file.display()
                    ));
                }
                token
            }
            Err(error) => {
                init_error = Some(format!(
                    "cannot read Workspace directory token file {}: {error}",
                    config.token_file.display()
                ));
                String::new()
            }
        };
        let http = match reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()
        {
            Ok(http) => Some(http),
            Err(error) => {
                init_error.get_or_insert_with(|| format!("invalid Workspace HTTP client: {error}"));
                None
            }
        };
        Self {
            config,
            http,
            token,
            init_error,
        }
    }

    pub fn init_error(&self) -> Option<&str> {
        self.init_error.as_deref()
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
        if let Some(error) = &self.init_error {
            return Err(Status::unavailable(format!(
                "Workspace affinity is unavailable: {error}"
            )));
        }
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
        let http = self.http.as_ref().ok_or((
            Status::unavailable("Workspace directory client is unavailable"),
            false,
        ))?;
        let mut url = url::Url::parse(endpoint)
            .map_err(|_| (Status::internal("invalid Workspace endpoint"), false))?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| (Status::internal("invalid Workspace endpoint"), false))?;
            path.pop_if_empty();
            path.push("v1").push("workspaces").push(owner);
        }
        let response = http
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
