//! Connector-owned management routes for fixed Registry image blobs.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use afs_error::{Error, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    middleware,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::service::NydusService;
use crate::{management::AdminToken, node::vfs::dfs::DistributedFs};

const MAX_RUNNING: usize = 4;
const MAX_RETAINED: usize = 256;
const RETAIN_FINISHED: Duration = Duration::from_secs(600);
const JOB_TIMEOUT: Duration = Duration::from_secs(900);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct SeedRequest {
    pub source: String,
    pub manifest_digest: String,
    pub node_ids: Vec<String>,
    pub request_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct WarmRequest {
    pub source: String,
    pub manifest_digest: String,
    pub ranges: Vec<BlobRange>,
    pub request_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct BlobRange {
    pub blob_digest: String,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Request {
    Seed(SeedRequest),
    Warm(WarmRequest),
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum JobState {
    Queued,
    Running,
    Ready,
    Failed,
}

#[derive(Clone, Serialize)]
struct Status {
    job_id: String,
    state: JobState,
    manifest_digest: String,
    node_ids: Vec<String>,
    completed_blobs: usize,
    error: Option<String>,
}

struct Job {
    request: Request,
    status: Status,
    finished: Option<Instant>,
}

pub(super) struct Jobs {
    service: Arc<NydusService>,
    dfs: Arc<DistributedFs>,
    records: Mutex<HashMap<Uuid, Job>>,
    running: Arc<Semaphore>,
}

impl Jobs {
    pub(super) fn new(service: Arc<NydusService>, dfs: Arc<DistributedFs>) -> Self {
        Self {
            service, dfs, records: Mutex::new(HashMap::new()),
            running: Arc::new(Semaphore::new(MAX_RUNNING)),
        }
    }

    pub(super) async fn stop(&self) {
        self.running.close();
        while self.running.available_permits() < MAX_RUNNING {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn submit(self: &Arc<Self>, request: Request) -> Result<Status> {
        let mut records = self.records.lock().map_err(|_| busy("Nydus job table poisoned"))?;
        records.retain(|_, job| job.finished.is_none_or(|done| done.elapsed() < RETAIN_FINISHED));
        let (source, manifest, id, nodes) = request.validate()?;
        if let Some(previous) = records.values().find(|job| job.request.id() == id) {
            if previous.request != request {
                return Err(Error::coded(afs_error::IO_ALREADY_EXISTS, "request_id already identifies another job"));
            }
            return Ok(previous.status.clone());
        }
        if records.len() >= MAX_RETAINED {
            return Err(busy("Nydus retained job limit reached"));
        }
        self.service.check_management_source(source)?;
        let permit = self.running.clone().try_acquire_owned()
            .map_err(|_| busy("Nydus management job limit reached"))?;
        let job_id = Uuid::new_v4();
        let status = Status {
            job_id: job_id.to_string(), state: JobState::Queued, manifest_digest: manifest.to_owned(),
            node_ids: nodes.to_vec(), completed_blobs: 0, error: None,
        };
        records.insert(job_id, Job { request: request.clone(), status: status.clone(), finished: None });
        let this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            this.update(job_id, |job| job.status.state = JobState::Running);
            let deadline = Instant::now() + JOB_TIMEOUT;
            let result = match request {
                Request::Seed(request) => {
                    this.service.seed(&this.dfs, request, deadline, |count| {
                        this.update(job_id, |job| job.status.completed_blobs = count);
                    }).await
                }
                Request::Warm(request) => this.service.prewarm(&this.dfs, request, deadline).await,
            };
            this.update(job_id, |job| {
                match result {
                    Ok(count) => {
                        job.status.state = JobState::Ready;
                        job.status.completed_blobs = count;
                    }
                    Err(error) => {
                        job.status.state = JobState::Failed;
                        job.status.error = Some(error.to_string());
                    }
                }
                job.finished = Some(Instant::now());
            });
        });
        Ok(status)
    }

    fn update(&self, job_id: Uuid, update: impl FnOnce(&mut Job)) {
        if let Ok(mut records) = self.records.lock() && let Some(job) = records.get_mut(&job_id) {
            update(job);
        }
    }

    fn status(&self, id: Uuid, seed: bool) -> Option<Status> {
        let records = self.records.lock().ok()?;
        let job = records.get(&id)?;
        (matches!(job.request, Request::Seed(_)) == seed
            && job.finished.is_none_or(|done| done.elapsed() < RETAIN_FINISHED))
            .then(|| job.status.clone())
    }
}

impl Request {
    fn id(&self) -> &str {
        match self {
            Self::Seed(request) => &request.request_id,
            Self::Warm(request) => &request.request_id,
        }
    }

    fn validate(&self) -> Result<(&str, &str, &str, &[String])> {
        let (source, manifest, nodes) = match self {
            Self::Seed(request) => {
                if request.node_ids.is_empty() || request.node_ids.len() > 4096
                    || request.node_ids.iter().any(|node| !crate::config::is_safe_node_id(node))
                    || request.node_ids.iter().collect::<std::collections::HashSet<_>>().len() != request.node_ids.len()
                {
                    return Err(invalid("node_ids requires unique valid AFS Node IDs"));
                }
                (&request.source, &request.manifest_digest, request.node_ids.as_slice())
            }
            Self::Warm(request) => {
                if request.ranges.is_empty() || request.ranges.len() > 8192 {
                    return Err(invalid("prewarm requires 1–8192 blob ranges"));
                }
                for range in &request.ranges {
                    super::registry::require_digest(&range.blob_digest)?;
                    if range.length == 0 || range.offset.checked_add(range.length).is_none() {
                        return Err(invalid("invalid blob range"));
                    }
                }
                (&request.source, &request.manifest_digest, &[][..])
            }
        };
        super::registry::validate_component(source)?;
        super::registry::require_digest(manifest)?;
        if Uuid::parse_str(self.id()).is_err() {
            return Err(invalid("request_id must be a UUID"));
        }
        Ok((source, manifest, self.id(), nodes))
    }
}

pub(super) fn routes(jobs: Arc<Jobs>, token: AdminToken) -> Router {
    Router::new()
        .route("/v1/admin/nydus/seed-jobs", post(seed))
        .route("/v1/admin/nydus/seed-jobs/{job_id}", get(seed_status))
        .route("/v1/admin/nydus/prewarm-jobs", post(warm))
        .route("/v1/admin/nydus/prewarm-jobs/{job_id}", get(warm_status))
        .route_layer(middleware::from_fn_with_state(token, crate::management::require_admin))
        .with_state(jobs)
}

async fn seed(State(jobs): State<Arc<Jobs>>, Json(request): Json<SeedRequest>)
    -> std::result::Result<(StatusCode, Json<Status>), crate::error::RestError>
{
    Ok((StatusCode::ACCEPTED, Json(jobs.submit(Request::Seed(request))?)))
}

async fn warm(State(jobs): State<Arc<Jobs>>, Json(request): Json<WarmRequest>)
    -> std::result::Result<(StatusCode, Json<Status>), crate::error::RestError>
{
    Ok((StatusCode::ACCEPTED, Json(jobs.submit(Request::Warm(request))?)))
}

async fn seed_status(State(jobs): State<Arc<Jobs>>, Path(id): Path<String>)
    -> std::result::Result<Json<Status>, StatusCode>
{
    let id = Uuid::parse_str(&id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(jobs.status(id, true).ok_or(StatusCode::NOT_FOUND)?))
}

async fn warm_status(State(jobs): State<Arc<Jobs>>, Path(id): Path<String>)
    -> std::result::Result<Json<Status>, StatusCode>
{
    let id = Uuid::parse_str(&id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(jobs.status(id, false).ok_or(StatusCode::NOT_FOUND)?))
}

fn invalid(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}

fn busy(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_UNAVAILABLE, message)
}
