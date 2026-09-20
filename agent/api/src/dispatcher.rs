//! Gateway client: cached discovery and preferred Dispatcher, never instance-pool selection.
use crate::{Error, Result};
use adx_agent_core::{
    dispatcher::*,
    routing::{DispatcherMember, HashRing},
    Scope,
};
use adx_agent_core::{
    limits,
    transport::{service_origin, validate_service_token, RequestProgress},
};
use adx_agent_store::RedisRepository;
use async_trait::async_trait;
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{watch, Mutex as AsyncMutex};

#[async_trait]
pub trait Membership: Send + Sync {
    async fn members(&self) -> Result<Vec<DispatcherMember>>;
}
#[async_trait]
impl Membership for RedisRepository {
    async fn members(&self) -> Result<Vec<DispatcherMember>> {
        self.dispatchers().await.map_err(Into::into)
    }
}
#[async_trait]
pub trait Dispatch: Send + Sync {
    async fn resolve(&self, request: &ResolveRequest) -> Result<Target>;
    async fn release(&self, scope: &Scope) -> Result<()>;
    async fn release_instance(&self, scope: &Scope, instance_id: &str) -> Result<()>;
}
struct View {
    ring: HashRing,
    refreshed: Option<Instant>,
    attempted: Option<Instant>,
}
pub struct DispatcherClient {
    membership: Arc<dyn Membership>,
    client: Client,
    token: String,
    allow_http: bool,
    view: Mutex<View>,
    refresh_gate: AsyncMutex<()>,
    refresh_interval: Duration,
    timeout: Duration,
}
impl DispatcherClient {
    pub fn new(
        membership: Arc<dyn Membership>,
        token: String,
        timeout: Duration,
        ca_pem: Option<&[u8]>,
        allow_http: bool,
    ) -> Result<Self> {
        validate_service_token(&token).map_err(Error::Invalid)?;
        if timeout.is_zero() {
            return Err(Error::Invalid(
                "Dispatcher deadline must be positive".into(),
            ));
        }
        let mut builder = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(limits::CONNECT_TIMEOUT))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = ca_pem {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(pem)
                    .map_err(|_| Error::Invalid("invalid Dispatcher CA".into()))?,
            );
        }
        Ok(Self {
            membership,
            timeout,
            client: builder.build().map_err(|_| {
                Error::Invalid("Dispatcher HTTP client initialization failed".into())
            })?,
            token,
            allow_http,
            view: Mutex::new(View {
                ring: HashRing::default(),
                refreshed: None,
                attempted: None,
            }),
            refresh_gate: AsyncMutex::new(()),
            refresh_interval: Duration::from_secs(10),
        })
    }
    /// One shared discovery/failover budget plus Gateway response overhead.
    pub fn request_budget(&self) -> Duration {
        self.timeout
            .saturating_add(limits::DISPATCHER_FINISH_TIMEOUT)
    }
    fn origin(&self, address: &str) -> Result<Url> {
        service_origin(address, self.allow_http).map_err(Error::Unavailable)
    }
    pub async fn refresh(&self, force: bool) -> Result<()> {
        let _gate = self.refresh_gate.lock().await;
        {
            let view = self.view.lock().expect("Dispatcher view");
            if view
                .attempted
                .is_some_and(|t| t.elapsed() < Duration::from_millis(250))
            {
                return Ok(());
            }
            if !force
                && view
                    .refreshed
                    .is_some_and(|t| t.elapsed() < self.refresh_interval)
            {
                return Ok(());
            }
        }
        self.view.lock().expect("Dispatcher view").attempted = Some(Instant::now());
        let members = tokio::time::timeout(Duration::from_secs(3), self.membership.members())
            .await
            .map_err(|_| Error::Unavailable("Dispatcher discovery timed out".into()))??;
        for member in &members {
            self.origin(&member.address)?;
        }
        let mut view = self.view.lock().expect("Dispatcher view");
        view.ring = HashRing::new(members);
        view.refreshed = Some(Instant::now());
        Ok(())
    }
    async fn preferred(
        &self,
        scope: &Scope,
        exclude: Option<&DispatcherMember>,
    ) -> Result<DispatcherMember> {
        scope.validate().map_err(Error::Invalid)?;
        let refresh = self.refresh(false).await;
        let view = self.view.lock().expect("Dispatcher view");
        if view
            .refreshed
            .is_none_or(|t| t.elapsed() > Duration::from_secs(30))
        {
            return Err(refresh.err().unwrap_or_else(|| {
                Error::Unavailable("Dispatcher membership unavailable".into())
            }));
        }
        view.ring
            .preferred(scope)
            .filter(|m| Some(*m) != exclude)
            .or_else(|| view.ring.members().iter().find(|m| Some(*m) != exclude))
            .cloned()
            .ok_or_else(|| Error::Unavailable("no available Dispatcher".into()))
    }
    async fn once<T: Serialize + Sync>(
        &self,
        member: &DispatcherMember,
        path: &str,
        body: &T,
        deadline: tokio::time::Instant,
        deadline_ms: u64,
        progress: &RequestProgress,
    ) -> Result<Vec<u8>> {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| Error::Unavailable("Dispatcher request budget exhausted".into()))?;
        let url = self
            .origin(&member.address)?
            .join(path)
            .expect("static Dispatcher path");
        let request = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .header(DEADLINE_HEADER, deadline_ms.to_string())
            .timeout(remaining)
            .json(body);
        progress.start_write(); // A sent RPC can have modified remote state.
        let mut response = request.send().await.map_err(|e| {
            if e.is_connect() {
                Error::Unavailable("Dispatcher connection failed".into())
            } else {
                Error::OutcomeUnknown(
                    "Dispatcher response lost; original Session/affinity identity retained".into(),
                )
            }
        })?;
        let status = response.status();
        let mut bytes = vec![];
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::OutcomeUnknown("Dispatcher response interrupted".into()))?
        {
            if bytes.len() + chunk.len() > limits::HTTP_JSON_BYTES {
                return Err(Error::OutcomeUnknown(
                    "Dispatcher response exceeds limit".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if status.is_success() {
            Ok(bytes)
        } else {
            Err(serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                Error::Unavailable(format!("Dispatcher rejected request ({status})"))
            }))
        }
    }
    async fn call<T: Serialize + Sync>(
        &self,
        scope: &Scope,
        path: &str,
        body: &T,
        retry_selection: bool,
    ) -> Result<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let deadline_ms = adx_agent_core::unix_time_millis()
            .checked_add(
                u64::try_from(self.timeout.as_millis())
                    .map_err(|_| Error::Invalid("Dispatcher timeout overflow".into()))?,
            )
            .ok_or_else(|| Error::Invalid("Dispatcher deadline overflow".into()))?;
        let progress = RequestProgress::default();
        let work = async {
            let first = self.preferred(scope, None).await?;
            let result = self
                .once(&first, path, body, deadline, deadline_ms, &progress)
                .await;
            if matches!(
                result,
                Err(Error::Unavailable(_) | Error::OutcomeUnknown(_))
            ) && tokio::time::Instant::now() < deadline
            {
                let _ = self.refresh(true).await;
                // Selection may fail over once within the original budget. Lifecycle
                // mutations are never replayed automatically.
                if retry_selection && tokio::time::Instant::now() < deadline {
                    if let Ok(next) = self.preferred(scope, Some(&first)).await {
                        return self
                            .once(&next, path, body, deadline, deadline_ms, &progress)
                            .await;
                    }
                }
            }
            result
        };
        tokio::time::timeout_at(deadline, work)
            .await
            .unwrap_or_else(|_| {
                Err(if progress.may_have_written() {
                    Error::OutcomeUnknown(
                        "Dispatcher response exceeded the shared request deadline".into(),
                    )
                } else {
                    Error::Unavailable(
                        "Dispatcher discovery exceeded the shared request deadline".into(),
                    )
                })
            })
    }
    fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T> {
        serde_json::from_slice(body)
            .map_err(|_| Error::OutcomeUnknown("invalid Dispatcher success response".into()))
    }
    pub async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            tokio::select! {result=stop.changed()=>{if result.is_err()||*stop.borrow(){return;}},_=self.refresh(false)=>()}
            tokio::select! {result=stop.changed()=>{if result.is_err()||*stop.borrow(){return;}},_=tokio::time::sleep(self.refresh_interval)=>()}
        }
    }
}
#[async_trait]
impl Dispatch for DispatcherClient {
    async fn resolve(&self, request: &ResolveRequest) -> Result<Target> {
        let bytes = self
            .call(&request.scope, "/internal/adx/v1/resolve", request, true)
            .await?;
        let target: Target = Self::decode(&bytes)?;
        if target.scope != request.scope
            || target.session_generation.is_empty()
            || target.tenant != request.scope.tenant
            || target.instance_id.is_empty()
            || target.sandbox_id.is_empty()
        {
            return Err(Error::Unavailable(
                "Dispatcher target identity mismatch".into(),
            ));
        }
        Ok(target)
    }
    async fn release(&self, scope: &Scope) -> Result<()> {
        self.call(
            scope,
            "/internal/adx/v1/release",
            &ScopeRequest {
                scope: scope.clone(),
            },
            false,
        )
        .await?;
        Ok(())
    }
    async fn release_instance(&self, scope: &Scope, instance_id: &str) -> Result<()> {
        self.call(
            scope,
            "/internal/adx/v1/release-instance",
            &ReleaseInstanceRequest {
                scope: scope.clone(),
                instance_id: instance_id.into(),
            },
            false,
        )
        .await?;
        Ok(())
    }
}
