//! Event-driven alias convergence. Membership refresh is periodic; Binding scans are not.
use crate::{
    registration::{RegistrationConfig, RegistrationLease},
    Activator, Error, Result,
};
use adx_agent_core::{
    discovery::{ranked_endpoints, ActivatorEndpoint},
    BindingPhase, Scope, TemplateRef,
};
use adx_agent_store::{discovery::RedisRegistry, MetadataChange, RedisChanges, RedisRepository};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::Instant,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReconcileSettings {
    pub concurrency: usize,
    pub request_timeout_seconds: u64,
    pub membership_refresh_seconds: u64,
}
impl Default for ReconcileSettings {
    fn default() -> Self {
        Self {
            concurrency: 8,
            request_timeout_seconds: 60,
            membership_refresh_seconds: 5,
        }
    }
}
impl ReconcileSettings {
    fn validate(&self) -> Result<()> {
        if !(1..=128).contains(&self.concurrency)
            || !(1..=3600).contains(&self.request_timeout_seconds)
            || !(1..=300).contains(&self.membership_refresh_seconds)
        {
            return Err(Error::Invalid("invalid alias reconciliation limits".into()));
        }
        Ok(())
    }
}

pub struct Reconciliation {
    task: Option<JoinHandle<()>>,
    registration: Option<RegistrationLease>,
}
impl Drop for Reconciliation {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl Reconciliation {
    /// Validates notifications and membership before returning. Registration uses the same
    /// registry as request affinity. All restarts subscribe first, then scan durable state.
    pub async fn start(
        activator: Arc<Activator>,
        store: RedisRepository,
        registry: RedisRegistry,
        registration: RegistrationConfig,
        settings: ReconcileSettings,
    ) -> Result<Self> {
        settings.validate()?;
        let changes = store.subscribe_changes().await?;
        let id = registration.instance_id.clone();
        let lease = RegistrationLease::start(registry.clone(), registration, true).await?;
        let members = registry.members().await?;
        activator.subscription_ready.store(false, Ordering::Release);
        let task = tokio::spawn(async move {
            let mut changes = Some(changes);
            let mut members = members;
            loop {
                activator.subscription_ready.store(false, Ordering::Release);
                activator.invalidate_all().await;
                let subscription = match changes.take() {
                    Some(subscription) => Ok(subscription),
                    None => store.subscribe_changes().await.map_err(Error::from),
                };
                match subscription {
                    Ok(mut subscription) => {
                        let result = run(
                            &activator,
                            &registry,
                            &id,
                            &settings,
                            &mut members,
                            &mut subscription,
                        )
                        .await;
                        if let Err(error) = result {
                            tracing::warn!(%error,"alias reconciliation restarting from durable state");
                        }
                    }
                    Err(error) => tracing::warn!(%error,"alias change subscription unavailable"),
                }
                activator.subscription_ready.store(false, Ordering::Release);
                activator.invalidate_all().await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Ok(Self {
            task: Some(task),
            registration: Some(lease),
        })
    }
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(registration) = self.registration.take() {
            registration.shutdown().await?;
        }
        Ok(())
    }
}

fn owns(scope: &Scope, id: &str, members: &[ActivatorEndpoint]) -> bool {
    ranked_endpoints(scope, members)
        .first()
        .is_some_and(|endpoint| endpoint.id == id)
}

async fn scan(
    activator: &Activator,
    id: &str,
    members: &[ActivatorEndpoint],
    pending: &mut BTreeMap<Scope, Instant>,
    filter: Option<(&str, &str, &str)>,
) -> Result<()> {
    let index = match filter {
        Some((tenant, template, alias)) => {
            adx_agent_store::Index::bindings(tenant, template, &TemplateRef::Alias(alias.into()))?
        }
        None => adx_agent_store::Index::all_bindings(),
    };
    let mut after = None;
    loop {
        let page = activator
            .state
            .binding_records(&index, after.as_deref(), 128)
            .await?;
        if page.is_empty() {
            return Ok(());
        }
        for (key, binding) in page {
            after = Some(key);
            if !owns(&binding.scope, id, members) {
                continue;
            }
            // Stable fixed-version bindings do not acquire upgrade work.
            if matches!(binding.scope.template_ref, TemplateRef::Version(_))
                && binding.phase == BindingPhase::Active
                && binding.secondary.is_none()
            {
                continue;
            }
            pending.insert(binding.scope, Instant::now());
        }
    }
}

async fn run(
    activator: &Arc<Activator>,
    registry: &RedisRegistry,
    id: &str,
    settings: &ReconcileSettings,
    members: &mut Vec<ActivatorEndpoint>,
    changes: &mut RedisChanges,
) -> Result<()> {
    let mut pending = BTreeMap::new();
    members.sort_by(|a, b| a.id.cmp(&b.id));
    scan(activator, id, members, &mut pending, None).await?;
    activator.subscription_ready.store(true, Ordering::Release);
    let mut running = BTreeSet::new();
    let mut jobs: JoinSet<(Scope, Result<bool>, Option<Instant>)> = JoinSet::new();
    let mut refresh =
        tokio::time::interval(Duration::from_secs(settings.membership_refresh_seconds));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut wake = tokio::time::interval(Duration::from_millis(100));
    wake.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            event=changes.recv()=>match event? {
                MetadataChange::Alias{tenant,template,alias}=>scan(activator,id,members,&mut pending,Some((&tenant,&template,&alias))).await?,
                MetadataChange::Binding(scope)=>{
                    activator.invalidate(&scope).await;
                    if owns(&scope,id,members) { pending.insert(scope,Instant::now()); }
                }
            },
            _=refresh.tick()=>match registry.members().await {
                Ok(mut snapshot)=>{
                    snapshot.sort_by(|a,b|a.id.cmp(&b.id));
                    if snapshot!=*members {
                        *members=snapshot;
                        pending.retain(|scope,_|owns(scope,id,members));
                        activator.invalidate_all().await;
                        scan(activator,id,members,&mut pending,None).await?;
                    }
                }
                Err(error)=>tracing::warn!(%error,"Activator membership refresh failed; retaining snapshot"),
            },
            completed=jobs.join_next(), if !jobs.is_empty()=>{
                let (scope,result,retry_at)=completed.ok_or_else(||Error::Unavailable("reconcile worker disappeared".into()))?
                    .map_err(|_|Error::Unavailable("reconcile worker failed; rescan required".into()))?;
                running.remove(&scope);
                if owns(&scope,id,members) {
                    match result {
                        Ok(true)=>(),
                        Ok(false)=>{pending.entry(scope).or_insert(Instant::now()+Duration::from_secs(1));},
                        Err(Error::CreationDeferred { .. })=>{
                            pending.insert(scope, retry_at.unwrap_or_else(Instant::now));
                        }
                        Err(error)=>{
                            tracing::warn!(%error,?scope,"Binding convergence waiting or blocked");
                            if !matches!(error,Error::Invalid(_)|Error::Unsupported(_)) {
                                pending.entry(scope).or_insert(Instant::now()+Duration::from_secs(5));
                            }
                        }
                    }
                }
            },
            _=wake.tick()=>{},
        }
        let ready: Vec<_> = pending
            .iter()
            .filter(|(scope, at)| **at <= Instant::now() && !running.contains(*scope))
            .take(settings.concurrency.saturating_sub(jobs.len()))
            .map(|(scope, _)| scope.clone())
            .collect();
        for scope in ready {
            pending.remove(&scope);
            if !owns(&scope, id, members) {
                continue;
            }
            running.insert(scope.clone());
            let worker = activator.clone();
            let timeout = Duration::from_secs(settings.request_timeout_seconds);
            jobs.spawn(async move {
                let deadline = adx_agent_core::transport::capped_deadline(None, timeout);
                let result =
                    match tokio::time::timeout(timeout, worker.reconcile_binding(&scope, deadline))
                        .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(Error::OutcomeUnknown(
                            "reconciliation step timed out; inspect saved Env".into(),
                        )),
                    };
                let retry_at = match &result {
                    Err(Error::CreationDeferred {
                        retry_after_unix_ms,
                    }) => Some(
                        Instant::now()
                            + Duration::from_millis(
                                retry_after_unix_ms
                                    .saturating_sub(adx_agent_core::unix_time_millis())
                                    .min(60_000),
                            ),
                    ),
                    _ => None,
                };
                (scope, result, retry_at)
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_agent_core::environment::*;
    use adx_agent_store::{Index, Key, MemoryRepository, Record, Repository, Transaction};

    #[derive(Default)]
    struct ObservedRepository {
        memory: MemoryRepository,
        pages: std::sync::Mutex<Vec<Index>>,
    }
    #[async_trait::async_trait]
    impl Repository for ObservedRepository {
        async fn get(&self, key: &Key) -> adx_agent_store::Result<Option<Record>> {
            self.memory.get(key).await
        }
        async fn commit(&self, tx: &Transaction) -> adx_agent_store::Result<bool> {
            self.memory.commit(tx).await
        }
        async fn page(
            &self,
            index: &Index,
            after: Option<&str>,
            limit: usize,
        ) -> adx_agent_store::Result<Vec<(String, Record)>> {
            self.pages.lock().unwrap().push(index.clone());
            self.memory.page(index, after, limit).await
        }
    }
    struct UnusedEnvironment;
    #[async_trait::async_trait]
    impl Environment for UnusedEnvironment {
        async fn get(
            &self,
            _: &str,
            _: &str,
        ) -> std::result::Result<Option<EnvironmentObservation>, EnvironmentError> {
            panic!("scanning must not query Platform")
        }
        async fn create(
            &self,
            _: &CreateEnvironment,
        ) -> std::result::Result<EnvironmentObservation, EnvironmentError> {
            panic!("scanning must not create an Env")
        }
        async fn delete(
            &self,
            _: &str,
            _: &str,
        ) -> std::result::Result<EnvironmentObservation, EnvironmentError> {
            panic!("scanning must not delete an Env")
        }
    }
    #[tokio::test]
    async fn alias_scan_reads_only_the_matching_index() {
        let repository = Arc::new(ObservedRepository::default());
        let state = adx_agent_store::AgentState::new(repository.clone());
        let draft=serde_json::from_value(serde_json::json!({"name":"agent","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
        state.publish("tenant", &draft).await.unwrap();
        for alias in ["production", "other"] {
            state.put_alias(&serde_json::from_value(serde_json::json!({"tenant":"tenant","template":"agent","alias":alias,"seed":"stable","versions":[{"version":"1","weight":1}]})).unwrap(),None).await.unwrap();
            state
                .create_binding(Scope {
                    tenant: "tenant".into(),
                    template: "agent".into(),
                    template_ref: TemplateRef::Alias(alias.into()),
                    binding_id: "workspace".into(),
                })
                .await
                .unwrap();
        }
        let activator = Activator::new(state, Arc::new(UnusedEnvironment));
        let members = vec![ActivatorEndpoint {
            id: "owner".into(),
            url: "http://owner:8091".into(),
        }];
        let mut pending = BTreeMap::new();
        scan(
            &activator,
            "owner",
            &members,
            &mut pending,
            Some(("tenant", "agent", "production")),
        )
        .await
        .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending.keys().next().unwrap().template_ref,
            TemplateRef::Alias("production".into())
        );
        let expected =
            Index::bindings("tenant", "agent", &TemplateRef::Alias("production".into())).unwrap();
        assert!(
            repository
                .pages
                .lock()
                .unwrap()
                .iter()
                .all(|index| *index == expected),
            "an alias event must not scan all bindings"
        );
    }
}
