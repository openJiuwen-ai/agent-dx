//! HIXL startup and the OwnerFs pieces that share its runtime.
//!
//! One runtime group serves every HIXL lane of the process, and it starts
//! before the first Meta registration so a required mode fails startup before
//! any mount exists. The group is shut down when the Node stops.

use crate::{
    config::{Config, DataMode},
    node::rpc,
    runtime::BoxError,
};
use std::{sync::Arc, time::Duration};

/// The started HIXL runtime plus the OwnerFs reader built on it.
#[derive(Default)]
pub(super) struct Hixl {
    runtime: Option<Arc<afs_transport::hixl::HixlRuntimeGroup>>,
    reader: Option<Arc<rpc::peer::owner_hixl::OwnerHixlReader>>,
    metrics: Option<rpc::hixl::HixlMetrics>,
}

impl Hixl {
    /// Starts the shared runtime for a configured HIXL candidate. Auto can
    /// skip a local runtime that explicitly reports unsupported.
    pub(super) async fn start(
        &mut self,
        cfg: &Config,
        obs: &crate::runtime::Observability,
        owner_metrics: &rpc::OwnerRpcMetrics,
        timeout: Duration,
    ) -> Result<(), BoxError> {
        {
            let configured = cfg.hixl.enabled(cfg.data_mode, cfg.ownerfs);
            self.runtime = if configured {
                let options = cfg.hixl.runtime_options_per_device();
                match tokio::task::spawn_blocking(move || {
                    afs_transport::hixl::HixlRuntimeGroup::start(options)
                })
                .await?
                {
                    Ok(runtime) => Some(runtime),
                    Err(error)
                        if cfg.data_mode == DataMode::Auto
                            && error.kind == afs_transport::hixl::HixlErrorKind::Unsupported =>
                    {
                        afs_logging::warn!("ownerfs.hixl_startup_fallback"; "reason" => "local capability unsupported");
                        None
                    }
                    Err(error) => return Err(rpc::hixl::runtime_error(error).into()),
                }
            } else {
                None
            };
            let metrics = if configured {
                let metrics = rpc::hixl::HixlMetrics::register(&obs.registry)?;
                match &self.runtime {
                    Some(runtime) => metrics.snapshot(&runtime.status()),
                    None => metrics.observe(rpc::hixl::Operation::Fallback, Duration::ZERO, true),
                }
                Some(metrics)
            } else {
                None
            };
            let reader = match &self.runtime {
                Some(runtime) => {
                    let mut reader = rpc::peer::owner_hixl::OwnerHixlReader::new(
                        runtime.clone(),
                        cfg.id.clone(),
                        rpc::peer::owner_hixl::OwnerHixlPolicy {
                            mode: cfg.data_mode,
                            snapshot_reads: cfg.hixl.ownerfs_snapshot_reads,
                            buffered_writes: cfg.hixl.ownerfs_buffered_writes,
                            max_group_bytes: cfg.hixl.ownerfs_read_group_bytes,
                            max_inflight: cfg.hixl.ownerfs_read_group_max_inflight,
                            host_budget_bytes: cfg.hixl.ownerfs_read_group_host_budget_bytes,
                            min_file_bytes: cfg.hixl.ownerfs_min_file_bytes,
                            min_write_bytes: cfg.hixl.ownerfs_min_write_bytes,
                            receive_slots: cfg.hixl.receive_slots,
                            sessions_per_home: cfg.hixl.ownerfs_sessions_per_home,
                            session_idle: Duration::from_millis(cfg.hixl.session_idle_ms),
                            batch_max_file_bytes: if cfg.hixl.ownerfs_batch_read {
                                cfg.hixl
                                    .ownerfs_batch_max_file_bytes
                                    .min(cfg.hixl.window_bytes as u64)
                            } else {
                                0
                            },
                        },
                        timeout,
                    )?
                    .with_owner_metrics(owner_metrics.clone());
                    if let Some(metrics) = &metrics {
                        reader = reader.with_metrics(metrics.clone());
                    }
                    Some(Arc::new(reader))
                }
                _ => None,
            };
            self.reader = reader;
            self.metrics = metrics;
            Ok(())
        }
    }

    pub(super) fn runtime(&self) -> Option<Arc<afs_transport::hixl::HixlRuntimeGroup>> {
        self.runtime.clone()
    }

    pub(super) fn reader(&self) -> Option<Arc<rpc::peer::owner_hixl::OwnerHixlReader>> {
        self.reader.clone()
    }

    pub(super) fn metrics(&self) -> Option<rpc::hixl::HixlMetrics> {
        self.metrics.clone()
    }

    /// Wait for the native lanes after the Node's services and mounts stop.
    pub(super) async fn shutdown(&mut self) -> Result<(), BoxError> {
        let Some(runtime) = self.runtime.take() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || runtime.shutdown())
            .await?
            .map_err(|error| rpc::hixl::runtime_error(error).into())
    }
}

/// Retires expired Home sessions every five seconds until shutdown. A failed
/// tick is logged and retried; it never ends the Node's other services.
pub(super) fn spawn_expiry(
    services: &mut crate::runtime::Services,
    source: Arc<rpc::data::owner_hixl::OwnerHixlService>,
) {
    const PERIOD: Duration = Duration::from_secs(5);
    let stop = services.stop.subscribe();
    services.spawn(async move {
        let mut tick = tokio::time::interval(PERIOD);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let shutdown = crate::runtime::cancelled(stop);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tick.tick() => {
                    if let Err(status) = source.expire().await {
                        afs_logging::warn!("ownerfs.hixl_expiry_failed"; "error" => status.message());
                    }
                }
            }
        }
    });
}
