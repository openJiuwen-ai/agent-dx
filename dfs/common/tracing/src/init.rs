use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use afs_metrics::TraceRuntimeMetrics;
use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{
        BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, Span, SpanData,
        SpanExporter, SpanProcessor,
    },
};
use tracing_subscriber::{
    Layer as _, filter::LevelFilter, filter::Targets, layer::SubscriberExt as _,
    util::SubscriberInitExt as _,
};

use crate::{ProcessIdentity, TracingConfig, TracingError};

/// Keeps the provider alive and flushes the bounded exporter queue on shutdown.
pub struct TracingGuard {
    provider: Option<SdkTracerProvider>,
    // A synchronous SDK host may not have a Tokio reactor. In that case the
    // tracing runtime owns one small reactor for the tonic OTLP channel.
    owned_runtime: Option<tokio::runtime::Runtime>,
}

impl TracingGuard {
    pub fn shutdown(mut self) -> Result<(), TracingError> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<(), TracingError> {
        let Some(provider) = self.provider.take() else {
            return Ok(());
        };
        let result = provider
            .shutdown()
            .map_err(|error| TracingError::Shutdown(error.to_string()));
        drop(self.owned_runtime.take());
        result
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

pub fn init_process_tracing(
    config: &TracingConfig,
    identity: ProcessIdentity,
    metrics: Option<TraceRuntimeMetrics>,
) -> Result<TracingGuard, TracingError> {
    config.validate()?;
    afs_metrics::install_exemplar_provider(crate::current_exemplar);
    if !config.enabled {
        // 仅进程入口拥有 Subscriber。显式禁用可切断 tracing/log 在无
        // dispatcher 时的 Span→日志回退，业务 log facade 的桥接保持不变。
        // SDK 不调用本入口；宿主已有 Subscriber 时返回冲突而不是覆盖。
        tracing::subscriber::set_global_default(tracing::subscriber::NoSubscriber::default())
            .map_err(|error| TracingError::Subscriber(error.to_string()))?;
        return Ok(TracingGuard {
            provider: None,
            owned_runtime: None,
        });
    }

    let owned_runtime = if tokio::runtime::Handle::try_current().is_err() {
        Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|error| TracingError::Runtime(error.to_string()))?,
        )
    } else {
        None
    };
    // Tonic creates its lazy HTTP/2 channel through the currently entered
    // reactor. Node/Meta use their existing runtime; a synchronous host enters
    // the owned reactor only for initialization and keeps it alive in the guard.
    let _runtime_scope = owned_runtime.as_ref().map(tokio::runtime::Runtime::enter);

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp_endpoint.clone())
        .with_timeout(config.export_timeout)
        .build()
        .map_err(|error| TracingError::Exporter(error.to_string()))?;
    let queued = Arc::new(AtomicUsize::new(0));
    let exporter = InstrumentedExporter {
        inner: exporter,
        queued: Arc::clone(&queued),
        metrics: metrics.clone(),
    };
    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(config.queue_capacity)
        .with_max_export_batch_size(config.max_export_batch_size)
        .with_scheduled_delay(config.batch_interval)
        .build();
    let batch = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config)
        .build();
    let processor = InstrumentedProcessor {
        inner: batch,
        queued,
        max_queue: config.queue_capacity,
        metrics,
    };
    let resource = Resource::builder()
        .with_service_name(identity.service_name.clone())
        .with_attribute(KeyValue::new("service.instance.id", identity.instance))
        .build();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sample_ratio,
        ))))
        .with_span_processor(processor)
        .with_resource(resource)
        .build();
    let tracer = provider.tracer(identity.service_name);
    // Never export Hyper/Tonic/OTLP's own diagnostic spans through the same
    // exporter: doing so creates an observability feedback loop. AFS owns the
    // public span contract, while dependency diagnostics remain in logs.
    let afs_targets = Targets::new()
        .with_default(LevelFilter::OFF)
        .with_target("afs", LevelFilter::TRACE)
        .with_target("afs_client", LevelFilter::TRACE)
        .with_target("afs_server", LevelFilter::TRACE)
        .with_target("afs_tracing", LevelFilter::TRACE);
    tracing_subscriber::registry()
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(afs_targets),
        )
        .try_init()
        .map_err(|error| TracingError::Subscriber(error.to_string()))?;
    drop(_runtime_scope);

    Ok(TracingGuard {
        provider: Some(provider),
        owned_runtime,
    })
}

struct InstrumentedExporter<E> {
    inner: E,
    queued: Arc<AtomicUsize>,
    metrics: Option<TraceRuntimeMetrics>,
}

impl<E: fmt::Debug> fmt::Debug for InstrumentedExporter<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentedExporter")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<E> SpanExporter for InstrumentedExporter<E>
where
    E: SpanExporter,
{
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let batch_len = batch.len();
        subtract_saturating(&self.queued, batch_len);
        if let Some(metrics) = &self.metrics {
            metrics.set_queue_depth(self.queued.load(Ordering::Relaxed));
        }
        let started = Instant::now();
        let future = self.inner.export(batch);
        let metrics = self.metrics.clone();
        async move {
            let result = future.await;
            if let Some(metrics) = metrics {
                metrics.record_export_batch(batch_len, started.elapsed(), result.is_ok());
            }
            result
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

struct InstrumentedProcessor<P> {
    inner: P,
    queued: Arc<AtomicUsize>,
    max_queue: usize,
    metrics: Option<TraceRuntimeMetrics>,
}

impl<P> fmt::Debug for InstrumentedProcessor<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstrumentedProcessor")
            .field("max_queue", &self.max_queue)
            .finish_non_exhaustive()
    }
}

impl<P> SpanProcessor for InstrumentedProcessor<P>
where
    P: SpanProcessor,
{
    fn on_start(&self, span: &mut Span, context: &opentelemetry::Context) {
        self.inner.on_start(span, context);
    }

    fn on_end(&self, span: SpanData) {
        if !span.span_context.is_sampled() {
            return;
        }
        if self
            .queued
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value < self.max_queue).then_some(value + 1)
            })
            .is_err()
        {
            if let Some(metrics) = &self.metrics {
                metrics.record_dropped_span("queue_full");
            }
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.set_queue_depth(self.queued.load(Ordering::Relaxed));
        }
        self.inner.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

fn subtract_saturating(value: &AtomicUsize, amount: usize) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}
