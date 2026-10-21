//! afs-node 的进程入口：配置解析 → 观测初始化 → 服务装配与运行。
//! 入口只做组装，不写文件业务。`--print-config` 在创建监听/挂载之前返回，
//! 方便检查 TOML 与 CLI 合并结果；其成功不表示服务已经启动。

use afs::{
    config::{Cli, Config, Role},
    runtime::{self, BoxError, Observability},
};

#[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), BoxError> {
    let cfg = Config::resolve(Role::Node, Cli::parse_process())?;
    if cfg.print_config {
        println!("{}", serde_json::to_string_pretty(&cfg)?);
        return Ok(());
    }
    #[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
    let allocator_initialized_arenas = allocator::initialize()?;
    // processctl's default stop wait is 20s. Leave a margin for the caller
    // to observe failure. Register the independent signal observer before the
    // business executor so its polling cannot delay arming the process budget.
    let deadline = runtime::ShutdownDeadline::for_process(std::time::Duration::from_secs(15))?;
    let trigger = deadline.trigger();
    let executor = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let obs = Observability::new()?;
    // Guard 必须留到 run 返回之后，退出时才能刷新日志与导出尚未发送的 Trace。
    let guards = runtime::initialize(&cfg, "afs-node", &obs)?;
    #[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
    allocator::report("startup");
    #[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
    afs_logging::info!("Node allocator initialized before business runtime";
        "configured_initialized_arenas" => allocator_initialized_arenas);
    #[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
    let allocator_sampler = match allocator::start_sampler(&executor, &obs.registry) {
        Ok(sampler) => Some(sampler),
        Err(error) => {
            afs_logging::warn!("Node allocator sampler unavailable"; "error" => error.to_string());
            None
        }
    };
    #[cfg(not(all(target_os = "linux", feature = "node-jemalloc")))]
    afs_logging::info!("Node allocator"; "allocator" => "system");
    let result = executor.block_on(afs::node::run_with_shutdown(cfg, obs, trigger.clone()));
    // Includes failure-path cleanup and runtime blocking workers. A running
    // worker is still owned; returning from run does not mean it was cancelled.
    trigger.arm();
    #[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
    if let Some(sampler) = allocator_sampler {
        sampler.abort();
        executor.block_on(async {
            let _ = sampler.await;
        });
    }
    drop(executor);
    #[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
    allocator::report("shutdown");
    drop(guards);
    deadline.complete();
    result
}

#[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
mod allocator {
    use afs::runtime::BoxError;
    use afs_metrics::{IntCounter, IntGauge, Registry};
    use tikv_jemalloc_ctl::{arenas, background_thread, epoch, raw, stats, version};

    const DIRTY_DECAY_MS: isize = 1_000;
    const MUZZY_DECAY_MS: isize = 0;

    struct Metrics {
        allocated: IntGauge,
        active: IntGauge,
        resident: IntGauge,
        retained: IntGauge,
        background_thread: IntGauge,
        dirty_decay: IntGauge,
        muzzy_decay: IntGauge,
        sample_valid: IntGauge,
        sample_errors: IntCounter,
    }

    impl Metrics {
        fn register(registry: &Registry) -> Result<Self, BoxError> {
            fn gauge(registry: &Registry, name: &str, help: &str) -> Result<IntGauge, BoxError> {
                let value = IntGauge::new(name, help)?;
                registry.register(Box::new(value.clone()))?;
                Ok(value)
            }
            let sample_errors = IntCounter::new(
                "afs_node_allocator_sample_errors_total",
                "Failed jemalloc observations; never used for business admission",
            )?;
            registry.register(Box::new(sample_errors.clone()))?;
            Ok(Self {
                allocated: gauge(
                    registry,
                    "afs_node_allocator_allocated_bytes",
                    "jemalloc live application allocations; distinct from write-buffer live capacity",
                )?,
                active: gauge(
                    registry,
                    "afs_node_allocator_active_bytes",
                    "jemalloc active pages",
                )?,
                resident: gauge(
                    registry,
                    "afs_node_allocator_resident_bytes",
                    "jemalloc resident estimate; distinct from Linux process RSS",
                )?,
                retained: gauge(
                    registry,
                    "afs_node_allocator_retained_bytes",
                    "jemalloc retained virtual mappings; not physical RSS",
                )?,
                background_thread: gauge(
                    registry,
                    "afs_node_allocator_background_thread_enabled",
                    "Actual jemalloc background-thread control",
                )?,
                dirty_decay: gauge(
                    registry,
                    "afs_node_allocator_dirty_decay_ms",
                    "jemalloc decay default for future arenas in milliseconds",
                )?,
                muzzy_decay: gauge(
                    registry,
                    "afs_node_allocator_muzzy_decay_ms",
                    "jemalloc muzzy decay default for future arenas in milliseconds",
                )?,
                sample_valid: gauge(
                    registry,
                    "afs_node_allocator_sample_valid",
                    "One when the latest complete allocator observation succeeded",
                )?,
                sample_errors,
            })
        }

        fn update(&self) -> Result<(), BoxError> {
            epoch::advance()?;
            // Collect and convert all fields before publishing an observation;
            // an error leaves old values explicitly invalid, never partly new.
            let allocated = i64::try_from(stats::allocated::read()?)?;
            let active = i64::try_from(stats::active::read()?)?;
            let resident = i64::try_from(stats::resident::read()?)?;
            let retained = i64::try_from(stats::retained::read()?)?;
            let background = i64::from(background_thread::read()?);
            let (dirty, muzzy) = decay()?;
            let dirty = i64::try_from(dirty)?;
            let muzzy = i64::try_from(muzzy)?;
            self.allocated.set(allocated);
            self.active.set(active);
            self.resident.set(resident);
            self.retained.set(retained);
            self.background_thread.set(background);
            self.dirty_decay.set(dirty);
            self.muzzy_decay.set(muzzy);
            self.sample_valid.set(1);
            Ok(())
        }

        fn sample(&self, previously_failed: bool) -> bool {
            if let Err(error) = self.update() {
                self.sample_valid.set(0);
                self.sample_errors.inc();
                if !previously_failed {
                    afs_logging::warn!("Node allocator observation failed"; "error" => error.to_string());
                }
                true
            } else {
                false
            }
        }
    }

    pub(super) fn start_sampler(
        executor: &tokio::runtime::Runtime,
        registry: &Registry,
    ) -> Result<tokio::task::JoinHandle<()>, BoxError> {
        let metrics = Metrics::register(registry)?;
        let mut failed = metrics.sample(false);
        Ok(executor.spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                failed = metrics.sample(failed);
            }
        }))
    }

    pub(super) fn initialize() -> Result<u32, BoxError> {
        // Configure future arenas first, including any created by control
        // lookup below. Per-arena decay controls require a real arena index;
        // MALLCTL_ARENAS_ALL is not supported by these setters.
        // SAFETY: These literal, NUL-terminated jemalloc 5.3.1 controls
        // accept ssize_t, represented by isize on every supported Linux target.
        unsafe {
            raw::write(b"arenas.dirty_decay_ms\0", DIRTY_DECAY_MS)?;
            raw::write(b"arenas.muzzy_decay_ms\0", MUZZY_DECAY_MS)?;
        }
        let mut initialized_arenas = 0;
        for index in 0..arenas::narenas::read()? {
            let initialized = format!("arena.{index}.initialized\0");
            // SAFETY: The index is within jemalloc's reported arena range and
            // this internally generated, NUL-terminated control returns bool.
            if !unsafe { raw::read::<bool>(initialized.as_bytes())? } {
                continue;
            }
            let dirty = format!("arena.{index}.dirty_decay_ms\0");
            let muzzy = format!("arena.{index}.muzzy_decay_ms\0");
            // SAFETY: Only initialized real arena indices are used. Both
            // internally generated, NUL-terminated controls accept/return
            // ssize_t, exactly isize on Linux. No arena is destroyed here.
            let applied = unsafe {
                raw::write(dirty.as_bytes(), DIRTY_DECAY_MS)?;
                raw::write(muzzy.as_bytes(), MUZZY_DECAY_MS)?;
                (
                    raw::read::<isize>(dirty.as_bytes())?,
                    raw::read::<isize>(muzzy.as_bytes())?,
                )
            };
            if applied != (DIRTY_DECAY_MS, MUZZY_DECAY_MS) {
                return Err(std::io::Error::other(
                    "Node jemalloc existing-arena decay configuration did not apply",
                )
                .into());
            }
            initialized_arenas += 1;
        }
        // Enabling after allocator initialization avoids bootstrap recursion.
        background_thread::write(true)?;
        let (dirty, muzzy) = decay()?;
        if !background_thread::read()? || dirty != DIRTY_DECAY_MS || muzzy != MUZZY_DECAY_MS {
            return Err(std::io::Error::other(
                "Node jemalloc reclamation configuration did not apply",
            )
            .into());
        }
        Ok(initialized_arenas)
    }

    fn decay() -> Result<(isize, isize), BoxError> {
        // SAFETY: Both literal controls return ssize_t; isize has exactly
        // that representation on Linux. No caller-supplied key/type is used.
        unsafe {
            Ok((
                raw::read(b"arenas.dirty_decay_ms\0")?,
                raw::read(b"arenas.muzzy_decay_ms\0")?,
            ))
        }
    }

    pub(super) fn report(phase: &'static str) {
        let result = (|| -> Result<(), BoxError> {
            epoch::advance()?;
            let (dirty, muzzy) = decay()?;
            afs_logging::info!("Node allocator";
                "phase" => phase,
                "allocator" => "jemalloc",
                "version" => version::read()?,
                "background_thread" => background_thread::read()?,
                "dirty_decay_ms" => dirty,
                "muzzy_decay_ms" => muzzy,
                "allocated_bytes" => stats::allocated::read()?,
                "active_bytes" => stats::active::read()?,
                "resident_bytes" => stats::resident::read()?,
                "retained_bytes" => stats::retained::read()?);
            Ok(())
        })();
        if let Err(error) = result {
            afs_logging::warn!("Node allocator statistics unavailable";
                "phase" => phase, "error" => error.to_string());
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        static ALLOCATOR_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

        #[test]
        fn node_allocator_applies_decay_and_accounts_rust_body() {
            let _serial = ALLOCATOR_TESTS.lock().unwrap();
            assert!(initialize().unwrap() >= 1);
            let allocated = tikv_jemalloc_ctl::thread::allocatedp::read().unwrap();
            let deallocated = tikv_jemalloc_ctl::thread::deallocatedp::read().unwrap();
            epoch::advance().unwrap();
            let before = stats::allocated::read().unwrap();
            let thread_before = allocated.get();
            let mut body = Vec::<u8>::new();
            body.try_reserve_exact(8 * 1024 * 1024).unwrap();
            body.resize(8 * 1024 * 1024, 0xa5);
            std::hint::black_box(body.as_mut_slice());
            let thread_during = allocated.get();
            epoch::advance().unwrap();
            let during = stats::allocated::read().unwrap();
            // A process-wide current-live delta also includes test-harness
            // frees. Attribute the body to this thread's monotonic allocator
            // counter instead; no epoch/tcache merge is needed for that API.
            assert!(thread_during >= thread_before + body.capacity() as u64);
            assert!(
                during >= body.len(),
                "process allocated before={before}, during={during}"
            );
            assert!(background_thread::read().unwrap());
            assert_eq!(decay().unwrap(), (DIRTY_DECAY_MS, MUZZY_DECAY_MS));
            assert_eq!(body[body.len() - 1], 0xa5);
            let release_before = deallocated.get();
            let capacity = body.capacity() as u64;
            std::hint::black_box(&body);
            drop(body);
            assert!(deallocated.get() >= release_before + capacity);
        }

        #[test]
        fn allocator_observation_uses_fixed_process_registry_metrics() {
            let _serial = ALLOCATOR_TESTS.lock().unwrap();
            let registry = Registry::new();
            let metrics = Metrics::register(&registry).unwrap();
            metrics.update().unwrap();
            assert_eq!(metrics.sample_valid.get(), 1);
            assert_eq!(metrics.sample_errors.get(), 0);
            assert!(metrics.allocated.get() > 0);
            let families = registry.gather();
            assert_eq!(families.len(), 9);
            for family in families {
                assert!(family.name().starts_with("afs_node_allocator_"));
                assert_eq!(family.get_metric().len(), 1);
                assert!(family.get_metric()[0].get_label().is_empty());
            }
        }

        #[test]
        fn cp09v2_actual_tonic_server_encoder_retained_allocation_is_bounded() {
            use afs_protocol::node_data::{
                DfsReadRangeReply, DfsReadRangesFrame, DfsReadSessionEnd, DfsReadSessionFrame,
                dfs_read_ranges_frame, dfs_read_session_frame,
            };
            use http_body::Body;
            use prost::Message;
            use tonic::codec::{EncodeBody, EncodeBuf, Encoder};

            struct SessionEncoder;
            impl Encoder for SessionEncoder {
                type Item = DfsReadSessionFrame;
                type Error = tonic::Status;
                fn encode(
                    &mut self,
                    frame: Self::Item,
                    buf: &mut EncodeBuf<'_>,
                ) -> Result<(), Self::Error> {
                    frame
                        .encode(buf)
                        .map_err(|error| tonic::Status::internal(error.to_string()))
                }
            }
            let _serial = ALLOCATOR_TESTS.lock().unwrap();
            initialize().unwrap();
            let allocated = tikv_jemalloc_ctl::thread::allocatedp::read().unwrap();
            let deallocated = tikv_jemalloc_ctl::thread::deallocatedp::read().unwrap();
            let executor = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let frames = std::sync::Arc::new(vec![
                DfsReadSessionFrame {
                    body: Some(dfs_read_session_frame::Body::Frame(DfsReadRangesFrame {
                        body: Some(dfs_read_ranges_frame::Body::Compact(DfsReadRangeReply {
                            operation_index: u32::MAX,
                            data: vec![73; 128 * 1024].into(),
                            range_checksum: vec![19; 32],
                        })),
                    })),
                },
                DfsReadSessionFrame {
                    body: Some(dfs_read_session_frame::Body::End(DfsReadSessionEnd {
                        read_id: "r".repeat(128),
                        attempt_id: "a".repeat(128),
                    })),
                },
            ]);
            for pending_between_messages in [false, true] {
                let source = frames.clone();
                let mut index = 0;
                let mut pending = false;
                // poll_fn for Stream uses an explicit small local adapter;
                // fixture message owners remain outside the measured lifetime.
                struct Frames<F>(F);
                impl<F> tokio_stream::Stream for Frames<F>
                where
                    F: FnMut(
                            &mut std::task::Context<'_>,
                        )
                            -> std::task::Poll<Option<Result<DfsReadSessionFrame, tonic::Status>>>
                        + Unpin,
                {
                    type Item = Result<DfsReadSessionFrame, tonic::Status>;
                    fn poll_next(
                        mut self: std::pin::Pin<&mut Self>,
                        cx: &mut std::task::Context<'_>,
                    ) -> std::task::Poll<Option<Self::Item>> {
                        (self.0)(cx)
                    }
                }
                let source = Frames(move |cx: &mut std::task::Context<'_>| {
                    if pending {
                        pending = false;
                        cx.waker().wake_by_ref();
                        return std::task::Poll::Pending;
                    }
                    if index == 128 {
                        return std::task::Poll::Ready(None);
                    }
                    let frame = source[index % 2].clone();
                    index += 1;
                    pending = pending_between_messages;
                    std::task::Poll::Ready(Some(Ok(frame)))
                });
                let mut delivered_bytes = 0;
                let a0 = allocated.get();
                let d0 = deallocated.get();
                let mut body = Box::pin(EncodeBody::new_server(
                    SessionEncoder,
                    source,
                    None,
                    Default::default(),
                    Some(128 * 1024 + 128),
                ));
                let mut peak = 0;
                while let Some(frame) =
                    executor.block_on(std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)))
                {
                    let frame = frame.unwrap();
                    let retained = (allocated.get() - a0).saturating_sub(deallocated.get() - d0);
                    peak = peak.max(retained);
                    if let Ok(bytes) = frame.into_data() {
                        delivered_bytes += bytes.len();
                    }
                }
                assert!(delivered_bytes >= 64 * 128 * 1024);
                assert!(
                    peak <= 512 * 1024,
                    "server encoder retained={peak}, pending={pending_between_messages}"
                );
                drop(body);
                assert_eq!(std::sync::Arc::strong_count(&frames), 1);
                eprintln!(
                    "cp09v2 actual server encoder retained: pending={pending_between_messages}, peak_bytes={peak}"
                );
            }
        }

        #[test]
        fn cp09v2_actual_tonic_decoder_retained_allocation_is_bounded() {
            use afs_protocol::node_data::{
                DfsReadRangeReply, DfsReadRangesFrame, DfsReadSessionEnd, DfsReadSessionFrame,
                dfs_read_ranges_frame, dfs_read_session_frame,
            };
            use prost::Message;
            use tokio_stream::StreamExt;
            use tonic::codec::{DecodeBuf, Decoder, Streaming};

            struct SessionDecoder;
            impl Decoder for SessionDecoder {
                type Item = DfsReadSessionFrame;
                type Error = tonic::Status;
                fn decode(
                    &mut self,
                    buf: &mut DecodeBuf<'_>,
                ) -> Result<Option<Self::Item>, Self::Error> {
                    Message::decode(buf)
                        .map(Some)
                        .map_err(|error| tonic::Status::internal(error.to_string()))
                }
            }

            let _serial = ALLOCATOR_TESTS.lock().unwrap();
            initialize().unwrap();
            let executor = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let allocated = tikv_jemalloc_ctl::thread::allocatedp::read().unwrap();
            let deallocated = tikv_jemalloc_ctl::thread::deallocatedp::read().unwrap();
            let compact = DfsReadSessionFrame {
                body: Some(dfs_read_session_frame::Body::Frame(DfsReadRangesFrame {
                    body: Some(dfs_read_ranges_frame::Body::Compact(DfsReadRangeReply {
                        operation_index: 0,
                        data: vec![73; 128 * 1024].into(),
                        range_checksum: vec![19; 32],
                    })),
                })),
            };
            let end = DfsReadSessionFrame {
                body: Some(dfs_read_session_frame::Body::End(DfsReadSessionEnd {
                    read_id: "read".to_owned(),
                    attempt_id: "attempt".to_owned(),
                })),
            };
            let mut wire = Vec::new();
            for _ in 0..64 {
                for frame in [&compact, &end] {
                    let message = frame.encode_to_vec();
                    wire.push(0);
                    wire.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
                    wire.extend_from_slice(&message);
                }
            }
            let wire = prost::bytes::Bytes::from(wire);
            for pattern in [&[16384_usize][..], &[131072][..], &[1, 4, 8192, 131072][..]] {
                let mut chunks = Vec::new();
                let mut offset = 0;
                while offset < wire.len() {
                    let length = pattern[chunks.len() % pattern.len()].min(wire.len() - offset);
                    chunks.push(wire.slice(offset..offset + length));
                    offset += length;
                }
                // Hold all fixture owners outside the measured codec lifetime:
                // their frees must not hide decoder allocation in the delta.
                let chunks = std::sync::Arc::new(chunks);
                let source = chunks.clone();
                let body = axum::body::Body::from_stream(
                    tokio_stream::iter(0..source.len())
                        .map(move |index| Ok::<_, std::io::Error>(source[index].clone())),
                );
                let a0 = allocated.get();
                let d0 = deallocated.get();
                let mut stream = Streaming::new_response(
                    SessionDecoder,
                    body,
                    axum::http::StatusCode::OK,
                    None,
                    Some(128 * 1024 + 128),
                );
                let mut peak = 0;
                for index in 0..128 {
                    let frame = executor.block_on(stream.message()).unwrap().unwrap();
                    if index % 2 == 0 {
                        assert_eq!(frame, compact);
                    } else {
                        assert_eq!(frame, end);
                    }
                    let retained = (allocated.get() - a0).saturating_sub(deallocated.get() - d0);
                    peak = peak.max(retained);
                    drop(frame);
                }
                assert!(
                    peak <= 768 * 1024,
                    "retained decoder/frame allocation={peak}, pattern={pattern:?}"
                );
                drop(stream);
                assert_eq!(std::sync::Arc::strong_count(&chunks), 1);
                eprintln!(
                    "cp09v2 actual tonic decoder retained allocation: pattern={pattern:?}, peak_bytes={peak}"
                );
            }
            // This observes real pinned tonic decoding and allocator-backed live
            // owners at every message. It is not a transient malloc high-water,
            // HTTP2-window measurement, server encoder result or performance run.
        }

        #[test]
        fn allocator_sampler_is_cancelled_and_joined_before_runtime_drop() {
            let _serial = ALLOCATOR_TESTS.lock().unwrap();
            let executor = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let registry = Registry::new();
            let sampler = start_sampler(&executor, &registry).unwrap();
            sampler.abort();
            assert!(executor.block_on(sampler).unwrap_err().is_cancelled());
            assert_eq!(registry.gather().len(), 9);
        }
    }
}
