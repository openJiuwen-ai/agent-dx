//! afs-node 的进程入口：配置解析 → 观测初始化 → 服务装配与运行。
//! 入口只做组装，不写文件业务。`--print-config` 在创建监听/挂载之前返回，
//! 方便检查 TOML 与 CLI 合并结果；其成功不表示服务已经启动。

use afs::{
    config::{Cli, Config, Role},
    runtime::{self, BoxError, Observability},
};
use clap::Parser;

#[cfg(all(target_os = "linux", feature = "node-jemalloc"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), BoxError> {
    let cfg = Config::resolve(Role::Node, Cli::parse())?;
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
