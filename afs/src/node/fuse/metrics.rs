//! Per-mount FUSE callback metrics.

/// Implemented first-party FUSE callback entries, including failed requests.
/// This is neither kernel wire opcode accounting nor application syscall accounting.
/// Read/write size histograms observe callback input bytes, not completed payloads.
#[derive(Clone)]
pub struct FuseRequestMetrics {
    pub(super) requests: afs_metrics::IntCounterVec,
    pub(super) io_size_bytes: afs_metrics::HistogramVec,
    pub(super) copy_range_duration: afs_metrics::HistogramVec,
    pub(super) write_duration: afs_metrics::HistogramVec,
    pub(super) front_write_admissions: afs_metrics::IntCounterVec,
}

impl FuseRequestMetrics {
    pub fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        registry.get_or_register(|registry| {
            let requests = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_fuse_callbacks_total", "Implemented FUSE callback entries received by filesystem and operation, including failures."),
                &["filesystem", "operation"],
            )?;
            let io_size_bytes = afs_metrics::HistogramVec::new(
                afs_metrics::HistogramOpts::new(
                    "afs_fuse_callback_size_bytes",
                    "Requested read and supplied write bytes at FUSE callback entry, including failures; not application syscall or completed payload sizes.",
                )
                .buckets(vec![4096.0, 65_536.0, 262_144.0, 1_048_576.0]),
                &["filesystem", "operation"],
            )?;
            afs_metrics::register_collector(registry, &requests)?;
            afs_metrics::register_collector(registry, &io_size_bytes)?;
            let copy_range_duration = afs_metrics::HistogramVec::new(
                afs_metrics::HistogramOpts::new(
                    "afs_fuse_copy_file_range_duration_seconds",
                    "Dispatched copy_file_range queue and execution wall time.",
                ),
                &["filesystem", "stage"],
            )?;
            afs_metrics::register_collector(registry, &copy_range_duration)?;
            let write_duration = afs_metrics::HistogramVec::new(
                afs_metrics::HistogramOpts::new(
                    "afs_fuse_write_duration_seconds",
                    "Dispatched write admission queue and backend execution wall time, including failures.",
                ).buckets(vec![0.0001, 0.001, 0.01, 0.1, 1.0, 5.0, 30.0, 60.0]),
                &["filesystem", "source", "stage"],
            )?;
            afs_metrics::register_collector(registry, &write_duration)?;
            let front_write_admissions = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_fuse_front_write_admissions_total",
                    "Ordinary queued writes by admission outcome: acknowledged at once, acknowledged after waiting, or rejected with the reason.",
                ),
                &["filesystem", "outcome"],
            )?;
            afs_metrics::register_collector(registry, &front_write_admissions)?;
            for filesystem in ["ownerfs", "dfs"] {
                for source in ["page_cache", "write_call"] {
                    for stage in ["queue", "execute"] {
                        write_duration.with_label_values(&[filesystem, source, stage]);
                    }
                }
                for stage in ["queue", "execute"] {
                    copy_range_duration.with_label_values(&[filesystem, stage]);
                }
                for operation in FUSE_CALLBACKS {
                    requests.with_label_values(&[filesystem, operation]);
                }
                for operation in ["read", "write"] {
                    io_size_bytes.with_label_values(&[filesystem, operation]);
                }
                for outcome in super::front_write::ADMISSION_OUTCOMES {
                    front_write_admissions.with_label_values(&[filesystem, outcome]);
                }
            }
            Ok(Self { requests, io_size_bytes, copy_range_duration, write_duration, front_write_admissions })
        })
    }

    pub(super) fn record(&self, filesystem: &'static str, operation: &'static str) {
        self.requests
            .with_label_values(&[filesystem, operation])
            .inc();
    }

    // Observe only the scalar callback size: no payload access, timing, or
    // per-request labels. Prometheus supplies the cumulative +Inf bucket.
    pub(super) fn record_io(
        &self,
        filesystem: &'static str,
        operation: &'static str,
        bytes: usize,
    ) {
        self.record(filesystem, operation);
        self.io_size_bytes
            .with_label_values(&[filesystem, operation])
            .observe(bytes as f64);
    }
}

pub(super) const FUSE_CALLBACKS: &[&str] = &[
    "init",
    "destroy",
    "lookup",
    "forget",
    "getattr",
    "statfs",
    "setattr",
    "readlink",
    "mkdir",
    "unlink",
    "rmdir",
    "symlink",
    "rename",
    "link",
    "open",
    "read",
    "write",
    "copy_file_range",
    "flush",
    "release",
    "fsync",
    "opendir",
    "readdir",
    "releasedir",
    "fsyncdir",
    "create",
    "getlk",
    "setlk",
    "mknod",
    "access",
    "getxattr",
    "listxattr",
    "setxattr",
    "removexattr",
];
