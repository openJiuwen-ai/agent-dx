//! Node 内部通信边界。
//!
//! control/data 接收其他 Node 请求；peer 调用其他 Node；meta 调用中心。
//! 控制统一 gRPC，文件内容可用 gRPC 或单边 RDMA。通道选择不改变业务成功合同。
//! 本模块理解文件操作，公共 transport 只处理传输机制；业务与资源锁归业务所有者。

pub mod control;
pub mod data;
pub mod meta;
pub mod peer;

#[cfg(feature = "ownerfs")]
use afs_metrics::{
    HistogramOpts, HistogramVec, IntCounterVec, MetricsError, Opts, Registry, register_collector,
};

/// Bounded per-method timing of OwnerFiles RPCs. The same registry is exposed
/// by the Node /metrics endpoint, so a W2 phase can compare the B-side round
/// trip with A-side handler work without turning on per-request log output.
///
/// `payload_bytes_total` records successful logical OwnerFiles file payload
/// completions, not wire bytes. Labels are bounded to `side=client|server`,
/// `direction=read|write` and `plane=grpc|rdma`. It excludes control messages,
/// RDMA handshakes, close messages and client prefetch-cache hits.
#[cfg(feature = "ownerfs")]
#[derive(Clone)]
pub struct OwnerRpcMetrics {
    duration_seconds: HistogramVec,
    payload_bytes_total: IntCounterVec,
}

#[cfg(feature = "ownerfs")]
impl OwnerRpcMetrics {
    pub fn register(registry: &Registry) -> Result<Self, MetricsError> {
        registry.get_or_register(|registry| {
            let duration_seconds = HistogramVec::new(
                HistogramOpts::new(
                    "afs_ownerfiles_rpc_duration_seconds",
                    "OwnerFiles client round trip and Home handler duration by method.",
                )
                .buckets(vec![
                    0.000_05, 0.000_1, 0.000_2, 0.000_4, 0.000_8, 0.001_6, 0.003_2, 0.006_4,
                    0.012_8,
                ]),
                &["side", "method"],
            )?;
            let payload_bytes_total = IntCounterVec::new(
                Opts::new(
                    "afs_ownerfiles_payload_bytes_total",
                    "Successful logical OwnerFiles payload bytes by side, direction and data plane.",
                ),
                &["side", "direction", "plane"],
            )?;
            register_collector(registry, &duration_seconds)?;
            register_collector(registry, &payload_bytes_total)?;
            for side in ["client", "server"] {
                for direction in ["read", "write"] {
                    for plane in ["grpc", "rdma"] {
                        payload_bytes_total.with_label_values(&[side, direction, plane]);
                    }
                }
            }
            Ok(Self {
                duration_seconds,
                payload_bytes_total,
            })
        })
    }

    pub fn observe(&self, side: &'static str, method: &'static str, elapsed: std::time::Duration) {
        self.duration_seconds
            .with_label_values(&[side, method])
            .observe(elapsed.as_secs_f64());
    }

    pub fn record_payload(
        &self,
        side: &'static str,
        direction: &'static str,
        plane: &'static str,
        bytes: u64,
    ) {
        self.payload_bytes_total
            .with_label_values(&[side, direction, plane])
            .inc_by(bytes);
    }
}
