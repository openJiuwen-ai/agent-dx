//! Wire negotiation shared by the HIXL RPC adapters.

use crate::node::data_plane::hixl::SlotChecksum;
use afs_protocol::node_data::{HixlMemoryKind, HixlSlotChecksumAlgorithm, HixlTransport};
use afs_transport::hixl::{EngineInfo, ReadTransport};

impl SlotChecksum {
    /// The reader's offer in preference order: hardware CRC32C first.
    pub(crate) const OFFER: [SlotChecksum; 2] = [SlotChecksum::Crc32c, SlotChecksum::Blake3];

    /// Home's choice: CRC32C when offered, otherwise BLAKE3, which every
    /// reader verifies.
    pub(crate) fn negotiate(offered: &[i32]) -> Self {
        if offered.contains(&(HixlSlotChecksumAlgorithm::Crc32c as i32)) {
            Self::Crc32c
        } else {
            Self::Blake3
        }
    }

    /// Algorithm a Home reply names; an unset field is a Home without the
    /// negotiation and means BLAKE3.
    pub(crate) fn from_reply(algorithm: i32) -> Option<Self> {
        match HixlSlotChecksumAlgorithm::try_from(algorithm) {
            Ok(HixlSlotChecksumAlgorithm::Blake3) => Some(Self::Blake3),
            Ok(HixlSlotChecksumAlgorithm::Crc32c) => Some(Self::Crc32c),
            _ => None,
        }
    }

    pub(crate) fn wire(self) -> HixlSlotChecksumAlgorithm {
        match self {
            Self::Blake3 => HixlSlotChecksumAlgorithm::Blake3,
            Self::Crc32c => HixlSlotChecksumAlgorithm::Crc32c,
        }
    }
}

pub(crate) const HIXL_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TransportParameters {
    pub(super) protocol_version: u32,
    pub(super) transport: HixlTransport,
    pub(super) memory_kind: HixlMemoryKind,
    pub(super) buffer_pool_count: u32,
    pub(super) buffer_pool_size_mib: u32,
    pub(super) hccs_domain_id: Option<u64>,
}

impl TransportParameters {
    pub(super) fn matches_wire(
        self,
        protocol_version: u32,
        transport: i32,
        memory_kind: i32,
        buffer_pool_count: u32,
        buffer_pool_size_mib: u32,
        hccs_domain_id: Option<u64>,
    ) -> bool {
        protocol_version == self.protocol_version
            && transport == self.transport as i32
            && memory_kind == self.memory_kind as i32
            && buffer_pool_count == self.buffer_pool_count
            && buffer_pool_size_mib == self.buffer_pool_size_mib
            && hccs_domain_id == self.hccs_domain_id
    }
}

pub(super) fn transport_parameters(info: &EngineInfo) -> Result<TransportParameters, &'static str> {
    let count = u32::try_from(info.buffer_pool_count)
        .map_err(|_| "HIXL buffer-pool count exceeds protocol range")?;
    let size = u32::try_from(info.buffer_pool_size_mib)
        .map_err(|_| "HIXL buffer-pool size exceeds protocol range")?;
    if (count == 0) != (size == 0) {
        return Err("HIXL buffer-pool dimensions must both be zero or positive");
    }
    use HixlMemoryKind::{ClassicHost, FabricHost};
    let (transport, memory_kind) = match (info.transport, info.hccs_domain_id) {
        (ReadTransport::Roce, None) => (HixlTransport::Roce, ClassicHost),
        (ReadTransport::Hccs, Some(_)) if count > 0 => (HixlTransport::Hccs, ClassicHost),
        // The native runtime obtained the domain from the device. Zero is
        // a valid domain; absence is not.
        (ReadTransport::HccsFabric, Some(_)) if count == 0 => (HixlTransport::Hccs, FabricHost),
        (ReadTransport::Roce, Some(_)) => {
            return Err("HIXL RoCE must not advertise an HCCS domain");
        }
        (ReadTransport::Hccs, _) => {
            return Err("HIXL HCCS requires a buffer pool and a super-pod domain");
        }
        (ReadTransport::HccsFabric, _) => {
            return Err("HIXL Fabric requires zero buffer-pool dimensions and a super-pod domain");
        }
    };
    Ok(TransportParameters {
        protocol_version: HIXL_PROTOCOL_VERSION,
        transport,
        memory_kind,
        buffer_pool_count: count,
        buffer_pool_size_mib: size,
        hccs_domain_id: info.hccs_domain_id,
    })
}

/// The source refused the typed capability before any data submission.
pub(crate) fn is_unsupported(error: &afs_error::Error) -> bool {
    error.code() == afs_error::NODE_TRANSFER_UNSUPPORTED
        && error.kind() == afs_error::ErrorKind::Unimplemented
}

#[cfg(test)]
mod tests {
    use crate::node::data_plane::hixl::HixlMetrics;
    use afs_metrics::Registry;
    use afs_transport::hixl::{HixlReadTiming, ReadTransport, RuntimeState, RuntimeStatus};
    use std::time::Duration;

    #[cfg(feature = "ownerfs")]
    #[test]
    fn roce_relay_is_distinct_from_direct_and_requires_matching_pool_dimensions() {
        let mut info = super::test_support::engine("192.0.2.1:7920", 1, 4096);
        let direct = super::transport_parameters(&info).unwrap();
        info.buffer_pool_count = 4;
        assert!(super::transport_parameters(&info).is_err());
        info.buffer_pool_size_mib = 8;
        let relay = super::transport_parameters(&info).unwrap();
        assert!(relay.matches_wire(
            1,
            afs_protocol::node_data::HixlTransport::Roce as i32,
            afs_protocol::node_data::HixlMemoryKind::ClassicHost as i32,
            4,
            8,
            None,
        ));
        assert_ne!(direct, relay);
        info.hccs_domain_id = Some(7);
        assert!(super::transport_parameters(&info).is_err());
    }

    #[cfg(feature = "ownerfs")]
    #[test]
    fn v1_encodes_explicit_classic_memory_kind() {
        use afs_protocol::node_data::{
            HixlMemoryKind, HixlTransport, OwnerNegotiateHixlReply, OwnerNegotiateHixlRequest,
        };
        use prost::Message;

        for (transport, count, size, domain, request_bytes, reply_bytes) in [
            (
                HixlTransport::Roce,
                0,
                0,
                None,
                &[0x20, 1, 0x28, 1, 0x58, 1][..],
                &[0x18, 1, 0x20, 1, 0x68, 1][..],
            ),
            (
                HixlTransport::Hccs,
                4,
                8,
                Some(7),
                &[0x20, 1, 0x28, 2, 0x30, 4, 0x38, 8, 0x40, 7, 0x58, 1][..],
                &[0x18, 1, 0x20, 2, 0x28, 4, 0x30, 8, 0x38, 7, 0x68, 1][..],
            ),
        ] {
            let request = OwnerNegotiateHixlRequest {
                protocol_version: 1,
                transport: transport as i32,
                buffer_pool_count: count,
                buffer_pool_size_mib: size,
                hccs_domain_id: domain,
                memory_kind: HixlMemoryKind::ClassicHost as i32,
                ..Default::default()
            };
            let reply = OwnerNegotiateHixlReply {
                protocol_version: 1,
                transport: transport as i32,
                buffer_pool_count: count,
                buffer_pool_size_mib: size,
                hccs_domain_id: domain,
                memory_kind: HixlMemoryKind::ClassicHost as i32,
                ..Default::default()
            };
            assert_eq!(request.encode_to_vec().as_slice(), request_bytes);
            assert_eq!(reply.encode_to_vec().as_slice(), reply_bytes);
            assert_eq!(
                OwnerNegotiateHixlRequest::decode(request_bytes).unwrap(),
                request
            );
            assert_eq!(OwnerNegotiateHixlReply::decode(reply_bytes).unwrap(), reply);
        }
    }

    #[test]
    fn hbm_budget_is_reported_separately_from_host_allocations() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        let status = RuntimeStatus {
            state: RuntimeState::Running,
            generation: 1,
            allocated_bytes: 32 * 1024 * 1024,
            quarantined_bytes: 0,
            export_slots: 4,
            receive_slots_in_use: 0,
            export_slots_in_use: 0,
            transport: ReadTransport::Hccs,
            relay_hbm_budget_bytes: 64 * 1024 * 1024,
            hccs_domain_id: Some(7),
        };
        metrics.snapshot(&status);
        let gauges: std::collections::HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| {
                (
                    family.name().to_owned(),
                    family.get_metric()[0].get_gauge().get_value(),
                )
            })
            .collect();
        assert_eq!(
            gauges["afs_hixl_allocated_bytes"],
            (32 * 1024 * 1024) as f64
        );
        assert_eq!(
            gauges["afs_hixl_relay_hbm_budget_bytes"],
            (64 * 1024 * 1024) as f64
        );
    }

    #[test]
    fn lane_metrics_have_bounded_lane_and_result_labels() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        metrics.record_lane_validated_bytes(0, 3);
        metrics.record_lane_validated_bytes(1, 5);
        metrics.record_lane_validated_bytes(16, 99);
        metrics.observe_lane_read(0, Duration::from_millis(2), true);
        metrics.observe_lane_read(0, Duration::from_millis(3), false);
        metrics.observe_lane_read(16, Duration::from_millis(99), true);
        let families: std::collections::HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| (family.name().to_owned(), family))
            .collect();

        let bytes = families["afs_hixl_lane_validated_read_bytes_total"].get_metric();
        assert_eq!(bytes.len(), 2);
        assert_eq!(
            bytes
                .iter()
                .map(|metric| metric.get_counter().get_value())
                .sum::<f64>(),
            8.0
        );

        let reads = families["afs_hixl_lane_read_duration_seconds"].get_metric();
        assert_eq!(reads.len(), 2);
        let histograms = reads.iter().map(|metric| metric.get_histogram());
        assert_eq!(
            histograms
                .clone()
                .map(|histogram| histogram.get_sample_count())
                .sum::<u64>(),
            2
        );
        let seconds = histograms
            .map(|histogram| histogram.get_sample_sum())
            .sum::<f64>();
        assert!((seconds - 0.005).abs() < f64::EPSILON);
        assert!(reads.iter().all(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "lane" && label.value() == "0")
        }));
    }

    #[test]
    fn native_batch_evidence_and_reader_timings_skip_unavailable_spans() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        metrics.observe_native_batch(25, 100 * 1024 * 1024);
        metrics.observe_read_timing(&HixlReadTiming {
            owner_queue: Duration::from_millis(2),
            sdk_transfer: Some(Duration::from_millis(3)),
            output_copy: None,
        });
        let families: std::collections::HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| (family.name().to_owned(), family))
            .collect();
        let counter = |name: &str| families[name].get_metric()[0].get_counter().get_value();
        let histogram = |name: &str| families[name].get_metric()[0].get_histogram().clone();

        assert_eq!(counter("afs_hixl_reader_native_batch_calls_total"), 1.0);
        assert_eq!(
            counter("afs_hixl_reader_native_batch_bytes_total"),
            (100 * 1024 * 1024) as f64
        );
        let descriptors = histogram("afs_hixl_reader_native_batch_descriptors");
        assert_eq!(descriptors.get_sample_count(), 1);
        assert!((descriptors.get_sample_sum() - 25.0).abs() < f64::EPSILON);
        let sizes = histogram("afs_hixl_reader_native_batch_size_bytes");
        assert_eq!(sizes.get_sample_sum(), (100 * 1024 * 1024) as f64);
        let covered = sizes
            .get_bucket()
            .iter()
            .find(|bucket| bucket.upper_bound() == (100 * 1024 * 1024) as f64)
            .map(|bucket| bucket.cumulative_count());
        assert_eq!(covered, Some(1));

        for (name, samples) in [
            ("afs_hixl_reader_owner_queue_seconds", 1),
            ("afs_hixl_reader_sdk_transfer_seconds", 1),
            ("afs_hixl_reader_output_copy_seconds", 0),
        ] {
            let observed = families[name]
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_count())
                .sum::<u64>();
            assert_eq!(observed, samples, "{name}");
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support;
