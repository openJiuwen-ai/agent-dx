//! Read-only Prometheus exposition of allocation ledgers (not runtime usage).
use adx_core::{
    scheduling::{DeviceKind, DeviceLedger},
    ResourceLedger,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub struct Text {
    output: String,
    declared: BTreeSet<String>,
}
impl Text {
    pub fn gauge(&mut self, name: &str, labels: &[(&str, String)], value: u64) {
        if self.declared.insert(name.into()) {
            self.output.push_str(&format!("# TYPE {name} gauge\n"));
        }
        let labels = labels
            .iter()
            .map(|(key, value)| {
                let escaped = value
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n");
                format!("{key}=\"{escaped}\"")
            })
            .collect::<Vec<_>>()
            .join(",");
        let labels = if labels.is_empty() {
            labels
        } else {
            format!("{{{labels}}}")
        };
        self.output.push_str(&format!("{name}{labels} {value}\n"));
    }
    pub fn finish(self) -> String {
        self.output
    }
}

pub fn resources(
    out: &mut Text,
    prefix: &str,
    labels: &[(&str, String)],
    scalar: &ResourceLedger,
    devices: &DeviceLedger,
    accepting: bool,
    devices_fresh: bool,
) {
    let capacity = scalar.capacity();
    let used = scalar.used();
    for (resource, capacity, used) in [
        ("cpu_millis", capacity.cpu_millis, used.cpu_millis),
        ("memory_bytes", capacity.memory_bytes, used.memory_bytes),
        ("disk_bytes", capacity.disk_bytes, used.disk_bytes),
    ] {
        for (state, value) in [
            ("capacity", capacity),
            ("reserved", used),
            (
                "available",
                if accepting {
                    capacity.saturating_sub(used)
                } else {
                    0
                },
            ),
            ("overcommitted", used.saturating_sub(capacity)),
        ] {
            out.gauge(&format!("{prefix}_{state}_{resource}"), labels, value);
        }
    }
    // Include allocations whose device disappeared or became unhealthy, rather
    // than deriving reservations from capacity minus current free inventory.
    let mut models: BTreeMap<(DeviceKind, String), (u64, u64, u64)> = BTreeMap::new();
    for d in devices.inventory() {
        let row = models.entry((d.kind, d.model.clone())).or_default();
        row.0 += u64::from(d.healthy);
    }
    for d in devices.allocated() {
        models.entry((d.kind, d.model.clone())).or_default().1 += 1;
    }
    for d in devices.available() {
        models.entry((d.kind, d.model.clone())).or_default().2 += 1;
    }
    for ((kind, model), (capacity, reserved, free)) in models {
        let mut labels = labels.to_vec();
        labels.push((
            "kind",
            match kind {
                DeviceKind::Gpu => "gpu",
                DeviceKind::Npu => "npu",
            }
            .into(),
        ));
        labels.push(("model", model));
        for (state, value) in [
            ("capacity", capacity),
            ("reserved", reserved),
            (
                "available",
                if accepting && devices_fresh { free } else { 0 },
            ),
            ("overcommitted", reserved.saturating_sub(capacity)),
        ] {
            let mut labels = labels.clone();
            labels.push(("state", state.into()));
            out.gauge(&format!("{prefix}_devices"), &labels, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::scheduling::{Device, DeviceAllocation, DeviceRequest};
    #[test]
    fn missing_and_unhealthy_devices_keep_reservations_and_escape_models() {
        let mut ledger = DeviceLedger::default();
        let model = "M\"\\\n".to_string();
        ledger
            .update(vec![Device {
                id: 0,
                kind: DeviceKind::Gpu,
                model: model.clone(),
                healthy: true,
            }])
            .unwrap();
        ledger
            .reserve(
                "a",
                &[DeviceRequest {
                    kind: DeviceKind::Gpu,
                    model: None,
                    count: 1,
                }],
                &[DeviceAllocation {
                    id: 0,
                    kind: DeviceKind::Gpu,
                    model: model.clone(),
                }],
            )
            .unwrap();
        for inventory in [
            vec![Device {
                id: 0,
                kind: DeviceKind::Gpu,
                model: model.clone(),
                healthy: false,
            }],
            vec![],
        ] {
            ledger.update(inventory).unwrap();
            let mut out = Text::default();
            resources(
                &mut out,
                "test",
                &[],
                &ResourceLedger::new(Default::default()),
                &ledger,
                true,
                true,
            );
            let text = out.finish();
            assert!(text.contains("model=\"M\\\"\\\\\\n\",state=\"reserved\"} 1\n"));
            assert!(text.contains("state=\"available\"} 0\n"));
            assert!(text.contains("state=\"overcommitted\"} 1\n"));
            assert_eq!(text.matches("# TYPE test_devices gauge").count(), 1);
        }
    }
}
