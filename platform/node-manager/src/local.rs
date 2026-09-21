//! Temporary reservations use the normal Admission ledger. The token only
//! identifies who may release/transfer that hold; it is not a second ledger.
use super::*;
use adx_core::scheduling::{select_devices, DeviceAllocation};

#[derive(Clone, Debug)]
pub struct LocalReservation {
    pub token: Option<String>,
    pub devices: Vec<DeviceAllocation>,
}
pub(crate) struct LocalHold {
    pub spec: InstanceSpec,
    pub token: String,
    pub devices: Vec<DeviceAllocation>,
}
impl NodeManager {
    pub fn reserve_local(&self, spec: &InstanceSpec) -> Result<LocalReservation> {
        spec.validate()?;
        if self.is_draining() {
            return Err(Error::Unavailable("node draining".into()));
        }
        let instances = self.instances.lock().expect("shared state lock poisoned");
        if let Some((old, assignment, _)) = instances.get(&spec.id) {
            if old != spec {
                return Err(Error::Conflict);
            }
            return Ok(LocalReservation {
                token: None,
                devices: assignment.devices.clone(),
            });
        }
        let mut holds = self.local_holds.lock().expect("shared state lock poisoned");
        if let Some(hold) = holds.get(&spec.id) {
            if hold.spec != *spec {
                return Err(Error::Conflict);
            }
            return Ok(LocalReservation {
                token: Some(hold.token.clone()),
                devices: hold.devices.clone(),
            });
        }
        let mut admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        let devices = select_devices(&spec.scheduling.devices, &admission.devices.available())?;
        let token = format!("local-claim:{}", uuid::Uuid::new_v4());
        admission.reserve(
            &token,
            spec,
            &Assignment {
                instance_id: spec.id.clone(),
                node_id: self.node_id.clone(),
                shard_id: 0,
                generation: 0,
                devices: devices.clone(),
            },
        )?;
        holds.insert(
            spec.id.clone(),
            LocalHold {
                spec: spec.clone(),
                token: token.clone(),
                devices: devices.clone(),
            },
        );
        Ok(LocalReservation {
            token: Some(token),
            devices,
        })
    }
    /// Only after a definitive losing/fallback response, never after a timeout.
    pub fn release_local(&self, id: &str, reservation: &LocalReservation) -> Result<()> {
        let mut holds = self.local_holds.lock().expect("shared state lock poisoned");
        if let Some(hold) = holds.get(id) {
            if reservation.token.as_ref() == Some(&hold.token) {
                self.services
                    .admission
                    .lock()
                    .expect("shared state lock poisoned")
                    .release(&hold.token)?;
                holds.remove(id);
            }
        }
        Ok(())
    }
    pub(crate) fn adopt_local(&self, spec: &InstanceSpec, assignment: &Assignment) -> Result<bool> {
        let mut holds = self.local_holds.lock().expect("shared state lock poisoned");
        let Some(hold) = holds.get(&spec.id) else {
            return Ok(false);
        };
        if hold.spec != *spec {
            return Err(Error::Conflict);
        }
        let mut admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        let mut scalar = admission.ledger.clone();
        let mut devices = admission.devices.clone();
        scalar.release(&hold.token)?;
        devices.release(&hold.token)?;
        let adopted = hold.devices == assignment.devices;
        if adopted {
            let runtime_id = format!("{}-{}", spec.id, assignment.generation);
            scalar.restore(&runtime_id, spec.resources)?;
            devices.restore(&runtime_id, &assignment.devices)?;
        }
        admission.ledger = scalar;
        admission.devices = devices;
        holds.remove(&spec.id);
        Ok(adopted)
    }
}
