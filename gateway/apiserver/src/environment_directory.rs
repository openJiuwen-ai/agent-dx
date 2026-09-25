use adx_protocol::control as pb;
use std::collections::{BTreeMap, BTreeSet};
use tonic::Status;

#[derive(Default)]
pub(crate) struct EnvironmentDirectory {
    epoch: Option<u64>,
    revision: u64,
    entries: BTreeMap<String, pb::GetEnvironmentResponse>,
}

impl EnvironmentDirectory {
    pub fn clear(&mut self) {
        self.epoch = None;
        self.revision = 0;
        self.entries.clear();
    }

    pub fn update(&mut self, frame: pb::EnvironmentDirectoryFrame) -> Result<(), Status> {
        let result = self.apply(frame);
        if result.is_err() {
            self.clear();
        }
        result
    }

    fn apply(&mut self, frame: pb::EnvironmentDirectoryFrame) -> Result<(), Status> {
        if frame.epoch == 0 || frame.revision == 0 {
            return Err(Status::data_loss("invalid environment directory version"));
        }
        let mut next = if frame.reset {
            if frame.base_revision != 0 {
                return Err(Status::data_loss("invalid environment directory reset"));
            }
            BTreeMap::new()
        } else {
            let Some(epoch) = self.epoch else {
                return Err(Status::failed_precondition(
                    "environment directory reset required",
                ));
            };
            if epoch != frame.epoch {
                return Err(Status::failed_precondition(
                    "environment directory epoch changed",
                ));
            }
            if frame.base_revision != self.revision || frame.revision <= frame.base_revision {
                return Err(Status::out_of_range("environment directory history gap"));
            }
            self.entries.clone()
        };
        let mut touched = BTreeSet::new();
        for published in frame.upserts {
            let (id, value) = response(published)?;
            if !touched.insert(id.clone()) {
                return Err(Status::data_loss("duplicate environment directory entry"));
            }
            if !frame.reset {
                if let Some(old) = next.get(&id) {
                    if version(old)? > version(&value)? {
                        continue;
                    }
                }
            }
            next.insert(id, value);
        }
        for id in frame.deleted {
            if id.is_empty() || !touched.insert(id.clone()) {
                return Err(Status::data_loss("invalid environment directory deletion"));
            }
            next.remove(&id);
        }
        self.epoch = Some(frame.epoch);
        self.revision = frame.revision;
        self.entries = next;
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<pb::GetEnvironmentResponse, Status> {
        if self.epoch.is_none() {
            return Err(Status::unavailable(
                "environment directory not synchronized",
            ));
        }
        self.entries
            .get(id)
            .cloned()
            .ok_or_else(|| Status::not_found("instance not found"))
    }

    pub fn list(&self) -> Result<Vec<pb::GetEnvironmentResponse>, Status> {
        if self.epoch.is_none() {
            return Err(Status::unavailable(
                "environment directory not synchronized",
            ));
        }
        Ok(self.entries.values().cloned().collect())
    }

    pub fn put(&mut self, value: pb::GetEnvironmentResponse) -> Result<(), Status> {
        let published = pb::PublishedEnvironment {
            record: value.record,
            node_address: value.node_address,
            relay_address: value.relay_address,
        };
        let (id, value) = response(published)?;
        if let Some(old) = self.entries.get(&id) {
            if version(old)? > version(&value)? {
                return Ok(());
            }
        }
        self.entries.insert(id, value);
        Ok(())
    }
}

fn response(
    value: pb::PublishedEnvironment,
) -> Result<(String, pb::GetEnvironmentResponse), Status> {
    let record = value
        .record
        .ok_or_else(|| Status::data_loss("environment directory record missing"))?;
    let spec = record
        .spec
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment directory spec missing"))?;
    let assignment = record
        .assignment
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment directory assignment missing"))?;
    if spec.id.is_empty()
        || spec.tenant_id.is_empty()
        || assignment.environment_id != spec.id
        || assignment.node_id.is_empty()
        || assignment.generation == 0
        || value.node_address.is_empty()
        || value.relay_address.is_empty()
    {
        return Err(Status::data_loss("invalid environment directory entry"));
    }
    Ok((
        spec.id.clone(),
        pb::GetEnvironmentResponse {
            record: Some(record),
            node_address: value.node_address,
            relay_address: value.relay_address,
        },
    ))
}

fn version(value: &pb::GetEnvironmentResponse) -> Result<(u64, u64), Status> {
    let record = value
        .record
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment directory record missing"))?;
    let generation = record
        .assignment
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment directory assignment missing"))?
        .generation;
    Ok((generation, record.revision))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, generation: u64, revision: u64) -> pb::PublishedEnvironment {
        pb::PublishedEnvironment {
            record: Some(pb::EnvironmentRecord {
                spec: Some(pb::EnvironmentSpec {
                    id: id.into(),
                    tenant_id: "tenant".into(),
                    ..Default::default()
                }),
                assignment: Some(pb::Assignment {
                    environment_id: id.into(),
                    node_id: "node".into(),
                    generation,
                    ..Default::default()
                }),
                revision,
                ..Default::default()
            }),
            node_address: "node:9000".into(),
            relay_address: "node:9443".into(),
        }
    }

    fn frame(
        epoch: u64,
        revision: u64,
        base_revision: u64,
        reset: bool,
        upserts: Vec<pb::PublishedEnvironment>,
        deleted: Vec<&str>,
    ) -> pb::EnvironmentDirectoryFrame {
        pb::EnvironmentDirectoryFrame {
            epoch,
            revision,
            base_revision,
            reset,
            upserts,
            deleted: deleted.into_iter().map(str::to_owned).collect(),
        }
    }

    #[test]
    fn full_then_incremental_updates_and_deletes() {
        let mut directory = EnvironmentDirectory::default();
        assert_eq!(
            directory.get("one").unwrap_err().code(),
            tonic::Code::Unavailable
        );
        directory
            .update(frame(7, 10, 0, true, vec![entry("one", 1, 1)], vec![]))
            .unwrap();
        assert_eq!(directory.get("one").unwrap().record.unwrap().revision, 1);
        directory
            .update(frame(7, 12, 10, false, vec![entry("one", 1, 2)], vec![]))
            .unwrap();
        assert_eq!(directory.get("one").unwrap().record.unwrap().revision, 2);
        directory
            .update(frame(7, 13, 12, false, vec![], vec!["one"]))
            .unwrap();
        assert_eq!(
            directory.get("one").unwrap_err().code(),
            tonic::Code::NotFound
        );
    }

    #[test]
    fn revision_gap_or_epoch_change_requires_reset() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(7, 10, 0, true, vec![entry("one", 1, 1)], vec![]))
            .unwrap();
        assert_eq!(
            directory
                .update(frame(7, 12, 9, false, vec![], vec![]))
                .unwrap_err()
                .code(),
            tonic::Code::OutOfRange
        );
        assert_eq!(
            directory.get("one").unwrap_err().code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            directory
                .update(frame(8, 13, 12, false, vec![], vec![]))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        directory
            .update(frame(8, 20, 0, true, vec![entry("two", 2, 1)], vec![]))
            .unwrap();
        assert!(directory.get("two").is_ok());
    }

    #[test]
    fn stale_local_results_never_replace_newer_ownership() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(7, 10, 0, true, vec![entry("one", 2, 4)], vec![]))
            .unwrap();
        let stale = entry("one", 1, 99);
        directory
            .put(pb::GetEnvironmentResponse {
                record: stale.record,
                node_address: stale.node_address,
                relay_address: stale.relay_address,
            })
            .unwrap();
        let assignment = directory
            .get("one")
            .unwrap()
            .record
            .unwrap()
            .assignment
            .unwrap();
        assert_eq!(assignment.generation, 2);
    }

    #[test]
    fn delayed_incremental_never_reverts_a_newer_targeted_read() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(7, 10, 0, true, vec![entry("one", 1, 1)], vec![]))
            .unwrap();
        let current = entry("one", 2, 4);
        directory
            .put(pb::GetEnvironmentResponse {
                record: current.record,
                node_address: current.node_address,
                relay_address: current.relay_address,
            })
            .unwrap();
        directory
            .update(frame(7, 11, 10, false, vec![entry("one", 1, 2)], vec![]))
            .unwrap();
        let record = directory.get("one").unwrap().record.unwrap();
        assert_eq!(record.assignment.unwrap().generation, 2);
        assert_eq!(record.revision, 4);
    }

    #[test]
    fn synchronized_list_uses_the_current_directory_only() {
        let mut directory = EnvironmentDirectory::default();
        assert_eq!(
            directory.list().unwrap_err().code(),
            tonic::Code::Unavailable
        );
        directory
            .update(frame(
                7,
                10,
                0,
                true,
                vec![entry("two", 1, 1), entry("one", 1, 1)],
                vec![],
            ))
            .unwrap();
        let listed = directory.list().unwrap();
        let ids: Vec<_> = listed
            .iter()
            .map(|value| {
                value
                    .record
                    .as_ref()
                    .unwrap()
                    .spec
                    .as_ref()
                    .unwrap()
                    .id
                    .as_str()
            })
            .collect();
        assert_eq!(ids, ["one", "two"]);
        directory
            .update(frame(7, 11, 10, false, vec![], vec!["one"]))
            .unwrap();
        assert_eq!(directory.list().unwrap().len(), 1);
        directory.clear();
        assert_eq!(
            directory.list().unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }
}
