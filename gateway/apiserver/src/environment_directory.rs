use adx_protocol::control as pb;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::ops::Bound::{Excluded, Unbounded};
use tonic::Status;

#[derive(Debug)]
pub(crate) struct EnvironmentPage {
    pub entries: Vec<pb::GetEnvironmentResponse>,
    pub next_after: Option<String>,
}

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
        if frame.reset {
            if frame.base_revision != 0 {
                return Err(Status::data_loss("invalid environment directory reset"));
            }
            let mut next = BTreeMap::new();
            apply_changes(&mut next, frame.upserts, frame.deleted, false)?;
            self.entries = next;
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
            apply_changes(&mut self.entries, frame.upserts, frame.deleted, true)?;
        }
        self.epoch = Some(frame.epoch);
        self.revision = frame.revision;
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<pb::GetEnvironmentResponse, Status> {
        if let Some(value) = self.entries.get(id) {
            return Ok(value.clone());
        }
        if self.epoch.is_some() {
            Err(Status::not_found("instance not found"))
        } else {
            Err(Status::unavailable(
                "environment directory not synchronized",
            ))
        }
    }

    pub fn page<F>(
        &self,
        after: Option<&str>,
        limit: NonZeroUsize,
        mut visible: F,
    ) -> Result<EnvironmentPage, Status>
    where
        F: FnMut(&pb::GetEnvironmentResponse) -> Result<bool, Status>,
    {
        if self.epoch.is_none() {
            return Err(Status::unavailable(
                "environment directory not synchronized",
            ));
        }
        let mut entries = Vec::with_capacity(limit.get());
        let mut last_id = None;
        let lower = after
            .map(|after| Excluded(after.to_owned()))
            .unwrap_or(Unbounded);
        for (id, value) in self.entries.range((lower, Unbounded)) {
            if !visible(value)? {
                continue;
            }
            if entries.len() == limit.get() {
                return Ok(EnvironmentPage {
                    entries,
                    next_after: last_id,
                });
            }
            entries.push(value.clone());
            last_id = Some(id.clone());
        }
        Ok(EnvironmentPage {
            entries,
            next_after: None,
        })
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
        let retired = value.record.as_ref().is_some_and(|record| {
            record.state == pb::EnvironmentState::Deleted as i32 && !record.resources_held
        });
        if retired {
            // A confirmed delete releases the name immediately, even when its
            // watch deletion arrived before this targeted RPC result. The
            // version check above protects a concurrently recreated generation.
            self.entries.remove(&id);
        } else {
            self.entries.insert(id, value);
        }
        Ok(())
    }
}

fn apply_changes(
    entries: &mut BTreeMap<String, pb::GetEnvironmentResponse>,
    upserts: Vec<pb::PublishedEnvironment>,
    deleted: Vec<String>,
    preserve_newer: bool,
) -> Result<(), Status> {
    let mut touched = BTreeSet::new();
    for published in upserts {
        let (id, value) = response(published)?;
        if !touched.insert(id.clone()) {
            return Err(Status::data_loss("duplicate environment directory entry"));
        }
        if preserve_newer {
            if let Some(old) = entries.get(&id) {
                if version(old)? > version(&value)? {
                    continue;
                }
            }
        }
        entries.insert(id, value);
    }
    for id in deleted {
        if id.is_empty() || !touched.insert(id.clone()) {
            return Err(Status::data_loss("invalid environment directory deletion"));
        }
        entries.remove(&id);
    }
    Ok(())
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
    fn confirmed_delete_removes_the_old_name_without_waiting_for_watch() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(7, 10, 0, true, vec![entry("one", 1, 1)], vec![]))
            .unwrap();
        let mut deleted = entry("one", 1, 2);
        deleted.record.as_mut().unwrap().state = pb::EnvironmentState::Deleted as i32;
        deleted.record.as_mut().unwrap().resources_held = false;
        directory.put(response(deleted).unwrap().1).unwrap();
        assert_eq!(
            directory.get("one").unwrap_err().code(),
            tonic::Code::NotFound
        );
        assert!(directory.entries.is_empty());
    }

    #[test]
    fn late_delete_result_never_removes_a_newer_generation() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(7, 10, 0, true, vec![entry("one", 2, 1)], vec![]))
            .unwrap();
        let mut deleted = entry("one", 1, 99);
        deleted.record.as_mut().unwrap().state = pb::EnvironmentState::Deleted as i32;
        deleted.record.as_mut().unwrap().resources_held = false;
        directory.put(response(deleted).unwrap().1).unwrap();
        assert_eq!(
            directory
                .get("one")
                .unwrap()
                .record
                .unwrap()
                .assignment
                .unwrap()
                .generation,
            2
        );
    }

    #[test]
    fn delete_result_after_watch_removal_does_not_reinsert_old_ownership() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(7, 10, 0, true, vec![], vec![]))
            .unwrap();
        let mut deleted = entry("one", 1, 2);
        deleted.record.as_mut().unwrap().state = pb::EnvironmentState::Deleted as i32;
        deleted.record.as_mut().unwrap().resources_held = false;
        directory.put(response(deleted).unwrap().1).unwrap();
        assert_eq!(
            directory.get("one").unwrap_err().code(),
            tonic::Code::NotFound
        );
        assert!(directory.entries.is_empty());
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
    fn targeted_owner_is_queryable_before_the_first_full_snapshot() {
        let mut directory = EnvironmentDirectory::default();
        let (_, owner) = response(entry("local", 1, 1)).unwrap();

        directory.put(owner).unwrap();

        assert_eq!(
            directory
                .get("local")
                .unwrap()
                .record
                .unwrap()
                .spec
                .unwrap()
                .id,
            "local"
        );
        assert_eq!(
            directory.get("missing").unwrap_err().code(),
            tonic::Code::Unavailable
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
    fn incremental_update_does_not_clone_a_large_untouched_directory() {
        let mut directory = EnvironmentDirectory::default();
        let entries = (0..10_000)
            .map(|index| entry(&format!("environment-{index:05}"), 1, 1))
            .collect();
        directory
            .update(frame(7, 10, 0, true, entries, vec![]))
            .unwrap();
        let untouched_address = directory.entries["environment-09999"].node_address.as_ptr();

        directory
            .update(frame(
                7,
                11,
                10,
                false,
                vec![entry("environment-00000", 1, 2)],
                vec![],
            ))
            .unwrap();

        assert_eq!(directory.entries.len(), 10_000);
        assert_eq!(
            directory.entries["environment-09999"].node_address.as_ptr(),
            untouched_address,
            "an incremental frame must not clone entries it does not touch"
        );
    }

    #[test]
    fn invalid_incremental_frame_clears_partially_updated_directory() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(
                7,
                10,
                0,
                true,
                vec![entry("one", 1, 1), entry("two", 1, 1)],
                vec![],
            ))
            .unwrap();
        let mut invalid = entry("invalid", 1, 1);
        invalid.node_address.clear();

        let error = directory
            .update(frame(
                7,
                11,
                10,
                false,
                vec![entry("one", 1, 2), invalid],
                vec![],
            ))
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::DataLoss);
        assert_eq!(
            directory.get("one").unwrap_err().code(),
            tonic::Code::Unavailable
        );
        assert!(directory.entries.is_empty());
    }

    #[test]
    fn synchronized_pages_are_bounded_and_stably_ordered() {
        let mut directory = EnvironmentDirectory::default();
        assert_eq!(
            directory
                .page(None, NonZeroUsize::new(1).unwrap(), |_| Ok(true))
                .unwrap_err()
                .code(),
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
        let first = directory
            .page(None, NonZeroUsize::new(1).unwrap(), |_| Ok(true))
            .unwrap();
        let ids: Vec<_> = first
            .entries
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
        assert_eq!(ids, ["one"]);
        assert_eq!(first.next_after.as_deref(), Some("one"));
        let second = directory
            .page(
                first.next_after.as_deref(),
                NonZeroUsize::new(1).unwrap(),
                |_| Ok(true),
            )
            .unwrap();
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.next_after, None);
        assert_eq!(
            second.entries[0]
                .record
                .as_ref()
                .unwrap()
                .spec
                .as_ref()
                .unwrap()
                .id,
            "two"
        );
        directory
            .update(frame(7, 11, 10, false, vec![], vec!["one"]))
            .unwrap();
        assert_eq!(
            directory
                .page(Some("one"), NonZeroUsize::new(1).unwrap(), |_| Ok(true))
                .unwrap()
                .entries
                .len(),
            1
        );
        directory.clear();
        assert_eq!(
            directory
                .page(None, NonZeroUsize::new(1).unwrap(), |_| Ok(true))
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
    }

    #[test]
    fn page_skips_invisible_entries_without_exceeding_the_clone_budget() {
        let mut directory = EnvironmentDirectory::default();
        directory
            .update(frame(
                7,
                10,
                0,
                true,
                (0..10_000)
                    .map(|index| entry(&format!("environment-{index:05}"), 1, 1))
                    .collect(),
                vec![],
            ))
            .unwrap();

        let page = directory
            .page(None, NonZeroUsize::new(100).unwrap(), |value| {
                let id = &value.record.as_ref().unwrap().spec.as_ref().unwrap().id;
                Ok(id.ends_with('0'))
            })
            .unwrap();

        assert_eq!(page.entries.len(), 100);
        assert_eq!(page.next_after.as_deref(), Some("environment-00990"));
    }
}
