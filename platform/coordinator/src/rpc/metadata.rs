//! Platform-owned metadata authorization, filtering and bounded pagination.
//! Entrypoint adapters may supply a subset predicate; the query has no protocol model.
use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const METADATA_CURSOR_VERSION: u8 = 1;
const MAX_METADATA_CURSOR_BYTES: usize = 4096;
// Leave protobuf map-entry framing headroom below the 1 MiB response budget.
const MAX_METADATA_RESPONSE_BYTES: usize = 960 * 1024;

fn metadata_state(state: EnvironmentState) -> i32 {
    match state {
        EnvironmentState::Pending => pb::EnvironmentState::Pending as i32,
        EnvironmentState::Starting => pb::EnvironmentState::Starting as i32,
        EnvironmentState::Running => pb::EnvironmentState::Running as i32,
        EnvironmentState::Pausing => pb::EnvironmentState::Pausing as i32,
        EnvironmentState::Paused => pb::EnvironmentState::Paused as i32,
        EnvironmentState::Resuming => pb::EnvironmentState::Resuming as i32,
        EnvironmentState::Deleting => pb::EnvironmentState::Deleting as i32,
        EnvironmentState::Deleted => pb::EnvironmentState::Deleted as i32,
        EnvironmentState::Failed => pb::EnvironmentState::Failed as i32,
    }
}

fn metadata_query_digest(
    caller: &pb::CallerContext,
    filter: &adx_core::metadata::UserMetadata,
    states: &BTreeSet<i32>,
    scope: &str,
) -> String {
    let mut input = format!(
        "v{METADATA_CURSOR_VERSION};tenant:{}:{};admin:{};scope:{}",
        caller.tenant_id.len(),
        caller.tenant_id,
        caller.administrator,
        scope
    );
    for state in states {
        input.push_str(&format!(";state:{state}"));
    }
    for (key, value) in filter {
        input.push_str(&format!(
            ";key:{}:{};value:{}:{}",
            key.len(),
            key,
            value.len(),
            value
        ));
    }
    format!("{:x}", Sha256::digest(input.as_bytes()))
}

fn hex(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn unhex(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let bytes = bytes
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            u8::try_from((high << 4) | low).ok()
        })
        .collect::<Option<Vec<_>>>()?;
    String::from_utf8(bytes).ok()
}

fn encode_metadata_cursor(query_digest: &str, last_environment_id: &str) -> String {
    format!(
        "{METADATA_CURSOR_VERSION}.{query_digest}.{}",
        hex(last_environment_id)
    )
}

fn decode_metadata_cursor(cursor: &str, query_digest: &str) -> Result<String> {
    if cursor.len() > MAX_METADATA_CURSOR_BYTES {
        return Err(Error::Invalid("invalid metadata cursor".into()));
    }
    if cursor.is_empty() {
        return Ok(String::new());
    }
    let mut parts = cursor.split('.');
    let valid = matches!(parts.next(), Some("1"))
        && matches!(parts.next(), Some(digest) if digest == query_digest)
        && parts.clone().count() == 1;
    let last_environment_id = parts.next().and_then(unhex);
    if !valid {
        return Err(Error::Invalid("invalid metadata cursor".into()));
    }
    last_environment_id.ok_or_else(|| Error::Invalid("invalid metadata cursor".into()))
}

fn metadata_record(stored: StoredEnvironment) -> pb::EnvironmentMetadataRecord {
    let state = metadata_state(stored.effective_record().state);
    pb::EnvironmentMetadataRecord {
        environment_id: stored.spec.id,
        user_metadata: stored.user_metadata.into_iter().collect(),
        state,
    }
}

fn metadata_matches(stored: &StoredEnvironment, filter: &adx_core::metadata::UserMetadata) -> bool {
    filter
        .iter()
        .all(|(key, value)| stored.user_metadata.get(key) == Some(value))
}

impl CoordinatorRpc {
    pub(super) async fn get_environment_metadata_rpc(
        &self,
        request: Request<pb::GetEnvironmentMetadataRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentMetadataRecord>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let request = request.into_inner();
        let state = self.0.state.lock().await;
        let stored = state.session.get(&request.id).await.map_err(status)?;
        tenant(request.caller.as_ref(), &stored.spec.tenant_id)?;
        Ok(Response::new(metadata_record(stored)))
    }
    pub(super) async fn list_environment_metadata_rpc(
        &self,
        request: Request<pb::ListEnvironmentMetadataRequest>,
    ) -> std::result::Result<Response<pb::ListEnvironmentMetadataResponse>, Status> {
        self.query_environment_metadata(request, "all", |_| true)
            .await
    }

    pub(super) async fn query_environment_metadata(
        &self,
        request: Request<pb::ListEnvironmentMetadataRequest>,
        scope: &str,
        include: impl Fn(&StoredEnvironment) -> bool,
    ) -> std::result::Result<Response<pb::ListEnvironmentMetadataResponse>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let request = request.into_inner();
        let caller = request
            .caller
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("caller required"))?;
        if caller.tenant_id.is_empty() && !caller.administrator {
            return Err(Status::unauthenticated("tenant required"));
        }
        let filter = request
            .metadata_filter
            .into_iter()
            .collect::<adx_core::metadata::UserMetadata>();
        adx_core::metadata::validate_user_metadata(&filter).map_err(status)?;
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        if !(1..=100).contains(&limit) || request.cursor.len() > MAX_METADATA_CURSOR_BYTES {
            return Err(Status::invalid_argument("invalid metadata page"));
        }
        let states = request
            .states
            .into_iter()
            .map(|state| {
                let state = pb::EnvironmentState::try_from(state)
                    .map_err(|_| Error::Invalid("invalid environment state".into()))?;
                if state == pb::EnvironmentState::Unspecified {
                    return Err(Error::Invalid("invalid environment state".into()));
                }
                Ok(state as i32)
            })
            .collect::<Result<BTreeSet<_>>>()
            .map_err(status)?;
        let query_digest = metadata_query_digest(caller, &filter, &states, scope);
        let cursor = decode_metadata_cursor(&request.cursor, &query_digest).map_err(status)?;
        let state = self.0.state.lock().await;
        let saved = state.session.metadata_snapshot().await.map_err(status)?;
        let mut records = saved
            .environments
            .into_values()
            .filter(|stored| {
                (caller.administrator || stored.spec.tenant_id == caller.tenant_id)
                    && include(stored)
                    && (states.is_empty()
                        || states.contains(&metadata_state(stored.effective_record().state)))
                    && metadata_matches(stored, &filter)
                    && stored.spec.id > cursor
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.spec.id.cmp(&right.spec.id));
        let matching_records = records.len();
        let mut response_bytes = 0usize;
        let mut page = Vec::new();
        for stored in records {
            let record = metadata_record(stored);
            let record_bytes = record.environment_id.len()
                + record
                    .user_metadata
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
                + std::mem::size_of::<i32>();
            if page.len() == limit
                || response_bytes.saturating_add(record_bytes) > MAX_METADATA_RESPONSE_BYTES
            {
                break;
            }
            response_bytes += record_bytes;
            page.push(record);
        }
        let next_cursor = if page.len() < matching_records {
            page.last()
                .map(|last| encode_metadata_cursor(&query_digest, &last.environment_id))
                .unwrap_or_default()
        } else {
            String::new()
        };
        Ok(Response::new(pb::ListEnvironmentMetadataResponse {
            records: page,
            next_cursor,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_cursor_is_bound_to_the_authenticated_query() {
        let filter: adx_core::metadata::UserMetadata =
            [("project".into(), "alpha".into())].into_iter().collect();
        let states: BTreeSet<i32> = [pb::EnvironmentState::Running as i32].into_iter().collect();
        let caller = pb::CallerContext {
            tenant_id: "tenant-a".into(),
            administrator: false,
        };
        let digest = metadata_query_digest(&caller, &filter, &states, "all");
        let cursor = encode_metadata_cursor(&digest, "environment-1");
        assert_eq!(
            decode_metadata_cursor(&cursor, &digest).unwrap(),
            "environment-1"
        );

        let other = pb::CallerContext {
            tenant_id: "tenant-b".into(),
            administrator: false,
        };
        let other_digest = metadata_query_digest(&other, &filter, &states, "all");
        assert!(decode_metadata_cursor(&cursor, &other_digest).is_err());
        let subset_digest = metadata_query_digest(&caller, &filter, &states, "subset");
        assert!(decode_metadata_cursor(&cursor, &subset_digest).is_err());
    }

    #[test]
    fn metadata_cursor_rejects_non_ascii_and_oversized_input_without_panicking() {
        for cursor in ["1.digest.中", "1.digest.😀", "1.digest.a"] {
            assert!(decode_metadata_cursor(cursor, "digest").is_err());
        }
        assert!(
            decode_metadata_cursor(&"x".repeat(MAX_METADATA_CURSOR_BYTES + 1), "digest").is_err()
        );
    }
}
