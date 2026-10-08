//! Shared control messages and execution-scoped bearer credentials.
use adx_core::{runtime::*, Error, Result};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

pub const MAX_CONTROL_MESSAGE_BYTES: usize = 65_536;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOperation {
    Status,
    Prepare(PrepareCheckpoint),
    Abort(AbortCheckpoint),
    Finish(FinishWorkloadCheckpoint),
}

fn credential(secret: &[u8], identity: &RuntimeIdentity) -> Result<Hmac<Sha256>> {
    identity.validate()?;
    if secret.len() < 32 {
        return Err(Error::Invalid(
            "runtime control secret requires at least 32 bytes".into(),
        ));
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .map_err(|_| Error::Invalid("invalid runtime control secret".into()))?;
    mac.update(b"adx.runtime.control.v1\0");
    mac.update(&serde_json::to_vec(identity).map_err(|e| Error::Invalid(e.to_string()))?);
    Ok(mac)
}
/// The node secret stays on the host. Only the HMAC for this execution is injected.
pub fn execution_token(secret: &[u8], identity: &RuntimeIdentity) -> Result<String> {
    let bytes = credential(secret, identity)?.finalize().into_bytes();
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
pub fn verify_token(secret: &[u8], identity: &RuntimeIdentity, token: &str) -> Result<()> {
    if token.len() != 64 || !token.is_ascii() {
        return Err(Error::Conflict);
    }
    let mut bytes = [0; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&token[i * 2..i * 2 + 2], 16).map_err(|_| Error::Conflict)?;
    }
    credential(secret, identity)?
        .verify_slice(&bytes)
        .map_err(|_| Error::Conflict)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credential_is_bound_to_execution_and_generation() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let mut identity = RuntimeIdentity {
            environment_id: "i".into(),
            runtime_id: "i-1".into(),
            ownership_generation: 1,
        };
        let token = execution_token(secret, &identity).unwrap();
        verify_token(secret, &identity, &token).unwrap();
        identity.ownership_generation = 2;
        assert!(verify_token(secret, &identity, &token).is_err());
        identity.ownership_generation = 1;
        identity.runtime_id = "i-1-r3".into();
        assert!(verify_token(secret, &identity, &token).is_err());
    }
}
