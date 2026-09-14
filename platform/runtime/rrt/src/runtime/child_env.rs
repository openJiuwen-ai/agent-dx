use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::process::Command;
use std::sync::{OnceLock, RwLock};

fn cache() -> &'static RwLock<Vec<(OsString, OsString)>> {
    static CACHE: OnceLock<RwLock<Vec<(OsString, OsString)>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(filtered(std::env::vars_os())))
}

fn filtered<I>(environment: I) -> Vec<(OsString, OsString)>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    environment
        .into_iter()
        .filter(|(key, _)| !is_reserved(key))
        .collect()
}

pub(crate) fn initialize() {
    replace(std::env::vars_os());
}

pub(crate) fn refresh_from_map(environment: &HashMap<String, String>) {
    replace(
        environment
            .iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value))),
    );
}

fn replace<I>(environment: I)
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    *cache().write().expect("child environment cache poisoned") = filtered(environment);
}

pub(crate) fn snapshot() -> Vec<(OsString, OsString)> {
    cache()
        .read()
        .expect("child environment cache poisoned")
        .clone()
}

pub(crate) fn apply(command: &mut Command) {
    command.env_clear();
    command.envs(snapshot());
}

pub(crate) fn apply_tokio(command: &mut tokio::process::Command) {
    command.env_clear();
    command.envs(snapshot());
}

pub(crate) fn apply_pty(command: &mut portable_pty::CommandBuilder) {
    command.env_clear();
    for (key, value) in snapshot() {
        command.env(key, value);
    }
}

pub(crate) fn is_reserved(key: &OsStr) -> bool {
    key.to_string_lossy().starts_with("ADX_")
}

pub(crate) fn validate_override(key: &str) -> Result<(), String> {
    if is_reserved(OsStr::new(key)) {
        return Err(format!(
            "environment variable {key} uses the reserved ADX_ prefix"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_reserved_runtime_environment() {
        let values = filtered([
            (OsString::from("PATH"), OsString::from("/bin")),
            (
                OsString::from("ADX_IMAGE_PROCESS_CONFIG"),
                OsString::from("/tmp/spec"),
            ),
            (OsString::from("USER_VALUE"), OsString::from("ok")),
        ]);
        assert_eq!(values.len(), 2);
        assert!(values.iter().all(|(key, _)| !is_reserved(key)));
    }

    #[test]
    fn rejects_reserved_user_override() {
        assert!(validate_override("ADX_INTERNAL").is_err());
        assert!(validate_override("USER_VALUE").is_ok());
    }
}
