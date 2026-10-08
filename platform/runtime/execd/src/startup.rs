// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! Runtime startup barrier used by fork-based warm starts.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const SEED_FILE_ENV: &str = "ADX_SEED_FILE";
const ENV_FILE_ENV: &str = "ADX_ENV_FILE";
const CHECKPOINT_HANDOFF_FILE_ENV: &str = "ADX_CHECKPOINT_HANDOFF_FILE";
const GVISOR_CHECKPOINT_FILE: &str = "/proc/gvisor/checkpoint";
const GVISOR_SPEC_ENVIRON_FILE: &str = "/proc/gvisor/spec_environ";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointOutcome {
    Resume,
    Restore,
    Error,
}

pub(crate) struct CheckpointHandoff {
    path: PathBuf,
    file: File,
}

/// Block on one read from a configured seed file, then refresh the process
/// environment before any runtime configuration or Tokio worker is created.
pub fn prepare_environment() -> io::Result<()> {
    wait_for_seed_file()?;

    if let Some(env_file) = std::env::var_os(ENV_FILE_ENV).filter(|value| !value.is_empty()) {
        refresh_environment_from_file(Path::new(&env_file));
    }
    Ok(())
}

fn wait_for_seed_file() -> io::Result<()> {
    let Some(seed_file) = std::env::var_os(SEED_FILE_ENV).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let path = Path::new(&seed_file);
    println!("[adx-execd] begin reading seed file: {}", path.display());

    let mut file = File::open(path)?;
    let mut byte = [0_u8; 1];
    let bytes_read = file.read(&mut byte)?;

    println!(
        "[adx-execd] finished reading seed file: {}, bytes_read={bytes_read}",
        path.display(),
    );
    Ok(())
}

fn refresh_environment_from_file(path: &Path) {
    let environment = match read_environment_file(path) {
        Ok(environment) => environment,
        Err(error) => {
            eprintln!(
                "[adx-execd] failed to load environment file {}: {error}",
                path.display()
            );
            return;
        }
    };

    for (key, value) in environment {
        std::env::set_var(key, value);
    }
}

pub(crate) fn restore_environment_file_path() -> Option<PathBuf> {
    configured_or_gvisor_path(ENV_FILE_ENV, GVISOR_SPEC_ENVIRON_FILE)
}

pub(crate) fn checkpoint_handoff_file_path() -> Option<PathBuf> {
    let fallback = Path::new(GVISOR_CHECKPOINT_FILE);
    select_checkpoint_handoff_path(
        std::env::var_os(CHECKPOINT_HANDOFF_FILE_ENV).as_deref(),
        fallback,
        fallback.exists(),
    )
}

fn select_checkpoint_handoff_path(
    configured: Option<&OsStr>,
    runtime_fallback: &Path,
    runtime_fallback_exists: bool,
) -> Option<PathBuf> {
    if let Some(path) = configured.filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(path));
    }
    runtime_fallback_exists.then(|| runtime_fallback.to_path_buf())
}

pub(crate) fn open_checkpoint_handoff() -> io::Result<Option<CheckpointHandoff>> {
    let Some(path) = checkpoint_handoff_file_path() else {
        return Ok(None);
    };
    let file = File::open(&path)?;
    Ok(Some(CheckpointHandoff { path, file }))
}

fn configured_or_gvisor_path(environment_key: &str, gvisor_path: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(environment_key)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    {
        // An explicit handoff path is configuration, not a best-effort hint.
        // Keep it so readers fail closed if it is temporarily unavailable;
        // silently falling back to restored in-process identity could reconnect
        // using the source execution identity.
        return Some(path);
    }
    let path = PathBuf::from(gvisor_path);
    path.exists().then_some(path)
}

pub(crate) async fn wait_for_checkpoint_handoff(
    handoff: CheckpointHandoff,
) -> io::Result<CheckpointOutcome> {
    tokio::task::spawn_blocking(move || read_checkpoint_outcome(handoff))
        .await
        .map_err(|error| io::Error::other(format!("checkpoint barrier task failed: {error}")))?
}

fn read_checkpoint_outcome(mut handoff: CheckpointHandoff) -> io::Result<CheckpointOutcome> {
    let mut content = String::new();
    handoff.file.read_to_string(&mut content)?;
    match content.trim() {
        "resume" => Ok(CheckpointOutcome::Resume),
        "restore" => Ok(CheckpointOutcome::Restore),
        "error" => Ok(CheckpointOutcome::Error),
        outcome => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unexpected checkpoint outcome {:?} from {}",
                outcome,
                handoff.path.display()
            ),
        )),
    }
}

pub(crate) fn read_environment_file(path: &Path) -> io::Result<HashMap<String, String>> {
    let content = std::fs::read(path)?;
    let nul_separated = content.contains(&0);
    let entries: Vec<&[u8]> = if nul_separated {
        content.split(|byte| *byte == 0).collect()
    } else {
        content.split(|byte| *byte == b'\n').collect()
    };
    let mut environment = HashMap::new();
    for (index, raw_entry) in entries.into_iter().enumerate() {
        let line_number = index + 1;
        let raw_line = std::str::from_utf8(raw_entry).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid UTF-8 environment entry {}:{}: {error}",
                    path.display(),
                    line_number
                ),
            )
        })?;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((raw_key, raw_value)) = line.split_once('=') else {
            eprintln!(
                "[adx-execd] invalid environment entry {}:{}: missing '='",
                path.display(),
                line_number
            );
            continue;
        };
        let key = raw_key.trim();
        let value = if nul_separated {
            raw_value
        } else {
            strip_quotes(raw_value.trim())
        };
        if key.is_empty() || key.contains('\0') || value.contains('\0') {
            eprintln!(
                "[adx-execd] invalid environment entry {}:{}",
                path.display(),
                line_number
            );
            continue;
        }

        environment.insert(key.to_string(), value.to_string());
    }
    Ok(environment)
}

fn strip_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if matches!(
            (bytes[0], bytes[bytes.len() - 1]),
            (b'"', b'"') | (b'\'', b'\'')
        ) {
            return &value[1..value.len() - 1];
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::{select_checkpoint_handoff_path, strip_quotes};
    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};

    #[test]
    fn strip_matching_quotes_only() {
        assert_eq!(strip_quotes("\"quoted value\""), "quoted value");
        assert_eq!(strip_quotes("'quoted value'"), "quoted value");
        assert_eq!(strip_quotes("\"unmatched'"), "\"unmatched'");
        assert_eq!(strip_quotes("plain"), "plain");
    }

    #[test]
    fn dedicated_checkpoint_handoff_path_wins_over_runtime_fallback() {
        assert_eq!(
            select_checkpoint_handoff_path(
                Some(OsStr::new("/run/sandboxd/checkpoint")),
                Path::new("/proc/gvisor/checkpoint"),
                true,
            ),
            Some(PathBuf::from("/run/sandboxd/checkpoint")),
        );
        assert_eq!(
            select_checkpoint_handoff_path(
                Some(OsStr::new("")),
                Path::new("/proc/gvisor/checkpoint"),
                true,
            ),
            Some(PathBuf::from("/proc/gvisor/checkpoint")),
        );
    }
}
