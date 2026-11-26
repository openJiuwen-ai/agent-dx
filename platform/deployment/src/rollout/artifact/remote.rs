//! Task-private acquisition. Neither downloaded content nor package scripts execute.
use super::*;
use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader, Seek, SeekFrom},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

const MAX_ARCHIVE: u64 = 512 * 1024 * 1024;
const MAX_DECODED: u64 = 2 * 1024 * 1024 * 1024;
const MAX_HEADERS: usize = 10_000;

fn budget(deadline: Instant, stopped: &AtomicBool) -> Result<Duration> {
    if stopped.load(Ordering::Acquire) {
        return Err("supervisor stopping".into());
    }
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| "preparation deadline exceeded".into())
}

fn retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
}

enum Attempt {
    Success,
    Retry {
        delay: Duration,
        cause: Box<dyn std::error::Error + Send + Sync>,
    },
}

fn retry_delay(value: Option<&str>, fallback: Duration, remaining: Duration) -> Result<Duration> {
    let delay = match value.map(str::trim) {
        Some(value) if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
            Duration::from_secs(
                value
                    .parse::<u64>()
                    .map_err(|_| "Retry-After exceeds preparation budget")?,
            )
        }
        _ => fallback,
    };
    if delay > remaining {
        return Err("Retry-After or backoff exceeds preparation budget".into());
    }
    Ok(delay)
}

fn transport_attempt(error: reqwest::Error, delay: Duration) -> Result<Attempt> {
    if error.is_timeout() || error.is_connect() || error.is_body() || error.is_request() {
        Ok(Attempt::Retry {
            delay,
            cause: error.into(),
        })
    } else {
        Err(error.into())
    }
}

fn downloaded_chunk(count: u64, bytes: &[u8]) -> Result<u64> {
    count
        .checked_add(u64::try_from(bytes.len())?)
        .filter(|size| *size <= MAX_ARCHIVE)
        .ok_or_else(|| "archive exceeds limit".into())
}

fn downloaded_identity(
    count: u64,
    length: Option<u64>,
    hash: Sha256,
    expected: &str,
) -> Result<()> {
    if length.is_some_and(|size| size != count) {
        return Err("archive HTTP length mismatch".into());
    }
    if format!("{:x}", hash.finalize()) != expected {
        return Err("archive digest mismatch".into());
    }
    Ok(())
}

async fn download(
    remote: &super::super::RemoteArchive,
    file: &mut File,
    deadline: Instant,
    stopped: &AtomicBool,
) -> Result<()> {
    budget(deadline, stopped)?;
    adx_transport::install_crypto_provider();
    let client = reqwest::Client::builder()
        .https_only(true)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .build()?;
    for attempt in 0..3 {
        budget(deadline, stopped)?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        let fallback = Duration::from_secs(1 << attempt);
        let result = async {
            let mut response = match client.get(&remote.url).send().await {
                Ok(response) => response,
                Err(error) => return transport_attempt(error, fallback),
            };
            if !response.status().is_success() {
                let status = response.status();
                if !retryable(status) {
                    return Err(format!("archive HTTP status {status}").into());
                }
                let delay = retry_delay(
                    response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok()),
                    fallback,
                    budget(deadline, stopped)?,
                )?;
                return Ok(Attempt::Retry {
                    delay,
                    cause: format!("archive HTTP status {status}").into(),
                });
            }
            let length = response.content_length();
            if length.is_some_and(|size| size > MAX_ARCHIVE) {
                return Err("archive HTTP length exceeds limit".into());
            }
            let mut count = 0u64;
            let mut hash = Sha256::new();
            loop {
                budget(deadline, stopped)?;
                let chunk = match response.chunk().await {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => break,
                    Err(error) => return transport_attempt(error, fallback),
                };
                budget(deadline, stopped)?;
                count = downloaded_chunk(count, &chunk)?;
                file.write_all(&chunk)?;
                hash.update(&chunk);
            }
            downloaded_identity(count, length, hash, &remote.archive_sha256)?;
            Ok(Attempt::Success)
        };
        // Poll cancellation independently of a stalled connection or response body.
        tokio::pin!(result);
        let outcome = loop {
            let left = budget(deadline, stopped)?;
            tokio::select! {
                result = &mut result => break result,
                () = tokio::time::sleep(left.min(Duration::from_millis(100))) => {
                    budget(deadline, stopped)?;
                }
            }
        };
        let delay = match outcome? {
            Attempt::Success => return Ok(()),
            Attempt::Retry { cause, .. } if attempt == 2 => return Err(cause),
            Attempt::Retry { delay, .. } => delay,
        };
        let remaining = budget(deadline, stopped)?;
        if delay > remaining {
            return Err("retry exceeds preparation budget".into());
        }
        let until = Instant::now()
            .checked_add(delay)
            .filter(|until| *until <= deadline)
            .ok_or("retry exceeds preparation budget")?;
        while Instant::now() < until {
            let left = budget(deadline, stopped)?;
            tokio::time::sleep(
                left.min(until.saturating_duration_since(Instant::now()))
                    .min(Duration::from_millis(100)),
            )
            .await;
        }
    }
    Err("archive request attempts exhausted".into())
}

fn output_path(path: &Path, directory: bool) -> Result<String> {
    let value = path.to_str().ok_or("non-UTF8 archive path")?;
    if directory && matches!(value, "./" | ".") {
        return Ok(String::new());
    }
    let value = value.strip_prefix("./").unwrap_or(value);
    let value = if directory {
        value.strip_suffix('/').unwrap_or(value)
    } else {
        value
    };
    if value.is_empty()
        || value.len() > 4096
        || value.contains(['\\', '\0'])
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err("unsafe archive path".into());
    }
    Ok(value.to_owned())
}

fn scan(
    compressed: &mut File,
    expected: ExpectedBinary<'_>,
    output: &Path,
    deadline: Instant,
    stopped: &AtomicBool,
    decoded_cap: u64,
) -> Result<PreparedBinary> {
    budget(deadline, stopped)?;
    compressed.seek(SeekFrom::Start(0))?;
    let decoder = flate2::bufread::GzDecoder::new(BufReader::new(compressed));
    let decoded_limit = decoded_cap.checked_add(1).ok_or("decoded limit overflow")?;
    let mut archive = tar::Archive::new(BudgetedRead {
        inner: decoder.take(decoded_limit),
        deadline,
        stopped,
    });
    let temporary = output.with_extension("partial");
    let mut owns_temporary = false;
    let mut published = false;
    let result = (|| {
        let relative = format!("bin/{}", expected.role.binary());
        let mut target = None;
        let mut manifest = None;
        let mut digest = None;
        let mut paths = BTreeSet::new();
        let mut last_padded_end = 0u64;
        let mut pending_longname: Option<Vec<u8>> = None;
        // Inspect every header before tar can consume or allocate for metadata.
        for (index, entry) in archive.entries()?.raw(true).enumerate() {
            budget(deadline, stopped)?;
            if index >= MAX_HEADERS {
                return Err("too many archive entries".into());
            }
            let mut entry = entry?;
            let kind = entry.header().entry_type();
            let size = entry.size();
            if !(kind.is_file() || kind.is_dir() || kind.is_gnu_longname())
                || entry.link_name()?.is_some()
            {
                return Err("unsupported archive entry type".into());
            }
            if kind.is_gnu_longname() {
                if size == 0 || size > 4096 || pending_longname.is_some() {
                    return Err("invalid GNU longname metadata".into());
                }
                let mut name = vec![0; usize::try_from(size)?];
                entry.read_exact(&mut name)?;
                pending_longname = Some(name);
                continue;
            }
            let name = if let Some(mut name) = pending_longname.take() {
                if name.last() == Some(&0) {
                    name.pop();
                }
                output_path(Path::new(std::str::from_utf8(&name)?), kind.is_dir())?
            } else {
                output_path(&entry.path()?, kind.is_dir())?
            };
            if !paths.insert(name.clone()) {
                return Err("duplicate archive path".into());
            }
            let is_manifest = name == "manifest.json";
            let is_target = name == relative;
            if (kind.is_dir() && size != 0)
                || ((is_manifest || is_target) && !kind.is_file())
                || (is_manifest && size > MAX_MANIFEST)
                || (is_target && size > MAX_BINARY)
            {
                return Err("invalid archive entry size or type".into());
            }
            last_padded_end = entry
                .raw_file_position()
                .checked_add(size.checked_add(511).ok_or("archive size overflow")? / 512 * 512)
                .ok_or("archive position overflow")?;
            if is_manifest {
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes)?;
                manifest = Some(parse_manifest(&bytes, &expected)?);
            } else if is_target {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(&temporary)?;
                owns_temporary = true;
                digest = Some(copy_binary(&mut entry, &mut file, deadline, stopped)?);
                target = Some(file);
            } else {
                std::io::copy(&mut entry, &mut std::io::sink())?;
            }
        }
        if pending_longname.is_some() {
            return Err("dangling GNU longname metadata".into());
        }
        let mut decoded = archive.into_inner();
        let consumed = decoded_limit - decoded.inner.limit();
        if consumed
            != last_padded_end
                .checked_add(512)
                .ok_or("archive position overflow")?
        {
            return Err("missing first tar zero header".into());
        }
        let mut tail = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let count = decoded.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            if buffer[..count].iter().any(|byte| *byte != 0) {
                return Err("nonzero tar trailer".into());
            }
            tail += u64::try_from(count)?;
        }
        let total = decoded_limit - decoded.inner.limit();
        if tail < 512 || total > decoded_cap || !total.is_multiple_of(512) {
            return Err("invalid tar trailer or expanded size".into());
        }
        let mut compressed = decoded.inner.into_inner().into_inner();
        if !compressed.fill_buf()?.is_empty() {
            return Err("gzip trailing data or second member".into());
        }
        let prepared = manifest.ok_or("missing manifest")?;
        if digest.as_deref() != Some(prepared.sha256.as_str()) {
            return Err("artifact digest mismatch or missing fixed binary".into());
        }
        let target = target.ok_or("missing fixed binary")?;
        target.set_permissions(std::fs::Permissions::from_mode(0o700))?;
        target.sync_all()?;
        drop(target);
        budget(deadline, stopped)?;
        std::fs::rename(&temporary, output)?;
        published = true;
        File::open(output.parent().ok_or("missing artifact parent")?)?.sync_all()?;
        Ok(prepared)
    })();
    if result.is_err() {
        if owns_temporary {
            let _ = std::fs::remove_file(&temporary);
        }
        if published {
            let _ = std::fs::remove_file(output);
        }
    }
    result
}

pub(in crate::rollout) fn prepare(
    directory: &Path,
    request: &Submission,
    deadline: Instant,
    stopped: &AtomicBool,
    runtime: &tokio::runtime::Handle,
) -> Result<super::PreparedBinary> {
    request.validate()?;
    let remote = request
        .remote_archive
        .as_ref()
        .ok_or("remote descriptor required")?;
    prepare_binary(
        directory,
        remote,
        super::ExpectedBinary {
            role: request.role,
            commit: &request.commit,
            sha256: request.sha256.as_deref(),
        },
        deadline,
        stopped,
        runtime,
    )
}

pub(crate) fn prepare_binary(
    directory: &Path,
    remote: &super::super::RemoteArchive,
    expected: super::ExpectedBinary<'_>,
    deadline: Instant,
    stopped: &AtomicBool,
    runtime: &tokio::runtime::Handle,
) -> Result<super::PreparedBinary> {
    budget(deadline, stopped)?;
    remote.validate()?;
    let archive_path = directory.join("archive.partial");
    let mut compressed = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&archive_path)?;
    let result = (|| {
        runtime.block_on(download(remote, &mut compressed, deadline, stopped))?;
        compressed.sync_all()?;
        scan(
            &mut compressed,
            expected,
            &directory.join("executable"),
            deadline,
            stopped,
            MAX_DECODED,
        )
    })();
    drop(compressed);
    let _ = std::fs::remove_file(&archive_path);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn tar_bytes(entries: &[(&str, u8, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, kind, bytes) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::new(*kind));
            header.set_size(bytes.len() as u64);
            header.set_mode(0o777);
            header.set_cksum();
            builder.append_data(&mut header, path, *bytes).unwrap();
        }
        builder.into_inner().unwrap()
    }

    fn expected() -> ExpectedBinary<'static> {
        ExpectedBinary {
            role: crate::config::Role::Relay,
            commit: "cccccccccccccccccccccccccccccccccccccccc",
            sha256: None,
        }
    }

    fn package(entries: &[(&str, u8, &[u8])]) -> Vec<u8> {
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "commit": expected().commit, "target": host_target().unwrap(),
            "files": {
                "bin/adx-relay": format!("{:x}", Sha256::digest(b"selected executable")),
                "sdk/ignored.whl": "0".repeat(64)
            }
        }))
        .unwrap();
        let mut entries = entries.to_vec();
        entries.push(("./bin/adx-relay", b'0', b"selected executable"));
        entries.push(("./manifest.json", b'0', &manifest));
        tar_bytes(&entries)
    }

    fn scans(bytes: &[u8]) -> Result<String> {
        scans_with(bytes, MAX_DECODED, false)
    }

    fn scans_with(bytes: &[u8], cap: u64, stopped: bool) -> Result<String> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("archive");
        std::fs::write(&path, bytes)?;
        let output = root.path().join("executable");
        let result = scan(
            &mut File::open(&path)?,
            expected(),
            &output,
            Instant::now() + Duration::from_secs(900),
            &AtomicBool::new(stopped),
            cap,
        );
        assert_eq!(output.exists(), result.is_ok());
        assert!(!output.with_extension("partial").exists());
        assert_eq!(
            std::fs::read_dir(root.path())?.count(),
            1 + usize::from(result.is_ok())
        );
        if result.is_ok() {
            assert_eq!(std::fs::read(output)?, b"selected executable");
        }
        result.map(|prepared| prepared.sha256)
    }

    #[test]
    fn gnu_long_wheel_is_skipped_and_manifest_can_follow_the_binary() {
        let wheel = format!("./sdk/{}_aarch64.whl", "sdk".repeat(50));
        let digest = scans(&gzip(&package(&[
            ("./", b'5', b""),
            (&wheel, b'0', b"wheel"),
            ("sdk/ignored.whl", b'0', b"ignored SDK payload"),
        ])))
        .unwrap();
        assert_eq!(
            digest,
            format!("{:x}", Sha256::digest(b"selected executable"))
        );
    }

    #[test]
    fn gzip_and_tar_termination_are_required() {
        let valid = package(&[("./file", b'0', b"payload")]);
        assert!(scans(&gzip(&valid)).is_ok());
        for cut in [1024, 1536] {
            assert!(scans(&gzip(&valid[..cut])).is_err());
        }
        let mut trailing = valid.clone();
        trailing.push(1);
        assert!(scans(&gzip(&trailing)).is_err());
        let mut crc = gzip(&valid);
        let offset = crc.len() - 8;
        crc[offset] ^= 1;
        assert!(scans(&crc).is_err());
        let mut members = gzip(&valid);
        members.extend(gzip(b""));
        assert!(scans(&members).is_err());
        let mut garbage = gzip(&valid);
        garbage.push(0);
        assert!(scans(&garbage).is_err());
    }

    #[test]
    fn unsafe_types_sizes_paths_and_duplicate_names_are_rejected() {
        for (path, kind) in [
            ("manifest.json", b'0'),
            ("bin/adx-relay", b'0'),
            ("link", b'1'),
            ("link", b'2'),
            ("metadata", b'x'),
            ("metadata", b'g'),
            ("metadata", b'K'),
            ("sparse", b'S'),
            ("device", b'3'),
            ("device", b'4'),
            ("fifo", b'6'),
        ] {
            assert!(
                scans(&gzip(&package(&[(path, kind, b"")]))).is_err(),
                "type={kind}"
            );
        }
        assert!(scans(&gzip(&package(&[("directory", b'5', b"payload")]))).is_err());
        assert!(scans(&gzip(&package(&[
            ("directory", b'5', b""),
            ("./directory/", b'5', b"")
        ])))
        .is_err());
        for (path, size) in [
            ("manifest.json", MAX_MANIFEST + 1),
            ("bin/adx-relay", MAX_BINARY + 1),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(size);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_cksum();
            let mut bytes = header.as_bytes().to_vec();
            bytes.extend([0; 1024]);
            assert!(scans(&gzip(&bytes)).is_err());
        }
        for path in [
            "/root",
            "../file",
            "./../file",
            "././file",
            "a/./b",
            "a//b",
            "a\\b",
        ] {
            assert!(output_path(Path::new(path), false).is_err(), "{path}");
        }
    }

    #[test]
    fn gnu_longname_metadata_is_bounded_and_must_describe_one_entry() {
        let mut name = vec![b'a'; 4096];
        name.push(0);
        assert!(scans(&gzip(&package(&[
            ("longname", b'L', &name),
            ("placeholder", b'0', b""),
        ])))
        .is_err());
        for entries in [
            vec![("longname", b'L', b"file\0".as_slice())],
            vec![
                ("longname", b'L', b"file\0".as_slice()),
                ("longname", b'L', b"file\0".as_slice()),
            ],
        ] {
            assert!(scans(&gzip(&tar_bytes(&entries))).is_err());
        }
        let mut header = tar::Header::new_gnu();
        header.set_path("longname").unwrap();
        header.set_entry_type(tar::EntryType::GNULongName);
        header.set_size(MAX_DECODED);
        header.set_cksum();
        assert!(scans(&gzip(header.as_bytes()))
            .unwrap_err()
            .to_string()
            .contains("longname"));
    }

    #[test]
    fn entry_count_is_bounded_and_stopped_scan_does_not_publish() {
        let names: Vec<_> = (0..MAX_HEADERS)
            .map(|index| format!("dir{index}"))
            .collect();
        let entries: Vec<_> = names
            .iter()
            .map(|name| (name.as_str(), b'5', b"".as_slice()))
            .collect();
        assert!(scans(&gzip(&package(&entries)))
            .unwrap_err()
            .to_string()
            .contains("too many archive entries"));
        let longnames: Vec<_> = (0..MAX_HEADERS / 2)
            .map(|index| format!("file{index}\0"))
            .collect();
        let entries: Vec<_> = longnames
            .iter()
            .flat_map(|name| {
                [
                    ("longname", b'L', name.as_bytes()),
                    ("placeholder", b'0', b"".as_slice()),
                ]
            })
            .collect();
        assert!(scans(&gzip(&package(&entries)))
            .unwrap_err()
            .to_string()
            .contains("too many archive entries"));
        assert!(scans_with(&gzip(&package(&[])), MAX_DECODED, true).is_err());
    }

    #[test]
    fn only_recoverable_http_statuses_retry() {
        for status in [429, 500, 502, 503, 504] {
            assert!(retryable(reqwest::StatusCode::from_u16(status).unwrap()));
        }
        for status in [200, 301, 302, 307, 308, 400, 401, 403, 404, 501, 505] {
            assert!(!retryable(reqwest::StatusCode::from_u16(status).unwrap()));
        }
    }

    #[test]
    fn download_boundaries_without_network() {
        assert_eq!(
            downloaded_chunk(MAX_ARCHIVE - 1, b"x").unwrap(),
            MAX_ARCHIVE
        );
        assert!(downloaded_chunk(MAX_ARCHIVE, b"x").is_err());
        assert!(downloaded_chunk(u64::MAX, b"x").is_err());
        let hash = Sha256::new_with_prefix(b"payload");
        let digest = format!("{:x}", Sha256::digest(b"payload"));
        assert!(downloaded_identity(7, Some(7), hash.clone(), &digest).is_ok());
        assert!(downloaded_identity(7, Some(8), hash.clone(), &digest).is_err());
        assert!(downloaded_identity(7, None, hash, &"0".repeat(64)).is_err());
    }

    #[test]
    fn retry_after_seconds_and_shared_remaining_budget() {
        let fallback = Duration::from_secs(2);
        let remaining = Duration::from_secs(60);
        for (value, seconds) in [
            (Some("12"), 12),
            (Some("Wed, 11 Nov 2026 12:00:30 GMT"), 2),
            (Some("0"), 0),
            (Some("invalid"), 2),
            (None, 2),
        ] {
            assert_eq!(
                retry_delay(value, fallback, remaining).unwrap(),
                Duration::from_secs(seconds)
            );
        }
        for value in ["61", "18446744073709551616"] {
            assert!(
                retry_delay(Some(value), fallback, remaining).is_err(),
                "{value}"
            );
        }
        assert_eq!(
            retry_delay(Some("60"), fallback, remaining).unwrap(),
            remaining
        );
        // A later retry sees only the remainder, not a new preparation interval.
        assert!(retry_delay(Some("30"), fallback, Duration::from_secs(29)).is_err());
        assert!(retry_delay(None, fallback, Duration::from_secs(1)).is_err());
        assert_eq!(
            retry_delay(Some("0"), fallback, Duration::ZERO).unwrap(),
            Duration::ZERO
        );
    }

    #[test]
    fn decoded_cap_counts_headers_and_padding_before_publication() {
        let tar = package(&[]);
        let total = u64::try_from(tar.len()).unwrap();
        for cap in [1, 511, 512, 1024, total - 1, total] {
            assert_eq!(
                scans_with(&gzip(&tar), cap, false).is_ok(),
                cap == total,
                "cap={cap}"
            );
        }
    }

    #[test]
    fn expired_or_stopped_remote_preparation_creates_nothing() {
        let root = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let request = Submission {
            component_config: None,
            selection: super::super::super::Selection::LocalManual,
            task_id: "remote".into(),
            role: crate::config::Role::Relay,
            service_id: "relay".into(),
            commit: "c".repeat(40),
            sha256: Some("b".repeat(64)),
            process: None,
            remote_archive: Some(super::super::super::RemoteArchive {
                url: "https://must-not-connect.invalid/archive.tar.gz".into(),
                archive_sha256: "a".repeat(64),
            }),
        };
        for (deadline, stopped) in [
            (Instant::now(), false),
            (Instant::now() + Duration::from_secs(900), true),
        ] {
            assert!(prepare(
                root.path(),
                &request,
                deadline,
                &AtomicBool::new(stopped),
                runtime.handle()
            )
            .is_err());
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        }
    }
}
