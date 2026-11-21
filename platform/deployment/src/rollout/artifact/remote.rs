//! Task-private acquisition. Neither downloaded content nor package scripts execute.
use super::*;
use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader, Seek, SeekFrom},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime},
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

fn retry_delay(
    value: Option<&str>,
    now: SystemTime,
    fallback: Duration,
    remaining: Duration,
) -> Result<Duration> {
    let delay = match value.map(str::trim) {
        Some(value) if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
            Duration::from_secs(
                value
                    .parse::<u64>()
                    .map_err(|_| "Retry-After exceeds preparation budget")?,
            )
        }
        Some(value) => httpdate::parse_http_date(value)
            .ok()
            .map(|date| date.duration_since(now).unwrap_or(Duration::ZERO))
            .unwrap_or(fallback),
        None => fallback,
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
                    SystemTime::now(),
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
        || value.contains('\\')
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
    output: Option<&Path>,
    deadline: Instant,
    stopped: &AtomicBool,
    decoded_cap: u64,
) -> Result<()> {
    budget(deadline, stopped)?;
    compressed.seek(SeekFrom::Start(0))?;
    let decoder = flate2::bufread::GzDecoder::new(BufReader::new(compressed));
    let decoded_limit = decoded_cap.checked_add(1).ok_or("decoded limit overflow")?;
    let mut archive = tar::Archive::new(decoder.take(decoded_limit));
    let mut headers = 0usize;
    let mut last_padded_end = 0u64;
    let mut paths = BTreeSet::new();
    let mut root_seen = false;
    {
        let mut entries = archive.entries()?.raw(output.is_none());
        for entry in &mut entries {
            budget(deadline, stopped)?;
            let mut entry = entry?;
            headers = headers.checked_add(1).ok_or("archive header overflow")?;
            if headers > MAX_HEADERS {
                return Err("too many archive headers".into());
            }
            let kind = entry.header().entry_type();
            let size = entry.size();
            let longname = kind.as_byte() == b'L';
            if !(matches!(kind.as_byte(), b'0' | 0 | b'5') || (output.is_none() && longname)) {
                return Err("unsupported archive header type".into());
            }
            if (longname && size > 4096) || (kind.is_dir() && size != 0) {
                return Err("invalid archive metadata size".into());
            }
            last_padded_end = entry
                .raw_file_position()
                .checked_add(size.checked_add(511).ok_or("archive size overflow")? / 512 * 512)
                .ok_or("archive position overflow")?;
            let mut target = None;
            if let Some(output) = output {
                let name = output_path(&entry.path()?, kind.is_dir())?;
                if name.is_empty() {
                    if root_seen {
                        return Err("duplicate archive root".into());
                    }
                    root_seen = true;
                } else {
                    if !paths.insert(name.clone()) {
                        return Err("duplicate archive path".into());
                    }
                    let path = output.join(&name);
                    if kind.is_dir() {
                        std::fs::create_dir_all(&path)?;
                        if !std::fs::symlink_metadata(&path)?.is_dir() {
                            return Err("archive directory conflict".into());
                        }
                    } else {
                        std::fs::create_dir_all(path.parent().ok_or("missing archive parent")?)?;
                        target = Some(
                            OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .mode(0o600)
                                .custom_flags(libc::O_NOFOLLOW)
                                .open(&path)?,
                        );
                    }
                }
            }
            let mut buffer = [0u8; 64 * 1024];
            loop {
                budget(deadline, stopped)?;
                let count = entry.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                if let Some(file) = target.as_mut() {
                    file.write_all(&buffer[..count])?;
                }
            }
        }
    }
    let mut decoded = archive.into_inner();
    let consumed = decoded_limit - decoded.limit();
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
        budget(deadline, stopped)?;
        let count = decoded.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if buffer[..count].iter().any(|byte| *byte != 0) {
            return Err("nonzero tar trailer".into());
        }
        tail += count as u64;
    }
    let total = decoded_limit - decoded.limit();
    if tail < 512 || total > decoded_cap || total % 512 != 0 {
        return Err("invalid tar trailer or expanded size".into());
    }
    let mut compressed = decoded.into_inner().into_inner();
    if !compressed.fill_buf()?.is_empty() {
        return Err("gzip trailing data or second member".into());
    }
    Ok(())
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
    let unpacked = directory.join("unpacked.partial");
    let mut compressed = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&archive_path)?;
    let mut owns_unpacked = false;
    let result = (|| {
        runtime.block_on(download(remote, &mut compressed, deadline, stopped))?;
        compressed.sync_all()?;
        // Open the exclusively created task-private file read-only once; both
        // passes retain that descriptor rather than reopening between passes.
        let mut readonly = child(&File::open(directory)?, "archive.partial", false)?;
        drop(compressed);
        scan(&mut readonly, None, deadline, stopped, MAX_DECODED)?;
        budget(deadline, stopped)?;
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(&unpacked)?;
        owns_unpacked = true;
        let package = unpacked.join("package");
        std::fs::DirBuilder::new().mode(0o700).create(&package)?;
        scan(
            &mut readonly,
            Some(&package),
            deadline,
            stopped,
            MAX_DECODED,
        )?;
        let pinned = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&package)?;
        super::prepare_package(
            &pinned,
            expected,
            &directory.join("executable"),
            deadline,
            stopped,
        )
    })();
    let _ = std::fs::remove_file(&archive_path);
    if owns_unpacked {
        let _ = std::fs::remove_dir_all(&unpacked);
    }
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

    fn scans(bytes: &[u8]) -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("archive");
        std::fs::write(&path, bytes)?;
        let mut file = File::open(&path)?;
        let deadline = Instant::now() + Duration::from_secs(900);
        let stopped = AtomicBool::new(false);
        scan(&mut file, None, deadline, &stopped, MAX_DECODED)?;
        let output = root.path().join("output");
        std::fs::create_dir(&output)?;
        scan(&mut file, Some(&output), deadline, &stopped, MAX_DECODED)
    }

    #[test]
    fn real_gnu_root_long_wheel_and_schema_one_prepare() {
        let root = tempfile::tempdir().unwrap();
        let commit = "c".repeat(40);
        let binary = b"selected executable";
        let wheel = format!("./sdk/{}_aarch64.whl", "sdk".repeat(50));
        let digest = format!("{:x}", Sha256::digest(binary));
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "commit": commit, "target": host_target().unwrap(),
            "profile": "release", "with_afs": false,
            "files": {"bin/adx-relay": digest, (wheel.strip_prefix("./").unwrap()): format!("{:x}", Sha256::digest(b"wheel"))}
        })).unwrap();
        let bytes = gzip(&tar_bytes(&[
            ("./", b'5', b""),
            ("./manifest.json", b'0', &manifest),
            ("./bin/adx-relay", b'0', binary),
            (&wheel, b'0', b"wheel"),
        ]));
        let archive = root.path().join("archive");
        std::fs::write(&archive, bytes).unwrap();
        let mut file = File::open(archive).unwrap();
        let deadline = Instant::now() + Duration::from_secs(900);
        let stopped = AtomicBool::new(false);
        scan(&mut file, None, deadline, &stopped, MAX_DECODED).unwrap();
        let package = root.path().join(&commit);
        std::fs::create_dir(&package).unwrap();
        scan(&mut file, Some(&package), deadline, &stopped, MAX_DECODED).unwrap();
        let request = Submission {
            component_config: None,
            selection: super::super::super::Selection::LocalManual,
            task_id: "remote".into(),
            role: crate::config::Role::Relay,
            service_id: "relay".into(),
            commit,
            sha256: Some(digest),
            remote_archive: None,
            process: None,
        };
        let output = root.path().join("executable");
        super::super::prepare_cancellable(root.path(), &request, &output, deadline, &stopped)
            .unwrap();
        assert_eq!(std::fs::read(output).unwrap(), binary);
    }

    #[test]
    fn gzip_and_tar_termination_are_required() {
        let valid = tar_bytes(&[("./file", b'0', b"payload")]);
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
    fn metadata_types_sizes_and_output_conflicts_are_rejected() {
        for kind in [b'K', b'x', b'g', b'S', b'1', b'2', b'3', b'4', b'6'] {
            assert!(scans(&gzip(&tar_bytes(&[("entry", kind, b"")]))).is_err());
        }
        assert!(scans(&gzip(&tar_bytes(&[(
            "././@LongLink",
            b'L',
            &vec![b'a'; 4097]
        )])))
        .is_err());
        assert!(scans(&gzip(&tar_bytes(&[("dir", b'5', b"payload")]))).is_err());
        for entries in [
            vec![
                ("file", b'0', b"".as_slice()),
                ("./file", b'0', b"".as_slice()),
            ],
            vec![
                ("dir", b'0', b"".as_slice()),
                ("dir/file", b'0', b"".as_slice()),
            ],
            vec![
                ("dir/file", b'0', b"".as_slice()),
                ("dir", b'0', b"".as_slice()),
            ],
            vec![
                ("dir", b'5', b"".as_slice()),
                ("dir/", b'5', b"".as_slice()),
            ],
        ] {
            assert!(scans(&gzip(&tar_bytes(&entries))).is_err());
        }
        for path in [
            "/root",
            "../file",
            "./../file",
            "a/./b",
            "a//b",
            "a\\b",
            "././file",
        ] {
            assert!(output_path(Path::new(path), false).is_err(), "{path}");
        }
        assert_eq!(
            output_path(Path::new("./manifest.json"), false).unwrap(),
            "manifest.json"
        );
    }

    #[test]
    fn header_count_is_bounded_and_stopped_scan_does_not_extract() {
        let entries = vec![("file", b'0', b"".as_slice()); MAX_HEADERS + 1];
        assert!(scans(&gzip(&tar_bytes(&entries))).is_err());
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("archive");
        std::fs::write(&path, gzip(&tar_bytes(&[("file", b'0', b"payload")]))).unwrap();
        let mut file = File::open(path).unwrap();
        assert!(scan(
            &mut file,
            Some(root.path()),
            Instant::now() + Duration::from_secs(900),
            &AtomicBool::new(true),
            MAX_DECODED
        )
        .is_err());
        assert!(!root.path().join("file").exists());
        assert!(budget(Instant::now(), &AtomicBool::new(false)).is_err());
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
    fn retry_after_seconds_dates_and_shared_remaining_budget() {
        let now = httpdate::parse_http_date("Wed, 11 Nov 2026 12:00:00 GMT").unwrap();
        let fallback = Duration::from_secs(2);
        let remaining = Duration::from_secs(60);
        for (value, seconds) in [
            (Some("12"), 12),
            (Some(" 12 "), 12),
            (Some("Wed, 11 Nov 2026 12:00:30 GMT"), 30),
            (Some("Wed, 11 Nov 2026 11:59:59 GMT"), 0),
            (Some("0"), 0),
            (Some("invalid"), 2),
            (Some("-1"), 2),
            (Some(""), 2),
            (None, 2),
        ] {
            assert_eq!(
                retry_delay(value, now, fallback, remaining).unwrap(),
                Duration::from_secs(seconds)
            );
        }
        for value in [
            "61",
            "18446744073709551615",
            "18446744073709551616",
            "9999999999999999999999999999999999999999999999999999999999",
            "Wed, 11 Nov 2026 12:01:01 GMT",
        ] {
            assert!(
                retry_delay(Some(value), now, fallback, remaining).is_err(),
                "{value}"
            );
        }
        assert_eq!(
            retry_delay(Some("60"), now, fallback, remaining).unwrap(),
            remaining
        );
        // A later retry sees only the remainder, not a new preparation interval.
        assert!(retry_delay(Some("30"), now, fallback, Duration::from_secs(29)).is_err());
        assert!(retry_delay(None, now, fallback, Duration::from_secs(1)).is_err());
        assert_eq!(
            retry_delay(Some("0"), now, fallback, Duration::ZERO).unwrap(),
            Duration::ZERO
        );
    }

    #[test]
    fn decoded_cap_counts_headers_and_padding_in_both_passes() {
        let root = tempfile::tempdir().unwrap();
        let tar = tar_bytes(&[("./file", b'0', b"x")]);
        let total = u64::try_from(tar.len()).unwrap();
        let path = root.path().join("archive");
        std::fs::write(&path, gzip(&tar)).unwrap();
        let mut file = File::open(path).unwrap();
        let deadline = Instant::now() + Duration::from_secs(900);
        let stopped = AtomicBool::new(false);
        for extract in [false, true] {
            for cap in [1, 511, 512, 1024, total - 1, total] {
                let output = root.path().join(format!("output-{extract}-{cap}"));
                std::fs::create_dir(&output).unwrap();
                let result = scan(
                    &mut file,
                    extract.then_some(output.as_path()),
                    deadline,
                    &stopped,
                    cap,
                );
                assert_eq!(result.is_ok(), cap == total, "extract={extract}, cap={cap}");
            }
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
