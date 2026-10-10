//! Node-owned host files for sandboxd's runtime stdout and stderr redirects.
//! A live backend keeps its file descriptors open, so only terminated streams
//! are compressed or removed here.
use flate2::{write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeLogPolicy {
    pub directory: PathBuf,
    /// Count is per runtime; its stdout and stderr files form one retained pair.
    pub max_terminated: usize,
    pub max_age_seconds: u64,
    /// Budget for terminated streams only; live output remains backend-owned.
    pub max_total_bytes: u64,
    /// Give filelog time to read a closed stream before compression.
    pub compress_after_seconds: u64,
    pub interval_seconds: u64,
}

impl Default for RuntimeLogPolicy {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("/opt/adx/logs/runtime"),
            max_terminated: 50,
            max_age_seconds: 86_400,
            max_total_bytes: 1024 * 1024 * 1024,
            compress_after_seconds: 300,
            interval_seconds: 60,
        }
    }
}

impl RuntimeLogPolicy {
    pub fn validate(&self) -> io::Result<()> {
        if !self.directory.is_absolute()
            || self.max_terminated == 0
            || self.max_age_seconds == 0
            || self.max_total_bytes == 0
            || self.interval_seconds == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "absolute runtime log directory and positive retention limits required",
            ));
        }
        Ok(())
    }

    pub fn paths(&self, runtime_id: &str) -> io::Result<(String, String)> {
        self.validate()?;
        if runtime_id.is_empty()
            || runtime_id.len() > 240
            || runtime_id.starts_with('.')
            || !runtime_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime ID is not a safe log file name",
            ));
        }
        let path = |extension| {
            self.directory
                .join(format!("{runtime_id}.{extension}"))
                .into_os_string()
                .into_string()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "log path is not UTF-8"))
        };
        Ok((path("out")?, path("err")?))
    }
}

#[derive(Clone)]
pub struct RuntimeLogs {
    policy: RuntimeLogPolicy,
}

#[derive(Default)]
struct Streams {
    files: Vec<(PathBuf, u64)>,
    bytes: u64,
}

impl RuntimeLogs {
    pub fn new(policy: RuntimeLogPolicy) -> io::Result<Self> {
        policy.validate()?;
        fs::create_dir_all(&policy.directory)?;
        let metadata = fs::symlink_metadata(&policy.directory)?;
        if !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime log directory must not be a symlink",
            ));
        }
        fs::set_permissions(&policy.directory, fs::Permissions::from_mode(0o700))?;
        Ok(Self { policy })
    }

    pub fn interval_seconds(&self) -> u64 {
        self.policy.interval_seconds
    }

    pub fn paths(&self, runtime_id: &str) -> io::Result<(String, String)> {
        self.policy.paths(runtime_id)
    }

    /// Call only after a successful authoritative sandboxd inventory. An
    /// unavailable backend cannot prove that a runtime has terminated.
    pub fn collect(&self, active: &HashSet<String>, now: SystemTime) -> io::Result<usize> {
        let now_seconds = now
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs();
        let mut files = self.scan()?;
        let mut ended = self.load_index()?;
        ended.retain(|runtime_id, _| files.contains_key(runtime_id));
        for runtime_id in files.keys() {
            if active.contains(runtime_id) {
                ended.remove(runtime_id);
            } else {
                ended.entry(runtime_id.clone()).or_insert(now_seconds);
            }
        }
        // Persist the first observed termination time before any deletion.
        self.save_index(&ended)?;
        for (runtime_id, first_seen) in &ended {
            if now_seconds.saturating_sub(*first_seen) < self.policy.compress_after_seconds {
                continue;
            }
            if let Some(streams) = files.get(runtime_id) {
                for (path, _) in &streams.files {
                    if path.extension().is_some_and(|extension| extension == "gz") {
                        continue;
                    }
                    compress(path)?;
                }
            }
        }
        files = self.scan()?;
        let mut ordered: Vec<_> = ended.iter().map(|(id, time)| (id.clone(), *time)).collect();
        ordered.sort_by_key(|(id, time)| (*time, id.clone()));
        let mut count = ordered.len();
        let mut bytes: u128 = ordered
            .iter()
            .filter_map(|(id, _)| files.get(id))
            .map(|stream| u128::from(stream.bytes))
            .sum();
        let mut removed = 0;
        for (runtime_id, first_seen) in ordered {
            if count <= self.policy.max_terminated
                && bytes <= u128::from(self.policy.max_total_bytes)
                && now_seconds.saturating_sub(first_seen) < self.policy.max_age_seconds
            {
                continue;
            }
            if let Some(streams) = files.get(&runtime_id) {
                for (path, _) in &streams.files {
                    fs::remove_file(path)?;
                }
                bytes = bytes.saturating_sub(u128::from(streams.bytes));
            }
            ended.remove(&runtime_id);
            count -= 1;
            removed += 1;
        }
        self.save_index(&ended)?;
        Ok(removed)
    }

    fn scan(&self) -> io::Result<BTreeMap<String, Streams>> {
        let mut groups = BTreeMap::<String, Streams>::new();
        for entry in fs::read_dir(&self.policy.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(runtime_id) = name
                .strip_suffix(".out")
                .or_else(|| name.strip_suffix(".err"))
                .or_else(|| name.strip_suffix(".out.gz"))
                .or_else(|| name.strip_suffix(".err.gz"))
            else {
                continue;
            };
            if self.paths(runtime_id).is_err() {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() {
                continue;
            }
            let streams = groups.entry(runtime_id.to_owned()).or_default();
            streams.bytes = streams.bytes.saturating_add(metadata.len());
            streams.files.push((entry.path(), metadata.len()));
        }
        Ok(groups)
    }

    fn index_path(&self) -> PathBuf {
        self.policy.directory.join(".terminated.json")
    }

    fn load_index(&self) -> io::Result<BTreeMap<String, u64>> {
        match fs::read(self.index_path()) {
            Ok(content) => serde_json::from_slice(&content).map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(error) => Err(error),
        }
    }

    fn save_index(&self, index: &BTreeMap<String, u64>) -> io::Result<()> {
        let temp = self.policy.directory.join(".terminated.json.tmp");
        let content = serde_json::to_vec(index).map_err(io::Error::other)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temp)?;
        file.write_all(&content)?;
        file.sync_all()?;
        fs::rename(temp, self.index_path())
    }
}

fn compress(source: &Path) -> io::Result<()> {
    let target = PathBuf::from(format!("{}.gz", source.display()));
    let temp = PathBuf::from(format!("{}.tmp", target.display()));
    let mut input = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(source)?;
    let output = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temp)?;
    let mut encoder = GzEncoder::new(output, Compression::fast());
    io::copy(&mut input, &mut encoder)?;
    let output: File = encoder.finish()?;
    output.sync_all()?;
    fs::rename(temp, target)?;
    fs::remove_file(source)
}
