//! Node capacity sources. Observations replace capacity, never allocated usage.
use adx_core::{
    scheduling::{Device, DeviceKind},
    Error, Resources, Result,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Limited};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("resource observation: {error}"))
}
fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("resource observation: {error}"))
}

#[derive(Clone, Debug, Deserialize)]
pub struct Observation {
    pub capacity: Resources,
    #[serde(default)]
    pub devices: Vec<Device>,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSource {
    File {
        path: PathBuf,
    },
    Sandboxd {
        socket: PathBuf,
        valid_for_seconds: u64,
    },
    Auto {
        disk_path: PathBuf,
        valid_for_seconds: u64,
    },
}
impl ResourceSource {
    pub async fn sample(&self, timeout: Duration) -> Result<(Observation, Duration)> {
        let (observation, valid) = match self {
            Self::File { path } => {
                #[derive(Deserialize)]
                struct FileObservation {
                    #[serde(flatten)]
                    observation: Observation,
                    valid_until_unix_seconds: u64,
                }
                let o: FileObservation =
                    serde_json::from_slice(&tokio::fs::read(path).await.map_err(unavailable)?)
                        .map_err(invalid)?;
                let valid = o
                    .valid_until_unix_seconds
                    .checked_sub(crate::checkpoint::now()?)
                    .filter(|v| *v > 0)
                    .ok_or_else(|| unavailable("expired capacity file"))?;
                (o.observation, valid)
            }
            Self::Sandboxd {
                socket,
                valid_for_seconds,
            } => {
                let sample = tokio::time::timeout(timeout, async {
                    let stream = tokio::net::UnixStream::connect(socket)
                        .await
                        .map_err(unavailable)?;
                    let (mut sender, connection) =
                        hyper::client::conn::http1::handshake(TokioIo::new(stream))
                            .await
                            .map_err(unavailable)?;
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    let request = hyper::Request::get("http://localhost/resource")
                        .header(hyper::header::HOST, "localhost")
                        .body(Empty::<Bytes>::new())
                        .map_err(invalid)?;
                    let response = sender.send_request(request).await.map_err(unavailable)?;
                    if response.status() != hyper::StatusCode::OK {
                        return Err(unavailable(response.status()));
                    }
                    let body = Limited::new(response.into_body(), 1024 * 1024)
                        .collect()
                        .await
                        .map_err(unavailable)?
                        .to_bytes();
                    parse_external(&body)
                })
                .await
                .map_err(|_| unavailable("sandboxd resource request timed out"))??;
                (sample, *valid_for_seconds)
            }
            Self::Auto {
                disk_path,
                valid_for_seconds,
            } => {
                let path = disk_path.clone();
                let sample = tokio::task::spawn_blocking(move || detect(&path))
                    .await
                    .map_err(unavailable)??;
                (sample, *valid_for_seconds)
            }
        };
        if valid == 0 {
            return Err(invalid("validity must be positive"));
        }
        Ok((observation, Duration::from_secs(valid)))
    }
}

pub fn parse_external(bytes: &[u8]) -> Result<Observation> {
    #[derive(Deserialize)]
    struct External {
        cpu: u64,
        mem: u64,
        storage: Option<u64>,
        #[serde(default)]
        xpu: Vec<Xpu>,
    }
    #[derive(Deserialize)]
    struct Xpu {
        r#type: String,
        product_model: String,
        device_ids: Vec<u32>,
    }
    let source: External = serde_json::from_slice(bytes).map_err(invalid)?;
    let capacity = Resources {
        cpu_millis: source
            .cpu
            .checked_mul(1000)
            .ok_or_else(|| invalid("CPU overflow"))?,
        memory_bytes: source.mem,
        disk_bytes: source.storage.unwrap_or(0),
    };
    let mut devices = vec![];
    let mut seen = BTreeSet::new();
    for xpu in source.xpu {
        let kind = match xpu.r#type.to_ascii_lowercase().as_str() {
            "gpu" => DeviceKind::Gpu,
            "npu" => DeviceKind::Npu,
            _ => return Err(invalid("unsupported XPU type")),
        };
        if xpu.product_model.is_empty() {
            return Err(invalid("empty XPU model"));
        }
        for id in xpu.device_ids {
            if !seen.insert((kind, id)) {
                return Err(invalid("duplicate XPU identity"));
            }
            devices.push(Device {
                kind,
                id,
                model: xpu.product_model.clone(),
                healthy: true,
            });
        }
    }
    Ok(Observation { capacity, devices })
}

pub fn cpu_quota(input: &str) -> Result<Option<u64>> {
    let fields: Vec<_> = input.split_whitespace().collect();
    if fields.len() != 2 {
        return Err(invalid("CPU quota format"));
    }
    let period: u64 = fields[1].parse().map_err(invalid)?;
    if period == 0 {
        return Err(invalid("CPU period is zero"));
    }
    if fields[0] == "max" || fields[0] == "-1" {
        return Ok(None);
    }
    let quota: u64 = fields[0].parse().map_err(invalid)?;
    Ok(Some(
        quota
            .checked_mul(1000)
            .ok_or_else(|| invalid("CPU quota overflow"))?
            / period,
    ))
}
pub fn cpu_set_count(input: &str) -> Result<u64> {
    let mut cpus = BTreeSet::new();
    for range in input.trim().split(',') {
        let (first, last) = range.split_once('-').unwrap_or((range, range));
        let first: u32 = first.parse().map_err(invalid)?;
        let last: u32 = last.parse().map_err(invalid)?;
        if last < first || last > 1_000_000 {
            return Err(invalid("CPU set range"));
        }
        for cpu in first..=last {
            if !cpus.insert(cpu) {
                return Err(invalid("overlapping CPU set"));
            }
        }
    }
    Ok(cpus.len() as u64)
}

fn optional(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(value) => Ok(Some(value)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(unavailable(e)),
    }
}
fn memory_info(key: &str) -> Result<u64> {
    let mem = std::fs::read_to_string("/proc/meminfo").map_err(unavailable)?;
    let line = mem
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .ok_or_else(|| unavailable("missing meminfo field"))?;
    line.split_whitespace()
        .next()
        .ok_or_else(|| invalid("empty memory value"))?
        .parse::<u64>()
        .map_err(invalid)?
        .checked_mul(1024)
        .ok_or_else(|| invalid("memory overflow"))
}

/// Resolve the current process's mounted cgroup hierarchies, including a Pod's
/// namespace root. Return each visible ancestor so a parent limit also applies.
fn cgroups() -> Result<Vec<(PathBuf, bool)>> {
    let memberships = std::fs::read_to_string("/proc/self/cgroup").map_err(unavailable)?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").map_err(unavailable)?;
    let mut result = BTreeSet::new();
    for line in mounts.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let fields: Vec<_> = before.split_whitespace().collect();
        let types: Vec<_> = after.split_whitespace().collect();
        if fields.len() < 5 || types.len() < 3 || !matches!(types[0], "cgroup" | "cgroup2") {
            continue;
        }
        let v2 = types[0] == "cgroup2";
        let unescape = |text: &str| {
            text.replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\134", "\\")
        };
        let root = PathBuf::from(unescape(fields[3]));
        let mount = PathBuf::from(unescape(fields[4]));
        for member in memberships.lines() {
            let part: Vec<_> = member.splitn(3, ':').collect();
            if part.len() != 3 {
                continue;
            }
            let matches = if v2 {
                part[0] == "0" && part[1].is_empty()
            } else {
                part[1]
                    .split(',')
                    .any(|c| types[2].split(',').any(|v| v == c))
            };
            if !matches {
                continue;
            }
            let membership = Path::new(part[2]);
            // A private cgroup namespace represents its mounted root as '/'.
            let relative = if membership == Path::new("/") {
                Path::new("")
            } else {
                membership
                    .strip_prefix(&root)
                    .map_err(|_| unavailable("cgroup mount does not contain process"))?
            };
            if relative
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(unavailable("cgroup parent path outside namespace"));
            }
            let mut path = mount.join(relative);
            loop {
                result.insert((path.clone(), v2));
                if path == mount || !path.pop() {
                    break;
                }
            }
        }
    }
    Ok(result.into_iter().collect())
}

// libc statvfs field widths differ between supported Unix targets.
#[allow(clippy::unnecessary_cast)]
fn disk(path: &Path) -> Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(invalid)?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: path is NUL terminated and stat points to writable storage.
        if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(unavailable(std::io::Error::last_os_error()));
        }
        // SAFETY: statvfs initialized the output on success.
        let stat = unsafe { stat.assume_init() };
        let size = stat.f_frsize as u64;
        Ok((
            (stat.f_blocks as u64).saturating_mul(size),
            (stat.f_bavail as u64).saturating_mul(size),
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(unavailable("statvfs unsupported"))
    }
}

pub fn detect(disk_path: &Path) -> Result<Observation> {
    let mut cpu = std::thread::available_parallelism()
        .map_err(unavailable)?
        .get() as u64
        * 1000;
    let mut memory = memory_info("MemTotal:")?;
    for (path, v2) in cgroups()? {
        let quota = if v2 {
            optional(&path.join("cpu.max"))?
        } else {
            optional(&path.join("cpu.cfs_quota_us"))?
                .map(|q| {
                    Ok(format!(
                        "{} {}",
                        q.trim(),
                        std::fs::read_to_string(path.join("cpu.cfs_period_us"))
                            .map_err(unavailable)?
                            .trim()
                    ))
                })
                .transpose()?
        };
        if let Some(q) = quota {
            if let Some(limit) = cpu_quota(&q)? {
                cpu = cpu.min(limit);
            }
        }
        let set_name = if v2 {
            "cpuset.cpus.effective"
        } else {
            "cpuset.cpus"
        };
        if let Some(set) = optional(&path.join(set_name))?.filter(|v| !v.trim().is_empty()) {
            cpu = cpu.min(cpu_set_count(&set)? * 1000);
        }
        let mem_name = if v2 {
            "memory.max"
        } else {
            "memory.limit_in_bytes"
        };
        if let Some(mem) = optional(&path.join(mem_name))?.filter(|v| v.trim() != "max") {
            memory = memory.min(mem.trim().parse().map_err(invalid)?);
        }
    }
    let capacity = Resources {
        cpu_millis: cpu,
        memory_bytes: memory,
        disk_bytes: disk(disk_path)?.1,
    };
    Ok(Observation {
        capacity,
        devices: vec![],
    })
}

#[derive(Clone, Copy, Debug)]
pub struct Pressure {
    pub memory_percent: u8,
    pub disk_percent: u8,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PressurePolicy {
    pub memory_high_percent: u8,
    pub memory_low_percent: u8,
    pub disk_high_percent: u8,
    pub disk_low_percent: u8,
}
impl Default for PressurePolicy {
    fn default() -> Self {
        Self {
            memory_high_percent: 90,
            memory_low_percent: 80,
            disk_high_percent: 90,
            disk_low_percent: 80,
        }
    }
}
/// Hysteresis level of one resource. Values inside the band between the low
/// and high thresholds keep the previous level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PressureLevel {
    #[default]
    Normal,
    High,
}

/// Per-resource pressure observation. The hysteresis level is the
/// authoritative state; the percent value is the last successful
/// observation that produced it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourcePressure {
    pub level: PressureLevel,
    /// Last successful observation, 0-100.
    pub percent: u8,
    /// Unix seconds of the last level transition; 0 = never transitioned.
    pub last_transition_unix_seconds: u64,
}

/// Node-level structured pressure snapshot published on every observation
/// round.
///
/// `observed_at_unix_seconds == 0` marks a node with no successful sample
/// yet (unconfigured or never sampled). Levels and the timestamp freeze
/// during failed samples while `consecutive_failures` grows: admission
/// stays fail-closed on a broken probe, and consumers deciding whether to
/// relieve load can refuse to act on stale evidence instead.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePressureState {
    pub memory: ResourcePressure,
    pub disk: ResourcePressure,
    /// Unix seconds of the last successful sample; 0 = never observed.
    pub observed_at_unix_seconds: u64,
    /// Consecutive failed samples since the last success.
    pub consecutive_failures: u32,
}

/// One observation round's combined output. `accepting` keeps the exact
/// historical admission semantics (true = the node accepts new work);
/// `state` is the structured snapshot for observers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PressureUpdate {
    pub accepting: bool,
    pub state: NodePressureState,
}

/// Wire form of the board state for the node heartbeat. Borrowed input: the
/// heartbeat path owns the observation round and converts without cloning.
impl From<&NodePressureState> for adx_protocol::control::NodePressureReport {
    fn from(state: &NodePressureState) -> Self {
        fn resource(pressure: &ResourcePressure) -> adx_protocol::control::NodePressureResource {
            let level = match pressure.level {
                PressureLevel::Normal => adx_protocol::control::NodePressureLevel::Normal,
                PressureLevel::High => adx_protocol::control::NodePressureLevel::High,
            };
            adx_protocol::control::NodePressureResource {
                level: i32::from(level),
                percent: u32::from(pressure.percent),
                last_transition_unix_seconds: pressure.last_transition_unix_seconds,
            }
        }
        Self {
            memory: Some(resource(&state.memory)),
            disk: Some(resource(&state.disk)),
            observed_at_unix_seconds: state.observed_at_unix_seconds,
            consecutive_failures: state.consecutive_failures,
        }
    }
}

/// Heartbeat wire form of the pressure board state. `None` while the node
/// has no successful sample: an absent field means "no trustworthy
/// observation", never "observed at the epoch".
pub fn report_from(state: &NodePressureState) -> Option<adx_protocol::control::NodePressureReport> {
    (state.observed_at_unix_seconds != 0).then(|| state.into())
}

pub struct PressureGate {
    policy: PressurePolicy,
    /// Admission bit, driven by the historical two-resource formula.
    accepting: bool,
    /// Structured per-resource observation.
    state: NodePressureState,
}
impl PressureGate {
    pub fn new(policy: PressurePolicy) -> Result<Self> {
        if policy.memory_low_percent >= policy.memory_high_percent
            || policy.disk_low_percent >= policy.disk_high_percent
            || policy.memory_high_percent > 100
            || policy.disk_high_percent > 100
        {
            return Err(invalid("pressure thresholds require low < high <= 100"));
        }
        Ok(Self {
            policy,
            accepting: true,
            state: NodePressureState::default(),
        })
    }
    /// Successful sample: advances the admission bit and the structured
    /// snapshot together. The admission formula is unchanged: pressure
    /// opens only when both resources fall back to or below their low
    /// thresholds and closes as soon as either crosses its high threshold.
    pub fn update(&mut self, pressure: Pressure) -> PressureUpdate {
        if pressure.memory_percent >= self.policy.memory_high_percent
            || pressure.disk_percent >= self.policy.disk_high_percent
        {
            self.accepting = false;
        } else if pressure.memory_percent <= self.policy.memory_low_percent
            && pressure.disk_percent <= self.policy.disk_low_percent
        {
            self.accepting = true;
        }
        // A clock failure keeps the previous stamp: a sample whose
        // observation time cannot be trusted does not count as fresh
        // evidence either way.
        let stamp = crate::checkpoint::now().unwrap_or(self.state.observed_at_unix_seconds);
        self.state.memory = advance(
            self.state.memory.clone(),
            pressure.memory_percent,
            self.policy.memory_high_percent,
            self.policy.memory_low_percent,
            stamp,
        );
        self.state.disk = advance(
            self.state.disk.clone(),
            pressure.disk_percent,
            self.policy.disk_high_percent,
            self.policy.disk_low_percent,
            stamp,
        );
        self.state.observed_at_unix_seconds = stamp;
        self.state.consecutive_failures = 0;
        PressureUpdate {
            accepting: self.accepting,
            state: self.state.clone(),
        }
    }
    /// Failed sample: freezes the levels and the observation stamp, bumps
    /// the failure counter, and reports closed admission for this round.
    /// The hysteresis phase survives, so the next successful sample resumes
    /// from the previous level.
    pub fn fail(&mut self) -> PressureUpdate {
        self.state.consecutive_failures = self.state.consecutive_failures.saturating_add(1);
        PressureUpdate {
            accepting: false,
            state: self.state.clone(),
        }
    }
}

/// Per-resource hysteresis: crossing the high threshold raises the level,
/// falling to or below the low threshold lowers it, and the band in between
/// keeps the previous level.
fn advance(
    current: ResourcePressure,
    percent: u8,
    high: u8,
    low: u8,
    stamp: u64,
) -> ResourcePressure {
    let level = if percent >= high {
        PressureLevel::High
    } else if percent <= low {
        PressureLevel::Normal
    } else {
        current.level
    };
    ResourcePressure {
        last_transition_unix_seconds: if level == current.level {
            current.last_transition_unix_seconds
        } else {
            stamp
        },
        level,
        percent,
    }
}
pub fn pressure(disk_path: &Path) -> Result<Pressure> {
    let percent = |used: u64, total: u64| -> u8 {
        if total == 0 {
            100
        } else {
            ((u128::from(used) * 100 / u128::from(total)).min(100)) as u8
        }
    };
    let total = memory_info("MemTotal:")?;
    let mut memory_percent = percent(total.saturating_sub(memory_info("MemAvailable:")?), total);
    for (path, v2) in cgroups()? {
        let (limit, usage) = if v2 {
            ("memory.max", "memory.current")
        } else {
            ("memory.limit_in_bytes", "memory.usage_in_bytes")
        };
        if let Some(limit) = optional(&path.join(limit))?.filter(|v| v.trim() != "max") {
            let used = std::fs::read_to_string(path.join(usage)).map_err(unavailable)?;
            memory_percent = memory_percent.max(percent(
                used.trim().parse().map_err(invalid)?,
                limit.trim().parse().map_err(invalid)?,
            ));
        }
    }
    let (total, available) = disk(disk_path)?;
    Ok(Pressure {
        memory_percent,
        disk_percent: percent(total.saturating_sub(available), total),
    })
}
