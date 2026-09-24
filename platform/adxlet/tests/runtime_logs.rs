use adxlet::runtime_logs::{RuntimeLogPolicy, RuntimeLogs};
use flate2::read::GzDecoder;
use std::{
    collections::HashSet,
    fs,
    io::Read,
    time::{Duration, SystemTime},
};

fn policy(root: &std::path::Path) -> RuntimeLogPolicy {
    RuntimeLogPolicy {
        directory: root.to_path_buf(),
        max_terminated: 50,
        max_age_seconds: 86_400,
        max_total_bytes: 1_024 * 1_024 * 1_024,
        compress_after_seconds: 300,
        interval_seconds: 60,
    }
}

#[test]
fn runtime_paths_are_distinct_and_cannot_escape_the_log_directory() {
    let directory = tempfile::tempdir().unwrap();
    let logs = RuntimeLogs::new(policy(directory.path())).unwrap();
    let (out, err) = logs.paths("sandbox-1-r2").unwrap();
    assert_eq!(
        out,
        directory.path().join("sandbox-1-r2.out").to_str().unwrap()
    );
    assert_eq!(
        err,
        directory.path().join("sandbox-1-r2.err").to_str().unwrap()
    );
    assert!(logs.paths("../outside").is_err());
    assert!(logs.paths("sandbox/other").is_err());
}

#[test]
fn active_logs_are_preserved_and_terminated_logs_expire_after_a_day() {
    let directory = tempfile::tempdir().unwrap();
    let logs = RuntimeLogs::new(policy(directory.path())).unwrap();
    fs::write(directory.path().join("active-1.out"), "active").unwrap();
    fs::write(directory.path().join("done-1.out"), "finished").unwrap();
    let now = SystemTime::now();
    logs.collect(&HashSet::from(["active-1".into()]), now)
        .unwrap();
    logs.collect(
        &HashSet::from(["active-1".into()]),
        now + Duration::from_secs(86_401),
    )
    .unwrap();
    assert!(directory.path().join("active-1.out").exists());
    assert!(!directory.path().join("done-1.out").exists());
    assert!(!directory.path().join("done-1.out.gz").exists());
}

#[test]
fn terminated_log_pairs_are_capped_at_fifty() {
    let directory = tempfile::tempdir().unwrap();
    let logs = RuntimeLogs::new(policy(directory.path())).unwrap();
    for n in 0..51 {
        fs::write(directory.path().join(format!("run-{n:02}.out")), "out").unwrap();
        fs::write(directory.path().join(format!("run-{n:02}.err")), "err").unwrap();
    }
    logs.collect(&HashSet::new(), SystemTime::now()).unwrap();
    let count = fs::read_dir(directory.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".out"))
        .count();
    assert_eq!(count, 50);
}

#[test]
fn terminated_streams_compress_after_collector_window() {
    let directory = tempfile::tempdir().unwrap();
    let logs = RuntimeLogs::new(policy(directory.path())).unwrap();
    fs::write(directory.path().join("done-1.out"), "hello\n").unwrap();
    let now = SystemTime::now();
    logs.collect(&HashSet::new(), now).unwrap();
    assert!(directory.path().join("done-1.out").exists());
    logs.collect(&HashSet::new(), now + Duration::from_secs(301))
        .unwrap();
    assert!(!directory.path().join("done-1.out").exists());
    let mut body = String::new();
    GzDecoder::new(fs::File::open(directory.path().join("done-1.out.gz")).unwrap())
        .read_to_string(&mut body)
        .unwrap();
    assert_eq!(body, "hello\n");
}

#[test]
fn restart_preserves_termination_time_and_byte_budget_removes_oldest_pair() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = policy(directory.path());
    config.max_total_bytes = 7;
    config.compress_after_seconds = 10_000;
    let logs = RuntimeLogs::new(config.clone()).unwrap();
    fs::write(directory.path().join("old-1.out"), "12345").unwrap();
    let now = SystemTime::now();
    logs.collect(&HashSet::new(), now).unwrap();
    fs::write(directory.path().join("new-1.out"), "12345").unwrap();
    RuntimeLogs::new(config)
        .unwrap()
        .collect(&HashSet::new(), now + Duration::from_secs(1))
        .unwrap();
    assert!(!directory.path().join("old-1.out").exists());
    assert!(directory.path().join("new-1.out").exists());
}

#[test]
fn invalid_or_corrupt_gc_index_never_deletes_log_files() {
    let directory = tempfile::tempdir().unwrap();
    let logs = RuntimeLogs::new(policy(directory.path())).unwrap();
    fs::write(directory.path().join("done-1.out"), "preserve").unwrap();
    fs::write(directory.path().join(".terminated.json"), "not-json").unwrap();
    assert!(logs.collect(&HashSet::new(), SystemTime::now()).is_err());
    assert!(directory.path().join("done-1.out").exists());
}
