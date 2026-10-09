//! Minimal producer-side logging benchmark used by `scripts/bench_logging.sh`.

use std::{env, time::Instant};

use afs_logging::{LogOutput, LoggingConfig, ProcessIdentity, init_process_logging};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let records = env::args()
        .nth(1)
        .as_deref()
        .unwrap_or("100000")
        .parse::<u64>()?;
    let max_file_size = env::args()
        .nth(2)
        .as_deref()
        .unwrap_or("18446744073709551615")
        .parse::<u64>()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("logging-bench.log");
    let guard = init_process_logging(
        &LoggingConfig {
            output: LogOutput::File(path.clone()),
            async_queue_capacity: 1024,
            max_file_size,
            max_backups: 1024,
            ..LoggingConfig::default()
        },
        ProcessIdentity::new("afs-logging-bench", "local"),
    )?;

    let started = Instant::now();
    for sequence in 0..records {
        afs_logging::info!(
            "benchmark event";
            "event" => "logging.benchmark",
            "sequence" => sequence,
            "payload_bytes" => 4096,
        );
    }
    let enqueue_elapsed = started.elapsed();
    // Give the consumer one scheduling window, then emit a marker. Under
    // DropAndReport this next record first publishes the accumulated drop count.
    std::thread::sleep(std::time::Duration::from_millis(200));
    afs_logging::info!("benchmark drain marker"; "event" => "logging.benchmark.finished");
    drop(guard);
    let total_elapsed = started.elapsed();
    let bytes = std::fs::metadata(&path)?.len();
    let files = std::fs::read_dir(directory.path())?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    let archives = files.iter().filter(|candidate| *candidate != &path).count();
    let mut written_records = 0_u64;
    let mut reported_drops = 0_u64;
    for candidate in &files {
        if let Ok(text) = std::fs::read_to_string(candidate) {
            written_records += text.lines().count() as u64;
            for line in text
                .lines()
                .filter(|line| line.contains("slog-async: logger dropped messages"))
            {
                if let Ok(record) = serde_json::from_str::<serde_json::Value>(line) {
                    reported_drops += record["count"].as_u64().unwrap_or_default();
                }
            }
        }
    }

    println!(
        "records={records} enqueue_ms={:.3} total_ms={:.3} producer_ns_per_record={:.1} current_bytes={bytes} archives={archives} written_records={written_records} reported_drops={reported_drops}",
        enqueue_elapsed.as_secs_f64() * 1000.0,
        total_elapsed.as_secs_f64() * 1000.0,
        enqueue_elapsed.as_nanos() as f64 / records.max(1) as f64,
    );
    Ok(())
}
