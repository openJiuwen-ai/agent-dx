//! One fixed 1GiB POSIX write plus real FullSync. Content preparation and verification excluded.
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{fd::IntoRawFd, unix::fs::FileExt},
    path::PathBuf,
    time::Instant,
};
fn monotonic_ns() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        // SAFETY: aligned writable timespec remains valid for the call; no pointer is retained.
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) },
        0
    );
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = PathBuf::from(args.next().ok_or("file argument missing")?);
    let output = PathBuf::from(args.next().ok_or("output argument missing")?);
    let size = 1024 * 1024 * 1024;
    let block = 1024 * 1024;
    let mut payload = vec![0_u8; size];
    let mut state = 20261022_u64 ^ 0xd6e8feb86659fd93;
    for part in payload.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        part.copy_from_slice(&state.to_le_bytes()[..part.len()]);
    }
    let expected = blake3::hash(&payload);
    let mut calls = Vec::with_capacity(1024);
    let complete = Instant::now();
    let open_start_ns = monotonic_ns();
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let open_end_ns = monotonic_ns();
    let wall = Instant::now();
    let start_ns = monotonic_ns();
    for (index, bytes) in payload.chunks(block).enumerate() {
        let start = monotonic_ns();
        file.write_all_at(bytes, (index * block) as u64)?;
        let end = monotonic_ns();
        calls.push((index, start, end));
    }
    let data_end_ns = monotonic_ns();
    file.sync_all()?;
    let end_ns = monotonic_ns();
    let wall_seconds = wall.elapsed().as_secs_f64();
    let close_start_ns = monotonic_ns();
    let fd = file.into_raw_fd();
    // SAFETY: ownership of this live descriptor was transferred above; close is called once.
    if unsafe { libc::close(fd) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let close_end_ns = monotonic_ns();
    let open_to_close_seconds = complete.elapsed().as_secs_f64();
    let verify = Instant::now();
    let mut reader = File::open(&path)?;
    assert_eq!(reader.metadata()?.len(), size as u64);
    let mut hasher = blake3::Hasher::new();
    let mut scratch = vec![0_u8; 4 * block];
    loop {
        let length = reader.read(&mut scratch)?;
        if length == 0 {
            break;
        }
        hasher.update(&scratch[..length]);
    }
    assert_eq!(hasher.finalize(), expected);
    let mut latency: Vec<f64> = calls.iter().map(|(_, a, b)| (b - a) as f64 / 1e6).collect();
    latency.sort_by(f64::total_cmp);
    let records: Vec<_> = calls
        .into_iter()
        .map(|(index, a, b)| {
            serde_json::json!({
                "seq":index,"offset":index*block,"bytes":block,"start_ns":a,"end_ns":b
            })
        })
        .collect();
    let result = serde_json::json!({
        "status":"passed","bytes":size,"block_bytes":block,"workers":1,
        "start_ns":start_ns,"data_end_ns":data_end_ns,"end_ns":end_ns,
        "wall_seconds":wall_seconds,"data_seconds":(data_end_ns-start_ns) as f64/1e9,
        "sync_seconds":(end_ns-data_end_ns) as f64/1e9,"mib_s":1024.0/wall_seconds,
        "open_seconds":(open_end_ns-open_start_ns) as f64/1e9,
        "close_seconds":(close_end_ns-close_start_ns) as f64/1e9,
        "open_to_close_seconds":open_to_close_seconds,
        "open_start_ns":open_start_ns,"close_end_ns":close_end_ns,
        "p50_ms":latency[512],"p99_ms":latency[1013],
        "blake3":expected.to_hex().to_string(),"full_content_verified":true,
        "verify_seconds":verify.elapsed().as_secs_f64(),"calls":records,
        "timing":"primary first pwrite to same-fd FullSync return; open-close separately; no generation/hash/verification in either"
    });
    let mut destination = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)?;
    serde_json::to_writer(&mut destination, &result)?;
    destination.write_all(b"\n")?;
    println!(
        "{}",
        serde_json::json!({"status":"passed","wall_seconds":wall_seconds,"open_to_close_seconds":open_to_close_seconds})
    );
    Ok(())
}
