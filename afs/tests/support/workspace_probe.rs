//! Trusted idle init and final-view observer for the experimental workspace container.
//! This binary does not grant Home authority or publish production readiness.
use std::{
    fs::File,
    io,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn stop(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("idle") if args.len() == 1 => idle()?,
        Some("identity") if args.len() == 1 => println!("{}", identity()?),
        _ => return Err("expected idle or identity".into()),
    }
    Ok(())
}

#[allow(unsafe_code)]
fn idle() -> io::Result<()> {
    // SAFETY: handlers remain installed for process lifetime; they only update
    // a lock-free atomic. PID1 needs explicit TERM/INT handlers to terminate.
    unsafe {
        if libc::signal(libc::SIGTERM, stop as *const () as libc::sighandler_t) == libc::SIG_ERR
            || libc::signal(libc::SIGINT, stop as *const () as libc::sighandler_t) == libc::SIG_ERR
        {
            return Err(io::Error::last_os_error());
        }
    }
    while !STOP.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn identity() -> io::Result<serde_json::Value> {
    let source = File::open("/workspace")?;
    let metadata = source.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
    }
    let namespace = File::open("/proc/thread-self/ns/mnt")?.metadata()?;
    let id = mount_id(&source, libc::STATX_MNT_ID)?;
    let unique = mount_id(&source, 0x4000)?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
    let flags = mountinfo
        .lines()
        .find_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            (fields.len() > 6 && fields[0].parse::<u64>().ok() == Some(id))
                .then(|| fields[5].split(',').map(str::to_owned).collect::<Vec<_>>())
        })
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
    if !flags.iter().any(|f| f == "nosuid") || !flags.iter().any(|f| f == "nodev") {
        return Err(io::Error::from_raw_os_error(libc::EPERM));
    }
    Ok(serde_json::json!({
        "source":{"dev":metadata.dev(),"ino":metadata.ino()},
        "namespace":{"dev":namespace.dev(),"ino":namespace.ino()},
        "mount_id":id,"unique_mount_id":unique,"flags":flags,
        "scope":"final-view identity; not production READY or revocation ACK"
    }))
}

#[allow(unsafe_code)]
fn mount_id(file: &File, mask: u32) -> io::Result<u64> {
    // SAFETY: output is initialized writable storage for this synchronous statx call.
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    // SAFETY: stat and the empty C string stay valid; AT_EMPTY_PATH binds lookup to file.
    let result = unsafe {
        libc::statx(
            file.as_raw_fd(),
            c"".as_ptr(),
            libc::AT_EMPTY_PATH,
            mask,
            &mut stat,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    if stat.stx_mask & mask != mask {
        return Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP));
    }
    Ok(stat.stx_mnt_id)
}
