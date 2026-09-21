use std::io;

const DEFAULT_NOFILE_SOFT_LIMIT: libc::rlim_t = 65_536;
const NOFILE_ENV: &str = "ADX_NOFILE_SOFT_LIMIT";

/// Raise the inherited soft FD limit before resource budgets are calculated.
/// Lower hard limits are respected, and an already higher soft limit is never
/// reduced.
pub fn raise_nofile_soft_limit_from_env() -> io::Result<libc::rlim_t> {
    let requested = match std::env::var(NOFILE_ENV) {
        Ok(value) => value.parse::<u64>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid {NOFILE_ENV}: {error}"),
            )
        })?,
        Err(std::env::VarError::NotPresent) => DEFAULT_NOFILE_SOFT_LIMIT,
        Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidInput, error)),
    };

    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes the supplied plain structure and retains no
    // pointer. setrlimit receives the same initialized structure.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let desired = limit.rlim_cur.max(requested.min(limit.rlim_max));
    if desired != limit.rlim_cur {
        limit.rlim_cur = desired;
        // SAFETY: limit is fully initialized, is valid for this call, and setrlimit does not
        // retain its pointer.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(limit.rlim_cur)
}
