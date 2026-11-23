use std::io;

const DEFAULT_NOFILE_SOFT_LIMIT: libc::rlim_t = 65_536;
const NOFILE_ENV: &str = "ADX_NOFILE_SOFT_LIMIT";

/// Only locally generated environment-parse errors are configuration failures.
/// OS EINVAL remains a runtime resource error even though its kind is InvalidInput.
pub fn is_configuration_error(error: &io::Error) -> bool {
    error.raw_os_error().is_none() && error.kind() == io::ErrorKind::InvalidInput
}

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
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid FD limit environment",
            ))
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_invalid_input_is_not_environment_syntax() {
        assert!(!is_configuration_error(&io::Error::from_raw_os_error(
            libc::EINVAL
        )));
        assert!(is_configuration_error(&io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid FD limit"
        )));
    }
}
