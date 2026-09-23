//! Minimal namespace init. Run before any threads, Tokio or tracing exporters.
//! The service child owns its command/PTY waits. PID 1 only waits for that child
//! and reparented orphans, so it cannot consume managed command exit statuses.

#[cfg(not(target_os = "linux"))]
pub fn enter() -> std::io::Result<Option<i32>> {
    Ok(None)
}

#[cfg(target_os = "linux")]
pub fn enter() -> std::io::Result<Option<i32>> {
    use std::io;
    // SAFETY: called once by main before starting any threads. No Rust signal
    // handlers are installed; blocked signals are consumed with sigwaitinfo.
    unsafe {
        if libc::getpid() != 1 {
            return Ok(None);
        }
        let mut signals: libc::sigset_t = std::mem::zeroed();
        let mut previous: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut signals);
        for signal in [
            libc::SIGCHLD,
            libc::SIGTERM,
            libc::SIGINT,
            libc::SIGHUP,
            libc::SIGQUIT,
            libc::SIGUSR1,
            libc::SIGUSR2,
            libc::SIGWINCH,
            libc::SIGCONT,
            libc::SIGTSTP,
            libc::SIGTTIN,
            libc::SIGTTOU,
        ] {
            libc::sigaddset(&mut signals, signal);
        }
        if libc::sigprocmask(libc::SIG_BLOCK, &signals, &mut previous) < 0 {
            return Err(io::Error::last_os_error());
        }
        let child = libc::fork();
        if child < 0 {
            let error = io::Error::last_os_error();
            libc::sigprocmask(libc::SIG_SETMASK, &previous, std::ptr::null_mut());
            return Err(error);
        }
        if child == 0 {
            if libc::setpgid(0, 0) < 0 {
                libc::_exit(127);
            }
            if libc::sigprocmask(libc::SIG_SETMASK, &previous, std::ptr::null_mut()) < 0 {
                libc::_exit(127);
            }
            return Ok(None);
        }
        // Also set from the parent to close the first-signal/fork race.
        libc::setpgid(child, child);
        loop {
            loop {
                let mut status = 0;
                let pid = libc::waitpid(-1, &mut status, libc::WNOHANG);
                if pid == child {
                    let code = if libc::WIFEXITED(status) {
                        libc::WEXITSTATUS(status)
                    } else if libc::WIFSIGNALED(status) {
                        128 + libc::WTERMSIG(status)
                    } else {
                        continue;
                    };
                    return Ok(Some(code));
                }
                if pid == 0 {
                    break;
                }
                if pid < 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() == Some(libc::EINTR) {
                        continue;
                    }
                    return Err(error);
                }
            }
            let signal = libc::sigwaitinfo(&signals, std::ptr::null_mut());
            if signal < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(error);
            }
            if signal != libc::SIGCHLD {
                libc::kill(-child, signal);
            }
        }
    }
}
