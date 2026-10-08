//! Linux process resource metrics for ADX services.
//! Scrapes read only the current process and do not spawn a sampler thread.

#[cfg(target_os = "linux")]
fn stat_fields(stat: &str) -> Option<Vec<&str>> {
    // comm is parenthesized and may itself contain spaces or parentheses.
    Some(stat.rsplit_once(") ")?.1.split_whitespace().collect())
}

#[cfg(target_os = "linux")]
pub fn metrics() -> String {
    let (Ok(stat), Ok(statm), Ok(status), Ok(fds), Ok(uptime)) = (
        std::fs::read_to_string("/proc/self/stat"),
        std::fs::read_to_string("/proc/self/statm"),
        std::fs::read_to_string("/proc/self/status"),
        std::fs::read_dir("/proc/self/fd"),
        std::fs::read_to_string("/proc/uptime"),
    ) else {
        return String::new();
    };
    let Some(fields) = stat_fields(&stat) else {
        return String::new();
    };
    // /proc/self/stat fields 14, 15 and 22 follow the two leading pid/comm fields.
    let Some((user, system, start)) = (|| {
        Some((
            fields.get(11)?.parse::<u64>().ok()?,
            fields.get(12)?.parse::<u64>().ok()?,
            fields.get(19)?.parse::<u64>().ok()?,
        ))
    })() else {
        return String::new();
    };
    let Some((virtual_pages, resident_pages)) = (|| {
        let mut fields = statm.split_whitespace();
        Some((
            fields.next()?.parse::<u64>().ok()?,
            fields.next()?.parse::<u64>().ok()?,
        ))
    })() else {
        return String::new();
    };
    let threads = status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:\t"))
        .and_then(|value| value.trim().parse::<u64>().ok());
    let (Some(threads), Some(system_uptime)) = (
        threads,
        uptime
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<f64>().ok()),
    ) else {
        return String::new();
    };
    // SAFETY: sysconf reads numeric process-global kernel settings without pointers.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    // SAFETY: sysconf reads numeric process-global kernel settings without pointers.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if ticks <= 0 || page_size <= 0 {
        return String::new();
    }
    let ticks = ticks as f64;
    let page_size = page_size as u64;
    let pid = std::process::id();
    let fd_count = fds.count();
    let process_uptime = (system_uptime - start as f64 / ticks).max(0.0);
    format!(
        "# TYPE process_cpu_time_seconds_total counter\nprocess_cpu_time_seconds_total{{process_pid=\"{pid}\",state=\"user\"}} {}\nprocess_cpu_time_seconds_total{{process_pid=\"{pid}\",state=\"system\"}} {}\n# TYPE process_memory_usage_bytes gauge\nprocess_memory_usage_bytes{{process_pid=\"{pid}\"}} {}\n# TYPE process_memory_virtual_bytes gauge\nprocess_memory_virtual_bytes{{process_pid=\"{pid}\"}} {}\n# TYPE process_open_file_descriptors gauge\nprocess_open_file_descriptors{{process_pid=\"{pid}\"}} {fd_count}\n# TYPE process_pid gauge\nprocess_pid{{process_pid=\"{pid}\"}} {pid}\n# TYPE process_threads gauge\nprocess_threads{{process_pid=\"{pid}\"}} {threads}\n# TYPE process_uptime_seconds gauge\nprocess_uptime_seconds{{process_pid=\"{pid}\"}} {process_uptime}\n",
        user as f64 / ticks,
        system as f64 / ticks,
        resident_pages.saturating_mul(page_size),
        virtual_pages.saturating_mul(page_size),
    )
}

#[cfg(not(target_os = "linux"))]
pub fn metrics() -> String {
    String::new()
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    #[test]
    fn stat_fields_handle_parentheses_in_process_name() {
        let fields = super::stat_fields("123 (test (worker)) S 1 2 3").unwrap();
        assert_eq!(fields, ["S", "1", "2", "3"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn current_process_exposes_legacy_dashboard_metrics() {
        let metrics = super::metrics();
        for name in [
            "process_cpu_time_seconds_total",
            "process_memory_usage_bytes",
            "process_memory_virtual_bytes",
            "process_open_file_descriptors",
            "process_pid",
            "process_threads",
            "process_uptime_seconds",
        ] {
            assert!(metrics.contains(name), "missing {name}");
        }
    }
}
