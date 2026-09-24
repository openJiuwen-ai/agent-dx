#[cfg(target_os = "linux")]
use super::*;

#[cfg(target_os = "linux")]
#[test]
#[ignore = "manual RSS sizing, run alone with --ignored --nocapture"]
fn env_cache_memory_profile() {
    fn rss_kib() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find(|line| line.starts_with("VmRSS:"))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap()
    }
    let count = CacheSettings::default().capacity;
    let baseline = rss_kib();
    let mut entries = LinkedHashMap::new();
    for i in 0..count {
        let scope = Scope {
            tenant: "t".repeat(36),
            template: "a".repeat(32),
            version: "v".repeat(8),
            environment_id: format!("{i:036}"),
        };
        let target = Target {
            environment: Environment {
                scope: scope.clone(),
                generation: "g".repeat(36),
                sandbox_id: "s".repeat(40),
                phase: EnvironmentPhase::Active,
            },
            service: vec![adx_agent_core::Service {
                protocol: adx_agent_core::Protocol::Http,
                port: 8080,
            }],
        };
        entries.insert(
            scope,
            Binding {
                target: Some(target),
                attempt: Arc::new(()),
                touched: Instant::now(),
            },
        );
    }
    std::hint::black_box(&entries);
    let delta = rss_kib().saturating_sub(baseline);
    println!("entries={count} scope_bytes={} binding_bytes={} rss_delta_kib={delta} approximate_bytes_per_entry={}", std::mem::size_of::<Scope>(), std::mem::size_of::<Binding>(), delta * 1024 / count);
    assert_eq!(entries.len(), count);
}
