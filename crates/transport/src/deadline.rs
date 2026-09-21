use std::time::Duration;
use tokio::time::Instant;

/// Return the smaller of the upstream remaining deadline and a local cap.
/// An elapsed upstream deadline is terminal and must not be replaced by a new
/// local timeout budget.
pub fn remaining(deadline: Instant, cap: Duration) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .map(|remaining| remaining.min(cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn preserves_the_upstream_budget() {
        let deadline = Instant::now() + Duration::from_secs(2);
        assert_eq!(
            remaining(deadline, Duration::from_secs(5)),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            remaining(deadline, Duration::from_secs(1)),
            Some(Duration::from_secs(1))
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(remaining(deadline, Duration::from_secs(1)), None);
    }
}
