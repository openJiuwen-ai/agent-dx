use super::*;
use std::time::Instant;

fn reply(remaining_ms: u64, absolute_expiry_ms: u64) -> RegisterNodeReply {
    RegisterNodeReply {
        lease_epoch: 7,
        expires_at_unix_ms: absolute_expiry_ms,
        lease_remaining_ms: remaining_ms,
        ..Default::default()
    }
}

#[test]
fn registration_lease_ignores_cross_machine_wall_clock_offsets() {
    let sent = Instant::now();
    let received = sent + Duration::from_secs(2);
    for absolute_expiry in [0, 1, u64::MAX] {
        let registration =
            checked_registration_readiness(reply(30_000, absolute_expiry), sent, received).unwrap();
        assert_eq!(
            registration.session_deadline,
            sent + Duration::from_secs(30)
        );
        assert_eq!(
            registration.session_deadline.duration_since(received),
            Duration::from_secs(28)
        );
    }
}

#[test]
fn registration_lease_rejects_missing_ttl_or_zero_epoch() {
    let sent = Instant::now();
    assert!(checked_registration_readiness(reply(0, u64::MAX), sent, sent).is_err());
    let mut missing_epoch = reply(30_000, u64::MAX);
    missing_epoch.lease_epoch = 0;
    assert!(checked_registration_readiness(missing_epoch, sent, sent).is_err());
}

#[test]
fn registration_lease_deducts_slow_response_and_rejects_exhausted_ttl() {
    let sent = Instant::now();
    let deadline = sent + Duration::from_secs(5);
    let registration =
        checked_registration_readiness(reply(5_000, u64::MAX), sent, sent + Duration::from_secs(4))
            .unwrap();
    assert_eq!(registration.session_deadline, deadline);
    for received in [deadline, deadline + Duration::from_secs(1)] {
        assert!(checked_registration_readiness(reply(5_000, u64::MAX), sent, received).is_err());
    }
}

#[test]
fn registration_lease_does_not_exceed_requested_ttl() {
    let sent = Instant::now();
    let registration =
        checked_registration_readiness(reply(u64::MAX, u64::MAX), sent, sent).unwrap();
    assert_eq!(
        registration.session_deadline,
        sent + Duration::from_secs(30)
    );
}
