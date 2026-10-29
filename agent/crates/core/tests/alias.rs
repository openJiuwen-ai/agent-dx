use adx_agent_core::alias::{AliasConfig, VersionWeight};

fn config(weights: &[(&str, u64)]) -> AliasConfig {
    AliasConfig {
        tenant: "tenant".into(),
        template: "agent".into(),
        alias: "production".into(),
        seed: "stable".into(),
        versions: weights
            .iter()
            .map(|(version, weight)| VersionWeight {
                version: (*version).into(),
                weight: *weight,
            })
            .collect(),
    }
}

#[test]
fn relative_weights_are_scale_and_order_independent() {
    let a = config(&[("1", 1), ("2", 3)]);
    let b = config(&[("2", 30), ("1", 10)]);
    for id in 0..1000 {
        let id = id.to_string();
        assert_eq!(a.select(&id).unwrap(), b.select(&id).unwrap());
    }
}

#[test]
fn expanding_second_version_keeps_previously_selected_bindings() {
    let before = config(&[("1", 95), ("2", 5)]);
    let after = config(&[("1", 9), ("2", 1)]);
    for id in 0..1000 {
        let id = id.to_string();
        if before.select(&id).unwrap() == "2" {
            assert_eq!(after.select(&id).unwrap(), "2");
        }
    }
}

#[test]
fn zero_weights_never_select_and_invalid_distributions_are_rejected() {
    let only = config(&[("1", 0), ("2", 7)]);
    for id in 0..100 {
        assert_eq!(only.select(&id.to_string()).unwrap(), "2");
    }
    for invalid in [
        config(&[]),
        config(&[("1", 0)]),
        config(&[("1", 1), ("1", 2)]),
        config(&[("1", u64::MAX), ("2", 1)]),
    ] {
        assert!(invalid.validate().is_err());
        assert!(invalid.select("binding").is_err());
    }
}

#[test]
fn selection_has_fixed_utf8_test_vectors() {
    let config = config(&[("1", 1), ("2", 3)]);
    for (id, version) in [
        ("0", "2"),
        ("1", "1"),
        ("2", "1"),
        ("workspace", "1"),
        ("用户A", "2"),
    ] {
        assert_eq!(config.select(id).unwrap(), version);
    }
}

#[test]
fn aliases_and_published_versions_have_disjoint_namespaces() {
    let mut named = config(&[("1", 1)]);
    named.alias = "12".into();
    assert!(named.validate().is_err());
    for version in ["v1-workspace", "0", "01", "18446744073709551616"] {
        assert!(config(&[(version, 1)]).validate().is_err());
    }
    let a = config(&[("2", 1), ("10", 3)]);
    let b = config(&[("10", 30), ("2", 10)]);
    for id in 0..100 {
        assert_eq!(
            a.select(&id.to_string()).unwrap(),
            b.select(&id.to_string()).unwrap()
        );
    }
}
