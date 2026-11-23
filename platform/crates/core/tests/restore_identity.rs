use adx_core::runtime::{RuntimeIdentity, RuntimeRestore};
fn identity(id: &str, generation: u64) -> RuntimeIdentity {
    RuntimeIdentity {
        environment_id: id.into(),
        runtime_id: format!("{id}-{generation}"),
        ownership_generation: generation,
    }
}
#[test]
fn clone_requires_exact_checkpoint_origin_and_a_new_environment() {
    let source = identity("source", 9);
    let target = identity("clone", 1);
    assert!(target.validate_restore_from(&source).is_err());
    assert!(RuntimeRestore {
        target: target.clone(),
        origin: None
    }
    .validate(&source)
    .is_err());
    assert!(RuntimeRestore {
        target: target.clone(),
        origin: Some(identity("source", 8))
    }
    .validate(&source)
    .is_err());
    assert!(RuntimeRestore {
        target: target.clone(),
        origin: Some(source.clone())
    }
    .validate(&source)
    .is_ok());
    assert!(RuntimeRestore {
        target: identity("source", 1),
        origin: Some(source.clone())
    }
    .validate(&source)
    .is_err());
    assert!(RuntimeRestore {
        target: source.clone(),
        origin: None
    }
    .validate(&source)
    .is_ok());
}

#[test]
fn explicit_origin_allows_same_environment_only_at_a_new_generation() {
    use adx_core::runtime::{RuntimeIdentity, RuntimeRestore};
    let source = RuntimeIdentity {
        environment_id: "i".into(),
        runtime_id: "i-1".into(),
        ownership_generation: 1,
    };
    let mut restore = RuntimeRestore {
        target: RuntimeIdentity {
            environment_id: "i".into(),
            runtime_id: "i-2-r2".into(),
            ownership_generation: 2,
        },
        origin: Some(source.clone()),
    };
    assert!(restore.validate(&source).is_ok());
    restore.target = source.clone();
    assert!(restore.validate(&source).is_err());
}

#[test]
fn long_environment_identity_is_bounded_and_owned_by_the_exact_environment_and_generation() {
    use adx_core::{runtime_id, runtime_id_for_revision, valid_runtime_id, MAX_RUNTIME_ID_BYTES};
    let environment = format!("{}-{}", "namespace".repeat(32), "n".repeat(256));
    let generation = u64::MAX;
    let initial = runtime_id(&environment, generation);
    let restarted = runtime_id_for_revision(&environment, generation, u64::MAX);
    assert!(restarted.len() <= MAX_RUNTIME_ID_BYTES);
    assert!(initial.is_ascii());
    assert!(valid_runtime_id(&environment, generation, &initial));
    assert!(valid_runtime_id(&environment, generation, &restarted));
    assert!(!valid_runtime_id(
        &format!("{environment}x"),
        generation,
        &initial
    ));
    assert!(!valid_runtime_id(&environment, generation - 1, &initial));
    assert_eq!(initial, runtime_id(&environment, generation));
    assert_ne!(initial, runtime_id(&initial, generation));
    assert_eq!(runtime_id("default-small", 1), "default-small-1");
}

#[test]
fn execution_ids_include_the_restart_suffix_in_the_filename_bound() {
    use adx_core::{runtime_id, runtime_id_for_revision, valid_runtime_id, MAX_RUNTIME_ID_BYTES};
    for length in 210..=256 {
        let environment = "n".repeat(length);
        let initial = runtime_id(&environment, 1);
        let restarted = runtime_id_for_revision(&environment, 1, u64::MAX);
        assert!(restarted.len() <= MAX_RUNTIME_ID_BYTES, "{length}");
        assert!(valid_runtime_id(&environment, 1, &restarted));
        if length + 2 <= MAX_RUNTIME_ID_BYTES {
            assert_eq!(initial, format!("{environment}-1"));
        }
    }
    let environment = "n".repeat(230);
    assert_eq!(
        runtime_id_for_revision(&environment, 1, 3),
        format!("{environment}-1-r3")
    );
}

#[test]
fn restore_from_readable_long_identity_preserves_revision_fencing() {
    use adx_core::{runtime_id, valid_runtime_id};
    let environment = format!("default-{}", "n".repeat(256));
    let source = identity(&environment, 7);
    assert!(valid_runtime_id(&environment, 7, &source.runtime_id));
    let mut target = RuntimeIdentity {
        environment_id: environment.clone(),
        runtime_id: format!("{}-r3", runtime_id(&environment, 7)),
        ownership_generation: 7,
    };
    assert!(target.validate_restore_from(&source).is_ok());
    let current = target.clone();
    target.runtime_id = format!("{}-r2", runtime_id(&environment, 7));
    assert!(target.validate_restore_from(&current).is_err());
    target.runtime_id = format!("{}-r4", runtime_id(&environment, 7));
    assert!(target.validate_restore_from(&current).is_ok());
    target.runtime_id = format!("{}-r4", runtime_id("another-environment", 7));
    assert!(target.validate_restore_from(&current).is_err());
    assert!(!valid_runtime_id(
        &environment,
        7,
        &format!("{}-r0", runtime_id(&environment, 7))
    ));
    assert!(!valid_runtime_id(
        &environment,
        7,
        &format!("{}-r18446744073709551616", runtime_id(&environment, 7))
    ));
}
