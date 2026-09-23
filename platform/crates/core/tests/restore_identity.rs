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
