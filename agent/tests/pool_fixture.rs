//! Test-only existing pool setup, not a production allocation API.
use adx_agent_core::{DesiredState, Instance, InstancePhase, Scope, Session};
use adx_agent_store::{Check, Key, Put, Record, Repository, Transaction};

pub async fn insert_instance(repo: &dyn Repository, scope: &Scope, id: &str, phase: InstancePhase) {
    let key = Key::new(
        "session",
        &[
            &scope.tenant,
            &scope.template,
            &scope.version,
            &scope.session_id,
        ],
    )
    .unwrap();
    let record = repo.get(&key).await.unwrap().unwrap();
    let mut session: Session = record.decode().unwrap();
    session.instances.insert(id.into());
    let instance = Instance {
        id: id.into(),
        tenant: scope.tenant.clone(),
        scope: scope.clone(),
        session_generation: session.generation.clone(),
        sandbox_id: id.into(),
        desired: DesiredState::Running,
        phase,
        status_message: None,
        create_deadline_ms: adx_agent_core::unix_time_millis()
            + adx_agent_core::limits::CREATE_TIMEOUT.as_millis() as u64,
    };
    let instance_key = Key::new("instance", &[&scope.tenant, id]).unwrap();
    let tx = Transaction::new(
        vec![
            Check {
                key: key.clone(),
                expected: Some(record.revision),
            },
            Check {
                key: instance_key.clone(),
                expected: None,
            },
        ],
        vec![
            Put {
                key,
                record: Record::new(&session).unwrap(),
            },
            Put {
                key: instance_key,
                record: Record::new(&instance).unwrap(),
            },
        ],
    )
    .unwrap();
    assert!(repo.commit(&tx).await.unwrap());
}
