use adx_agent_core::{
    AgentBinding, BindingPhase, EnvironmentRef, Scope, SecondaryEnv, SecondaryIntent, TemplateRef,
};

#[test]
fn alias_identity_is_independent_of_selected_environment() {
    let scope = Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        template_ref: TemplateRef::Alias("production".into()),
        binding_id: "workspace".into(),
    };
    let mut binding = AgentBinding {
        scope: scope.clone(),
        generation: "logical-generation".into(),
        current: EnvironmentRef::new("1".into(), "unrelated-env-one".into()),
        secondary: Some(SecondaryEnv {
            environment: EnvironmentRef::new("2".into(), "unrelated-env-two".into()),
            intent: SecondaryIntent::Preparing,
        }),
        phase: BindingPhase::Active,
        parameters: Default::default(),
        volumes: Default::default(),
        creation_attempts: Default::default(),
    };
    let secondary = binding.secondary.take().unwrap();
    let old = std::mem::replace(&mut binding.current, secondary.environment);
    binding.secondary = Some(SecondaryEnv {
        environment: old,
        intent: SecondaryIntent::Retiring,
    });
    assert_eq!(binding.scope, scope);
    assert_eq!(binding.generation, "logical-generation");
    assert_eq!(binding.current.version, "2");
    assert_eq!(
        binding.secondary.unwrap().environment.environment_id,
        "unrelated-env-one"
    );
}

#[test]
fn template_reference_has_no_binding_mode_field() {
    let scope = Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        template_ref: TemplateRef::Alias("production".into()),
        binding_id: "workspace".into(),
    };
    let value = serde_json::to_value(&scope).unwrap();
    assert_eq!(
        value["template_ref"],
        serde_json::json!({"alias":"production"})
    );
    assert_eq!(serde_json::from_value::<Scope>(value).unwrap(), scope);
    let mut fixed = scope;
    fixed.template_ref = TemplateRef::Version("1".into());
    assert!(fixed.validate().is_ok());
}
