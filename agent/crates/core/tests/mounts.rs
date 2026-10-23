use adx_agent_core::TemplateVersion;
use serde_json::json;

fn template(path: &str) -> TemplateVersion {
    serde_json::from_value(json!({"name":"app","version":"1","image":"app:1",
        "isolation_runtime":"runsc","resources":{"cpu_millis":1000,"memory_mib":512},
        "mounts":[{"type":"bind","host_path":path,"target":"/home/jiuwen","options":["rw"],
            "create_if_missing":true,"create_options":{"uid":10001,"gid":10001,"mode":"0700"}}]}))
    .unwrap()
}

#[test]
fn rendering_rejects_missing_parameters_and_path_escape() {
    use std::collections::BTreeMap;
    let t = template("/mnt/sfs/workspaces/${workspace}");
    assert!(t.render_mounts(&BTreeMap::new()).is_err());
    for value in [
        "", "/etc", "..", "a/../b", "a//b", "a/./b", "a/", "a\\b", "${other}", "a\n",
    ] {
        let parameters = BTreeMap::from([("workspace".into(), value.into())]);
        assert!(t.render_mounts(&parameters).is_err(), "{value:?}");
    }
    let parameters = BTreeMap::from([("workspace".into(), "tenant/user-1".into())]);
    let mounts = t.render_mounts(&parameters).unwrap();
    assert_eq!(mounts[0].host_path, "/mnt/sfs/workspaces/tenant/user-1");
    assert_eq!(mounts[0].target, "/home/jiuwen");
    assert_eq!(mounts[0].options, ["rw"]);
    assert!(mounts[0].create_if_missing);
    assert_eq!(
        serde_json::to_value(&mounts[0].create_options).unwrap(),
        json!({"uid":10001,"gid":10001,"mode":"0700"})
    );
}

#[test]
fn malformed_templates_and_unresolved_execution_are_rejected() {
    for path in [
        "relative/${workspace}",
        "/${workspace}",
        "/mnt/${}",
        "/mnt/${workspace",
        "/mnt/$workspace",
        "/mnt/${a.b}",
        "/mnt/../data/${workspace}",
    ] {
        assert!(template(path).validate().is_err(), "{path}");
    }
    let t = template("/mnt/sfs/${workspace}");
    assert!(adx_agent_core::environment::ExecutionSpec::from(&t)
        .validate()
        .is_err());
    let mut t = template("/mnt/sfs/user");
    t.mounts.push(t.mounts[0].clone());
    assert!(t.validate().is_err());
}

#[test]
fn mount_options_and_rendered_size_are_bounded() {
    let mut t = template("/mnt/sfs/${workspace}");
    for options in [vec!["rw", "ro"], vec!["rw", "rw"], vec!["bind"]] {
        t.mounts[0].options = options.into_iter().map(str::to_owned).collect();
        assert!(t.validate().is_err());
    }
    t.mounts[0].options = vec!["rw".into(), "nodev".into()];
    t.validate().unwrap();
    let parameters = std::collections::BTreeMap::from([("workspace".into(), "a".repeat(4096))]);
    assert!(t.render_mounts(&parameters).is_err());
    let valid = serde_json::to_value(template("/mnt/sfs/user")).unwrap();
    for attributes in [
        json!({"uid":-1,"gid":10001,"mode":"0700"}),
        json!({"uid":4294967295u64,"gid":10001,"mode":"0700"}),
        json!({"uid":10001,"gid":4294967295u64,"mode":"0700"}),
        json!({"uid":10001,"gid":10001,"mode":"755"}),
        json!({"uid":10001,"gid":10001,"mode":"0788"}),
        json!({"uid":10001,"gid":10001,"mode":"1777"}),
        json!({"uid":10001,"mode":"0700"}),
    ] {
        let mut value = valid.clone();
        value["mounts"][0]["create_options"] = attributes;
        assert!(serde_json::from_value::<TemplateVersion>(value)
            .map(|template| template.validate().is_err())
            .unwrap_or(true));
    }
    let mut disabled = template("/mnt/sfs/user");
    disabled.mounts[0].create_if_missing = false;
    assert!(disabled.validate().is_err());
    let t = template("/mnt/sfs/user-${workspace}/data");
    let parameters = std::collections::BTreeMap::from([("workspace".into(), "abc".into())]);
    assert_eq!(
        t.render_mounts(&parameters).unwrap()[0].host_path,
        "/mnt/sfs/user-abc/data"
    );
}

#[test]
fn environment_parameters_are_strings_while_mount_parameters_remain_relative_paths() {
    let mut template = template("/mnt/users/${workspace}");
    template
        .env
        .insert("API_BASE".into(), "${model_url}".into());
    template.env.insert("API_KEY".into(), "${key}".into());
    let parameters = adx_agent_core::parameters::parse(b"workspace=user&model_url=https%3A%2F%2Fmodel.example%2Fv1&key=a%26b%3Dc%2B%24%7Bliteral%7D").unwrap();
    let env = template.render_env(&parameters).unwrap();
    assert_eq!(env["API_BASE"], "https://model.example/v1");
    assert_eq!(env["API_KEY"], "a&b=c+${literal}");
    assert!(template.render_mounts(&parameters).is_ok());
    let bad = adx_agent_core::parameters::parse(b"workspace=%2Fetc&model_url=x&key=y").unwrap();
    assert!(template.render_mounts(&bad).is_err());
}
