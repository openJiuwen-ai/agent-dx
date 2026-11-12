use adx_core::EnvironmentSpec;
use std::collections::BTreeMap;

/// Isolated fake runtimes use one explicitly qualified execution profile.
pub fn qualify(spec: &mut EnvironmentSpec) -> BTreeMap<String, String> {
    spec.image.clear();
    spec.runtime_profile = Some(
        serde_json::from_value(serde_json::json!({
            "rootfs":{"runtime_class":spec.runtime_class,"type":"local","path":"/runtime/rootfs"},
            "bootstrap":{"type":"erofs","root":"/runtime/rootfs","target":"/rootfs",
                "entrypoint":["/rootfs/usr/local/bin/adx-execd"]}
        }))
        .unwrap(),
    );
    [
        ("ADX_RECOVERY_HOST".into(), "a".repeat(64)),
        (
            format!("ADX_RECOVERY_RUNTIME_{}", spec.runtime_class),
            "b".repeat(64),
        ),
        ("ADX_RECOVERY_ROOTFS_SHA256".into(), "c".repeat(64)),
        ("ADX_RECOVERY_ROOTFS_PATH".into(), "/runtime/rootfs".into()),
    ]
    .into()
}
