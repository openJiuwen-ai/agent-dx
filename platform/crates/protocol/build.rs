fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    // Shared target directories may be reused by different Git worktrees.
    // Resolve and track the actual manifest's protocol inputs, not a previous
    // worktree's build-script working directory.
    println!("cargo:rerun-if-env-changed=CARGO_MANIFEST_DIR");
    let manifest = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto = manifest.join("../../api/proto");
    for file in [
        "environment.proto",
        "environment_types.proto",
        "snapshot.proto",
        "snapshot_preparation.proto",
        "volume.proto",
        "credentials.proto",
        "routes.proto",
    ] {
        println!("cargo:rerun-if-changed={}", proto.join(file).display());
    }
    tonic_build::configure()
        .type_attribute(".adx.control.v1.NodeUpgradeControl", "#[derive(serde::Serialize, serde::Deserialize, Eq)] #[serde(deny_unknown_fields)]")
        .type_attribute(".adx.control.v1.NodeUpgradeGrant", "#[derive(serde::Serialize, serde::Deserialize, Eq)] #[serde(deny_unknown_fields)]")
        // Optional workload metadata must not inflate every nested RPC future.
        .boxed(".adx.control.v1.ExecutionOptions.readiness_probe")
        .boxed(".adx.control.v1.ExecutionOptions.healthcheck_probe")
        .boxed(".adx.control.v1.EnvironmentRecord.failure")
        .boxed(".adx.control.v1.ClaimEnvironmentResponse.outcome.owned")
        .boxed(".adx.control.v1.ClaimEnvironmentResponse.outcome.existing")
        .skip_debug(".adx.control.v1.EnvironmentCompatibility")
        .skip_debug(".adx.control.v1.EnvdServiceConfig")
        .skip_debug(".adx.control.v1.SandboxRouteCompatibility")
        .compile_protos(&[proto.join("environment.proto")], std::slice::from_ref(&proto))?;
    println!(
        "cargo:rerun-if-changed={}",
        proto.join("node.proto").display()
    );
    tonic_build::configure()
        .compile_protos(&[proto.join("node.proto")], std::slice::from_ref(&proto))?;
    println!(
        "cargo:rerun-if-changed={}",
        proto.join("runtime_control.proto").display()
    );
    tonic_build::configure().compile_protos(&[proto.join("runtime_control.proto")], &[proto])?;
    Ok(())
}
