fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    for file in [
        "instance.proto",
        "instance_types.proto",
        "snapshot.proto",
        "credentials.proto",
        "routes.proto",
    ] {
        println!("cargo:rerun-if-changed=../../api/proto/{file}");
    }
    tonic_build::configure()
        .boxed(".adx.control.v1.ClaimInstanceResponse.outcome.owned")
        .boxed(".adx.control.v1.ClaimInstanceResponse.outcome.existing")
        .compile_protos(&["../../api/proto/instance.proto"], &["../../api/proto"])?;
    println!("cargo:rerun-if-changed=../../api/proto/node.proto");
    tonic_build::configure()
        .compile_protos(&["../../api/proto/node.proto"], &["../../api/proto"])?;
    Ok(())
}
