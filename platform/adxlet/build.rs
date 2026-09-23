fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    let directory = std::fs::canonicalize(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../third_party/sandboxd"),
    )?;
    let file = directory.join("sandbox-api.proto");
    println!("cargo:rerun-if-changed={}", file.display());
    tonic_build::configure().compile_protos(&[file], &[directory])?;
    Ok(())
}
