fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    let directory = "../../../third_party/sandboxd";
    let file = format!("{directory}/sandbox-api.proto");
    println!("cargo:rerun-if-changed={file}");
    tonic_build::configure().compile_protos(&[file], &[directory])?;
    Ok(())
}
