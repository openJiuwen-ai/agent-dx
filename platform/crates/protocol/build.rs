fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    println!("cargo:rerun-if-changed=../../api/proto/control.proto");
    tonic_build::configure()
        .compile_protos(&["../../api/proto/control.proto"], &["../../api/proto"])?;
    println!("cargo:rerun-if-changed=../../api/proto/node.proto");
    tonic_build::configure()
        .compile_protos(&["../../api/proto/node.proto"], &["../../api/proto"])?;
    Ok(())
}
