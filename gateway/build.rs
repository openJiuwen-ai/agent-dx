fn main() {
    if let Ok(path) = protoc_bin_vendored::protoc_bin_path() {
        std::env::set_var("PROTOC", path);
    }
    println!("cargo:rerun-if-changed=../platform/api/proto/node.proto");
    tonic_build::configure()
        .compile_protos(
            &["../platform/api/proto/node.proto"],
            &["../platform/api/proto"],
        )
        .expect("compile node protocol");
}
