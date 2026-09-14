fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../../api/proto/legacy/rrt/rrt/v1/rrt.proto"], &["../../api/proto/legacy/rrt"])?;
    // RuntimeRPC protocol plus MetaData; compile once to avoid duplicate package definitions.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../api/proto/legacy/rrt/posix/runtime_rpc.proto",
                "../../api/proto/legacy/rrt/posix/resource.proto",
            ],
            &["../../api/proto/legacy/rrt/posix"],
        )?;
    Ok(())
}
