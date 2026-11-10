fn main() {
    // `rdma` feature 打开时，把 native/rdma.c 编译成一个很小的静态 C shim。
    // 这个 shim 只是 libibverbs FFI 适配层，不是 native FS、不是 NFS、也不是独立 daemon。
    // 没有 rdma feature 时仍声明 cfg 名称，避免 rustc unexpected-cfg 告警。
    println!("cargo:rerun-if-changed=native/rdma.c");
    println!("cargo:rerun-if-changed=native/rdma.h");

    if std::env::var_os("CARGO_FEATURE_RDMA").is_some() {
        cc::Build::new()
            .file("native/rdma.c")
            .warnings(false)
            .compile("afs_transport_rdma");
        println!("cargo:rustc-link-lib=ibverbs");
        println!("cargo:rustc-cfg=has_native_rdma");
        println!("cargo:rustc-check-cfg=cfg(has_native_rdma)");
    } else {
        println!("cargo:rustc-check-cfg=cfg(has_native_rdma)");
    }
}
