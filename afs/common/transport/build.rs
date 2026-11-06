fn main() {
    // `rdma` feature 打开时，把 native/rdma.c 编译成一个很小的静态 C shim。
    // 这个 shim 只是 libibverbs FFI 适配层，不是 native FS、不是 NFS、也不是独立 daemon。
    // 没有 rdma feature 时仍声明 cfg 名称，避免 rustc unexpected-cfg 告警。
    println!("cargo:rerun-if-changed=native/rdma.c");
    println!("cargo:rerun-if-changed=native/rdma.h");
    println!("cargo:rerun-if-changed=native/hixl.cc");
    println!("cargo:rerun-if-changed=native/hixl.h");
    println!("cargo:rerun-if-env-changed=ADX_CANN_PATH");

    if std::env::var_os("CARGO_FEATURE_RDMA").is_some() {
        cc::Build::new()
            .file("native/rdma.c")
            .warnings(false)
            .compile("afs_transport_rdma");
        println!("cargo:rustc-link-lib=ibverbs");
        println!("cargo:rustc-cfg=has_native_rdma");
    }
    println!("cargo:rustc-check-cfg=cfg(has_native_rdma)");

    if std::env::var_os("CARGO_FEATURE_HIXL").is_some() {
        if let Err(error) = build_hixl() {
            panic!("{error}");
        }
        println!("cargo:rustc-cfg=has_native_hixl");
    }
    println!("cargo:rustc-check-cfg=cfg(has_native_hixl)");
}

fn build_hixl() -> Result<(), String> {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        return Err("the hixl feature requires a Linux target with the CANN SDK".to_owned());
    }
    let configured = std::env::var("ADX_CANN_PATH")
        .map_err(|_| "ADX_CANN_PATH must point to a matching CANN 9.1.0 SDK (for example /opt/ascend/cann) when hixl is enabled".to_owned())?;
    let configured = std::path::PathBuf::from(configured);
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").map_err(|error| error.to_string())?;
    if arch != "x86_64" && arch != "aarch64" {
        return Err(format!(
            "HIXL requires a CANN-supported x86_64 or aarch64 Linux target, got {arch}"
        ));
    }
    let target_dir = format!("{arch}-linux");
    let sdk = if configured.join(&target_dir).join("include").is_dir() {
        configured.join(&target_dir)
    } else {
        configured
    };
    let include = sdk.join("include");
    let lib = sdk.join("lib64");
    let devlib_linux = sdk.join("devlib/linux").join(&arch);
    let devlib_device = sdk.join("devlib/device");
    for required in [
        include.join("hixl/hixl.h"),
        include.join("hixl/hixl_types.h"),
        include.join("acl/acl_rt.h"),
        lib.join("libcann_hixl.so"),
        lib.join("libascendcl.so"),
    ] {
        if !required.is_file() {
            return Err(format!(
                "ADX_CANN_PATH does not contain the required HIXL 9.1.0 SDK file: {}",
                required.display()
            ));
        }
    }
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("native/hixl.cc")
        .include(&include)
        .compile("afs_transport_hixl");
    println!("cargo:rustc-env=AFS_HIXL_SDK_PATH={}", sdk.display());
    println!("cargo:rustc-link-search=native={}", lib.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath-link,{}", lib.display());
    for directory in [&devlib_linux, &devlib_device] {
        if directory.is_dir() {
            println!("cargo:rustc-link-search=native={}", directory.display());
            println!(
                "cargo:rustc-link-arg=-Wl,-rpath-link,{}",
                directory.display()
            );
        }
    }
    println!("cargo:rustc-link-lib=dylib=cann_hixl");
    println!("cargo:rustc-link-lib=dylib=ascendcl");
    println!("cargo:rustc-link-lib=dylib=metadef");
    println!("cargo:rustc-link-lib=dylib=graph");
    println!("cargo:rustc-link-lib=dylib=stdc++");
    Ok(())
}
