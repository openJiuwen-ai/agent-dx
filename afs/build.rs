use std::process::Command;

fn main() {
    // Embed a build identity so every deployed process can log exactly which
    // source revision it is running. Deployment checks compare this against the
    // expected commit; a mismatch means the test would measure a stale binary.
    let mut id = String::from("unknown");
    if let Ok(out) = Command::new("git")
        .args(["rev-parse", "--short=9", "HEAD"])
        .output()
    {
        if out.status.success() {
            id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        }
    }
    if let Ok(out) = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
    {
        if out.status.success() && !out.stdout.is_empty() {
            id.push_str("-dirty");
        }
    }
    println!("cargo:rustc-env=AFS_BUILD_ID={id}");
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=build.rs");
}
