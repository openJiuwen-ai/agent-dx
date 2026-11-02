//! Compile-time build provenance.
//!
//! The crate version cannot distinguish two builds of the same source tree,
//! and several users may share a validation host. Reporting the commit and
//! whether the tree was dirty lets an operator prove which binary is running
//! without trusting a filename or an out-of-band checksum.
//!
//! Everything here degrades to `unknown` rather than failing the build: a
//! missing `git` or a source tarball without `.git` must still compile.

use std::{path::Path, process::Command};

fn main() {
    // `build.rs` runs with the crate directory as its working directory, so
    // the repository root is one level up. In a worktree `.git` is a pointer
    // file rather than a directory; git handles that itself as long as it is
    // invoked somewhere inside the work tree.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    println!("cargo:rerun-if-changed=build.rs");
    // The provenance inputs are environment overrides on this platform, so a
    // new commit without a source change must still trigger a rebuild. Without
    // this, cargo happily reuses the previous binary and the provenance lies.
    println!("cargo:rerun-if-env-changed=AFS_BUILD_COMMIT");
    println!("cargo:rerun-if-env-changed=AFS_BUILD_DIRTY");
    if root.join(".git").exists() {
        // A commit or checkout changes HEAD; the pointer file changes when a
        // worktree is re-attached.
        println!("cargo:rerun-if-changed={}", root.join(".git").display());
    }

    // The build wrapper supplies these when git cannot run inside the build
    // environment (for example a WSL checkout whose `.git` pointer uses a
    // Windows UNC path). Falling back to git keeps plain `cargo build` usable.
    let commit = std::env::var("AFS_BUILD_COMMIT")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| git(&root, &["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".into());
    let dirty = std::env::var("AFS_BUILD_DIRTY")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| match git(&root, &["status", "--porcelain"]) {
            // Empty status means the tree matches the commit.
            Some(status) if status.trim().is_empty() => "clean".to_owned(),
            Some(_) => "dirty".to_owned(),
            None => "unknown".to_owned(),
        });
    println!("cargo:rustc-env=AFS_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=AFS_BUILD_DIRTY={dirty}");

    // Capture the wall clock once, at compile time. `SOURCE_DATE_EPOCH` wins so
    // reproducible builds stay reproducible.
    let stamp = std::env::var("SOURCE_DATE_EPOCH").ok().or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|value| value.as_secs().to_string())
    });
    println!(
        "cargo:rustc-env=AFS_BUILD_TIME={}",
        stamp.unwrap_or_else(|| "unknown".into())
    );
}

fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
