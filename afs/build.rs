//! Build identity for deployment checks.
//!
//! Every binary reports the source revision it was built from, so a deployment
//! script can refuse to measure a stale binary. Getting this wrong is worse than
//! not having it: a linked worktree used to keep reporting the revision that was
//! current at the first build, because Cargo tracked `<repo>/.git/HEAD` while the
//! checkout's real HEAD lives under `.git/worktrees/<name>/HEAD`.
//!
//! The identity is therefore derived from the git command's own view of the
//! checkout (`rev-parse --git-path`), and the dirty flag only looks at tracked
//! files: build output inside the tree (`afs/target/`) is untracked and must not
//! turn every build into `-dirty`. A source archive without `.git` reports
//! `unknown` unless the build entry point supplies `AFS_BUILD_ID`; it never
//! borrows a revision from somewhere else.

use std::env;
use std::path::Path;
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// Paths whose content decides the identity. Cargo must re-run this script when
/// any of them move, otherwise a commit (or a `git add`) leaves the old id in
/// place for as long as the Rust sources are untouched.
fn watch_identity_inputs() {
    // A symbolic HEAD keeps the same content (`ref: refs/heads/<branch>`) across
    // commits inside one branch, so the loose ref it points at has to be tracked
    // as well; otherwise only the index mtime could trigger a rebuild, which is
    // not a dependable signal.
    if let Some(reference) = git(&["symbolic-ref", "--quiet", "HEAD"])
        && let Some(path) = git(&["rev-parse", "--git-path", &reference])
        && Path::new(&path).exists()
    {
        println!("cargo:rerun-if-changed={path}");
    }
    for spec in ["HEAD", "index"] {
        let Some(path) = git(&["rev-parse", "--git-path", spec]) else {
            continue;
        };
        if Path::new(&path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    // The checkout itself may be replaced (fresh clone at the same path).
    if let Some(common) = git(&["rev-parse", "--git-common-dir"])
        && Path::new(&common).exists()
    {
        println!("cargo:rerun-if-changed={common}/packed-refs");
    }
}

fn revision() -> Option<String> {
    git(&["rev-parse", "--short=9", "HEAD"])
}

fn tracked_changes() -> bool {
    // `--untracked-files=no`: the revision describes the tracked source state.
    git(&["status", "--porcelain", "--untracked-files=no"]).is_some_and(|out| !out.is_empty())
}

fn explicit_identity() -> Option<String> {
    let value = env::var("AFS_BUILD_ID").ok()?;
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let sanitised: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+') {
                c
            } else {
                '_'
            }
        })
        .collect();
    Some(sanitised)
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=AFS_BUILD_ID");
    watch_identity_inputs();

    let id = match (explicit_identity(), revision()) {
        (Some(explicit), _) => explicit,
        (None, Some(revision)) => {
            if tracked_changes() {
                format!("{revision}-dirty")
            } else {
                revision
            }
        }
        // Source archive without git history: say so instead of inventing a
        // revision. Set AFS_BUILD_ID to provide one from the build entry point.
        (None, None) => {
            println!(
                "cargo:warning=AFS build identity is unknown: no git metadata here and AFS_BUILD_ID is unset"
            );
            String::from("unknown")
        }
    };
    println!("cargo:rustc-env=AFS_BUILD_ID={id}");
}
