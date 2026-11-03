//! Build identity contract.
//!
//! The deployed binary reports the revision it was built from, and deployment
//! checks compare that against the expected commit. A stale id is worse than a
//! missing one: it makes a stale binary look current. The identity used to be
//! tracked through `<repo>/.git/HEAD`, which in a linked worktree points at the
//! main checkout's HEAD, so a commit inside the worktree kept the old id until
//! some Rust source changed.
//!
//! These tests fail when that regression comes back.

use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[test]
fn build_identity_is_never_empty() {
    let id = env!("AFS_BUILD_ID");
    assert!(!id.trim().is_empty(), "AFS_BUILD_ID must always be set");
}

#[test]
fn build_identity_matches_the_checkout_it_was_built_from() {
    let id = env!("AFS_BUILD_ID");
    let Some(head) = git(&["rev-parse", "--short=9", "HEAD"]) else {
        // Source archive without git metadata: the only honest answers are an
        // identity supplied by the build entry point or `unknown`.
        assert!(
            id == "unknown" || !id.is_empty(),
            "archive builds must report `unknown` or an explicit AFS_BUILD_ID, got {id:?}"
        );
        return;
    };

    let base = id.strip_suffix("-dirty").unwrap_or(id);
    assert_eq!(
        base, head,
        "AFS_BUILD_ID={id:?} does not match HEAD={head:?}: Cargo did not rebuild after the commit, \
         or the identity is being read from the wrong HEAD (linked worktree regression)"
    );
}

#[test]
fn dirty_flag_reflects_tracked_sources_only() {
    let id = env!("AFS_BUILD_ID");
    if id == "unknown" {
        return;
    }
    let Some(status) = git(&["status", "--porcelain", "--untracked-files=no"]) else {
        return;
    };
    let dirty = id.ends_with("-dirty");
    if dirty {
        // Build output inside the tree is untracked and must not set this flag,
        // so a dirty id has to be backed by a real tracked change.
        assert!(
            !status.is_empty(),
            "AFS_BUILD_ID says -dirty but tracked sources are clean"
        );
    }
}
