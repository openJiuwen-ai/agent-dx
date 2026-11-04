use super::canonical_mount_path;
use std::os::unix::fs::symlink;

#[cfg(target_os = "linux")]
#[test]
fn read_config_mount_bdi_updates_only_selected_setting_and_supports_zero() {
    let dir = tempfile::tempdir().unwrap();
    let setting = dir.path().join("read_ahead_kb");
    let other = dir.path().join("other_bdi");
    std::fs::write(&setting, "128\n").unwrap();
    std::fs::write(&other, "256\n").unwrap();
    assert_eq!(
        super::update_bdi_readahead(&setting, 16 * 1024 * 1024).unwrap(),
        (128, 16384)
    );
    assert_eq!(
        super::update_bdi_readahead(&setting, 0).unwrap(),
        (16384, 0)
    );
    assert_eq!(std::fs::read_to_string(other).unwrap(), "256\n");
    assert!(super::update_bdi_readahead(&dir.path().join("absent"), 16777216).is_err());
    assert!(!dir.path().join("absent").exists());
    std::fs::write(&setting, "not a number").unwrap();
    assert!(super::update_bdi_readahead(&setting, 16777216).is_err());
}

#[test]
fn mount_owner_resolves_ancestor_symlinks_once() {
    let root = tempfile::tempdir().unwrap();
    let actual_parent = root.path().join("actual");
    let actual_mount = actual_parent.join("mount");
    std::fs::create_dir_all(&actual_mount).unwrap();
    let alias = root.path().join("alias");
    symlink(&actual_parent, &alias).unwrap();

    assert_eq!(
        canonical_mount_path(&alias.join("mount")).unwrap(),
        actual_mount
    );
}
