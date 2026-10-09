use std::{ffi::OsString, os::unix::fs::symlink};

use afs::node::storage::{
    DirectoryHandle, FileHandle, FileStore, LocalFs, OpenSpec, RenameMode, StoragePath,
};

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap()
}

fn tempdir() -> tempfile::TempDir {
    let base = std::env::current_dir()
        .unwrap()
        .join("target")
        .join("storage-localfs-tests");
    std::fs::create_dir_all(&base).unwrap();
    tempfile::Builder::new()
        .prefix("case-")
        .tempdir_in(base)
        .unwrap()
}

#[test]
fn localfs_reads_and_writes_from_open_file_handles() {
    let temp = tempdir();
    let store = LocalFs::open(temp.path()).unwrap();
    store.mkdir(&path("job"), 0o755).unwrap();

    let file = store
        .open_file(
            &path("job/log.txt"),
            OpenSpec::new(libc::O_CREAT | libc::O_RDWR, 0o644),
        )
        .unwrap();

    assert_eq!(file.write_at(0, b"hello world").unwrap(), 11);
    file.flush().unwrap();

    let mut buffer = [0_u8; 5];
    assert_eq!(file.read_at(6, &mut buffer).unwrap(), 5);
    assert_eq!(&buffer, b"world");

    file.set_len(5).unwrap();
    assert_eq!(file.metadata().unwrap().len(), 5);
    file.sync_data().unwrap();
    file.sync_all().unwrap();
}

#[test]
fn localfs_handles_directories_and_root_sync() {
    let temp = tempdir();
    let store = LocalFs::open(temp.path()).unwrap();

    store.mkdir(&path("job"), 0o755).unwrap();
    store
        .open_file(
            &path("job/a.txt"),
            OpenSpec::new(libc::O_CREAT | libc::O_WRONLY, 0o644),
        )
        .unwrap();
    store
        .open_file(
            &path("job/b.txt"),
            OpenSpec::new(libc::O_CREAT | libc::O_WRONLY, 0o644),
        )
        .unwrap();

    let names = store.open_dir(&path("job")).unwrap().read_dir().unwrap();
    assert_eq!(
        names,
        vec![OsString::from("a.txt"), OsString::from("b.txt")]
    );

    assert!(store.metadata(&path("job")).unwrap().is_dir());
    store
        .open_dir(&StoragePath::root())
        .unwrap()
        .sync_all()
        .unwrap();
    store.sync_root().unwrap();
}

#[test]
fn localfs_renames_and_removes_under_root() {
    let temp = tempdir();
    let store = LocalFs::open(temp.path()).unwrap();
    store.mkdir(&path("job"), 0o755).unwrap();
    store
        .open_file(
            &path("job/source.txt"),
            OpenSpec::new(libc::O_CREAT | libc::O_WRONLY, 0o644),
        )
        .unwrap();

    store
        .rename(
            &path("job/source.txt"),
            &path("job/target.txt"),
            RenameMode::Replace,
        )
        .unwrap();
    assert!(store.metadata(&path("job/target.txt")).unwrap().is_file());

    store.remove_file(&path("job/target.txt")).unwrap();
    store.remove_dir(&path("job")).unwrap();
}

#[test]
fn localfs_rejects_bad_storage_paths_before_io() {
    for bad in ["/abs", "a//b", "a/./b", "a/../b", "a/", "a\0b"] {
        assert!(StoragePath::new(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn localfs_rejects_symlink_parent_traversal() {
    let temp = tempdir();
    let outside = tempdir();
    std::fs::write(outside.path().join("secret.txt"), b"secret").unwrap();
    symlink(outside.path(), temp.path().join("job")).unwrap();
    let store = LocalFs::open(temp.path()).unwrap();

    let err = store
        .open_file(&path("job/secret.txt"), OpenSpec::new(libc::O_RDONLY, 0))
        .unwrap_err();
    assert!(matches!(
        err.raw_os_error(),
        Some(libc::ELOOP | libc::ENOTDIR)
    ));
}

#[test]
fn localfs_reports_final_symlink_metadata_without_following() {
    let temp = tempdir();
    let outside = tempdir();
    std::fs::write(outside.path().join("secret.txt"), b"secret").unwrap();
    std::fs::create_dir(temp.path().join("job")).unwrap();
    symlink(
        outside.path().join("secret.txt"),
        temp.path().join("job/link"),
    )
    .unwrap();
    let store = LocalFs::open(temp.path()).unwrap();

    let metadata = store.metadata(&path("job/link")).unwrap();
    assert!(metadata.file_type().is_symlink());
}

#[test]
fn localfs_supports_no_replace_rename_when_destination_is_absent() {
    let temp = tempdir();
    let store = LocalFs::open(temp.path()).unwrap();
    store
        .open_file(
            &path("a"),
            OpenSpec::new(libc::O_CREAT | libc::O_WRONLY, 0o644),
        )
        .unwrap();

    store
        .rename(&path("a"), &path("b"), RenameMode::NoReplace)
        .unwrap();
    assert!(store.metadata(&path("b")).is_ok());
}

#[test]
fn localfs_no_replace_rename_rejects_existing_destination() {
    let temp = tempdir();
    let store = LocalFs::open(temp.path()).unwrap();
    for name in ["a", "b"] {
        store
            .open_file(
                &path(name),
                OpenSpec::new(libc::O_CREAT | libc::O_WRONLY, 0o644),
            )
            .unwrap();
    }

    let error = store
        .rename(&path("a"), &path("b"), RenameMode::NoReplace)
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(store.metadata(&path("a")).is_ok());
    assert!(store.metadata(&path("b")).is_ok());
}
