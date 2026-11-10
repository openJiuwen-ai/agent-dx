//! R72: a directory is listed in Meta pages on demand.
use super::*;

fn directory_fs(
    names: &[&str],
    page_cap: usize,
) -> (tempfile::TempDir, Arc<RecordingMeta>, DistributedFs) {
    let (temp, meta, fs) = test_fs();
    meta.inode.lock().unwrap().kind = InodeKind::Directory;
    meta.dir_page_cap.store(page_cap, Ordering::SeqCst);
    for name in names {
        add_entry(&meta, name);
    }
    (temp, meta, fs)
}

fn add_entry(meta: &RecordingMeta, name: &str) {
    meta.dir_entries.lock().unwrap().insert(
        name.as_bytes().to_vec(),
        DirEntryRecord {
            name: name.as_bytes().to_vec(),
            inode_id: InodeId::new(format!("inode:{name}")),
            kind: InodeKind::Regular,
        },
    );
}

fn names(rows: &[DirectoryEntry]) -> Vec<String> {
    rows.iter()
        .map(|row| row.name.to_string_lossy().into_owned())
        .collect()
}

fn root(fs: &DistributedFs) -> BackendInode {
    let inode_id = fs
        .meta
        .get_inode(&InodeId::new("inode:test"))
        .unwrap()
        .inode_id;
    fs.backend_inode(&inode_id).unwrap()
}

#[test]
fn readdir_fetches_pages_on_demand_in_name_order_with_exact_cookies() {
    let all = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
    let (_temp, meta, fs) = directory_fs(&all, 3);
    let dir = fs.opendir(&context(), root(&fs)).unwrap();
    assert_eq!(
        meta.dir_page_calls.load(Ordering::SeqCst),
        0,
        "opendir lists nothing"
    );
    let first = fs.readdir(&context(), dir, 0, 4).unwrap();
    assert_eq!(names(&first), ["a", "b", "c", "d"]);
    assert_eq!(
        first.iter().map(|row| row.next_cookie).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(meta.dir_page_calls.load(Ordering::SeqCst), 2);
    let mut cookie = first.last().unwrap().next_cookie;
    let mut listed = names(&first);
    loop {
        let rows = fs.readdir(&context(), dir, cookie, 4).unwrap();
        if rows.is_empty() {
            break;
        }
        cookie = rows.last().unwrap().next_cookie;
        listed.extend(names(&rows));
    }
    assert_eq!(listed, all);
    // seekdir back inside the fetched range is exact and costs no Meta call.
    let calls = meta.dir_page_calls.load(Ordering::SeqCst);
    assert_eq!(
        names(&fs.readdir(&context(), dir, 2, 2).unwrap()),
        ["c", "d"]
    );
    assert_eq!(meta.dir_page_calls.load(Ordering::SeqCst), calls);
    fs.releasedir(&context(), dir).unwrap();
}

#[test]
fn readdir_lists_unchanged_entries_exactly_once_across_concurrent_changes() {
    let all: Vec<String> = (0..20).map(|i| format!("n{i:02}")).collect();
    let refs: Vec<&str> = all.iter().map(String::as_str).collect();
    let (_temp, meta, fs) = directory_fs(&refs, 4);
    let dir = fs.opendir(&context(), root(&fs)).unwrap();
    let mut listed = names(&fs.readdir(&context(), dir, 0, 5).unwrap());
    // Between pages: remove one listed and one not yet listed name, add
    // names before and after the cursor.
    {
        let mut entries = meta.dir_entries.lock().unwrap();
        entries.remove(b"n01".as_slice());
        entries.remove(b"n15".as_slice());
    }
    add_entry(&meta, "a-before");
    add_entry(&meta, "z-after");
    let mut cookie = 5;
    loop {
        let rows = fs.readdir(&context(), dir, cookie, 5).unwrap();
        if rows.is_empty() {
            break;
        }
        cookie = rows.last().unwrap().next_cookie;
        listed.extend(names(&rows));
    }
    let mut seen = std::collections::HashSet::new();
    for name in &listed {
        assert!(seen.insert(name.clone()), "{name} listed twice");
    }
    for name in all
        .iter()
        .filter(|name| !["n01", "n15"].contains(&name.as_str()))
    {
        assert!(seen.contains(name), "unchanged {name} missing");
    }
    fs.releasedir(&context(), dir).unwrap();
}
