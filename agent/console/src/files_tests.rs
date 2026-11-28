use std::{fs,io::Read,os::unix::fs::symlink};

#[test]
fn excluded_mount_cannot_be_reached_with_noncanonical_path() {
    let excluded=vec!["private".to_string()];
    for path in ["./private/secret","private//secret","public/../private/secret"] {
        assert!(crate::files::check_excluded(path,&excluded).is_err(),"{path}");
    }
    assert!(crate::files::check_excluded("public/readme",&excluded).is_ok());
    assert!(crate::files::check_excluded("",&excluded).is_ok());
}

#[test]
fn storage_mount_requires_exact_readonly_backend_mount_not_plain_local_directory() {
    let mountinfo="25 1 0:23 / /srv/shared ro,nosuid - nfs4 server:/export ro,vers=4\n";
    assert!(crate::files::verify_mount(mountinfo,"/srv/shared","server:/export").is_ok());
    assert!(crate::files::verify_mount(mountinfo,"/srv/shared/sub","server:/export").is_err());
    assert!(crate::files::verify_mount(mountinfo,"/srv/shared","other:/export").is_err());
    assert!(crate::files::verify_mount(&mountinfo.replace("ro,nosuid","rw,nosuid"),"/srv/shared","server:/export").is_err());
}

#[test]
fn directory_listing_is_fd_pinned_bounded_and_never_follows_links_or_nested_mounts() {
    let root = std::env::temp_dir().join(format!("console-directory-{}",uuid::Uuid::new_v4()));
    fs::create_dir_all(root.join("selected/private")).unwrap();
    fs::write(root.join("selected/a.txt"),"abc").unwrap();
    symlink("/etc/passwd",root.join("selected/link")).unwrap();
    let safe = crate::files::SafeRoot::open(&root).unwrap();
    let directory = safe.open_relative("selected").unwrap();
    fs::rename(root.join("selected"),root.join("original")).unwrap();
    fs::create_dir(root.join("selected")).unwrap();
    fs::write(root.join("selected/wrong"),"wrong").unwrap();
    let entries = crate::files::list_directory(directory,3,"",&["private".into()]).unwrap();
    let value=serde_json::to_value(entries).unwrap();
    assert_eq!(value[0],serde_json::json!({"name":"a.txt","kind":"file","size":3,"accessible":true}));
    assert_eq!(value[1]["kind"],"symlink");
    assert_eq!(value[1]["accessible"],false);
    assert_eq!(value[1]["size"],serde_json::Value::Null);
    assert_eq!(value[2]["kind"],"mount_boundary");
    assert_eq!(value[2]["size"],serde_json::Value::Null);
    assert_eq!(crate::files::list_directory(safe.open_relative("original").unwrap(),2,"",&[]).err(),Some("directory_too_large"));
    assert_eq!(crate::files::list_directory(safe.open_relative("original/a.txt").unwrap(),10,"",&[]).err(),Some("not_directory"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fixed_root_open_rejects_escape_symlinks_and_special_files() {
    let root = std::env::temp_dir().join(format!("console-files-{}",uuid::Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    fs::create_dir(root.join("sub")).unwrap();
    fs::write(root.join("sub/plain"),"content").unwrap();
    symlink("sub",root.join("link")).unwrap();
    symlink("sub/plain",root.join("last")).unwrap();
    let socket = std::os::unix::net::UnixListener::bind(root.join("socket")).unwrap();
    let safe = crate::files::SafeRoot::open(&root).unwrap();
    let mut file = safe.open_relative("sub/plain").unwrap();
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
    assert_eq!(content,"content");
    for path in ["../outside","/etc/passwd","sub/../../etc/passwd","bad\0path"] {
        assert_eq!(safe.open_relative(path).err().unwrap(),"path_escape");
    }
    for path in ["link/plain","last"] { assert_eq!(safe.open_relative(path).err().unwrap(),"symlink_not_allowed"); }
    assert!(safe.open_relative("socket").is_err());
    symlink(&root,root.with_extension("link")).unwrap();
    assert!(crate::files::SafeRoot::open(&root.with_extension("link")).is_err());
    drop(socket);
    fs::remove_file(root.with_extension("link")).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn open_file_descriptor_stays_pinned_after_path_is_replaced() {
    let root = std::env::temp_dir().join(format!("console-files-{}",uuid::Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    fs::write(root.join("file"),"old").unwrap();
    let safe = crate::files::SafeRoot::open(&root).unwrap();
    let mut file = safe.open_relative("file").unwrap();
    fs::rename(root.join("file"),root.join("old")).unwrap();
    symlink("/etc/passwd",root.join("file")).unwrap();
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
    assert_eq!(content,"old");
    assert_eq!(safe.open_relative("file").err().unwrap(),"symlink_not_allowed");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn volume_wire_identity_and_tenant_prefix_are_checked_before_root_resolution() {
    let wire = serde_json::json!({"uid":"uid-a","revision":7,"spec":{"tenant_id":"t","id":"workspace","source":{"type":"directory","storage_domain":"sfs","path":"tenants/t/workspace"}},
        "environment_ids":[],"owner_ids":[],"snapshot_ids":[],"initialization":{"state":"ready","error":null},"occupancy":null});
    let mapping = serde_json::from_value(serde_json::json!({"revision":"map-1","domains":[{"storage_domain":"sfs","backend_id":"backend-a","read_only_root":"/srv/shared","tenant_paths":{"t":"tenants/t"}}]})).unwrap();
    let resolved = crate::files::resolve_volume(&wire,"t","workspace","uid-a",&mapping).unwrap();
    assert_eq!(resolved.relative_path,"tenants/t/workspace");
    assert_eq!(resolved.volume_revision,7);
    assert_eq!(resolved.backend_id,"backend-a");
    assert_eq!(crate::files::resolve_volume(&wire,"t","workspace","new-uid",&mapping).err(),Some("volume_identity_conflict"));
    let mut wrong = wire.clone();
    wrong["spec"]["tenant_id"] = "other".into();
    assert_eq!(crate::files::resolve_volume(&wrong,"t","workspace","uid-a",&mapping).err(),Some("volume_identity_conflict"));
    for path in ["tenants/t-other/workspace","tenants/t/../other","/etc","tenants/t//workspace"] {
        let mut wrong = wire.clone(); wrong["spec"]["source"]["path"] = path.into();
        assert!(crate::files::resolve_volume(&wrong,"t","workspace","uid-a",&mapping).is_err(),"{path}");
    }
    let mut wrong = wire.clone(); wrong["revision"] = 0.into();
    assert!(crate::files::resolve_volume(&wrong,"t","workspace","uid-a",&mapping).is_err());
}

#[test]
fn preview_enforces_actual_byte_limit_and_binary_detection() {
    let root = std::env::temp_dir().join(format!("console-preview-{}",uuid::Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    fs::write(root.join("text"),"hello").unwrap();
    fs::write(root.join("binary"),b"a\0b").unwrap();
    let safe = crate::files::SafeRoot::open(&root).unwrap();
    assert_eq!(crate::files::preview(safe.open_relative("text").unwrap(),5).unwrap(),"hello");
    assert_eq!(crate::files::preview(safe.open_relative("text").unwrap(),4),Err("preview_too_large"));
    assert_eq!(crate::files::preview(safe.open_relative("binary").unwrap(),10),Err("binary_file"));
    assert_eq!(crate::files::preview(safe.open_relative("").unwrap(),10),Err("not_regular_file"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn parent_mount_excludes_nested_targets_even_if_child_mount_unavailable() {
    let mounts: Vec<adx_agent_core::mounts::Mount> = serde_json::from_value(serde_json::json!([
        {"type":"bind","host_path":"/shared/root","target":"/data"},
        {"type":"bind","volume_id":"unavailable","target":"/data/private"},
        {"type":"bind","host_path":"/elsewhere","target":"/database"}
    ])).unwrap();
    let excluded = crate::files::excluded_subtrees("/data",&mounts);
    assert_eq!(excluded,vec!["private"]);
    assert_eq!(crate::files::check_excluded("private/key",&excluded),Err("mount_boundary_crossed"));
    assert_eq!(crate::files::check_excluded("private",&excluded),Err("mount_boundary_crossed"));
    assert!(crate::files::check_excluded("private-other/file",&excluded).is_ok());
}
