//! FD-relative Linux opens. Deployment/Volume authorization must precede this layer.
use std::{ffi::CString,fs::File,os::{fd::{AsRawFd,FromRawFd},unix::ffi::OsStrExt},path::{Component,Path}};

pub struct SafeRoot { directory: File }

#[derive(serde::Serialize)]
pub struct DirectoryEntry {
    pub name:String,
    pub kind:&'static str,
    pub size:Option<u64>,
    pub accessible:bool,
}
struct DirectoryStream(*mut libc::DIR);
impl Drop for DirectoryStream {
    fn drop(&mut self) {
        // SAFETY: fdopendir transferred an owned descriptor to this unique DIR handle.
        unsafe { libc::closedir(self.0); }
    }
}
/// Enumerate a fixed directory FD, never reopening its original pathname.
pub fn list_directory(directory:File,limit:usize,path:&str,excluded:&[String]) -> Result<Vec<DirectoryEntry>,&'static str> {
    use std::os::fd::IntoRawFd;
    if !directory.metadata().map_err(|_|"shared_storage_unavailable")?.is_dir() { return Err("not_directory"); }
    check_excluded(path,excluded)?;
    let fd=directory.into_raw_fd();
    // SAFETY: fd is an owned readable directory FD; on success fdopendir takes ownership.
    let stream=unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        // SAFETY: failed fdopendir did not take ownership of fd.
        unsafe { libc::close(fd); }
        return Err("shared_storage_unavailable");
    }
    let stream=DirectoryStream(stream);
    let mut entries=Vec::new();
    loop {
        // SAFETY: errno is thread-local; readdir owns its buffer until the next call.
        let entry=unsafe { *libc::__errno_location()=0; libc::readdir(stream.0) };
        if entry.is_null() {
            // SAFETY: errno is read on the same thread immediately after readdir.
            if unsafe { *libc::__errno_location() } != 0 { return Err("shared_storage_unavailable"); }
            break;
        }
        // SAFETY: successful readdir returns a NUL-terminated d_name, copied before another call.
        let name=unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if matches!(name,b"."|b"..") { continue; }
        if entries.len()>=limit { return Err("directory_too_large"); }
        let name=std::str::from_utf8(name).map_err(|_|"file_name_invalid")?.to_owned();
        let relative=if path.is_empty() { name.clone() } else { format!("{path}/{name}") };
        let (kind,size,accessible)=if check_excluded(&relative,excluded).is_err() {
            ("mount_boundary",None,false)
        } else {
            match open_at(fd,Path::new(&name),false) {
                Ok(file)=> {
                    let metadata=file.metadata().map_err(|_|"shared_storage_unavailable")?;
                    if metadata.is_dir() { ("dir",None,true) } else { ("file",Some(metadata.len()),true) }
                },
                Err("symlink_not_allowed")=>("symlink",None,false),
                Err("mount_boundary_crossed")=>("mount_boundary",None,false),
                Err("special_file_not_allowed")=>("special",None,false),
                Err(kind)=>return Err(kind),
            }
        };
        entries.push(DirectoryEntry {name,kind,size,accessible});
    }
    entries.sort_by(|a,b|a.name.cmp(&b.name));
    Ok(entries)
}

pub fn excluded_subtrees(target: &str,mounts: &[adx_agent_core::mounts::Mount]) -> Vec<String> {
    let prefix = format!("{}/",target.trim_end_matches('/'));
    let mut excluded: Vec<_> = mounts.iter().filter_map(|mount| mount.target.strip_prefix(&prefix))
        .filter(|relative| !relative.is_empty()).map(str::to_owned).collect();
    excluded.sort();
    excluded.dedup();
    excluded
}
pub fn check_excluded(path: &str,excluded: &[String]) -> Result<(),&'static str> {
    if !path.is_empty() && path.split('/').any(|part|matches!(part,""|"."|"..")) {
        return Err("path_escape");
    }
    if excluded.iter().any(|prefix| path == prefix || path.strip_prefix(prefix).is_some_and(|tail| tail.starts_with('/'))) {
        return Err("mount_boundary_crossed");
    }
    Ok(())
}

pub fn preview(file: File, limit: usize) -> Result<String,&'static str> {
    use std::io::Read;
    if !file.metadata().map_err(|_| "shared_storage_unavailable")?.is_file() { return Err("not_regular_file"); }
    let maximum = u64::try_from(limit).map_err(|_| "preview_too_large")?.checked_add(1).ok_or("preview_too_large")?;
    let mut bytes = Vec::new();
    file.take(maximum).read_to_end(&mut bytes).map_err(|_| "shared_storage_unavailable")?;
    if bytes.len() > limit { return Err("preview_too_large"); }
    if bytes.contains(&0) { return Err("binary_file"); }
    String::from_utf8(bytes).map_err(|_| "binary_file")
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageMappings { pub revision:String, pub domains:Vec<StorageDomain> }
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageDomain {
    pub storage_domain:String,
    pub backend_id:String,
    pub read_only_root:String,
    pub tenant_paths:std::collections::BTreeMap<String,String>,
    pub mount_source:Option<String>,
}
/// Require an exact, actually mounted, read-only backend from deployment mountinfo.
pub fn verify_mount(mountinfo:&str,root:&str,source:&str)->Result<(),&'static str> {
    let decode=|value:&str|value.replace("\\040"," ").replace("\\011","\t").replace("\\134","\\");
    for line in mountinfo.lines() {
        let Some((left,right))=line.split_once(" - ") else {continue;};
        let fields:Vec<_>=left.split_whitespace().collect();
        let backend:Vec<_>=right.split_whitespace().collect();
        if fields.len()>5 && backend.len()>1 && decode(fields[4])==root && decode(backend[1])==source
            && fields[5].split(',').any(|option|option=="ro") {return Ok(());}
    }
    Err("shared_storage_unavailable")
}
pub struct ResolvedVolume {
    pub relative_path:String,
    pub volume_revision:u64,
    pub backend_id:String,
    pub mapping_revision:String,
    pub read_only_root:String,
}
fn relative_directory(path: &str) -> bool {
    !path.is_empty() && !path.starts_with('/') && !path.chars().any(char::is_control)
        && path.split('/').all(|part| !matches!(part,""|"."|".."))
}
/// Validate the actual public Volume shape; this does not itself authorize an open.
pub fn resolve_volume(wire: &serde_json::Value,tenant: &str,id: &str,uid: &str,mappings: &StorageMappings) -> Result<ResolvedVolume,&'static str> {
    let volume: adx_core::volumes::Volume = serde_json::from_value(wire.clone()).map_err(|_| "volume_resolution_unavailable")?;
    volume.validate().map_err(|_| "volume_resolution_unavailable")?;
    if volume.uid != uid || volume.spec.id != id || volume.spec.tenant_id != tenant { return Err("volume_identity_conflict"); }
    let adx_core::volumes::VolumeSource::Directory { storage_domain,path } = volume.spec.source;
    let mut domains = mappings.domains.iter().filter(|domain| domain.storage_domain == storage_domain);
    let domain = domains.next().ok_or("shared_mount_unsupported")?;
    if domains.next().is_some() || mappings.revision.is_empty() || domain.backend_id.is_empty() || !domain.read_only_root.starts_with('/') {
        return Err("mount_context_conflict");
    }
    let prefix = domain.tenant_paths.get(tenant).ok_or("volume_path_forbidden")?;
    if !relative_directory(&path) || !relative_directory(prefix)
        || (path != *prefix && !path.strip_prefix(prefix).is_some_and(|tail| tail.starts_with('/'))) {
        return Err("volume_path_forbidden");
    }
    Ok(ResolvedVolume { relative_path:path,volume_revision:volume.revision,backend_id:domain.backend_id.clone(),
        mapping_revision:mappings.revision.clone(),read_only_root:domain.read_only_root.clone() })
}
#[repr(C)]
struct OpenHow { flags:u64, mode:u64, resolve:u64 }
const RESOLVE_NO_XDEV:u64 = 0x01;
const RESOLVE_NO_MAGICLINKS:u64 = 0x02;
const RESOLVE_NO_SYMLINKS:u64 = 0x04;
const RESOLVE_BENEATH:u64 = 0x08;

fn open_at(directory: i32, path: &Path, root: bool) -> Result<File,&'static str> {
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| "path_escape")?;
    let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | if root { libc::O_DIRECTORY } else { 0 };
    let how = OpenHow { flags:flags as u64, mode:0,resolve:RESOLVE_NO_MAGICLINKS|RESOLVE_NO_SYMLINKS|if root { 0 } else { RESOLVE_BENEATH|RESOLVE_NO_XDEV } };
    // SAFETY: CString and OpenHow remain live for the syscall; their pointers and sizes are valid.
    let fd = unsafe { libc::syscall(libc::SYS_openat2,directory,path.as_ptr(),&how,std::mem::size_of::<OpenHow>()) };
    if fd < 0 {
        return Err(match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::ELOOP) => "symlink_not_allowed",
            Some(libc::EXDEV) => "mount_boundary_crossed",
            Some(libc::ENOSYS|libc::EINVAL) => "mount_isolation_unavailable",
            _ => "shared_storage_unavailable",
        });
    }
    let fd = i32::try_from(fd).map_err(|_| "shared_storage_unavailable")?;
    // SAFETY: successful openat2 returned a new owned descriptor, transferred exactly once.
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(|_| "shared_storage_unavailable")?;
    if !metadata.is_file() && !metadata.is_dir() { return Err("special_file_not_allowed"); }
    Ok(file)
}
impl SafeRoot {
    pub fn from_directory(directory:File)->Result<Self,&'static str> {
        if !directory.metadata().map_err(|_|"shared_storage_unavailable")?.is_dir() {return Err("not_directory");}
        Ok(Self {directory})
    }
    /// Open a deployment-controlled root without following any symlink component.
    pub fn open(path: &Path) -> Result<Self,&'static str> {
        Ok(Self { directory:open_at(libc::AT_FDCWD,path,true)? })
    }
    /// Return the actual authorized handle, never a path to be reopened later.
    pub fn open_relative(&self, path: &str) -> Result<File,&'static str> {
        let path = Path::new(path);
        if path.is_absolute() || path.as_os_str().as_bytes().contains(&0) || path.components().any(|part| matches!(part,Component::ParentDir|Component::RootDir|Component::Prefix(_))) {
            return Err("path_escape");
        }
        open_at(self.directory.as_raw_fd(),if path.as_os_str().is_empty() { Path::new(".") } else { path },false)
    }
}
