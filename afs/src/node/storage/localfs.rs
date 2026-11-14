//! Safe local filesystem backend for node-owned data directories.
//!
//! LocalFs is the first concrete `FileStore` implementation. It is intentionally
//! small: it provides root-confined local POSIX primitives without WAL, ownership
//! state, image publication, or strong file-identity protection. Those rules live
//! in OwnerFs/DFS above this layer.

use std::{
    ffi::{CString, OsStr, OsString},
    fs::{self, File, OpenOptions},
    io,
    mem::MaybeUninit,
    os::{
        fd::AsRawFd,
        unix::{
            ffi::{OsStrExt, OsStringExt},
            fs::{DirBuilderExt, FileExt, OpenOptionsExt},
        },
    },
    path::{Path, PathBuf},
    ptr,
    time::{SystemTime, UNIX_EPOCH},
};

use super::{DirectoryHandle, FileHandle, FileStore, OpenSpec, RenameMode, StoragePath};
use crate::node::vfs::types::FilesystemCapacity;

const UNSUPPORTED_RENAME_MODE: &str = "rename mode requires unsupported renameat2 semantics";

/// Root-confined local disk store.
///
/// The root directory is opened once and all child operations are resolved from
/// that descriptor via `/proc/self/fd/<fd>/<child>`. Each traversed directory is
/// opened with `O_NOFOLLOW | O_DIRECTORY`; file opens add `O_NOFOLLOW` for the
/// leaf. This rejects symlink traversal without using unsafe Rust or adding an
/// `openat2` wrapper dependency.
#[derive(Debug)]
pub struct LocalFs {
    root: File,
    root_path: PathBuf,
}

impl LocalFs {
    /// Open a direct child of an already-open directory handle.
    ///
    /// The path-based `open_file` walks every component from the filesystem
    /// root again; callers that already hold the parent directory use this to
    /// resolve the leaf in a single step.
    #[cfg(feature = "ownerfs")]
    pub(crate) fn open_child_in(
        directory: &LocalDirectory,
        name: &OsStr,
        spec: OpenSpec,
    ) -> io::Result<LocalFile> {
        open_child_file(&directory.dir, name, spec).map(|file| LocalFile { file })
    }

    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root_path = root.as_ref().canonicalize()?;
        let root = open_dir_path(&root_path)?;
        Ok(Self { root, root_path })
    }

    #[must_use]
    pub fn root_path(&self) -> &Path {
        &self.root_path
    }

    #[cfg(feature = "ownerfs")]
    pub(crate) fn remove_tree(&self, path: &StoragePath) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let (parent, name) = self.open_parent(path)?;
        remove_tree_child(&parent, &name, parent.metadata()?.dev(), 0)
    }

    pub fn statvfs(&self) -> io::Result<FilesystemCapacity> {
        capacity_from_statvfs_fd(&self.root)
    }

    fn open_parent(&self, path: &StoragePath) -> io::Result<(File, OsString)> {
        let mut components = split_components(path)?;
        let leaf = components.pop().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "operation requires a non-root storage path",
            )
        })?;
        let parent = self.open_dir_components(&components)?;
        Ok((parent, leaf))
    }

    fn open_dir_components(&self, components: &[OsString]) -> io::Result<File> {
        let mut dir = self.root.try_clone()?;
        for component in components {
            dir = open_child_dir(&dir, component)?;
        }
        Ok(dir)
    }

    fn open_path_dir(&self, path: &StoragePath) -> io::Result<File> {
        if path.is_root() {
            return self.root.try_clone();
        }
        // One openat2 instead of one openat per component. The resolve flags
        // keep the same confinement and no-symlink policy as the walk.
        if let Some(result) = openat2_dir(&self.root, path.as_path().as_os_str()) {
            return result;
        }
        self.open_dir_components(&split_components(path)?)
    }
}

#[cfg(feature = "ownerfs")]
fn remove_tree_child(parent: &File, name: &OsStr, device: u64, depth: usize) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    if depth > 256 {
        return Err(io::Error::other(
            "recursive deletion nesting limit exceeded",
        ));
    }
    let path = proc_child(parent, name);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return fs::remove_file(path);
    }
    let directory = open_child_dir(parent, name)?;
    if directory.metadata()?.dev() != device {
        return Err(io::Error::other(
            "refusing to cross a filesystem during recursive deletion",
        ));
    }
    for entry in fs::read_dir(format!("/proc/self/fd/{}", directory.as_raw_fd()))? {
        remove_tree_child(&directory, &entry?.file_name(), device, depth + 1)?;
    }
    fs::remove_dir(path)
}

#[allow(unsafe_code)]
pub(crate) fn capacity_from_statvfs_fd(file: &File) -> io::Result<FilesystemCapacity> {
    let mut raw = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `raw` points to valid writable memory for one `statvfs` result,
    // and `file` is a live directory FD supplied by the caller. The kernel
    // writes the struct before returning 0 and does not retain the pointer.
    let result = unsafe { libc::fstatvfs(file.as_raw_fd(), raw.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a zero return from fstatvfs means the kernel initialized the
    // entire statvfs struct.
    let raw = unsafe { raw.assume_init() };
    let bsize = u32::try_from(raw.f_bsize).unwrap_or(u32::MAX);
    let frsize = match u32::try_from(raw.f_frsize).unwrap_or(u32::MAX) {
        0 => bsize,
        value => value,
    };
    Ok(FilesystemCapacity {
        blocks: raw.f_blocks,
        bfree: raw.f_bfree,
        bavail: raw.f_bavail,
        files: raw.f_files,
        ffree: raw.f_ffree,
        bsize,
        namelen: u32::try_from(raw.f_namemax).unwrap_or(u32::MAX),
        frsize,
    })
}

impl FileStore for LocalFs {
    type File = LocalFile;
    type Directory = LocalDirectory;

    fn open_file(&self, path: &StoragePath, spec: OpenSpec) -> io::Result<Self::File> {
        let (parent, leaf) = self.open_parent(path)?;
        let file = open_child_file(&parent, &leaf, spec)?;
        Ok(LocalFile { file })
    }

    fn open_dir(&self, path: &StoragePath) -> io::Result<Self::Directory> {
        Ok(LocalDirectory {
            dir: self.open_path_dir(path)?,
        })
    }

    fn metadata(&self, path: &StoragePath) -> io::Result<fs::Metadata> {
        if path.is_root() {
            return self.root.metadata();
        }
        let (parent, leaf) = self.open_parent(path)?;
        // Attribute lookup does not follow the final symlink. Intermediate
        // symlinks have already been rejected by `open_parent`.
        fs::symlink_metadata(proc_child(&parent, &leaf))
    }

    fn read_link(&self, path: &StoragePath) -> io::Result<OsString> {
        let (parent, leaf) = self.open_parent(path)?;
        fs::read_link(proc_child(&parent, &leaf)).map(|target| target.into_os_string())
    }

    fn symlink(&self, path: &StoragePath, target: &OsStr) -> io::Result<()> {
        let (parent, leaf) = self.open_parent(path)?;
        std::os::unix::fs::symlink(Path::new(target), proc_child(&parent, &leaf))
    }

    fn hard_link(&self, from: &StoragePath, to: &StoragePath) -> io::Result<()> {
        let (from_parent, from_leaf) = self.open_parent(from)?;
        let (to_parent, to_leaf) = self.open_parent(to)?;
        fs::hard_link(
            proc_child(&from_parent, &from_leaf),
            proc_child(&to_parent, &to_leaf),
        )
    }

    fn chmod(&self, path: &StoragePath, mode: u32) -> io::Result<()> {
        if path.is_root() {
            return fchmod(&self.root, mode);
        }
        let (parent, leaf) = self.open_parent(path)?;
        chmodat(&parent, &leaf, mode)
    }

    fn chown(&self, path: &StoragePath, uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
        if path.is_root() {
            return fchown(&self.root, uid, gid);
        }
        let (parent, leaf) = self.open_parent(path)?;
        chownat(&parent, &leaf, uid, gid)
    }

    fn set_times(
        &self,
        path: &StoragePath,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> io::Result<()> {
        if path.is_root() {
            return futimens(&self.root, atime, mtime);
        }
        let (parent, leaf) = self.open_parent(path)?;
        utimensat(&parent, &leaf, atime, mtime)
    }

    fn get_xattr(&self, path: &StoragePath, name: &OsStr) -> io::Result<Vec<u8>> {
        if path.is_root() {
            return fgetxattr(&self.root, name);
        }
        let (parent, leaf) = self.open_parent(path)?;
        lgetxattr(proc_child(&parent, &leaf), name)
    }

    fn list_xattr(&self, path: &StoragePath) -> io::Result<Vec<u8>> {
        if path.is_root() {
            return flistxattr(&self.root);
        }
        let (parent, leaf) = self.open_parent(path)?;
        llistxattr(proc_child(&parent, &leaf))
    }

    fn set_xattr(
        &self,
        path: &StoragePath,
        name: &OsStr,
        value: &[u8],
        flags: i32,
    ) -> io::Result<()> {
        if path.is_root() {
            return fsetxattr(&self.root, name, value, flags);
        }
        let (parent, leaf) = self.open_parent(path)?;
        lsetxattr(proc_child(&parent, &leaf), name, value, flags)
    }

    fn remove_xattr(&self, path: &StoragePath, name: &OsStr) -> io::Result<()> {
        if path.is_root() {
            return fremovexattr(&self.root, name);
        }
        let (parent, leaf) = self.open_parent(path)?;
        lremovexattr(proc_child(&parent, &leaf), name)
    }

    fn read_dir(&self, path: &StoragePath) -> io::Result<Vec<OsString>> {
        self.open_dir(path)?.read_dir()
    }

    fn mkdir(&self, path: &StoragePath, mode: u32) -> io::Result<()> {
        let (parent, leaf) = self.open_parent(path)?;
        fs::DirBuilder::new()
            .mode(mode)
            .create(proc_child(&parent, &leaf))
    }

    fn remove_file(&self, path: &StoragePath) -> io::Result<()> {
        let (parent, leaf) = self.open_parent(path)?;
        fs::remove_file(proc_child(&parent, &leaf))
    }

    fn remove_dir(&self, path: &StoragePath) -> io::Result<()> {
        let (parent, leaf) = self.open_parent(path)?;
        fs::remove_dir(proc_child(&parent, &leaf))
    }

    fn rename(&self, from: &StoragePath, to: &StoragePath, mode: RenameMode) -> io::Result<()> {
        if mode == RenameMode::Exchange {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                UNSUPPORTED_RENAME_MODE,
            ));
        }
        let (from_parent, from_leaf) = self.open_parent(from)?;
        let (to_parent, to_leaf) = self.open_parent(to)?;
        match mode {
            RenameMode::Replace => fs::rename(
                proc_child(&from_parent, &from_leaf),
                proc_child(&to_parent, &to_leaf),
            ),
            RenameMode::NoReplace => {
                renameat2_no_replace(&from_parent, &from_leaf, &to_parent, &to_leaf)
            }
            RenameMode::Exchange => unreachable!("Exchange returned above"),
        }
    }

    fn sync_root(&self) -> io::Result<()> {
        self.root.sync_all()
    }
}

/// Plain local file handle.
///
/// There is no userspace buffer in this implementation, so `flush` is a no-op.
/// Durable boundaries remain explicit: `sync_data` for file content and minimal
/// metadata, `sync_all` for full file metadata.
#[derive(Debug)]
pub struct LocalFile {
    file: File,
}

impl FileHandle for LocalFile {
    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.read_at(buffer, offset)
    }

    fn write_at(&self, offset: u64, buffer: &[u8]) -> io::Result<usize> {
        self.file.write_at(buffer, offset)
    }

    fn metadata(&self) -> io::Result<fs::Metadata> {
        self.file.metadata()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    fn chmod(&self, mode: u32) -> io::Result<()> {
        fchmod(&self.file, mode)
    }

    fn chown(&self, uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
        fchown(&self.file, uid, gid)
    }

    fn set_times(&self, atime: Option<SystemTime>, mtime: Option<SystemTime>) -> io::Result<()> {
        futimens(&self.file, atime, mtime)
    }

    fn get_xattr(&self, name: &OsStr) -> io::Result<Vec<u8>> {
        fgetxattr(&self.file, name)
    }

    fn list_xattr(&self) -> io::Result<Vec<u8>> {
        flistxattr(&self.file)
    }

    fn set_xattr(&self, name: &OsStr, value: &[u8], flags: i32) -> io::Result<()> {
        fsetxattr(&self.file, name, value, flags)
    }

    fn remove_xattr(&self, name: &OsStr) -> io::Result<()> {
        fremovexattr(&self.file, name)
    }

    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

#[derive(Debug)]
pub struct LocalDirectory {
    dir: File,
}

impl DirectoryHandle for LocalDirectory {
    fn metadata(&self) -> io::Result<fs::Metadata> {
        self.dir.metadata()
    }

    fn child_present(&self, name: &OsStr) -> io::Result<bool> {
        use std::os::fd::AsRawFd;
        let candidate = std::path::Path::new(name);
        if name.is_empty() || candidate.components().count() != 1 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let cname = std::ffi::CString::new(name.as_encoded_bytes())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: libc::stat is plain old data, so the all-zero bit pattern is
        // a valid (empty) value that fstatat then overwrites.
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: self.dir is a live directory descriptor, cname is a
        // NUL-terminated single component, and stat is a valid out-parameter for
        // the duration of the call.
        let rc = unsafe {
            libc::fstatat(
                self.dir.as_raw_fd(),
                cname.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(false);
        }
        Err(error)
    }

    fn read_dir(&self) -> io::Result<Vec<OsString>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(proc_fd_path(&self.dir))? {
            let entry = entry?;
            let name = entry.file_name();
            if name != OsStr::new(".") && name != OsStr::new("..") {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.dir.sync_all()
    }
}

fn optional_uid(value: Option<u32>) -> libc::uid_t {
    value.map_or(!0, |value| value as libc::uid_t)
}

fn optional_gid(value: Option<u32>) -> libc::gid_t {
    value.map_or(!0, |value| value as libc::gid_t)
}

#[allow(unsafe_code)]
fn fchmod(file: &File, mode: u32) -> io::Result<()> {
    // SAFETY: fd belongs to a live File and fchmod does not retain pointers.
    let result = unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn chmodat(parent: &File, leaf: &OsStr, mode: u32) -> io::Result<()> {
    let leaf = cstring(leaf)?;
    // SAFETY: parent fd is live, leaf is NUL-terminated and not retained.
    let result =
        unsafe { libc::fchmodat(parent.as_raw_fd(), leaf.as_ptr(), mode as libc::mode_t, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn fchown(file: &File, uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
    // SAFETY: fd belongs to a live File and fchown does not retain pointers.
    let result = unsafe { libc::fchown(file.as_raw_fd(), optional_uid(uid), optional_gid(gid)) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn chownat(parent: &File, leaf: &OsStr, uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
    let leaf = cstring(leaf)?;
    // SAFETY: parent fd is live, leaf is NUL-terminated and not retained.
    let result = unsafe {
        libc::fchownat(
            parent.as_raw_fd(),
            leaf.as_ptr(),
            optional_uid(uid),
            optional_gid(gid),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn timespec_or_omit(value: Option<SystemTime>) -> io::Result<libc::timespec> {
    match value {
        Some(value) => {
            let duration = value.duration_since(UNIX_EPOCH).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "time predates Unix epoch")
            })?;
            Ok(libc::timespec {
                tv_sec: duration.as_secs() as libc::time_t,
                tv_nsec: duration.subsec_nanos() as libc::c_long,
            })
        }
        None => Ok(libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT as libc::c_long,
        }),
    }
}

#[allow(unsafe_code)]
fn futimens(file: &File, atime: Option<SystemTime>, mtime: Option<SystemTime>) -> io::Result<()> {
    let times = [timespec_or_omit(atime)?, timespec_or_omit(mtime)?];
    // SAFETY: fd is live, pointer references a fixed-size stack array for call duration only.
    let result = unsafe { libc::futimens(file.as_raw_fd(), times.as_ptr()) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn utimensat(
    parent: &File,
    leaf: &OsStr,
    atime: Option<SystemTime>,
    mtime: Option<SystemTime>,
) -> io::Result<()> {
    let leaf = cstring(leaf)?;
    let times = [timespec_or_omit(atime)?, timespec_or_omit(mtime)?];
    // SAFETY: parent fd is live, leaf and times pointers are valid for call duration only.
    let result = unsafe {
        libc::utimensat(
            parent.as_raw_fd(),
            leaf.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn cstring(value: &OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains NUL byte"))
}

fn xattr_name(name: &OsStr) -> io::Result<CString> {
    cstring(name)
}

fn xattr_path(path: PathBuf) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))
}

#[allow(unsafe_code)]
fn fgetxattr(file: &File, name: &OsStr) -> io::Result<Vec<u8>> {
    let name = xattr_name(name)?;
    // SAFETY: pointers are valid and not retained; null buffer asks for size.
    let size = unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), ptr::null_mut(), 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buffer = vec![0_u8; size as usize];
    // SAFETY: buffer is allocated to the reported size and pointer is valid.
    let read = unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
        )
    };
    if read < 0 {
        Err(io::Error::last_os_error())
    } else {
        buffer.truncate(read as usize);
        Ok(buffer)
    }
}

#[allow(unsafe_code)]
fn lgetxattr(path: PathBuf, name: &OsStr) -> io::Result<Vec<u8>> {
    let path = xattr_path(path)?;
    let name = xattr_name(name)?;
    // SAFETY: pointers are valid and not retained; null buffer asks for size.
    let size = unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), ptr::null_mut(), 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buffer = vec![0_u8; size as usize];
    // SAFETY: buffer is allocated to the reported size and pointer is valid.
    let read = unsafe {
        libc::lgetxattr(
            path.as_ptr(),
            name.as_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
        )
    };
    if read < 0 {
        Err(io::Error::last_os_error())
    } else {
        buffer.truncate(read as usize);
        Ok(buffer)
    }
}

#[allow(unsafe_code)]
fn flistxattr(file: &File) -> io::Result<Vec<u8>> {
    // SAFETY: fd is live; null buffer asks for size.
    let size = unsafe { libc::flistxattr(file.as_raw_fd(), ptr::null_mut(), 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buffer = vec![0_u8; size as usize];
    // SAFETY: buffer is allocated to the reported size and pointer is valid.
    let read =
        unsafe { libc::flistxattr(file.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
    if read < 0 {
        Err(io::Error::last_os_error())
    } else {
        buffer.truncate(read as usize);
        Ok(buffer)
    }
}

#[allow(unsafe_code)]
fn llistxattr(path: PathBuf) -> io::Result<Vec<u8>> {
    let path = xattr_path(path)?;
    // SAFETY: path pointer is valid and not retained; null buffer asks for size.
    let size = unsafe { libc::llistxattr(path.as_ptr(), ptr::null_mut(), 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buffer = vec![0_u8; size as usize];
    // SAFETY: buffer is allocated to the reported size and pointer is valid.
    let read = unsafe { libc::llistxattr(path.as_ptr(), buffer.as_mut_ptr().cast(), buffer.len()) };
    if read < 0 {
        Err(io::Error::last_os_error())
    } else {
        buffer.truncate(read as usize);
        Ok(buffer)
    }
}

#[allow(unsafe_code)]
fn fsetxattr(file: &File, name: &OsStr, value: &[u8], flags: i32) -> io::Result<()> {
    let name = xattr_name(name)?;
    // SAFETY: fd is live, pointers are valid for call duration and not retained.
    let result = unsafe {
        libc::fsetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            flags,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn lsetxattr(path: PathBuf, name: &OsStr, value: &[u8], flags: i32) -> io::Result<()> {
    let path = xattr_path(path)?;
    let name = xattr_name(name)?;
    // SAFETY: pointers are valid for call duration and not retained.
    let result = unsafe {
        libc::lsetxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            flags,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn fremovexattr(file: &File, name: &OsStr) -> io::Result<()> {
    let name = xattr_name(name)?;
    // SAFETY: fd is live and name pointer is valid for call duration only.
    let result = unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn lremovexattr(path: PathBuf, name: &OsStr) -> io::Result<()> {
    let path = xattr_path(path)?;
    let name = xattr_name(name)?;
    // SAFETY: pointers are valid for call duration and not retained.
    let result = unsafe { libc::lremovexattr(path.as_ptr(), name.as_ptr()) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn split_components(path: &StoragePath) -> io::Result<Vec<OsString>> {
    let bytes = path.bytes();
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut components = Vec::new();
    for component in bytes.split(|byte| *byte == b'/') {
        if component.is_empty() || component == b"." || component == b".." || component.contains(&0)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage path contains an invalid component",
            ));
        }
        components.push(OsString::from_vec(component.to_vec()));
    }
    Ok(components)
}

fn open_dir_path(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
}

fn open_child_dir(parent: &File, leaf: &OsStr) -> io::Result<File> {
    openat_raw(
        parent,
        leaf,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        0,
    )
}

/// `openat` against a live directory descriptor.
///
/// The previous implementation opened `/proc/self/fd/<fd>/<name>`, which costs
/// the kernel an extra procfs path walk and symlink resolution per component.
#[allow(unsafe_code)]
fn openat_raw(parent: &File, leaf: &OsStr, flags: i32, mode: u32) -> io::Result<File> {
    let name = std::ffi::CString::new(leaf.as_encoded_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    loop {
        // SAFETY: `parent` owns a live directory descriptor and `name` is
        // NUL-terminated; the returned descriptor is adopted immediately.
        let fd = unsafe {
            libc::openat(
                std::os::fd::AsRawFd::as_raw_fd(parent),
                name.as_ptr(),
                flags | libc::O_CLOEXEC,
                mode as libc::c_uint,
            )
        };
        if fd >= 0 {
            // SAFETY: `fd` is a freshly returned descriptor owned by this call.
            return Ok(unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Resolve a whole relative path below `root` in one `openat2` call.
///
/// `RESOLVE_IN_ROOT` keeps the lookup confined to the root descriptor even for
/// `..`, and `RESOLVE_NO_SYMLINKS` preserves the `O_NOFOLLOW` policy the
/// component walk applied.
///
/// Returns `None` when the caller must fall back to the component walk: the
/// kernel lacks openat2 (`ENOSYS`), or the whole relative path does not fit the
/// kernel path limit (`ENAMETOOLONG`) while every single component still does.
/// Every other error is returned to the caller unchanged.
#[allow(unsafe_code)]
fn openat2_dir(root: &File, relative: &OsStr) -> Option<io::Result<File>> {
    let name = std::ffi::CString::new(relative.as_encoded_bytes()).ok()?;
    // SAFETY: libc::open_how is plain old data; the all-zero value is a valid
    // struct and every field used below is assigned before the syscall.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = (libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY) as u64;
    how.resolve = libc::RESOLVE_IN_ROOT | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_MAGICLINKS;
    // SAFETY: `how` is a fully initialised open_how and `name` is NUL-terminated.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            std::os::fd::AsRawFd::as_raw_fd(root),
            name.as_ptr(),
            &how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if fd >= 0 {
        // `syscall` returns `c_long`; a descriptor outside `c_int` cannot be a
        // `RawFd`, so refuse instead of truncating. The kernel never returns
        // one, which is why the error path does not try to close it.
        let Ok(raw_fd) = i32::try_from(fd) else {
            debug_assert!(
                false,
                "openat2 returned a descriptor wider than c_int: {fd}"
            );
            return Some(Err(io::Error::from_raw_os_error(libc::EOVERFLOW)));
        };
        // SAFETY: raw_fd is the non-negative descriptor the kernel just
        // returned to this process; nothing else owns it, so it is adopted here.
        return Some(Ok(unsafe { std::os::fd::FromRawFd::from_raw_fd(raw_fd) }));
    }
    let error = io::Error::last_os_error();
    // Fall back to the component walk when openat2 is unavailable *and* when the
    // whole relative path does not fit the kernel's path limit. A tree of legal
    // 200-byte directory names can exceed PATH_MAX as one string while every
    // component stays valid, and the component walk opens them one at a time.
    if matches!(
        error.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::ENAMETOOLONG)
    ) {
        return None;
    }
    Some(Err(error))
}

fn open_child_file(parent: &File, leaf: &OsStr, spec: OpenSpec) -> io::Result<File> {
    if spec.flags & libc::O_DIRECTORY != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "open_file does not accept O_DIRECTORY",
        ));
    }
    let access = spec.flags & libc::O_ACCMODE;
    if !matches!(access, libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported access mode",
        ));
    }
    let flags = access
        | spec.flags & (libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC | libc::O_APPEND)
        | passthrough_flags(spec.flags)
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW;
    openat_raw(parent, leaf, flags, spec.mode)
}

fn passthrough_flags(flags: i32) -> i32 {
    flags
        & !(libc::O_ACCMODE
            | libc::O_CREAT
            | libc::O_EXCL
            | libc::O_TRUNC
            | libc::O_APPEND
            | libc::O_DIRECTORY)
}

fn proc_child(parent: &File, leaf: &OsStr) -> PathBuf {
    proc_fd_path(parent).join(leaf)
}

#[allow(unsafe_code)]
fn renameat2_no_replace(
    from_parent: &File,
    from_leaf: &OsStr,
    to_parent: &File,
    to_leaf: &OsStr,
) -> io::Result<()> {
    let from = CString::new(from_leaf.as_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "source name contains NUL byte")
    })?;
    let to = CString::new(to_leaf.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination name contains NUL byte",
        )
    })?;
    // SAFETY: parent descriptors are live directory FDs opened by LocalFs.
    // `from` and `to` are NUL-terminated single path components validated by
    // StoragePath/open_parent. The syscall does not retain these pointers after
    // returning, and RENAME_NOREPLACE gives the required atomic no-clobber
    // semantics that cannot be implemented with a prior existence check.
    let result = unsafe {
        libc::renameat2(
            from_parent.as_raw_fd(),
            from.as_ptr(),
            to_parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn proc_fd_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statvfs_reports_capacity_from_open_root_fd() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalFs::open(temp.path()).unwrap();

        let capacity = local.statvfs().unwrap();

        assert!(capacity.bsize > 0);
        assert!(capacity.frsize > 0);
        assert!(capacity.namelen > 0);
        assert!(capacity.blocks >= capacity.bfree);
        assert!(capacity.bfree >= capacity.bavail);
    }

    /// A tree whose components are all legal can still exceed `PATH_MAX` as a
    /// single string: 25 directories of 200 bytes make a 5024-byte relative
    /// path. `openat2` rejects that with `ENAMETOOLONG`, which must fall back to
    /// the component walk instead of ending the walk with an error.
    #[test]
    fn relative_path_longer_than_path_max_falls_back_to_component_walk() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalFs::open(temp.path()).unwrap();

        let name = "d".repeat(200);
        create_deep_chain(&local.root, &name, 25);
        let mut relative = std::path::PathBuf::new();
        for _ in 0..25 {
            relative.push(&name);
        }
        assert!(relative.as_os_str().len() > 4096);

        let path = StoragePath::new(&relative).unwrap();
        let opened = local
            .open_path_dir(&path)
            .expect("deep path must stay reachable through the component walk");
        assert!(opened.metadata().unwrap().is_dir());
    }

    /// The fallback must not weaken confinement: a symlink component still has
    /// to be refused, however deep the path is.
    #[test]
    fn deep_path_fallback_keeps_no_symlink_policy() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalFs::open(temp.path()).unwrap();

        let name = "d".repeat(200);
        let depth = 25;
        // The chain itself cannot be created through one host path (> PATH_MAX),
        // so build it component by component exactly like a real deep tree.
        create_deep_chain(&local.root, &name, depth);
        std::os::unix::fs::symlink(temp.path().join(&name), temp.path().join("link")).unwrap();

        let mut escaped = std::path::PathBuf::from("link");
        for _ in 0..depth {
            escaped.push(&name);
        }
        assert!(escaped.as_os_str().len() > 4096);
        let error = local
            .open_path_dir(&StoragePath::new(&escaped).unwrap())
            .expect_err("symlink component must be rejected");
        // `O_NOFOLLOW | O_DIRECTORY` on a symlink to a directory reports
        // ENOTDIR on Linux; either errno is a refusal, never a silent follow.
        assert!(matches!(
            error.raw_os_error(),
            Some(libc::ELOOP) | Some(libc::ENOTDIR)
        ));
    }

    /// Create `<root>/<name>/<name>/...` `depth` levels deep, one `mkdirat` at a
    /// time, because the concatenated path is longer than `PATH_MAX`.
    fn create_deep_chain(root: &std::fs::File, name: &str, depth: usize) {
        let name_c = std::ffi::CString::new(name).unwrap();
        let mut dir = root.try_clone().unwrap();
        for _ in 0..depth {
            let fd = std::os::fd::AsRawFd::as_raw_fd(&dir);
            // SAFETY: `fd` is a live directory descriptor owned by `dir` and
            // `name_c` is a NUL-terminated component name.
            let created = unsafe { libc::mkdirat(fd, name_c.as_ptr(), 0o755) };
            assert_eq!(created, 0, "mkdirat failed: {}", io::Error::last_os_error());
            // SAFETY: same live descriptor and NUL-terminated name.
            let opened = unsafe {
                libc::openat(
                    fd,
                    name_c.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
                )
            };
            assert!(opened >= 0, "openat failed: {}", io::Error::last_os_error());
            // SAFETY: `opened` is a freshly returned descriptor owned by this call.
            dir = unsafe { <std::fs::File as std::os::fd::FromRawFd>::from_raw_fd(opened) };
        }
    }
}
