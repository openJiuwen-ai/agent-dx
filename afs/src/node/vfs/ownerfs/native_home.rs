use std::{
    ffi::{OsStr, OsString},
    fs,
    os::unix::fs::MetadataExt,
    sync::{Arc, Weak},
};

use super::*;

pub(super) struct NativeRootRef {
    anchor: Weak<RootAnchor>,
    #[cfg(test)]
    binding: root::PrivateRootBinding,
}

impl NativeRootRef {
    fn upgrade(&self) -> Option<Arc<RootAnchor>> {
        self.anchor.upgrade()
    }
    fn strong_count(&self) -> usize {
        self.anchor.strong_count()
    }
    fn new(anchor: &Arc<RootAnchor>) -> Self {
        Self {
            anchor: Arc::downgrade(anchor),
            #[cfg(test)]
            binding: root::PrivateRootBinding::from_grant(&anchor.grant),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryIdentity {
    pub(crate) dev: u64,
    pub(crate) ino: u64,
}

impl DirectoryIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Result<Self> {
        if !metadata.is_dir() {
            return Err(native_home_invalid("native Home source is not a directory"));
        }
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NamespaceIdentity {
    pub(crate) dev: u64,
    pub(crate) ino: u64,
}

impl NamespaceIdentity {
    fn current() -> Result<Self> {
        let metadata = fs::metadata("/proc/thread-self/ns/mnt").map_err(Error::from)?;
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
}

pub(super) struct RootAnchor {
    name: OsString,
    grant: RootGrant,
    data_dir: StoragePath,
    namespace: NamespaceIdentity,
    inode: BackendInode,
    source_identity: DirectoryIdentity,
    source: fs::File,
}

impl RootAnchor {
    fn entry(&self) -> Result<Entry> {
        let current = directory_identity(&self.source)?;
        if current != self.source_identity {
            return Err(native_home_invalid(
                "native Home source descriptor identity changed",
            ));
        }
        Ok(Entry {
            inode: self.inode,
            attributes: attributes_from_metadata(self.source.metadata().map_err(Error::from)?)?,
        })
    }
}

pub(crate) struct HomeExportAuthority {
    owner: Weak<LocalOwnerFs>,
    grant: RootGrant,
    name: OsString,
    data_dir: StoragePath,
    namespace: NamespaceIdentity,
    source_identity: DirectoryIdentity,
    anchor: Arc<RootAnchor>,
}

impl HomeExportAuthority {
    pub(crate) fn verify_current(&self, owner: &OwnerFs) -> Result<()> {
        let local = owner.require_native_home_owner()?;
        let held = self
            .owner
            .upgrade()
            .ok_or_else(|| native_home_invalid("native Home owner instance was dropped"))?;
        if !Arc::ptr_eq(&held, local) {
            return Err(native_home_invalid(
                "native Home authority belongs to another OwnerFs instance",
            ));
        }
        if NamespaceIdentity::current()? != self.namespace {
            return Err(native_home_invalid("native Home namespace changed"));
        }
        let current_id = root::root_id_from_name(&self.name)?;
        if current_id != self.grant.id {
            return Err(native_home_invalid("native Home root name changed"));
        }
        let root_use = local.roots.enter_root(&self.grant.id, RootRight::Write)?;
        if root_use.grant() != &self.grant {
            return Err(native_home_invalid("native Home grant is stale"));
        }
        if root_use.data_dir() != &self.data_dir {
            return Err(native_home_invalid("native Home data directory changed"));
        }
        check_native_home_grant(root_use.grant())?;
        drop(root_use);
        let reopened = open_storage_dir_no_follow(&local.disk, self.data_dir.as_path())?;
        if directory_identity(&reopened)? != self.source_identity {
            return Err(native_home_invalid("native Home source was replaced"));
        }
        if directory_identity(&self.anchor.source)? != self.source_identity {
            return Err(native_home_invalid(
                "native Home held source descriptor changed",
            ));
        }
        Ok(())
    }

    pub(crate) fn source_descriptor(&self) -> Result<fs::File> {
        self.anchor.source.try_clone().map_err(Error::from)
    }

    pub(crate) fn grant(&self) -> &RootGrant {
        &self.grant
    }

    pub(crate) fn name(&self) -> &OsStr {
        &self.name
    }

    pub(crate) fn source_identity(&self) -> DirectoryIdentity {
        self.source_identity
    }

    pub(crate) fn namespace(&self) -> NamespaceIdentity {
        self.namespace
    }

    #[cfg(test)]
    pub(super) fn data_dir(&self) -> &StoragePath {
        &self.data_dir
    }

    #[cfg(test)]
    pub(super) fn root_id(&self) -> &RootId {
        &self.grant.id
    }
}

impl OwnerFs {
    fn require_native_home_owner(&self) -> Result<&Arc<LocalOwnerFs>> {
        let cache = self
            .private_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !cache.native_home_eligible {
            return Err(native_home_invalid(
                "OwnerFs instance is not eligible for native Home authority",
            ));
        }
        drop(cache);
        self.local
            .as_ref()
            .ok_or_else(|| native_home_invalid("OwnerFs has no local Home authority"))
    }

    pub(crate) fn native_home_export_for_current_namespace(
        &self,
        name: &OsStr,
    ) -> Result<HomeExportAuthority> {
        self.create_native_home_export(name)
    }

    fn create_native_home_export(&self, name: &OsStr) -> Result<HomeExportAuthority> {
        let local = self.require_native_home_owner()?;
        let root_id = root::root_id_from_name(name)?;
        let root_use = local.roots.enter_root(&root_id, RootRight::Write)?;
        check_native_home_grant(root_use.grant())?;
        let grant = root_use.grant().clone();
        let data_dir = root_use.data_dir().clone();
        drop(root_use);

        let source = open_storage_dir_no_follow(&local.disk, data_dir.as_path())?;
        let source_identity = directory_identity(&source)?;
        let entry = local.lookup(backend_inode(OWNERFS_ROOT_INODE), name)?;
        let namespace = NamespaceIdentity::current()?;

        let anchor = {
            let mut cache = self.private_cache.lock().map_err(|_| poisoned())?;
            if !cache.native_home_eligible {
                return Err(native_home_invalid(
                    "OwnerFs instance is not eligible for native Home authority",
                ));
            }
            cache
                .native_home_roots
                .retain(|_, anchor| anchor.strong_count() > 0);
            match cache
                .native_home_roots
                .get(&root_id)
                .and_then(|anchor| anchor.upgrade())
            {
                Some(existing)
                    if existing.name == name
                        && existing.grant == grant
                        && existing.data_dir == data_dir
                        && existing.namespace == namespace
                        && existing.inode == entry.inode
                        && existing.source_identity == source_identity =>
                {
                    existing
                }
                Some(_) => {
                    return Err(native_home_invalid(
                        "native Home root anchor changed while authority is held",
                    ));
                }
                None => {
                    let anchor = Arc::new(RootAnchor {
                        name: name.to_os_string(),
                        grant: grant.clone(),
                        data_dir: data_dir.clone(),
                        namespace,
                        inode: entry.inode,
                        source_identity,
                        source,
                    });
                    cache
                        .native_home_roots
                        .insert(root_id.clone(), NativeRootRef::new(&anchor));
                    anchor
                }
            }
        };

        let authority = HomeExportAuthority {
            owner: Arc::downgrade(local),
            grant,
            name: name.to_os_string(),
            data_dir,
            namespace,
            source_identity,
            anchor,
        };
        authority.verify_current(self)?;
        Ok(authority)
    }

    pub(super) fn native_home_root_entry(&self, name: &OsStr) -> Result<Option<Entry>> {
        let root_id = match root::root_id_from_name(name) {
            Ok(root_id) => root_id,
            Err(_) => return Ok(None),
        };
        let anchor = {
            let mut cache = self.private_cache.lock().map_err(|_| poisoned())?;
            if !cache.native_home_eligible {
                return Ok(None);
            }
            let anchor = cache
                .native_home_roots
                .get(&root_id)
                .and_then(|anchor| anchor.upgrade());
            if anchor.is_none() {
                cache.native_home_roots.remove(&root_id);
            }
            anchor
        };
        anchor.map(|anchor| anchor.entry()).transpose()
    }

    pub(super) fn native_home_root_attributes(
        &self,
        inode: BackendInode,
    ) -> Result<Option<FileAttributes>> {
        let anchors = {
            let mut cache = self.private_cache.lock().map_err(|_| poisoned())?;
            if !cache.native_home_eligible {
                return Ok(None);
            }
            cache
                .native_home_roots
                .retain(|_, anchor| anchor.strong_count() > 0);
            cache
                .native_home_roots
                .values()
                .filter_map(|anchor| anchor.upgrade())
                .collect::<Vec<_>>()
        };
        for anchor in anchors {
            if anchor.inode == inode {
                return Ok(Some(anchor.entry()?.attributes));
            }
        }
        Ok(None)
    }
}

fn check_native_home_grant(grant: &RootGrant) -> Result<()> {
    if grant.home_node_id != grant.holder_node_id || grant.home_session_id != grant.session_id {
        return Err(native_home_invalid(
            "native Home requires a full local Home root grant",
        ));
    }
    for right in [RootRight::Lookup, RootRight::Read, RootRight::Write] {
        if !grant.rights.contains(&right) {
            return Err(native_home_invalid(
                "native Home root grant is missing required rights",
            ));
        }
    }
    Ok(())
}

fn directory_identity(file: &fs::File) -> Result<DirectoryIdentity> {
    let metadata = file.metadata().map_err(Error::from)?;
    DirectoryIdentity::from_metadata(&metadata)
}

fn native_home_invalid(message: &'static str) -> Error {
    Error::coded(afs_error::NODE_OWNER_INVALID_GRANT, message)
}
#[cfg(test)]
impl OwnerFs {
    pub(super) fn private_pf1_native_authority(
        &self,
        binding: &root::PrivateRootBinding,
    ) -> Result<PrivatePf1Class> {
        let anchor = {
            let cache = self.private_cache.lock().map_err(|_| poisoned())?;
            let Some(reference) = cache.native_home_roots.get(&binding.root_id) else {
                return Ok(PrivatePf1Class::UnknownBindingMissing);
            };
            if reference.binding != *binding {
                return Ok(PrivatePf1Class::UnknownBindingMissing);
            }
            reference.upgrade()
        };
        let Some(anchor) = anchor else {
            return Ok(PrivatePf1Class::ObservedDrained);
        };
        // An actual held anchor, not None, establishes this private lifecycle.
        let local = self.require_local()?;
        if NamespaceIdentity::current()? != anchor.namespace
            || root::PrivateRootBinding::from_grant(&anchor.grant) != *binding
            || directory_identity(&anchor.source)? != anchor.source_identity
            || directory_identity(&open_storage_dir_no_follow(
                &local.disk,
                anchor.data_dir.as_path(),
            )?)? != anchor.source_identity
        {
            return Err(native_home_invalid("PF1 native authority identity changed"));
        }
        Ok(PrivatePf1Class::ObservedPresent)
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use crate::node::vfs::Backend;

    fn fixture() -> (tempfile::TempDir, OwnerFs, RequestContext) {
        let (temp, fs, ctx, _disk) = super::super::native_home_tests::fixture(true);
        (temp, fs, ctx)
    }

    fn export(fs: &OwnerFs, name: &str) -> HomeExportAuthority {
        fs.native_home_export_for_current_namespace(OsStr::new(name))
            .unwrap()
    }

    fn assert_verify_rejects(mut authority: HomeExportAuthority, fs: &OwnerFs) {
        assert!(authority.verify_current(fs).is_ok());
        authority.grant.epoch += 1;
        assert!(authority.verify_current(fs).is_err());
    }

    #[test]
    fn duplicate_native_home_authority_preserves_older_anchor_after_newer_drop() {
        let (_temp, fs, ctx) = fixture();
        let root = super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");
        let first = export(&fs, "native");
        let second = export(&fs, "native");
        let root_id = first.grant.id.clone();

        fs.require_local().unwrap().roots.revoke_root(&root_id);
        drop(second);

        let anchored = Backend::lookup(&fs, &ctx, fs.root_inode(), OsStr::new("native")).unwrap();
        assert_eq!(anchored.inode, root.inode);
        Backend::getattr(&fs, &ctx, root.inode, None).unwrap();

        drop(first);
        assert!(Backend::lookup(&fs, &ctx, fs.root_inode(), OsStr::new("native")).is_err());
    }

    #[test]
    fn native_home_authority_rejects_each_stored_grant_field_mutation() {
        let (_temp, fs, ctx) = fixture();
        super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");

        assert_verify_rejects(export(&fs, "native"), &fs);

        let mut authority = export(&fs, "native");
        authority.grant.id.0.push_str("-changed");
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.grant.home_node_id.push_str("-changed");
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.grant.home_session_id.push_str("-changed");
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.grant.holder_node_id.push_str("-changed");
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.grant.session_id.push_str("-changed");
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.grant.access_generation += 1;
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.grant.fencing_token.push_str("-changed");
        assert!(authority.verify_current(&fs).is_err());

        for right in [RootRight::Lookup, RootRight::Read, RootRight::Write] {
            let mut authority = export(&fs, "native");
            authority
                .grant
                .rights
                .retain(|candidate| candidate != &right);
            assert!(authority.verify_current(&fs).is_err());
        }
    }

    #[test]
    fn duplicate_native_home_authority_rejects_changed_source_while_first_is_held() {
        let (temp, fs, ctx) = fixture();
        super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");
        let _first = export(&fs, "native");
        let second = export(&fs, "native");
        drop(second);

        let data_path = temp.path().join(_first.data_dir.as_path());
        let backup_path = temp.path().join("native-duplicate-source-replaced");
        fs::rename(&data_path, &backup_path).unwrap();
        fs::create_dir(&data_path).unwrap();

        match fs.native_home_export_for_current_namespace(OsStr::new("native")) {
            Ok(_) => panic!("duplicate native Home authority accepted changed source"),
            Err(error) => assert_eq!(error.code(), afs_error::NODE_OWNER_INVALID_GRANT),
        }
    }

    #[test]
    fn native_home_authority_rejects_private_context_mutations() {
        let (temp, fs, ctx) = fixture();
        super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");

        let mut authority = export(&fs, "native");
        authority.name = OsString::from("other");
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.data_dir = StoragePath::new("other-data-dir").unwrap();
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.namespace = NamespaceIdentity {
            dev: authority.namespace.dev.wrapping_add(1),
            ino: authority.namespace.ino,
        };
        assert!(authority.verify_current(&fs).is_err());

        let mut authority = export(&fs, "native");
        authority.source_identity = DirectoryIdentity {
            dev: authority.source_identity.dev,
            ino: authority.source_identity.ino.wrapping_add(1),
        };
        assert!(authority.verify_current(&fs).is_err());

        let authority = export(&fs, "native");
        let data_path = temp.path().join(authority.data_dir.as_path());
        let backup_path = temp.path().join("native-source-replaced");
        fs::rename(&data_path, &backup_path).unwrap();
        fs::create_dir(&data_path).unwrap();
        assert!(authority.verify_current(&fs).is_err());
    }

    #[test]
    fn native_home_anchor_does_not_authorize_child_open_or_data_after_revoke() {
        let (_temp, fs, ctx) = fixture();
        let root = super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");
        let created = Backend::create(
            &fs,
            &ctx,
            root.inode,
            OsStr::new("file"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
        let authority = export(&fs, "native");
        let root_id = authority.grant.id.clone();

        fs.require_local().unwrap().roots.revoke_root(&root_id);

        Backend::lookup(&fs, &ctx, fs.root_inode(), OsStr::new("native")).unwrap();
        Backend::getattr(&fs, &ctx, root.inode, None).unwrap();
        assert!(Backend::getattr(&fs, &ctx, created.entry.inode, Some(created.handle)).is_err());
        assert!(
            Backend::setattr(
                &fs,
                &ctx,
                created.entry.inode,
                Some(created.handle),
                &AttributeChange {
                    size: Some(0),
                    ..AttributeChange::default()
                },
            )
            .is_err()
        );
        assert!(Backend::lookup(&fs, &ctx, root.inode, OsStr::new("file")).is_err());
        assert!(Backend::open(&fs, &ctx, created.entry.inode, libc::O_RDONLY).is_err());
        assert!(Backend::read(&fs, &ctx, created.handle, 0, &mut [0; 1]).is_err());
        assert!(Backend::write(&fs, &ctx, created.handle, 0, b"denied").is_err());
        assert!(Backend::flush(&fs, &ctx, created.handle).is_err());
        assert!(Backend::fsync(&fs, &ctx, created.handle, SyncMode::Full).is_err());
        assert!(Backend::release(&fs, &ctx, created.handle).is_ok());
    }

    #[test]
    fn native_home_anchor_does_not_authorize_directory_data_after_revoke() {
        let (_temp, fs, ctx) = fixture();
        let root = super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");
        Backend::mkdir(&fs, &ctx, root.inode, OsStr::new("dir"), 0o755).unwrap();
        let dir = Backend::lookup(&fs, &ctx, root.inode, OsStr::new("dir")).unwrap();
        let handle = Backend::opendir(&fs, &ctx, dir.inode).unwrap();
        let authority = export(&fs, "native");
        let root_id = authority.grant.id.clone();

        fs.require_local().unwrap().roots.revoke_root(&root_id);

        assert!(Backend::readdir(&fs, &ctx, handle, 0, 16).is_err());
        assert!(Backend::fsyncdir(&fs, &ctx, handle, SyncMode::Full).is_err());
        assert!(Backend::releasedir(&fs, &ctx, handle).is_ok());
    }

    #[test]
    fn native_home_getattr_inode_none_on_open_unlinked_file_fails_after_revoke() {
        let (_temp, fs, ctx) = fixture();
        let root = super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");
        let created = Backend::create(
            &fs,
            &ctx,
            root.inode,
            OsStr::new("file"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
        Backend::unlink(&fs, &ctx, root.inode, OsStr::new("file")).unwrap();
        let authority = export(&fs, "native");
        let root_id = authority.grant.id.clone();

        fs.require_local().unwrap().roots.revoke_root(&root_id);

        assert!(Backend::getattr(&fs, &ctx, created.entry.inode, None).is_err());
        assert!(Backend::release(&fs, &ctx, created.handle).is_ok());
    }

    #[test]
    fn ordinary_getattr_inode_none_on_open_unlinked_file_keeps_existing_metadata_behavior() {
        let (_temp, fs, ctx, _disk) = super::super::native_home_tests::fixture(false);
        let root = super::super::native_home_tests::mkdir_root(&fs, &ctx, "ordinary");
        let created = Backend::create(
            &fs,
            &ctx,
            root.inode,
            OsStr::new("file"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
        Backend::unlink(&fs, &ctx, root.inode, OsStr::new("file")).unwrap();
        let root_id = root::root_id_from_name(OsStr::new("ordinary")).unwrap();

        fs.require_local().unwrap().roots.revoke_root(&root_id);

        Backend::getattr(&fs, &ctx, created.entry.inode, None).unwrap();
        assert!(Backend::release(&fs, &ctx, created.handle).is_ok());
    }

    #[test]
    fn native_home_anchor_ignores_non_root_parent_and_unknown_inode() {
        let (_temp, fs, ctx) = fixture();
        let root = super::super::native_home_tests::mkdir_root(&fs, &ctx, "native");
        let authority = export(&fs, "native");
        let root_id = authority.grant.id.clone();

        fs.require_local().unwrap().roots.revoke_root(&root_id);

        // `BackendInode` currently carries only the raw value, so a distinct
        // foreign namespace cannot be represented here. These checks cover the
        // meaningful available invalid parent/inode cases in the current type.
        assert!(Backend::lookup(&fs, &ctx, root.inode, OsStr::new("native")).is_err());
        assert!(Backend::getattr(&fs, &ctx, BackendInode { value: u64::MAX }, None).is_err());
    }
}
