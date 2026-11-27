//! Loading and ABI checks for the optional HIXL native module.
use std::ffi::{c_char, c_void};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::OnceLock;

use libloading::os::unix::{Library, RTLD_LOCAL, RTLD_NOW};

use super::{HIXL_UNSUPPORTED, NativeCallError};

const MODULE_NAME: &str = "libafs_hixl.so";
const ABI_VERSION: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Window {
    pub slot_id: u64,
    pub generation: u64,
    pub address: u64,
    pub length: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BatchWindow {
    pub slot_id: u64,
    pub remote_address: u64,
    pub length: u64,
}

// Keep each symbol name with its signature. These declarations mirror native/hixl.h.
macro_rules! bridge_api {
    ($(fn $name:ident($($arg:ident: $ty:ty),* $(,)?) $(-> $ret:ty)?;)*) => {
        pub(super) struct Api {
            $(pub(super) $name: unsafe extern "C" fn($($arg: $ty),*) $(-> $ret)?,)*
        }

        impl Api {
            fn resolve(library: &Library) -> Result<Self, libloading::Error> {
                // SAFETY: the module is supplied by the deployment, and its
                // ABI version was checked. The loader retains it for process life.
                unsafe {
                    Ok(Self { $($name: *library.get(stringify!($name).as_bytes())?,)* })
                }
            }
        }
    };
}

#[repr(C)]
pub(super) struct FabricOptions {
    enable_aicpu_unfold: u32,
    task_streams: u32,
    max_capacity_tb: u32,
    start_address_tb: u32,
    max_transfer_count_per_batch: u32,
}

impl From<super::FabricMemOptions> for FabricOptions {
    fn from(options: super::FabricMemOptions) -> Self {
        let encode = |value: Option<u16>| value.map_or(u32::MAX, u32::from);
        Self {
            enable_aicpu_unfold: options.unfold.map_or(u32::MAX, |mode| {
                u32::from(mode == super::FabricMemUnfold::Aicpu)
            }),
            task_streams: encode(options.task_streams),
            max_capacity_tb: encode(options.max_capacity_tb),
            start_address_tb: encode(options.start_address_tb),
            max_transfer_count_per_batch: encode(options.max_transfer_count_per_batch),
        }
    }
}

bridge_api! {
    fn afs_hixl_start(
        device_id: i32,
        engine_id: *const c_char,
        device_listen_port: u32,
        window_bytes: usize,
        receive_slots: usize,
        export_slots: usize,
        transport: u32,
        buffer_pool_count: usize,
        buffer_pool_size_mib: usize,
        fabric_options: *const FabricOptions,
        out_hccs_domain_id: *mut u64,
        out: *mut *mut c_void,
        error: *mut c_char,
        error_len: usize,
    ) -> u32;
    fn afs_hixl_connect(
        runtime: *mut c_void,
        peer: *const c_char,
        timeout_ms: u32,
        error: *mut c_char,
        error_len: usize,
    ) -> u32;
    fn afs_hixl_reserve_export(
        runtime: *mut c_void,
        slot_id: u64,
        generation: u64,
        len: usize,
        out: *mut Window,
        error: *mut c_char,
        error_len: usize,
    ) -> u32;
    fn afs_hixl_release_export(
        runtime: *mut c_void,
        slot_id: u64,
        generation: u64,
        error: *mut c_char,
        error_len: usize,
    ) -> u32;
    fn afs_hixl_read_sync_batch(
        runtime: *mut c_void,
        peer: *const c_char,
        windows: *const BatchWindow,
        count: usize,
        slot_addresses: *mut u64,
        timeout_ms: u32,
        dispatch_started: *mut u32,
        sdk_transfer_ns: *mut u64,
        error: *mut c_char,
        error_len: usize,
    ) -> u32;
    fn afs_hixl_write_batch(
        runtime: *mut c_void,
        peer: *const c_char,
        windows: *const BatchWindow,
        count: usize,
        bytes: *const u8,
        len: usize,
        timeout_ms: u32,
        dispatch_started: *mut u32,
        error: *mut c_char,
        error_len: usize,
    ) -> u32;
    fn afs_hixl_release_receives(
        runtime: *mut c_void,
        slot_ids: *const u64,
        count: usize,
        error: *mut c_char,
        error_len: usize,
    ) -> u32;
    fn afs_hixl_shutdown(runtime: *mut c_void, error: *mut c_char, error_len: usize)
    -> u32;
    fn afs_hixl_destroy(runtime: *mut c_void);
}

struct LoadedModule {
    // Never unload: rejected startup or shutdown can retain native runtimes,
    // and CANN may still run C/C++ exit handlers after the Node stops.
    _library: Library,
    api: Result<Api, NativeCallError>,
}

static MODULE: OnceLock<Result<LoadedModule, NativeCallError>> = OnceLock::new();

pub(super) fn api() -> Result<&'static Api, NativeCallError> {
    MODULE
        .get_or_init(|| load(module_path()?))
        .as_ref()
        .and_then(|module| module.api.as_ref())
        .map_err(Clone::clone)
}

fn module_path() -> Result<PathBuf, NativeCallError> {
    if let Some(path) = std::env::var_os("AFS_HIXL_MODULE") {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(load_error(
                "AFS_HIXL_MODULE must be an absolute path",
                false,
            ));
        }
        return Ok(path);
    }
    std::env::current_exe()
        .map(|path| path.with_file_name(MODULE_NAME))
        .map_err(|error| load_error(format!("cannot locate HIXL module: {error}"), false))
}

fn load(path: PathBuf) -> Result<LoadedModule, NativeCallError> {
    let label = path.display();
    let metadata = path.metadata().map_err(|error| {
        load_error(
            format!("HIXL module {label}: {error}"),
            error.kind() == ErrorKind::NotFound,
        )
    })?;
    if !metadata.is_file() {
        return Err(load_error(
            format!("HIXL module {label} is not a file"),
            false,
        ));
    }
    // SAFETY: the absolute path selects deployment-provided code. Resolve every
    // relocation before returning an API or starting any HIXL runtime.
    let library =
        unsafe { Library::open(Some(&path), RTLD_NOW | RTLD_LOCAL) }.map_err(|error| {
            let message = error.to_string();
            // glibc does not return a separate errno for a missing ELF dependency.
            // A missing file is unsupported; corrupt ELF, undefined symbols, and
            // version mismatches are deployment errors even in auto mode.
            let missing =
                message.ends_with(": cannot open shared object file: No such file or directory");
            load_error(
                format!("cannot load HIXL module {label}: {message}"),
                missing,
            )
        })?;
    let api = resolve(&library)
        .map_err(|message| load_error(format!("HIXL module {label}: {message}"), false));
    Ok(LoadedModule {
        _library: library,
        api,
    })
}

fn resolve(library: &Library) -> Result<Api, String> {
    // SAFETY: the deployment module must expose the stable ABI probe with this
    // signature. Check its result before interpreting any operational symbols.
    let version_probe =
        unsafe { library.get::<unsafe extern "C" fn() -> u32>(b"afs_hixl_abi_version") }
            .map_err(|error| format!("missing ABI version probe: {error}"))?;
    // SAFETY: calling the probe does not initialize the transport or hardware.
    let version = unsafe { version_probe() };
    if version != ABI_VERSION {
        return Err(format!(
            "ABI version {version} is incompatible with required version {ABI_VERSION}"
        ));
    }
    Api::resolve(library).map_err(|error| format!("incomplete ABI {ABI_VERSION}: {error}"))
}

fn load_error(message: impl Into<String>, unavailable: bool) -> NativeCallError {
    NativeCallError {
        status: unavailable.then_some(HIXL_UNSUPPORTED),
        message: message.into(),
        accepted: false,
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    #[test]
    fn fabric_wire_options_preserve_explicit_zero_and_omit_unset_fields() {
        let options = FabricOptions::from(super::super::FabricMemOptions {
            unfold: Some(super::super::FabricMemUnfold::Host),
            start_address_tb: Some(0),
            ..Default::default()
        });
        assert_eq!(options.enable_aicpu_unfold, 0);
        assert_eq!(options.start_address_tb, 0);
        assert_eq!(options.task_streams, u32::MAX);
        assert_eq!(options.max_capacity_tb, u32::MAX);
        assert_eq!(options.max_transfer_count_per_batch, u32::MAX);
    }

    #[test]
    fn only_missing_module_or_dependency_is_unsupported() {
        let temp = tempfile::tempdir().unwrap();
        let module = temp.path().join(MODULE_NAME);
        let missing = load(module.clone()).err().unwrap();
        assert_eq!(missing.status, Some(HIXL_UNSUPPORTED));
        assert!(missing.message.contains(MODULE_NAME));

        fs::write(&module, "not a shared library").unwrap();
        let invalid = load(module.clone()).err().unwrap();
        assert_eq!(invalid.status, None);

        let wrong_abi = temp.path().join("wrong-abi.so");
        compile(
            &wrong_abi,
            "unsigned afs_hixl_abi_version(void) { return 2; }",
            &[],
        );
        let incompatible = load(wrong_abi).unwrap_or_else(|error| panic!("{}", error.message));
        let error = incompatible.api.err().unwrap();
        assert_eq!(error.status, None);
        assert!(
            error
                .message
                .contains("ABI version 2 is incompatible with required version 3")
        );

        let library = temp.path().join("libafs_test_hixl_dependency.so");
        compile(&library, "unsigned dependency(void) { return 1; }", &[]);
        compile(
            &module,
            concat!(
                "extern unsigned dependency(void);\n",
                "unsigned afs_hixl_abi_version(void) { return dependency(); }",
            ),
            &[library.as_os_str()],
        );
        fs::remove_file(library).unwrap();
        let missing_dependency = load(module).err().unwrap();
        assert_eq!(missing_dependency.status, Some(HIXL_UNSUPPORTED));
        assert!(
            missing_dependency
                .message
                .contains("libafs_test_hixl_dependency.so")
        );
    }

    fn compile(output: &Path, source: &str, link: &[&std::ffi::OsStr]) {
        let source_path = output.with_extension("c");
        fs::write(&source_path, source).unwrap();
        let result = Command::new("cc")
            .args(["-shared", "-fPIC", "-Wl,-z,defs"])
            .arg(source_path)
            .args(link)
            .arg("-o")
            .arg(output)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}
