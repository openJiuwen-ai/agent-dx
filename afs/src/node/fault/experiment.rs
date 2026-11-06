//! Cause-and-effect switches for measurements: `AFS_DIAG_GC_COLLECTION_OFF=1`
//! stops collection requests (releases still flow) and `AFS_DIAG_REPAIR_OFF=1`
//! stops the repair loop. Compiled only with the `diagnostics` feature; a
//! product build has neither the code nor the variables.

fn enabled(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| value == "1")
}

pub(super) fn gc_collection_off() -> bool {
    enabled("AFS_DIAG_GC_COLLECTION_OFF")
}

pub(super) fn repair_loop_off() -> bool {
    enabled("AFS_DIAG_REPAIR_OFF")
}
