//! Cause-and-effect switches for measurements: `AFS_DIAG_GC_COLLECTION_OFF=1`
//! stops collection requests (releases still flow) and `AFS_DIAG_REPAIR_OFF=1`
//! stops the repair loop. `AFS_DIAG_READER_PIN_OFF=1` opens read-only handles
//! without a GC reference: unsafe (an unlinked open file can be collected), it
//! only bounds what deferring reader references could gain (D-S3-2).
//! Compiled only with the `diagnostics` feature; a product build has neither the
//! code nor the variables.

fn enabled(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| value == "1")
}

pub(super) fn gc_collection_off() -> bool {
    enabled("AFS_DIAG_GC_COLLECTION_OFF")
}

pub(super) fn repair_loop_off() -> bool {
    enabled("AFS_DIAG_REPAIR_OFF")
}

pub(crate) fn reader_pin_off() -> bool {
    enabled("AFS_DIAG_READER_PIN_OFF")
}
