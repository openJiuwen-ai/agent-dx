use afs_transport::hixl::{
    EngineInfo, ExportWindow, HixlErrorKind, HixlPhase, ReadTransport, RuntimeOptions, RuntimeState,
};

#[test]
fn hixl_public_contract_is_available_without_native_feature() {
    let options = RuntimeOptions::default();
    assert_eq!(options.transport, ReadTransport::Roce);
    assert!(!options.buffer_pool);
    assert_eq!(ReadTransport::Hccs.to_string(), "hccs");
    assert_eq!(ReadTransport::HccsFabric.to_string(), "hccs");
    assert!(ReadTransport::HccsFabric.is_hccs());
    assert!(ReadTransport::HccsFabric.is_fabric_mem());
    assert!(!ReadTransport::Hccs.is_fabric_mem());
    assert!(!ReadTransport::Roce.is_hccs());
    assert_eq!(options.window_bytes, 4 * 1024 * 1024);
    assert_eq!(options.receive_slots, 256);
    assert_eq!(options.export_slots, 256);
    let info = EngineInfo {
        engine_id: options.engine_id.clone(),
        generation: 1,
        window_bytes: options.window_bytes,
        transport: options.transport,
        buffer_pool_count: 0,
        buffer_pool_size_mib: 0,
        hccs_domain_id: None,
    };
    let window = ExportWindow {
        slot_id: 0,
        generation: 1,
        address: 0,
        length: 0,
    };
    assert_eq!(info.engine_id, options.engine_id);
    assert_eq!(window.generation, 1);
    assert_eq!(RuntimeState::Stopped.to_string(), "stopped");
    assert_eq!(HixlPhase::Start.to_string(), "start");
    assert_eq!(HixlPhase::CompleteRead.to_string(), "complete_read");
    assert_eq!(HixlErrorKind::Unsupported.to_string(), "unsupported");
}

#[test]
#[cfg(not(feature = "hixl"))]
fn start_reports_unsupported_when_native_feature_is_disabled() {
    let error = match afs_transport::hixl::HixlRuntime::start(RuntimeOptions::default()) {
        Ok(_) => panic!("disabled feature must reject runtime start"),
        Err(error) => error,
    };
    assert_eq!(error.kind, HixlErrorKind::Unsupported);
    assert_eq!(error.phase, HixlPhase::Start);
}
