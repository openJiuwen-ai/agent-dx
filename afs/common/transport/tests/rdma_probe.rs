#![cfg(feature = "rdma")]

use std::sync::{Mutex, MutexGuard};

use afs_transport::rdma::{MAX_CAPACITY, RdmaEndpoint};

const SMALL_CAPACITY: usize = 64 * 1024;
const REPEATED_CYCLES: usize = 4;

static RDMA_TEST_LOCK: Mutex<()> = Mutex::new(());

fn rdma_test_guard() -> MutexGuard<'static, ()> {
    RDMA_TEST_LOCK.lock().expect("RDMA test mutex poisoned")
}

fn rdma_device() -> Option<String> {
    std::env::var("AFS_TEST_RDMA_DEVICE")
        .ok()
        .filter(|value| !value.is_empty())
}

fn connected_pair(device: &str) -> (RdmaEndpoint, RdmaEndpoint) {
    connected_pair_with_capacity(device, afs_transport::rdma::CAPACITY)
}

fn connected_pair_with_capacity(device: &str, capacity: usize) -> (RdmaEndpoint, RdmaEndpoint) {
    let mut client = RdmaEndpoint::open_with_capacity(device, capacity).expect("client endpoint");
    let mut server = RdmaEndpoint::open_with_capacity(device, capacity).expect("server endpoint");
    let client_info = client.info().expect("client descriptor");
    let server_info = server.info().expect("server descriptor");

    // 真实握手顺序和 Node control 一致：server 先准备 RECV，再向 client 公开
    // server_info。这样 client 的零字节 SEND_WITH_IMM 到达时一定有接收槽。
    server.prepare_probe().expect("server posts probe receive");
    server
        .connect(&client_info)
        .expect("server connects client");
    client
        .connect(&server_info)
        .expect("client connects server");
    (client, server)
}

fn complete_probe(client: &mut RdmaEndpoint, server: &mut RdmaEndpoint) {
    client.send_probe(5000).expect("client probe send");
    server.wait_probe(5000).expect("server probe receive");
}

fn deterministic_payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let index = index as u64;
            let mixed = index
                .wrapping_mul(0x9E37_79B1_85EB_CA87)
                .rotate_left((seed % 63) as u32)
                ^ u64::from(seed);
            (mixed ^ (mixed >> 32) ^ (mixed >> 16)) as u8
        })
        .collect()
}

fn assert_full_read_then_write_round_trip(
    client: &mut RdmaEndpoint,
    server: &mut RdmaEndpoint,
    len: usize,
    seed: u8,
) {
    let client_payload = deterministic_payload(len, seed);
    client
        .put_local(&client_payload)
        .expect("client puts deterministic payload");
    server.transfer_read(len).expect("server reads client MR");
    assert_eq!(
        server.get_local(len).expect("server local bytes"),
        client_payload,
        "RDMA READ must preserve every byte"
    );

    let server_payload = deterministic_payload(len, seed.wrapping_add(91));
    server
        .put_local(&server_payload)
        .expect("server puts deterministic payload");
    server.transfer_write(len).expect("server writes client MR");
    assert_eq!(
        client.get_local(len).expect("client local bytes"),
        server_payload,
        "RDMA WRITE must preserve every byte"
    );
}

#[cfg(target_os = "linux")]
fn process_uverbs_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .filter(|target| target.to_string_lossy().contains("uverbs"))
        .count()
}

#[cfg(not(target_os = "linux"))]
fn process_uverbs_fd_count() -> usize {
    0
}

#[test]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
fn rdma_probe_success_keeps_one_sided_transfers_usable() {
    let _guard = rdma_test_guard();
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (mut client, mut server) = connected_pair(&device);

    complete_probe(&mut client, &mut server);

    client.put_local(b"abcdefgh").expect("client puts bytes");
    server.transfer_read(8).expect("server reads client MR");
    assert_eq!(
        server.get_local(8).expect("server local bytes"),
        b"abcdefgh"
    );

    server.put_local(b"ABCDEFGH").expect("server puts bytes");
    server.transfer_write(8).expect("server writes client MR");
    assert_eq!(
        client.get_local(8).expect("client local bytes"),
        b"ABCDEFGH"
    );
}

#[test]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
fn rdma_probe_wait_without_client_send_times_out_and_poisons_endpoint() {
    let _guard = rdma_test_guard();
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (_client, mut server) = connected_pair(&device);

    server
        .put_local(b"x")
        .expect("local put works before probe timeout");
    assert_eq!(
        server
            .get_local(1)
            .expect("local get works before probe timeout"),
        b"x"
    );

    let error = server
        .wait_probe(25)
        .expect_err("missing client probe must time out");
    assert!(error.to_string().contains("probe receive timeout"));
    server
        .put_local(b"x")
        .expect_err("timed-out probe rejects local put");
    server
        .get_local(1)
        .expect_err("timed-out probe rejects local get");
    assert!(
        server
            .transfer_read(1)
            .expect_err("timed-out probe poisons transfer read reuse")
            .to_string()
            .contains("poisoned")
    );
    assert!(
        server
            .transfer_write(1)
            .expect_err("timed-out probe poisons transfer write reuse")
            .to_string()
            .contains("poisoned")
    );
}

#[test]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
fn rdma_probe_rejects_duplicate_send() {
    let _guard = rdma_test_guard();
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (mut client, _server) = connected_pair(&device);

    client.send_probe(5000).expect("first probe send");
    let error = client
        .send_probe(5000)
        .expect_err("second probe send must be rejected");
    assert!(error.to_string().contains("probe already sent"));
}

#[test]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
fn rdma_probe_transfers_full_max_capacity_payload_in_both_directions() {
    let _guard = rdma_test_guard();
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (mut client, mut server) = connected_pair_with_capacity(&device, MAX_CAPACITY);

    complete_probe(&mut client, &mut server);
    assert_full_read_then_write_round_trip(&mut client, &mut server, MAX_CAPACITY, 0x5a);
}

#[test]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
fn rdma_probe_repeated_create_probe_transfer_drop_cycles_are_bounded() {
    let _guard = rdma_test_guard();
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let baseline_uverbs_fds = process_uverbs_fd_count();

    for cycle in 0..REPEATED_CYCLES {
        let (mut client, mut server) = connected_pair_with_capacity(&device, SMALL_CAPACITY);
        complete_probe(&mut client, &mut server);
        assert_full_read_then_write_round_trip(
            &mut client,
            &mut server,
            SMALL_CAPACITY,
            cycle as u8,
        );
    }

    assert_eq!(
        process_uverbs_fd_count(),
        baseline_uverbs_fds,
        "endpoint drop should close process-owned uverbs FDs; this does not prove CQ/MR/QP provider resources were fully reclaimed"
    );
}
