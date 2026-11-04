//! Golden bytes of the local Meta log containers: WAL frame, WAL segment
//! static header, tail slot, IDENTITY, CURRENT and checkpoint file header.
//!
//! Every value is built from fixed identities, hashes and a fixed
//! create record. A changed byte is a durable format change: bump the owning version
//! (Envelope `VERSION`, `AFSW`/`AFSI`/`AFSC` header versions, `AFSM` CURRENT
//! version) and regenerate with
//! `cargo test -p afs --lib local_log::format_golden_tests::print_local_log_goldens -- --ignored --nocapture`.

//!
//! S1d-5 changed exactly one golden on purpose: CURRENT moved from v2 to v3 to
//! carry the writer term (`AFSM\0\x03`, a `term` varint after the identity).
//! The frame, segment header, tail slot, IDENTITY and checkpoint goldens are
//! unchanged; the frame golden keeps term 0. The old v2 bytes stay below as
//! `CURRENT_V2` and must be refused.

use super::super::journal::encode_frame;
use super::super::log::format_golden_tests::golden_record;
use super::*;

fn identity() -> StoreIdentity {
    StoreIdentity {
        store_id: [1; 16],
        cluster_id: [2; 16],
        incarnation: [3; 16],
    }
}

fn first_frame() -> (Vec<u8>, LogTip) {
    // Term 0 keeps the frame golden byte-identical to its pre-term encoding:
    // the envelope layout did not change, only the value written there.
    encode_frame(
        identity(),
        LogTip::EMPTY.at(0),
        golden_record(),
        1024 * 1024,
    )
    .unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn cases() -> Vec<(&'static str, Vec<u8>)> {
    let (frame, tip) = first_frame();
    let tail = Tail {
        generation: 3,
        tip,
        end: LOG_HEADER_BYTES + frame.len() as u64,
    };
    let checkpoint = CheckpointRef {
        covered: tip,
        revision: StoreRevision(1),
        bytes: 18,
        digest: [8; 32],
    };
    let current = Current {
        identity: identity(),
        term: 7,
        current: Some(checkpoint),
        previous: None,
        segments: vec![
            SegmentRef {
                base: LogTip::EMPTY,
                base_revision: StoreRevision(0),
                sealed: Some((tail, StoreRevision(1))),
            },
            SegmentRef {
                base: tip,
                base_revision: StoreRevision(1),
                sealed: None,
            },
        ],
        retired_segments: Vec::new(),
        retired_images: Vec::new(),
    };
    let (image_header, image_ref) =
        image_header(identity(), tip, StoreRevision(1), b"checkpoint payload");
    vec![
        ("wal_frame.create", frame),
        (
            "wal_segment_header.empty_base",
            static_header(identity(), LogTip::EMPTY).to_vec(),
        ),
        (
            "wal_segment_header.after_first_record",
            static_header(identity(), tip).to_vec(),
        ),
        ("wal_tail_slot", tail_bytes(tail).to_vec()),
        ("identity", identity_bytes(identity())),
        ("current_v3", encode_current(&current).unwrap()),
        ("checkpoint_header", image_header.to_vec()),
        ("checkpoint_name", image_name(image_ref).into_bytes()),
    ]
}

#[test]
fn local_log_containers_match_golden() {
    let actual = cases();
    let names = actual.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    let golden = GOLDEN.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    assert_eq!(names, golden, "golden table must list every case");
    for ((name, bytes), (_, expected)) in actual.iter().zip(GOLDEN) {
        assert_eq!(
            hex(bytes),
            *expected,
            "{name} bytes changed: this is a durable format change; bump the \
             owning format version and regenerate with print_local_log_goldens"
        );
    }
}

#[test]
fn golden_containers_decode_with_current_readers() {
    let (frame, tip) = first_frame();
    let (record, decoded_tip, term) =
        decode_frame(identity(), LogTip::EMPTY, &frame, 1024 * 1024).unwrap();
    assert_eq!((decoded_tip, term), (tip, 0));
    assert_eq!(
        postcard::to_allocvec(&record).unwrap(),
        postcard::to_allocvec(&golden_record()).unwrap()
    );
    let tail = Tail {
        generation: 3,
        tip,
        end: LOG_HEADER_BYTES + frame.len() as u64,
    };
    assert_eq!(parse_tail(&tail_bytes(tail)).unwrap(), Some(tail));
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join(IDENTITY), identity_bytes(identity())).unwrap();
    assert_eq!(read_identity(temp.path()).unwrap(), identity());
}

/// The last v2 CURRENT golden (before S1d-5). A v2 manifest has no writer
/// term, so it is refused with a rebuild instruction, never adopted.
const CURRENT_V2: &str = "4146534d0002010101010101010101010101010101010202020202020202020202020202020203030303030303030303030303030303010139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b0112080808080808080808080808080808080808080808080808080808080808080800020000000000000000000000000000000000000000000000000000000000000000000001030139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7bf021010139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b01000000c935e98cc216b3c866068f2cde17e74efb5ab4271e6805c522e35057b1f3298bff8f509d";

#[test]
fn current_v2_golden_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let bytes = (0..CURRENT_V2.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&CURRENT_V2[index..index + 2], 16).unwrap())
        .collect::<Vec<_>>();
    std::fs::write(temp.path().join(CURRENT), bytes).unwrap();
    let error = read_current(temp.path(), identity()).unwrap_err();
    assert_eq!(error.code(), afs_error::STORAGE_FORMAT_INCOMPATIBLE);
    assert!(error.to_string().contains("version 2"), "{error}");
}

#[test]
#[ignore = "prints regenerated goldens"]
fn print_local_log_goldens() {
    for (name, bytes) in cases() {
        println!("    ({name:?}, {:?}),", hex(&bytes));
    }
}

const GOLDEN: &[(&str, &str)] = &[
    (
        "wal_frame.create",
        "000000e811a15ed40201010101010101010101010101010101020202020202020202020202020202020303030303030303030303030303030301000000000000000000000000000000000000000000000000000000000000000000000001016101720a0001b96000020006016e01700200ff01690007016e016900a403010201030405010176000104303066660200ff06020009016e01700200ff00000a01690101016101720a010505050505050505050505050505050505050505050505050505050505050505060100010161010039f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b",
    ),
    (
        "wal_segment_header.empty_base",
        "4146535700010101010101010101010101010101010102020202020202020202020202020202030303030303030303030303030303030000000000000000000000000000000000000000000000000000000000000000000000000000000002a7bc532424e5b4f58ef232beffad7c6b5825580568917d0d8a7ab0ab68347740bb1614000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "wal_segment_header.after_first_record",
        "414653570001010101010101010101010101010101010202020202020202020202020202020203030303030303030303030303030303000000000000000139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b0f3420b24cd814be873ef2ada44e04c4fc1589995406ed0964c10ea53f3fadad500d1004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "wal_tail_slot",
        "0000000000000003000000000000000100000000000010f039f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b0000000083576cc9",
    ),
    (
        "identity",
        "414653490001010101010101010101010101010101010202020202020202020202020202020203030303030303030303030303030303d79b9f8d3a7dc4b4b022ed798f60ffee6f3bac8e3e147d74faf2491e814713351155613f",
    ),
    (
        "current_v3",
        "4146534d000301010101010101010101010101010101020202020202020202020202020202020303030303030303030303030303030307010139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b0112080808080808080808080808080808080808080808080808080808080808080800020000000000000000000000000000000000000000000000000000000000000000000001030139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7bf021010139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b010000000684004eb612935b9a681d8ceef2f51603e26a1fd7e5300e0ccfee308b699afb008e8fca",
    ),
    (
        "checkpoint_header",
        "414653430001010101010101010101010101010101010202020202020202020202020202020203030303030303030303030303030303000000000000000139f40095025a4252e31c4612b6f4f936a35070a904527fdfe3c984d957d4fc7b000000000000000100000000000000129487ada3d989fb79095f54c0b5d86882277885938fbb2e9926d8a25b6ba7e67de2b5daf0",
    ),
    (
        "checkpoint_name",
        "636865636b706f696e742d30303030303030303030303030303030303030312d393438376164613364393839666237393039356635346330623564383638383232373738383539333866626232653939323664386132356236626137653637642e62696e",
    ),
];
