use super::{
    download_http::{HttpError, ResponsePlan},
    download_token::{CheckedDownload, DownloadVerifier, FileMetadata},
};
use http::{HeaderMap, Method, StatusCode};
use serde_json::{json, Value};

fn checked(kind: &str) -> CheckedDownload {
    checked_size(kind, 123)
}
fn checked_size(kind: &str, size: u64) -> CheckedDownload {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/jiuwen_download_tokens.json"
    ))
    .unwrap();
    use base64::Engine;
    let mut payload = fixture["vectors"][kind]["payload"].clone();
    if kind == "verified" {
        payload["size"] = json!(size);
    }
    let encoded =
        base64::engine::general_purpose::URL_SAFE.encode(serde_json::to_vec(&payload).unwrap());
    let signature = ring::hmac::sign(
        &ring::hmac::Key::new(
            ring::hmac::HMAC_SHA256,
            fixture["secret"].as_str().unwrap().as_bytes(),
        ),
        encoded.as_bytes(),
    );
    let signed = DownloadVerifier::new(fixture["secret"].as_str().unwrap())
        .unwrap()
        .verify(
            &format!("{encoded}.{}", hex::encode(signature)),
            100.0,
            None,
        )
        .unwrap();
    let registration = json!({
        "state":"committed", "asset_id":"ab".repeat(16),
        "sealed_path":"/assets/abababababababababababababababab.pdf",
        "expires_at":160.5, "size_bytes":size,
        "content_digest":format!("sha256:{}", "12".repeat(32))
    })
    .to_string();
    signed
        .check_file(
            "/assets",
            Some(registration.as_bytes()),
            FileMetadata {
                regular_file: true,
                size,
            },
            100.0,
        )
        .unwrap()
}
fn headers(range: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("range", range.parse().unwrap());
    headers
}
#[test]
fn verified_range_head_and_inline_match_the_existing_wire_contract() {
    let file = checked("verified");
    for (range, start, length) in [
        ("bytes=1-3", 1, 3),
        ("bytes=120-", 120, 3),
        ("bytes=-3", 120, 3),
        ("bytes=0-999", 0, 123),
        ("bytes=-999", 0, 123),
        ("bytes=0-99999999999999999999999999", 0, 123),
        ("bytes=-99999999999999999999999999", 0, 123),
    ] {
        let plan = ResponsePlan::new(&file, &Method::GET, &headers(range), true).unwrap();
        assert_eq!(plan.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!((plan.start, plan.length), (start, length));
        assert_eq!(
            plan.headers["content-range"],
            format!("bytes {start}-{}/123", start + length - 1)
        );
        assert_eq!(plan.headers["content-length"], length.to_string());
        assert_eq!(plan.headers["content-type"], "application/pdf");
        assert_eq!(
            plan.headers["content-disposition"],
            "inline; filename*=UTF-8''%E6%8A%A5%E5%91%8A.pdf"
        );
        assert_eq!(plan.headers["cache-control"], "no-store");
        assert_eq!(plan.headers["accept-ranges"], "bytes");
        assert!(!plan.empty_body);
    }
    let head = ResponsePlan::new(&file, &Method::HEAD, &headers("bytes=2-4"), false).unwrap();
    assert!(head.empty_body);
    assert_eq!(head.length, 3);
    assert_eq!(head.status, StatusCode::PARTIAL_CONTENT);
    assert!(head.headers["content-disposition"]
        .to_str()
        .unwrap()
        .starts_with("attachment;"));
}
#[test]
fn invalid_or_unsatisfiable_verified_ranges_are_not_forwarded() {
    let file = checked("verified");
    for range in [
        "bytes=123-",
        "bytes=4-2",
        "bytes=-0",
        "bytes=",
        "items=0-2",
        "bytes=1-2,4-5",
        "bytes=x-3",
    ] {
        assert!(
            matches!(
                ResponsePlan::new(&file, &Method::GET, &headers(range), false),
                Err(HttpError::Range(123))
            ),
            "{range}"
        );
    }
    let mut multiple = headers("bytes=1-2");
    multiple.append("range", "bytes=3-4".parse().unwrap());
    assert!(matches!(
        ResponsePlan::new(&file, &Method::GET, &multiple, false),
        Err(HttpError::Range(123))
    ));
    assert!(matches!(
        ResponsePlan::new(&file, &Method::POST, &HeaderMap::new(), false),
        Err(HttpError::Method)
    ));
}
#[test]
fn ordinary_downloads_ignore_range_and_inline_as_before() {
    let plan = ResponsePlan::new(
        &checked("ordinary"),
        &Method::GET,
        &headers("bytes=1-3"),
        true,
    )
    .unwrap();
    assert_eq!(plan.status, StatusCode::OK);
    assert_eq!((plan.start, plan.length), (0, 123));
    assert!(!plan.headers.contains_key("accept-ranges"));
    assert!(!plan.headers.contains_key("content-range"));
    assert_eq!(plan.headers["content-type"], "text/plain; charset=utf-8");
    assert!(plan.headers["content-disposition"]
        .to_str()
        .unwrap()
        .starts_with("attachment;"));
}
#[test]
fn empty_files_have_no_body_and_verified_ranges_are_unsatisfiable() {
    for kind in ["ordinary", "verified"] {
        let file = checked_size(kind, 0);
        let plan = ResponsePlan::new(&file, &Method::GET, &HeaderMap::new(), false).unwrap();
        assert!(plan.empty_body);
        assert_eq!(plan.headers["content-length"], "0");
        assert_eq!(plan.status, StatusCode::OK);
    }
    assert!(matches!(
        ResponsePlan::new(
            &checked_size("verified", 0),
            &Method::GET,
            &headers("bytes=0-"),
            false
        ),
        Err(HttpError::Range(0))
    ));
}
