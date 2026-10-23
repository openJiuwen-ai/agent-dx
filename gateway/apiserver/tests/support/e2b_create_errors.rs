use super::*;

async fn create_response(fixture: &Fixture) -> hyper::Response<Vec<u8>> {
    http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", "k".repeat(32))],
        r#"{"templateID":"base"}"#,
    )
    .await
}

fn assert_created_once(fixture: &Fixture) {
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    let owners = fixture.state.owners.lock().unwrap();
    assert_eq!(owners.len(), 1);
    assert_eq!(
        owners
            .values()
            .next()
            .unwrap()
            .record
            .as_ref()
            .unwrap()
            .state,
        pb::EnvironmentState::Running as i32,
    );
}

#[tokio::test]
async fn unknown_create_result_does_not_invite_a_new_sdk_create() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);

    let response = create_response(&fixture).await;

    assert_created_once(&fixture);
    assert_eq!(response.status(), 500);
    let body: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body["code"], 500);
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("injected missing create response"));
}

#[tokio::test]
async fn control_read_failure_after_create_does_not_invite_a_new_sdk_create() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .control_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);

    let response = create_response(&fixture).await;

    assert_created_once(&fixture);
    assert_eq!(response.status(), 500);
    let body: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body["code"], 500);
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("injected control read failure"));
}

#[tokio::test]
async fn create_deadline_still_returns_gateway_timeout() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .response_error
        .store(tonic::Code::DeadlineExceeded as usize, Ordering::SeqCst);

    let response = create_response(&fixture).await;

    assert_created_once(&fixture);
    assert_eq!(response.status(), 504);
}

#[tokio::test]
async fn authentication_unavailable_before_create_still_returns_service_unavailable() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .auth_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    let response = create_response(&fixture).await;

    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    assert_eq!(response.status(), 503);
}

#[tokio::test]
async fn sandbox_query_unavailable_still_returns_service_unavailable() {
    let fixture = Fixture::new().await;
    let response = create_response(&fixture).await;
    assert_eq!(response.status(), 201);
    let body: Value = serde_json::from_slice(response.body()).unwrap();
    let id = body["sandboxID"].as_str().unwrap();
    fixture
        .state
        .control_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);

    let response = http::request(
        fixture.api.clone(),
        "GET",
        &format!("/sandboxes/{id}"),
        &[("X-API-Key", "k".repeat(32))],
        "",
    )
    .await;

    assert_eq!(response.status(), 503);
}
