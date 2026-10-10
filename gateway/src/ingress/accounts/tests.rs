use super::*;
use http::{HeaderMap, HeaderValue};

#[test]
fn bearer_credentials_are_unambiguous_and_not_query_parameters() {
    let mut headers = HeaderMap::new();
    assert!(bearer(&headers).is_err());
    headers.insert("authorization", HeaderValue::from_static("Bearer abc"));
    assert_eq!(bearer(&headers).unwrap(), "abc");
    headers.append("authorization", HeaderValue::from_static("Bearer def"));
    assert!(bearer(&headers).is_err());
    headers.remove("authorization");
    headers.insert("authorization", HeaderValue::from_static("Basic abc"));
    assert!(bearer(&headers).is_err());
}

#[test]
fn login_request_does_not_accept_client_identity_or_secrets() {
    let request: LoginRequest =
        serde_json::from_str(r#"{"authorizationCode":"code","agreementVersion":"1"}"#).unwrap();
    assert_eq!(request.authorization_code, "code");
    for field in ["userId", "clientSecret", "clientId", "apiKey", "tenant"] {
        let value = serde_json::json!({"authorizationCode":"code", field:"forged"});
        assert!(serde_json::from_value::<LoginRequest>(value).is_err());
    }
}
