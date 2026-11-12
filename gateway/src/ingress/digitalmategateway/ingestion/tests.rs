use super::super::composio::{WebhookError, WebhookVerifier};
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use http::{HeaderMap, HeaderValue};
use ring::hmac;
use serde_json::json;

pub(super) const SECRET: &[u8] = b"controlled-webhook-secret";
pub(super) const NOW: u64 = 2_000_000_000;

#[test]
fn python_standard_library_hmac_vector_is_accepted() {
    // Frozen independently with Python hmac/hashlib/base64, not the Rust helper.
    let raw = br#"{"id":"msg_one","type":"composio.trigger.message","metadata":{"trigger_id":"ti_one","trigger_slug":"GMAIL_NEW_GMAIL_MESSAGE","connected_account_id":"ca_one","auth_config_id":"ac_custom","user_id":"dm_user"},"data":{}}"#;
    let mut headers = HeaderMap::new();
    headers.insert("webhook-id", HeaderValue::from_static("wh_one"));
    headers.insert("webhook-timestamp", HeaderValue::from_static("2000000000"));
    headers.insert(
        "webhook-signature",
        HeaderValue::from_static("v1,PuRH6Z49UkvwiHGNp2a++9+pScZVucqghSXFaeTzLGk="),
    );
    assert!(WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers, raw, NOW)
        .is_ok());
}

pub(super) fn body(id: &str, text: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"id":id,"type":"composio.trigger.message","metadata":{
        "trigger_id":"ti_one","trigger_slug":"GMAIL_NEW_GMAIL_MESSAGE","connected_account_id":"ca_one",
        "auth_config_id":"ac_custom","user_id":"dm_user"},"data":{"text":text}})).unwrap()
}

pub(super) fn headers(raw: &[u8], seconds: u64, key: &[u8]) -> HeaderMap {
    let timestamp = seconds.to_string();
    let mut message = format!("wh_one.{timestamp}.").into_bytes();
    message.extend_from_slice(raw);
    let signature =
        STANDARD.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), &message).as_ref());
    let mut headers = HeaderMap::new();
    headers.insert("webhook-id", HeaderValue::from_static("wh_one"));
    headers.insert("webhook-timestamp", timestamp.parse().unwrap());
    headers.insert(
        "webhook-signature",
        format!("v1,{signature}").parse().unwrap(),
    );
    headers
}

pub(super) fn binding() -> SubscriptionBinding {
    SubscriptionBinding {
        tenant: "tenant_one".into(),
        user_id: "user_one".into(),
        connection_id: "connection_one".into(),
        template: "digitalmate".into(),
        template_ref: "1".into(),
        subscription_id: "sub_one".into(),
        subscription_generation: 1,
        connection_generation: 3,
        workspace: "workspace_one".into(),
        binding_id: "binding_one".into(),
        binding_generation: "binding_epoch".into(),
        service_port: 1234,
        trigger_id: "ti_one".into(),
        trigger_slug: "GMAIL_NEW_GMAIL_MESSAGE".into(),
        account_id: "ca_one".into(),
        auth_config_id: "ac_custom".into(),
        composio_user_id: "dm_user".into(),
        enabled: true,
    }
}

fn event(id: &str, text: &str, binding: &SubscriptionBinding) -> PendingInput {
    let raw = body(id, text);
    let verifier = WebhookVerifier::new(&[SECRET]).unwrap();
    let verified = verifier
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    PendingInput::bind(verified, binding).unwrap()
}

#[test]
fn verifies_original_bytes_and_retains_event_identity_without_parsing_mail() {
    let raw = body("msg_one", "private mail");
    let verified = WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    assert_eq!(verified.raw_body(), raw);
    assert_eq!(verified.source_event_id(), "msg_one");
    assert_eq!(verified.digest(), hex::encode(Sha256::digest(&raw)));
}

#[test]
fn changed_payload_and_wrong_secret_are_rejected() {
    let raw = body("msg_one", "private mail");
    let verifier = WebhookVerifier::new(&[SECRET]).unwrap();
    assert_eq!(
        verifier
            .verify(
                &headers(&raw, NOW, SECRET),
                &body("msg_one", "changed"),
                NOW
            )
            .err(),
        Some(WebhookError::Signature)
    );
    assert_eq!(
        verifier
            .verify(&headers(&raw, NOW, b"wrong"), &raw, NOW)
            .err(),
        Some(WebhookError::Signature)
    );
}

#[test]
fn timestamp_window_refuses_replay_and_future_requests() {
    let raw = body("msg_one", "mail");
    let verifier = WebhookVerifier::new(&[SECRET]).unwrap();
    for time in [NOW - 301, NOW + 301] {
        assert_eq!(
            verifier
                .verify(&headers(&raw, time, SECRET), &raw, NOW)
                .err(),
            Some(WebhookError::Timestamp)
        );
    }
    assert!(verifier
        .verify(&headers(&raw, NOW - 300, SECRET), &raw, NOW)
        .is_ok());
}

#[test]
fn duplicate_missing_and_unversioned_headers_fail_closed() {
    let raw = body("msg_one", "mail");
    let verifier = WebhookVerifier::new(&[SECRET]).unwrap();
    let mut duplicate = headers(&raw, NOW, SECRET);
    duplicate.append("webhook-id", HeaderValue::from_static("wh_two"));
    assert_eq!(
        verifier.verify(&duplicate, &raw, NOW).err(),
        Some(WebhookError::InvalidHeaders)
    );
    let mut missing = headers(&raw, NOW, SECRET);
    missing.remove("webhook-timestamp");
    assert_eq!(
        verifier.verify(&missing, &raw, NOW).err(),
        Some(WebhookError::InvalidHeaders)
    );
    let mut old = headers(&raw, NOW, SECRET);
    old.insert("webhook-signature", HeaderValue::from_static("unversioned"));
    assert_eq!(
        verifier.verify(&old, &raw, NOW).err(),
        Some(WebhookError::Signature)
    );
}

#[test]
fn rotation_accepts_current_or_previous_trusted_secret() {
    let raw = body("msg_one", "mail");
    let verifier = WebhookVerifier::new(&[b"current", SECRET]).unwrap();
    assert!(verifier
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .is_ok());
    assert!(verifier
        .verify(&headers(&raw, NOW, b"current"), &raw, NOW)
        .is_ok());
    assert!(WebhookVerifier::new(&[]).is_err());
}

#[test]
fn unsupported_project_events_and_oversized_payloads_are_not_trigger_inputs() {
    let raw = br#"{"id":"msg_one","type":"composio.connected_account.expired","data":{}}"#;
    let verifier = WebhookVerifier::new(&[SECRET]).unwrap();
    assert_eq!(
        verifier.verify(&headers(raw, NOW, SECRET), raw, NOW).err(),
        Some(WebhookError::Envelope)
    );
    assert_eq!(
        verifier
            .verify(&HeaderMap::new(), &vec![b'x'; 1_048_577], NOW)
            .err(),
        Some(WebhookError::TooLarge)
    );
}

#[test]
fn signed_other_user_or_account_is_not_our_subscription() {
    for property in [
        "composio_user_id",
        "account_id",
        "auth_config_id",
        "trigger_id",
        "trigger_slug",
    ] {
        let mut current = binding();
        match property {
            "composio_user_id" => current.composio_user_id = "other".into(),
            "account_id" => current.account_id = "ca_other".into(),
            "auth_config_id" => current.auth_config_id = "ac_other".into(),
            "trigger_id" => current.trigger_id = "ti_other".into(),
            "trigger_slug" => current.trigger_slug = "OTHER".into(),
            _ => unreachable!(),
        }
        let raw = body("msg_one", "mail");
        let verified = WebhookVerifier::new(&[SECRET])
            .unwrap()
            .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
            .unwrap();
        assert_eq!(
            PendingInput::bind(verified, &current).err(),
            Some(Error::Refused)
        );
    }
}

#[test]
fn different_events_with_same_mail_text_are_both_kept() {
    let current = binding();
    let inputs = [
        event("msg_A", "same sender subject and thread", &current),
        event("msg_B", "same sender subject and thread", &current),
    ];
    let plan = prepare_batch(&inputs, &current, 50).unwrap();
    assert_eq!(plan.members.len(), 2);
    assert_eq!(plan.remaining, 0);
}

#[test]
fn only_exact_duplicate_is_absorbed_and_conflicting_replay_is_refused() {
    let current = binding();
    let exact = [
        event("msg_A", "mail", &current),
        event("msg_A", "mail", &current),
    ];
    assert_eq!(
        prepare_batch(&exact, &current, 50).unwrap().members.len(),
        1
    );
    let changed = [
        event("msg_A", "mail", &current),
        event("msg_A", "different", &current),
    ];
    assert_eq!(
        prepare_batch(&changed, &current, 50).err(),
        Some(Error::ConflictingInput)
    );
}

#[test]
fn batch_limit_retains_backlog_and_considers_every_pending_row() {
    let current = binding();
    let inputs: Vec<_> = (0..53)
        .rev()
        .map(|n| event(&format!("msg_{n:03}"), "mail", &current))
        .collect();
    let plan = prepare_batch(&inputs, &current, 50).unwrap();
    assert_eq!(plan.members.len(), 50);
    assert_eq!(plan.remaining, 3);
    assert_eq!(plan.members[0].source_event_id(), "msg_000");
    assert_eq!(
        prepare_batch(&inputs, &current, 0).err(),
        Some(Error::InvalidLimit)
    );
}

#[test]
fn revoked_or_replaced_subscription_and_rebuilt_binding_reject_old_input() {
    let current = binding();
    let inputs = [event("msg_A", "mail", &current)];
    for mut changed in [
        current.clone(),
        current.clone(),
        current.clone(),
        current.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        match changed.0 {
            0 => changed.1.enabled = false,
            1 => changed.1.subscription_generation += 1,
            2 => changed.1.connection_generation += 1,
            _ => changed.1.binding_generation = "rebuilt".into(),
        }
        assert_eq!(
            prepare_batch(&inputs, &changed.1, 50).err(),
            Some(Error::Refused)
        );
    }
}

#[test]
fn fixed_members_and_digest_are_stable_across_reconstruction() {
    let current = binding();
    let first = [
        event("msg_A", "mail", &current),
        event("msg_B", "mail", &current),
    ];
    let second = [
        event("msg_B", "mail", &current),
        event("msg_A", "mail", &current),
    ];
    assert_eq!(
        prepare_batch(&first, &current, 50).unwrap().digest,
        prepare_batch(&second, &current, 50).unwrap().digest
    );
    let third = [event("msg_C", "mail", &current)];
    assert_ne!(
        prepare_batch(&first, &current, 50).unwrap().batch_id,
        prepare_batch(&third, &current, 50).unwrap().batch_id
    );
}

#[test]
fn batch_payload_bytes_are_bounded_without_dropping_remaining_events() {
    let current = binding();
    let text = "x".repeat(450_000);
    let inputs = [
        event("msg_A", &text, &current),
        event("msg_B", &text, &current),
        event("msg_C", &text, &current),
    ];
    let plan = prepare_batch(&inputs, &current, 50).unwrap();
    assert_eq!(plan.members.len(), 2);
    assert_eq!(plan.remaining, 1);
    assert!(
        plan.members
            .iter()
            .map(|input| input.raw_body().len())
            .sum::<usize>()
            <= 1_048_576
    );
}

#[test]
fn non_mail_trigger_uses_the_same_generic_batch_logic() {
    let mut current = binding();
    current.trigger_slug = "GITHUB_COMMIT_EVENT".into();
    let raw = String::from_utf8(body("msg_commit", "not an email"))
        .unwrap()
        .replace("GMAIL_NEW_GMAIL_MESSAGE", "GITHUB_COMMIT_EVENT")
        .into_bytes();
    let verified = WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    let inputs = [PendingInput::bind(verified, &current).unwrap()];
    assert_eq!(
        prepare_batch(&inputs, &current, 50).unwrap().members.len(),
        1
    );
}
