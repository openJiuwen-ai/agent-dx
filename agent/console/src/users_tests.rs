#[test]
fn browser_user_query_rejects_invalid_encoding_and_unknown_parameters() {
    for raw in ["user_id=%FF","user_id=%","display_name=%G1","page_token=%F0%28","provider=unknown","status=unknown","page_size=0","user_id=a&user_id=b","tenant=other"] {
        assert!(crate::users::UserQuery::parse(raw).is_err(),"{raw}");
    }
    let query=crate::users::UserQuery::parse("key_alias=existing&provider=agc&page_size=2").unwrap();
    assert_eq!(query.page_size,2);
    assert_eq!(query.filters["key_alias"],"existing");
}

#[test]
fn accounts_projection_drops_internal_fields_and_rejects_invalid_shapes() {
    use serde_json::json;
    let value=crate::users::project_list(json!({"users":[{"user_id":"u","provider":"agc","encrypted_key":"hidden","token":"hidden"}],"total":1,"next_page_token":null,"degraded":[],"database":"hidden"})).unwrap();
    assert!(!value.to_string().contains("hidden"));
    assert_eq!(value["users"][0]["user_id"],"u");
    assert!(crate::users::project_list(json!({"users":null})).is_err());
    let detail=crate::users::project_detail(json!({"user":{"user_id":"u","encrypted_key":"hidden"},"litellm":{"user_id":"owner","key_state":"exists","master_key":"hidden","key":{"key_id":"v1","plaintext":"hidden"}},"quota":{"state":"unknown","master_key":"hidden"},"degraded":[]})).unwrap();
    assert!(!detail.to_string().contains("hidden"));
}
