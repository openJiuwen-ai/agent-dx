//! Operator-only import into the existing configuration center. No HTTP endpoint.
use super::{
    entrypoint::DigitalMateGatewayConfig,
    privacy_center::PrivacyCenter,
    privacy_cipher::{private_key_file, PrivacyCipher},
    privacy_grants::TokenSigner,
};
use crate::ingress::accounts::{
    AccountConfig, AccountStore, Error, ExpectedRevision, ServiceSecretName,
};
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path, sync::Arc};
use zeroize::{Zeroize, Zeroizing};

/// Existing operator command grammar, kept outside the shared service main.
/// `None` means ordinary no-argument service startup; imports are never HTTP routes.
pub async fn run_cli(
    arguments: Vec<std::ffi::OsString>,
) -> Option<Result<(), Box<dyn std::error::Error>>> {
    if arguments.is_empty() {
        return None;
    }
    Some(run_import_cli(&arguments).await)
}

async fn run_import_cli(
    arguments: &[std::ffi::OsString],
) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.len() != 3
        || !matches!(
            arguments[0].to_str(),
            Some("import-composio-key" | "import-composio-webhook-secret")
        )
    {
        return Err("usage: adx-ingress [import-composio-key|import-composio-webhook-secret <private-input.json> <new|revision>]".into());
    }
    let expected = arguments[2].to_str().ok_or("invalid secret revision")?;
    let path = Path::new(&arguments[1]);
    let revision = if arguments[0] == "import-composio-key" {
        import_composio_key(path, expected).await?
    } else {
        import_composio_webhook_secret(path, expected).await?
    };
    println!("Composio service secret imported; revision={revision}");
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ComposioImport {
    project_api_key: String,
    #[serde(default)]
    gmail_auth_config_id: String,
    #[serde(default)]
    test_account_email: String,
}

impl Drop for ComposioImport {
    fn drop(&mut self) {
        self.project_api_key.zeroize();
        self.test_account_email.zeroize();
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookImport {
    webhook_secrets: Vec<String>,
}
impl Drop for WebhookImport {
    fn drop(&mut self) {
        self.webhook_secrets.zeroize();
    }
}
fn parse_webhook_import(bytes: &[u8]) -> Result<WebhookImport, Error> {
    let value: WebhookImport = serde_json::from_slice(bytes).map_err(|_| Error::Invalid)?;
    if value.webhook_secrets.is_empty()
        || value.webhook_secrets.len() > 2
        || value
            .webhook_secrets
            .iter()
            .any(|value| value.is_empty() || value.len() > 4096)
    {
        return Err(Error::Invalid);
    }
    Ok(value)
}

fn parse_composio_import(bytes: &[u8]) -> Result<ComposioImport, Error> {
    let value: ComposioImport = serde_json::from_slice(bytes).map_err(|_| Error::Invalid)?;
    if value.project_api_key.is_empty()
        || value.project_api_key.len() > 4096
        || !value
            .project_api_key
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte))
        || (!value.gmail_auth_config_id.is_empty()
            && (!value.gmail_auth_config_id.starts_with("ac_")
                || value.gmail_auth_config_id.len() > 256
                || !value
                    .gmail_auth_config_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))))
        || value.test_account_email.len() > 254
        || value.test_account_email.chars().any(char::is_control)
    {
        return Err(Error::Invalid);
    }
    Ok(value)
}

fn read_composio_import(path: &Path) -> Result<ComposioImport, Error> {
    let bytes = private_key_file(path).map_err(|_| Error::Configuration)?;
    parse_composio_import(&bytes)
}

/// Imports the project key using the existing operator-provided database and root.
/// Does not create a new root, start listeners, mint user grants or contact Composio.
/// `expected` must be `new` or the current canonical revision for an explicit rotation.
/// Returns only the new revision; input, root and database details are never errors.
pub async fn import_composio_key(input: &Path, expected: &str) -> Result<String, Error> {
    let input = read_composio_import(input)?;
    import_secret(
        ServiceSecretName::ComposioProjectKey,
        input.project_api_key.as_bytes(),
        expected,
    )
    .await
}

/// Import one or two independently configured webhook signing secrets. The
/// project API key is never a webhook signing secret and has a separate input.
pub async fn import_composio_webhook_secret(input: &Path, expected: &str) -> Result<String, Error> {
    let bytes = private_key_file(input).map_err(|_| Error::Configuration)?;
    let input = parse_webhook_import(&bytes)?;
    let value =
        Zeroizing::new(serde_json::to_vec(&input.webhook_secrets).map_err(|_| Error::Invalid)?);
    import_secret(ServiceSecretName::ComposioWebhookSecret, &value, expected).await
}

async fn import_secret(
    name: ServiceSecretName,
    value: &[u8],
    expected: &str,
) -> Result<String, Error> {
    let expected = if expected == "new" {
        ExpectedRevision::New
    } else if uuid::Uuid::parse_str(expected).is_ok_and(|value| value.to_string() == expected) {
        ExpectedRevision::Existing(expected)
    } else {
        return Err(Error::Invalid);
    };
    let configuration = std::env::var_os("ADX_JIUWEN_CONFIG").ok_or(Error::Configuration)?;
    let gateway: DigitalMateGatewayConfig =
        serde_json::from_slice(&std::fs::read(configuration).map_err(|_| Error::Configuration)?)
            .map_err(|_| Error::Configuration)?;
    gateway.validate().map_err(|_| Error::Configuration)?;
    let account_path = std::env::var_os("ADX_ACCOUNT_CONFIG").ok_or(Error::Configuration)?;
    let account: AccountConfig =
        serde_json::from_slice(&std::fs::read(account_path).map_err(|_| Error::Configuration)?)
            .map_err(|_| Error::Configuration)?;
    let root_path = std::env::var_os("ADX_CREDENTIAL_KEY_FILE").ok_or(Error::Configuration)?;
    let root_bytes = private_key_file(Path::new(&root_path)).map_err(|_| Error::Configuration)?;
    let root_text = std::str::from_utf8(&root_bytes).map_err(|_| Error::Configuration)?;
    let decoded = Zeroizing::new(hex::decode(root_text.trim()).map_err(|_| Error::Configuration)?);
    let root =
        Zeroizing::new(<[u8; 32]>::try_from(decoded.as_slice()).map_err(|_| Error::Configuration)?);
    let cipher = PrivacyCipher::new(
        "gateway-v1".into(),
        BTreeMap::from([("gateway-v1".into(), *root)]),
    )
    .map_err(|_| Error::Configuration)?;
    let store = AccountStore::connect(&account).await?;
    let signer = Arc::new(TokenSigner::new(&root, &gateway.tenant));
    let center = PrivacyCenter::new(store, cipher, signer);
    center
        .put_service_secret(&gateway.tenant, name, Some(value), expected)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cli_routes_only_the_explicit_operator_commands() {
        assert!(run_cli(Vec::new()).await.is_none());
        for arguments in [
            vec!["--help"],
            vec!["unknown", "input", "new"],
            vec!["import-composio-key", "input"],
        ] {
            let result = run_cli(
                arguments
                    .into_iter()
                    .map(std::ffi::OsString::from)
                    .collect(),
            )
            .await
            .unwrap();
            assert!(result.is_err());
        }
    }

    #[test]
    fn webhook_import_has_a_distinct_bounded_secret_set() {
        assert!(parse_webhook_import(br#"{"webhook_secrets":["first","second"]}"#).is_ok());
        for invalid in [
            br#"{}"#.as_slice(),
            br#"{"webhook_secrets":[]}"#,
            br#"{"webhook_secrets":[""]}"#,
            br#"{"webhook_secrets":["one","two","three"]}"#,
            br#"{"project_api_key":"never-reuse-the-project-key-as-webhook-secret"}"#,
        ] {
            assert!(parse_webhook_import(invalid).is_err());
        }
    }

    #[test]
    fn imports_only_a_bounded_project_key_from_strict_json() {
        let input = parse_composio_import(br#"{"project_api_key":"synthetic-project-key","gmail_auth_config_id":"","test_account_email":""}"#).unwrap();
        assert_eq!(input.project_api_key.as_str(), "synthetic-project-key");
        for invalid in [
            br#"{}"#.as_slice(),
            br#"{"project_api_key":""}"#,
            br#"{"project_api_key":"test\nheader"}"#,
            br#"{"project_api_key":"one","project_api_key":"two"}"#,
            br#"{"project_api_key":"synthetic","user_id":"attacker"}"#,
        ] {
            assert!(parse_composio_import(invalid).is_err());
        }
        let oversized = serde_json::json!({"project_api_key":"k".repeat(4097)}).to_string();
        assert!(parse_composio_import(oversized.as_bytes()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn operator_import_rejects_links_and_other_user_read_permissions() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.json");
        std::fs::write(&input, br#"{"project_api_key":"synthetic-project-key"}"#).unwrap();
        std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(read_composio_import(&input).is_ok());
        let link = directory.path().join("link.json");
        symlink(&input, &link).unwrap();
        assert!(read_composio_import(&link).is_err());
        std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_composio_import(&input).is_err());
    }
}
