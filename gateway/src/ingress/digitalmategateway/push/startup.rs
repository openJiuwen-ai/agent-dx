//! Load the service account from a deployment-owned, read-only file.
use super::super::composio::{Error, PostgresBudget, RequestBudgetLimits};
use super::super::database::DatabaseConfig;
use super::{client::Client, jwt::Signer, Push};
use serde::Deserialize;
use std::{fs::File, io::Read, path::PathBuf, sync::Arc};
use zeroize::{Zeroize, Zeroizing};

const MAX_ACCOUNT_BYTES: u64 = 32_768;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PushConfig {
    pub service_account_file: PathBuf,
    pub organization: String,
    pub database: DatabaseConfig,
    pub limits: RequestBudgetLimits,
    #[serde(default = "default_category")]
    pub notification_category: String,
}

fn default_category() -> String {
    "WORK".into()
}

#[derive(Deserialize)]
struct ServiceAccount {
    project_id: String,
    key_id: String,
    sub_account: String,
    private_key: String,
}

impl Drop for ServiceAccount {
    fn drop(&mut self) {
        self.private_key.zeroize();
    }
}

fn parse_account(bytes: &[u8]) -> Result<ServiceAccount, Error> {
    let account: ServiceAccount =
        serde_json::from_slice(bytes).map_err(|_| Error::NotConfigured)?;
    let valid_id = |value: &str| {
        !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_graphic())
    };
    if account.project_id.is_empty()
        || account.project_id.len() > 32
        || !account.project_id.bytes().all(|byte| byte.is_ascii_digit())
        || !valid_id(&account.key_id)
        || !valid_id(&account.sub_account)
        || account.private_key.is_empty()
        || account.private_key.len() > 16_384
    {
        return Err(Error::NotConfigured);
    }
    Ok(account)
}

impl PushConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.limits.validate()?;
        if !self.service_account_file.is_absolute()
            || !self.database.valid()
            || self.organization.is_empty()
            || self.organization.len() > 256
            || self.organization.chars().any(char::is_control)
            || self.notification_category.is_empty()
            || self.notification_category.len() > 80
            || !self
                .notification_category
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(Error::NotConfigured);
        }
        Ok(())
    }

    pub(crate) async fn prepare(&self) -> Result<Push, Error> {
        self.validate()?;
        let (project_id, signer) = {
            // Kubernetes Secret volumes present each file as a controlled symlink.
            let file = File::open(&self.service_account_file).map_err(|_| Error::NotConfigured)?;
            if !file.metadata().map_err(|_| Error::NotConfigured)?.is_file() {
                return Err(Error::NotConfigured);
            }
            let mut bytes = Zeroizing::new(Vec::new());
            file.take(MAX_ACCOUNT_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| Error::NotConfigured)?;
            if bytes.len() as u64 > MAX_ACCOUNT_BYTES {
                return Err(Error::NotConfigured);
            }
            let account = parse_account(&bytes)?;
            let signer = Signer::from_pem(
                &account.private_key,
                account.key_id.clone(),
                account.sub_account.clone(),
            )?;
            (account.project_id.clone(), Arc::new(signer))
        };
        let pool = self
            .database
            .open()
            .await
            .map_err(|_| Error::NotConfigured)?;
        let budget = Arc::new(
            PostgresBudget::from_pool(pool, self.organization.clone(), self.limits.clone()).await?,
        );
        Ok(Push {
            client: Client::new(project_id, signer, budget)?,
            category: self.notification_category.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::parse_account;

    #[test]
    fn accepts_provider_metadata_without_allowing_it_to_select_an_endpoint() {
        let account = parse_account(
            br#"{"project_id":"123","key_id":"key","sub_account":"issuer","private_key":"sample","auth_uri":"https://unused.example"}"#,
        )
        .unwrap();
        assert_eq!(account.project_id, "123");
    }

    #[test]
    fn rejects_missing_fields_and_non_numeric_project_ids() {
        assert!(parse_account(br#"{"project_id":"123"}"#).is_err());
        assert!(parse_account(
            br#"{"project_id":"123/path","key_id":"key","sub_account":"issuer","private_key":"sample"}"#,
        )
        .is_err());
    }
}
