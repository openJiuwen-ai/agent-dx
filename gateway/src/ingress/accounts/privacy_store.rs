//! Conditional, user-scoped storage for DigitalMate privacy configuration.

use super::{AccountStore, Error, Result};

pub(crate) struct PrivacyOwner<'a> {
    pub tenant: &'a str,
    pub workspace: &'a str,
    pub user: &'a str,
}

impl PrivacyOwner<'_> {
    fn validate(&self, name: &str) -> Result<()> {
        for value in [self.tenant, self.workspace, self.user] {
            adx_agent_core::identifier(value, "privacy owner").map_err(|_| Error::Invalid)?;
            if value.len() > 128 {
                return Err(Error::Invalid);
            }
        }
        if !valid_name(name) {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

pub(crate) enum ExpectedRevision<'a> {
    New,
    Existing(&'a str),
}

pub(crate) struct StoredPrivacySecret {
    pub revision: String,
    pub key_id: Option<String>,
    pub ciphertext: Option<Vec<u8>>,
}

pub(crate) struct StoredPrivacyGrant {
    pub scope: adx_agent_core::Scope,
    pub workspace: String,
    pub user: String,
    pub generation: String,
    pub expires_at: i64,
    pub revoked: bool,
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|parsed| parsed.to_string() == value)
}

async fn secret_lock(
    tx: &deadpool_postgres::Transaction<'_>,
    owner: &PrivacyOwner<'_>,
    name: &str,
) -> Result<()> {
    let workspace = serde_json::to_string(&[owner.tenant, owner.workspace, owner.user])
        .map_err(|_| Error::Invalid)?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,741650271134))",
        &[&workspace],
    )
    .await
    .map_err(|_| Error::Unavailable)?;
    let key = serde_json::to_string(&[owner.tenant, owner.workspace, owner.user, name])
        .map_err(|_| Error::Invalid)?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,741650271133))",
        &[&key],
    )
    .await
    .map_err(|_| Error::Unavailable)?;
    Ok(())
}

async fn admit_new_key(
    tx: &deadpool_postgres::Transaction<'_>,
    owner: &PrivacyOwner<'_>,
) -> Result<()> {
    let count: i64 = tx.query_one("SELECT count(*) FROM (SELECT secret_key FROM adx_accounts.user_secrets WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3 UNION SELECT secret_key FROM adx_accounts.privacy_transactions WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3 AND state='pending') keys", &[&owner.tenant,&owner.workspace,&owner.user]).await.map_err(|_| Error::Unavailable)?.get(0);
    if count >= 64 {
        return Err(Error::Conflict);
    }
    Ok(())
}

async fn admit_transaction(
    tx: &deadpool_postgres::Transaction<'_>,
    owner: &PrivacyOwner<'_>,
) -> Result<()> {
    // Pending work is never reaped. Finished receipts remain available for 90 days.
    tx.execute("DELETE FROM adx_accounts.privacy_transactions WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3 AND state <> 'pending' AND finished_at < now() - interval '90 days'", &[&owner.tenant,&owner.workspace,&owner.user]).await.map_err(|_| Error::Unavailable)?;
    let count: i64 = tx.query_one("SELECT count(*) FROM adx_accounts.privacy_transactions WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3", &[&owner.tenant,&owner.workspace,&owner.user]).await.map_err(|_| Error::Unavailable)?.get(0);
    if count >= 4096 {
        return Err(Error::Conflict);
    }
    Ok(())
}

async fn pending(
    tx: &deadpool_postgres::Transaction<'_>,
    owner: &PrivacyOwner<'_>,
    name: &str,
) -> Result<bool> {
    Ok(tx.query_opt("SELECT transaction_id FROM adx_accounts.privacy_transactions WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3 AND secret_key=$4 AND state='pending'", &[&owner.tenant,&owner.workspace,&owner.user,&name]).await.map_err(|_| Error::Unavailable)?.is_some())
}

impl AccountStore {
    /// Store a revocable business authorization. The HMAC bearer is never stored.
    pub(crate) async fn privacy_issue_grant<F, Fut>(
        &self,
        digest: &str,
        owner: &PrivacyOwner<'_>,
        scope: &adx_agent_core::Scope,
        generation: &str,
        expires_at: i64,
        verify_current: F,
    ) -> Result<String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        owner.validate("authorization")?;
        if !valid_digest(digest)
            || !valid_name(generation)
            || scope.tenant != owner.tenant
            || scope.validate().is_err()
        {
            return Err(Error::Invalid);
        }
        let scope_json = serde_json::to_string(scope).map_err(|_| Error::Invalid)?;
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        secret_lock(&tx, owner, "authorization").await?;
        // Recheck under the existing owner lock: a delayed old admission must not
        // erase the current generation's authorization after another admission wins.
        verify_current().await?;
        tx.execute("DELETE FROM adx_accounts.privacy_grants WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3 AND (revoked_at IS NOT NULL OR expires_at < EXTRACT(EPOCH FROM clock_timestamp())::bigint OR (scope_json=$4 AND binding_generation<>$5))", &[&owner.tenant,&owner.workspace,&owner.user,&scope_json,&generation]).await.map_err(|_| Error::Unavailable)?;
        // Reconnects to the same live generation reuse their authorization, not another permanent row.
        if let Some(row) = tx.query_opt("UPDATE adx_accounts.privacy_grants g SET expires_at=GREATEST(g.expires_at,$6) FROM adx_accounts.users u WHERE g.tenant_id=$1 AND g.workspace_id=$2 AND g.user_id=$3 AND g.scope_json=$4 AND g.binding_generation=$5 AND g.revoked_at IS NULL AND u.user_id=g.user_id AND u.status='active' AND $6 > EXTRACT(EPOCH FROM clock_timestamp())::bigint AND $6 <= EXTRACT(EPOCH FROM clock_timestamp())::bigint + 2592061 RETURNING g.token_digest", &[&owner.tenant,&owner.workspace,&owner.user,&scope_json,&generation,&expires_at]).await.map_err(|_| Error::Unavailable)? {
            let id=row.get(0);
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(id);
        }
        // SQL recovery bound: 30 days + 60 seconds of clock skew + one second of epoch rounding.
        let row = tx.query_opt("INSERT INTO adx_accounts.privacy_grants(token_digest,tenant_id,workspace_id,user_id,scope_json,binding_generation,expires_at) SELECT $1,$2,$3,u.user_id,$5,$6,$7 FROM adx_accounts.users u WHERE u.user_id=$4 AND u.status='active' AND $7 > EXTRACT(EPOCH FROM clock_timestamp())::bigint AND $7 <= EXTRACT(EPOCH FROM clock_timestamp())::bigint + 2592061 AND (SELECT count(*) FROM adx_accounts.privacy_grants WHERE tenant_id=$2 AND workspace_id=$3 AND user_id=$4) < 64 ON CONFLICT DO NOTHING RETURNING token_digest", &[&digest,&owner.tenant,&owner.workspace,&owner.user,&scope_json,&generation,&expires_at]).await.map_err(|_| Error::Unavailable)?;
        let id = row.ok_or(Error::Conflict)?.get(0);
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(id)
    }

    pub(crate) async fn privacy_get_grant(
        &self,
        digest: &str,
    ) -> Result<Option<StoredPrivacyGrant>> {
        if !valid_digest(digest) {
            return Err(Error::Invalid);
        }
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let row = client.query_opt("SELECT g.scope_json,g.tenant_id,g.workspace_id,g.user_id,g.binding_generation,g.expires_at,g.revoked_at IS NOT NULL FROM adx_accounts.privacy_grants g JOIN adx_accounts.users u ON u.user_id=g.user_id WHERE g.token_digest=$1 AND u.status='active' AND g.expires_at > EXTRACT(EPOCH FROM clock_timestamp())::bigint", &[&digest]).await.map_err(|_| Error::Unavailable)?;
        row.map(|row| {
            let scope: adx_agent_core::Scope =
                serde_json::from_str(row.get::<_, &str>(0)).map_err(|_| Error::Unavailable)?;
            if scope.validate().is_err() || scope.tenant != row.get::<_, String>(1) {
                return Err(Error::Unavailable);
            }
            Ok(StoredPrivacyGrant {
                scope,
                workspace: row.get(2),
                user: row.get(3),
                generation: row.get(4),
                expires_at: row.get(5),
                revoked: row.get(6),
            })
        })
        .transpose()
    }

    pub(crate) async fn privacy_extend_grant(&self, digest: &str, expires_at: i64) -> Result<()> {
        if !valid_digest(digest) {
            return Err(Error::Invalid);
        }
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let changed = client.execute("UPDATE adx_accounts.privacy_grants g SET expires_at=GREATEST(g.expires_at,$2) FROM adx_accounts.users u WHERE g.token_digest=$1 AND u.user_id=g.user_id AND u.status='active' AND g.revoked_at IS NULL AND g.expires_at > EXTRACT(EPOCH FROM clock_timestamp())::bigint AND $2 <= EXTRACT(EPOCH FROM clock_timestamp())::bigint + 2592061", &[&digest,&expires_at]).await.map_err(|_| Error::Unavailable)?;
        if changed != 1 {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }

    pub(crate) async fn privacy_revoke_grant(&self, digest: &str) -> Result<()> {
        if !valid_digest(digest) {
            return Err(Error::Invalid);
        }
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        client.execute(
            "UPDATE adx_accounts.privacy_grants SET revoked_at=COALESCE(revoked_at,now()) WHERE token_digest=$1",
            &[&digest],
        ).await.map_err(|_| Error::Unavailable)?;
        Ok(())
    }

    /// A tombstone retains its revision, so stale refreshes cannot resurrect a revoked secret.
    pub(crate) async fn privacy_get(
        &self,
        owner: &PrivacyOwner<'_>,
        name: &str,
    ) -> Result<Option<StoredPrivacySecret>> {
        owner.validate(name)?;
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let row = client.query_opt(
            "SELECT s.revision,s.key_id,s.encrypted_value FROM adx_accounts.user_secrets s JOIN adx_accounts.users u ON u.user_id=s.user_id WHERE s.tenant_id=$1 AND s.workspace_id=$2 AND s.user_id=$3 AND s.secret_key=$4 AND u.status='active'",
            &[&owner.tenant, &owner.workspace, &owner.user, &name],
        ).await.map_err(|_| Error::Unavailable)?;
        row.map(|row| {
            let secret = StoredPrivacySecret {
                revision: row.get(0),
                key_id: row.get(1),
                ciphertext: row.get(2),
            };
            if secret.key_id.is_some() != secret.ciphertext.is_some() {
                return Err(Error::Unavailable);
            }
            Ok(secret)
        })
        .transpose()
    }

    /// Capture a stable head only if no workspace update is still staged.
    pub(crate) async fn privacy_exportable(
        &self,
        owner: &PrivacyOwner<'_>,
        name: &str,
    ) -> Result<Option<StoredPrivacySecret>> {
        owner.validate(name)?;
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        secret_lock(&tx, owner, name).await?;
        if pending(&tx, owner, name).await? {
            return Err(Error::Conflict);
        }
        let row = tx.query_opt("SELECT s.revision,s.key_id,s.encrypted_value FROM adx_accounts.user_secrets s JOIN adx_accounts.users u ON u.user_id=s.user_id WHERE s.tenant_id=$1 AND s.workspace_id=$2 AND s.user_id=$3 AND s.secret_key=$4 AND u.status='active'", &[&owner.tenant,&owner.workspace,&owner.user,&name]).await.map_err(|_| Error::Unavailable)?;
        let value = row.map(|row| StoredPrivacySecret {
            revision: row.get(0),
            key_id: row.get(1),
            ciphertext: row.get(2),
        });
        if value
            .as_ref()
            .is_some_and(|value| value.key_id.is_some() != value.ciphertext.is_some())
        {
            return Err(Error::Unavailable);
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(value)
    }

    /// Every successful mutation gets a new opaque revision, including recreation after deletion.
    pub(crate) async fn privacy_put(
        &self,
        owner: &PrivacyOwner<'_>,
        name: &str,
        key_id: &str,
        ciphertext: &[u8],
        expected: ExpectedRevision<'_>,
    ) -> Result<String> {
        owner.validate(name)?;
        if !valid_name(key_id) || !(28..=131_100).contains(&ciphertext.len()) {
            return Err(Error::Invalid);
        }
        let revision = uuid::Uuid::new_v4().to_string();
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        secret_lock(&tx, owner, name).await?;
        if pending(&tx, owner, name).await? {
            return Err(Error::Conflict);
        }
        if matches!(expected, ExpectedRevision::New) {
            admit_new_key(&tx, owner).await?;
        }
        let row = match expected {
            ExpectedRevision::New => tx.query_opt(
                "INSERT INTO adx_accounts.user_secrets(tenant_id,workspace_id,user_id,secret_key,revision,key_id,encrypted_value) SELECT $1,$2,u.user_id,$4,$5,$6,$7 FROM adx_accounts.users u WHERE u.user_id=$3 AND u.status='active' ON CONFLICT DO NOTHING RETURNING revision",
                &[&owner.tenant, &owner.workspace, &owner.user, &name, &revision, &key_id, &ciphertext],
            ).await,
            ExpectedRevision::Existing(previous) => tx.query_opt(
                "UPDATE adx_accounts.user_secrets s SET revision=$5,key_id=$6,encrypted_value=$7,updated_at=now() FROM adx_accounts.users u WHERE s.tenant_id=$1 AND s.workspace_id=$2 AND s.user_id=$3 AND s.secret_key=$4 AND s.revision=$8 AND u.user_id=s.user_id AND u.status='active' RETURNING s.revision",
                &[&owner.tenant, &owner.workspace, &owner.user, &name, &revision, &key_id, &ciphertext, &previous],
            ).await,
        }.map_err(|_| Error::Unavailable)?;
        let revision = row.map(|row| row.get(0)).ok_or(Error::Conflict)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(revision)
    }

    #[cfg(test)]
    pub(crate) async fn privacy_delete(
        &self,
        owner: &PrivacyOwner<'_>,
        name: &str,
        expected: &str,
    ) -> Result<String> {
        owner.validate(name)?;
        let revision = uuid::Uuid::new_v4().to_string();
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        secret_lock(&tx, owner, name).await?;
        if pending(&tx, owner, name).await? {
            return Err(Error::Conflict);
        }
        let row = tx.query_opt(
            "UPDATE adx_accounts.user_secrets s SET revision=$5,key_id=NULL,encrypted_value=NULL,updated_at=now() FROM adx_accounts.users u WHERE s.tenant_id=$1 AND s.workspace_id=$2 AND s.user_id=$3 AND s.secret_key=$4 AND s.revision=$6 AND u.user_id=s.user_id AND u.status='active' RETURNING s.revision",
            &[&owner.tenant, &owner.workspace, &owner.user, &name, &revision, &expected],
        ).await.map_err(|_| Error::Unavailable)?;
        let revision = row.map(|row| row.get(0)).ok_or(Error::Conflict)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(revision)
    }

    /// Stage without changing the visible secret. One pending writer owns each key.
    pub(crate) async fn privacy_stage(
        &self,
        transaction_id: &str,
        owner: &PrivacyOwner<'_>,
        name: &str,
        candidate: Option<(&str, &[u8])>,
        expected: ExpectedRevision<'_>,
    ) -> Result<String> {
        owner.validate(name)?;
        if !valid_uuid(transaction_id)
            || candidate.is_some_and(|(key, value)| {
                !valid_name(key) || !(28..=131_100).contains(&value.len())
            })
        {
            return Err(Error::Invalid);
        }
        let expected = match expected {
            ExpectedRevision::New => None,
            ExpectedRevision::Existing(value) if valid_uuid(value) => Some(value),
            _ => return Err(Error::Invalid),
        };
        let revision = uuid::Uuid::new_v4().to_string();
        let (key_id, ciphertext) = match candidate {
            Some((key, value)) => (Some(key), Some(value)),
            None => (None, None),
        };
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        secret_lock(&tx, owner, name).await?;
        let active = tx.query_opt("SELECT user_id FROM adx_accounts.users WHERE user_id=$1 AND status='active' FOR SHARE", &[&owner.user]).await.map_err(|_| Error::Unavailable)?;
        if active.is_none() || pending(&tx, owner, name).await? {
            return Err(Error::Conflict);
        }
        let current = tx.query_opt("SELECT revision FROM adx_accounts.user_secrets WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3 AND secret_key=$4", &[&owner.tenant,&owner.workspace,&owner.user,&name]).await.map_err(|_| Error::Unavailable)?.map(|row| row.get::<_, String>(0));
        if current.as_deref() != expected {
            return Err(Error::Conflict);
        }
        if current.is_none() {
            admit_new_key(&tx, owner).await?;
        }
        admit_transaction(&tx, owner).await?;
        let inserted = tx.query_opt("INSERT INTO adx_accounts.privacy_transactions(transaction_id,tenant_id,workspace_id,user_id,secret_key,expected_revision,candidate_revision,candidate_key_id,candidate_value,state) VALUES($1::text::uuid,$2,$3,$4,$5,$6,$7,$8,$9,'pending') ON CONFLICT DO NOTHING RETURNING candidate_revision", &[&transaction_id,&owner.tenant,&owner.workspace,&owner.user,&name,&expected,&revision,&key_id,&ciphertext]).await.map_err(|_| Error::Unavailable)?;
        if inserted.is_none() {
            return Err(Error::Conflict);
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(revision)
    }

    /// Idempotently make the staged value visible after the local commit receipt exists.
    pub(crate) async fn privacy_promote(
        &self,
        transaction_id: &str,
        owner: &PrivacyOwner<'_>,
        name: &str,
    ) -> Result<String> {
        owner.validate(name)?;
        if !valid_uuid(transaction_id) {
            return Err(Error::Invalid);
        }
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        secret_lock(&tx, owner, name).await?;
        let row = tx.query_opt("SELECT expected_revision,candidate_revision,candidate_key_id,candidate_value,state FROM adx_accounts.privacy_transactions WHERE transaction_id=$1::text::uuid AND tenant_id=$2 AND workspace_id=$3 AND user_id=$4 AND secret_key=$5 FOR UPDATE", &[&transaction_id,&owner.tenant,&owner.workspace,&owner.user,&name]).await.map_err(|_| Error::Unavailable)?.ok_or(Error::Conflict)?;
        let state: String = row.get(4);
        let revision: String = row.get(1);
        if state == "committed" {
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(revision);
        }
        if state != "pending" {
            return Err(Error::Conflict);
        }
        let active = tx.query_opt("SELECT user_id FROM adx_accounts.users WHERE user_id=$1 AND status='active' FOR SHARE", &[&owner.user]).await.map_err(|_| Error::Unavailable)?;
        if active.is_none() {
            return Err(Error::Conflict);
        }
        let expected: Option<String> = row.get(0);
        let current = tx.query_opt("SELECT revision FROM adx_accounts.user_secrets WHERE tenant_id=$1 AND workspace_id=$2 AND user_id=$3 AND secret_key=$4", &[&owner.tenant,&owner.workspace,&owner.user,&name]).await.map_err(|_| Error::Unavailable)?.map(|row| row.get::<_, String>(0));
        if current != expected {
            return Err(Error::Conflict);
        }
        let key_id: Option<String> = row.get(2);
        let ciphertext: Option<Vec<u8>> = row.get(3);
        if key_id.is_some() != ciphertext.is_some() {
            return Err(Error::Unavailable);
        }
        tx.execute("INSERT INTO adx_accounts.user_secrets(tenant_id,workspace_id,user_id,secret_key,revision,key_id,encrypted_value) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(tenant_id,workspace_id,user_id,secret_key) DO UPDATE SET revision=EXCLUDED.revision,key_id=EXCLUDED.key_id,encrypted_value=EXCLUDED.encrypted_value,updated_at=now()", &[&owner.tenant,&owner.workspace,&owner.user,&name,&revision,&key_id,&ciphertext]).await.map_err(|_| Error::Unavailable)?;
        tx.execute("UPDATE adx_accounts.privacy_transactions SET state='committed',candidate_key_id=NULL,candidate_value=NULL,finished_at=now() WHERE transaction_id=$1::text::uuid", &[&transaction_id]).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(revision)
    }

    /// Also records cancellation of an unknown request, fencing late-arriving stage requests.
    pub(crate) async fn privacy_abort(
        &self,
        transaction_id: &str,
        owner: &PrivacyOwner<'_>,
        name: &str,
    ) -> Result<()> {
        owner.validate(name)?;
        if !valid_uuid(transaction_id) {
            return Err(Error::Invalid);
        }
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        secret_lock(&tx, owner, name).await?;
        let row = tx.query_opt("SELECT state FROM adx_accounts.privacy_transactions WHERE transaction_id=$1::text::uuid AND tenant_id=$2 AND workspace_id=$3 AND user_id=$4 AND secret_key=$5 FOR UPDATE", &[&transaction_id,&owner.tenant,&owner.workspace,&owner.user,&name]).await.map_err(|_| Error::Unavailable)?;
        match row.map(|row| row.get::<_, String>(0)).as_deref() {
            Some("committed") => return Err(Error::Conflict),
            Some("aborted") => (),
            Some("pending") => {
                tx.execute("UPDATE adx_accounts.privacy_transactions SET state='aborted',candidate_key_id=NULL,candidate_value=NULL,finished_at=now() WHERE transaction_id=$1::text::uuid", &[&transaction_id]).await.map_err(|_| Error::Unavailable)?;
            }
            Some(_) => return Err(Error::Unavailable),
            None => {
                admit_transaction(&tx, owner).await?;
                let revision = uuid::Uuid::new_v4().to_string();
                let inserted = tx.query_opt("INSERT INTO adx_accounts.privacy_transactions(transaction_id,tenant_id,workspace_id,user_id,secret_key,candidate_revision,state,finished_at) VALUES($1::text::uuid,$2,$3,$4,$5,$6,'aborted',now()) ON CONFLICT DO NOTHING RETURNING transaction_id", &[&transaction_id,&owner.tenant,&owner.workspace,&owner.user,&name,&revision]).await.map_err(|_| Error::Unavailable)?;
                if inserted.is_none() {
                    return Err(Error::Conflict);
                }
            }
        }
        tx.commit().await.map_err(|_| Error::Unavailable)
    }

    pub(crate) async fn privacy_transaction_status(
        &self,
        transaction_id: &str,
        owner: &PrivacyOwner<'_>,
        name: &str,
    ) -> Result<Option<(String, String)>> {
        owner.validate(name)?;
        if !valid_uuid(transaction_id) {
            return Err(Error::Invalid);
        }
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let row = client.query_opt("SELECT t.state,t.candidate_revision FROM adx_accounts.privacy_transactions t JOIN adx_accounts.users u ON u.user_id=t.user_id WHERE t.transaction_id=$1::text::uuid AND t.tenant_id=$2 AND t.workspace_id=$3 AND t.user_id=$4 AND t.secret_key=$5 AND u.status='active'", &[&transaction_id,&owner.tenant,&owner.workspace,&owner.user,&name]).await.map_err(|_| Error::Unavailable)?;
        Ok(row.map(|row| (row.get(0), row.get(1))))
    }
}

#[cfg(test)]
mod tests {
    use super::{ExpectedRevision, PrivacyOwner};
    use crate::ingress::accounts::{AccountStore, Error};

    async fn prepare_schema(client: &tokio_postgres::Client) {
        // These tests can run concurrently against the same disposable database.
        client
            .query_one("SELECT pg_advisory_lock(741650271132)", &[])
            .await
            .unwrap();
        client
            .batch_execute(include_str!("schema.sql"))
            .await
            .unwrap();
        client
            .query_one("SELECT pg_advisory_unlock(741650271132)", &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires a dedicated disposable PostgreSQL database"]
    async fn privacy_storage_limits_new_keys_and_reaps_only_old_finished_transactions() {
        let url = std::env::var("DIGITALMATE_PRIVACY_TEST_DATABASE_URL").unwrap();
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        prepare_schema(&client).await;
        let user = format!("privacy-quota-{}", uuid::Uuid::new_v4());
        client.execute("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES($1,'test','privacy-test','test',$1,'1')", &[&user]).await.unwrap();
        let owner = PrivacyOwner {
            tenant: "tenant-a",
            workspace: "workspace-a",
            user: &user,
        };
        let store = AccountStore::from_url(&url, true, None).await.unwrap();
        client.execute("INSERT INTO adx_accounts.user_secrets(tenant_id,workspace_id,user_id,secret_key,revision) SELECT 'tenant-a','workspace-a',$1,'key-' || n,gen_random_uuid()::text FROM generate_series(1,64) n", &[&user]).await.unwrap();
        assert!(matches!(
            store
                .privacy_stage(
                    &uuid::Uuid::new_v4().to_string(),
                    &owner,
                    "extra",
                    Some(("key-v1", &[1u8; 32])),
                    ExpectedRevision::New
                )
                .await,
            Err(Error::Conflict)
        ));
        client
            .execute(
                "DELETE FROM adx_accounts.user_secrets WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        client.execute("INSERT INTO adx_accounts.privacy_transactions(transaction_id,tenant_id,workspace_id,user_id,secret_key,candidate_revision,state,finished_at) SELECT gen_random_uuid(),'tenant-a','workspace-a',$1,'old-' || n,gen_random_uuid()::text,'aborted',now()-interval '91 days' FROM generate_series(1,4096) n", &[&user]).await.unwrap();
        let transaction = uuid::Uuid::new_v4().to_string();
        store
            .privacy_stage(
                &transaction,
                &owner,
                "allowed",
                Some(("key-v1", &[1u8; 32])),
                ExpectedRevision::New,
            )
            .await
            .unwrap();
        let remaining: i64 = client
            .query_one(
                "SELECT count(*) FROM adx_accounts.privacy_transactions WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(remaining, 1);
        store
            .privacy_abort(&transaction, &owner, "allowed")
            .await
            .unwrap();
        client
            .execute(
                "DELETE FROM adx_accounts.privacy_transactions WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        client
            .execute("DELETE FROM adx_accounts.users WHERE user_id=$1", &[&user])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires a dedicated disposable PostgreSQL database"]
    async fn storage_preserves_isolation_and_rejects_stale_restore_and_refresh() {
        let url = std::env::var("DIGITALMATE_PRIVACY_TEST_DATABASE_URL")
            .expect("dedicated privacy test database required");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        prepare_schema(&client).await;
        let user = format!("privacy-test-{}", uuid::Uuid::new_v4());
        client.execute(
            "INSERT INTO adx_accounts.users(user_id, provider, developer_scope, client_id, union_id, agreement_version) VALUES ($1,'test','privacy-test','test',$1,'1')",
            &[&user],
        ).await.unwrap();
        let store = AccountStore::from_url(&url, true, None).await.unwrap();
        let owner = PrivacyOwner {
            tenant: "tenant-a",
            workspace: "workspace-a",
            user: &user,
        };
        let other = PrivacyOwner {
            tenant: "tenant-a",
            workspace: "workspace-b",
            user: &user,
        };
        assert!(store
            .privacy_get(&owner, "FEISHU_TOKEN")
            .await
            .unwrap()
            .is_none());

        let first = store
            .privacy_put(
                &owner,
                "FEISHU_TOKEN",
                "key-v1",
                &[1u8; 32],
                ExpectedRevision::New,
            )
            .await
            .unwrap();
        let saved = store
            .privacy_get(&owner, "FEISHU_TOKEN")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.revision, first);
        assert_eq!(saved.ciphertext.as_deref(), Some([1u8; 32].as_slice()));
        assert!(store
            .privacy_get(&other, "FEISHU_TOKEN")
            .await
            .unwrap()
            .is_none());

        let left = store.privacy_put(
            &owner,
            "FEISHU_TOKEN",
            "key-v1",
            &[2u8; 32],
            ExpectedRevision::Existing(&first),
        );
        let right = store.privacy_put(
            &owner,
            "FEISHU_TOKEN",
            "key-v1",
            &[3u8; 32],
            ExpectedRevision::Existing(&first),
        );
        let (left, right) = tokio::join!(left, right);
        assert!(matches!(
            (&left, &right),
            (Ok(_), Err(Error::Conflict)) | (Err(Error::Conflict), Ok(_))
        ));
        let current = left.or(right).unwrap();
        let deleted = store
            .privacy_delete(&owner, "FEISHU_TOKEN", &current)
            .await
            .unwrap();
        assert!(store
            .privacy_get(&owner, "FEISHU_TOKEN")
            .await
            .unwrap()
            .unwrap()
            .ciphertext
            .is_none());
        assert!(matches!(
            store
                .privacy_put(
                    &owner,
                    "FEISHU_TOKEN",
                    "key-v1",
                    &[4u8; 32],
                    ExpectedRevision::Existing(&current)
                )
                .await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            store
                .privacy_put(
                    &owner,
                    "FEISHU_TOKEN",
                    "key-v1",
                    &[4u8; 32],
                    ExpectedRevision::New
                )
                .await,
            Err(Error::Conflict)
        ));
        let restored = store
            .privacy_put(
                &owner,
                "FEISHU_TOKEN",
                "key-v1",
                &[5u8; 32],
                ExpectedRevision::Existing(&deleted),
            )
            .await
            .unwrap();
        assert_ne!(restored, deleted);

        let staged_id = uuid::Uuid::new_v4().to_string();
        let other_id = uuid::Uuid::new_v4().to_string();
        let candidate = store
            .privacy_stage(
                &staged_id,
                &owner,
                "TX_SECRET",
                Some(("key-v1", &[7u8; 32])),
                ExpectedRevision::New,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .privacy_transaction_status(&staged_id, &owner, "TX_SECRET")
                .await
                .unwrap(),
            Some(("pending".into(), candidate.clone()))
        );
        assert!(store
            .privacy_transaction_status(&staged_id, &other, "TX_SECRET")
            .await
            .unwrap()
            .is_none());
        assert!(store
            .privacy_get(&owner, "TX_SECRET")
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            store
                .privacy_stage(
                    &other_id,
                    &owner,
                    "TX_SECRET",
                    Some(("key-v1", &[8u8; 32])),
                    ExpectedRevision::New
                )
                .await,
            Err(Error::Conflict)
        ));
        assert_eq!(
            store
                .privacy_promote(&staged_id, &owner, "TX_SECRET")
                .await
                .unwrap(),
            candidate
        );
        assert_eq!(
            store
                .privacy_transaction_status(&staged_id, &owner, "TX_SECRET")
                .await
                .unwrap(),
            Some(("committed".into(), candidate.clone()))
        );
        assert_eq!(
            store
                .privacy_promote(&staged_id, &owner, "TX_SECRET")
                .await
                .unwrap(),
            candidate
        );
        assert_eq!(
            store
                .privacy_get(&owner, "TX_SECRET")
                .await
                .unwrap()
                .unwrap()
                .ciphertext
                .as_deref(),
            Some([7u8; 32].as_slice())
        );
        let aborted = uuid::Uuid::new_v4().to_string();
        store
            .privacy_stage(
                &aborted,
                &owner,
                "TX_SECRET",
                Some(("key-v1", &[8u8; 32])),
                ExpectedRevision::Existing(&candidate),
            )
            .await
            .unwrap();
        store
            .privacy_abort(&aborted, &owner, "TX_SECRET")
            .await
            .unwrap();
        store
            .privacy_abort(&aborted, &owner, "TX_SECRET")
            .await
            .unwrap();
        assert!(matches!(
            store.privacy_promote(&aborted, &owner, "TX_SECRET").await,
            Err(Error::Conflict)
        ));
        let removed = uuid::Uuid::new_v4().to_string();
        store
            .privacy_stage(
                &removed,
                &owner,
                "TX_SECRET",
                None,
                ExpectedRevision::Existing(&candidate),
            )
            .await
            .unwrap();
        let tombstone = store
            .privacy_promote(&removed, &owner, "TX_SECRET")
            .await
            .unwrap();
        assert!(store
            .privacy_get(&owner, "TX_SECRET")
            .await
            .unwrap()
            .unwrap()
            .ciphertext
            .is_none());
        assert!(matches!(
            store
                .privacy_stage(
                    &uuid::Uuid::new_v4().to_string(),
                    &owner,
                    "TX_SECRET",
                    Some(("key-v1", &[9u8; 32])),
                    ExpectedRevision::Existing(&candidate)
                )
                .await,
            Err(Error::Conflict)
        ));
        assert_ne!(tombstone, candidate);
        let missing = uuid::Uuid::new_v4().to_string();
        store
            .privacy_abort(&missing, &owner, "OTHER_SECRET")
            .await
            .unwrap();
        assert!(matches!(
            store
                .privacy_stage(
                    &missing,
                    &owner,
                    "OTHER_SECRET",
                    Some(("key-v1", &[9u8; 32])),
                    ExpectedRevision::New
                )
                .await,
            Err(Error::Conflict)
        ));

        client
            .execute(
                "UPDATE adx_accounts.users SET status='disabled' WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        assert!(store
            .privacy_get(&owner, "FEISHU_TOKEN")
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            store
                .privacy_put(
                    &owner,
                    "FEISHU_TOKEN",
                    "key-v1",
                    &[6u8; 32],
                    ExpectedRevision::Existing(&restored)
                )
                .await,
            Err(Error::Conflict)
        ));
        client
            .execute(
                "DELETE FROM adx_accounts.privacy_transactions WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        client
            .execute(
                "DELETE FROM adx_accounts.user_secrets WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        client
            .execute("DELETE FROM adx_accounts.users WHERE user_id=$1", &[&user])
            .await
            .unwrap();
    }
}
