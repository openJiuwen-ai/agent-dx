//! Sole writer for the dedicated PostgreSQL ingestion schema. Never accesses
//! account/privacy/connection tables or shares another owner's private pool.
use super::super::composio::VerifiedWebhook;
use super::{DeliveryBatch, DurableAck, Error, PendingInput, SubscriptionBinding};
use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio_postgres::Transaction;

#[derive(Clone)]
pub struct InboxLimits {
    pub max_pending_events: i64,
    pub max_pending_bytes: i64,
    pub per_subscription_events: i64,
    pub debounce: Duration,
    pub dedupe_retention: Duration,
}

#[derive(Clone)]
pub struct Inbox {
    pool: Pool,
    limits: InboxLimits,
}

pub struct ReceiveReceipt {
    pub source_event_id: String,
    pub digest: String,
    pub duplicate: bool,
}

pub struct Lease {
    pub(super) workspace: String,
    pub(super) epoch: i64,
    pub batch: DeliveryBatch,
}

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::Unavailable
}
fn millis(value: Duration) -> Result<i64, Error> {
    i64::try_from(value.as_millis()).map_err(|_| Error::InvalidLimit)
}
fn generation(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::Refused)
}
fn encode(binding: &SubscriptionBinding) -> Result<String, Error> {
    serde_json::to_string(binding).map_err(|_| Error::InvalidManifest)
}
fn decode(value: &str) -> Result<SubscriptionBinding, Error> {
    serde_json::from_str(value).map_err(|_| Error::InvalidManifest)
}
fn workspace_key(binding: &SubscriptionBinding) -> Result<String, Error> {
    let bytes = serde_json::to_vec(&(&binding.tenant, &binding.user_id, &binding.workspace))
        .map_err(|_| Error::InvalidManifest)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

impl Inbox {
    /// Explicit operator startup against a dedicated bounded ingestion pool.
    /// Pool construction/TLS/credentials belong to trusted runtime configuration.
    pub async fn initialize(pool: Pool, limits: InboxLimits) -> Result<Self, Error> {
        let client = pool.get().await.map_err(unavailable)?;
        client
            .batch_execute(include_str!("schema.sql"))
            .await
            .map_err(unavailable)?;
        drop(client);
        Self::from_pool(pool, limits).await
    }

    pub async fn from_pool(pool: Pool, limits: InboxLimits) -> Result<Self, Error> {
        if limits.max_pending_events <= 0
            || limits.max_pending_bytes < 1_048_576
            || limits.per_subscription_events <= 0
            || limits.per_subscription_events > limits.max_pending_events
            || limits.debounce > Duration::from_secs(30)
            || !(Duration::from_secs(86_400)..=Duration::from_secs(30 * 86_400))
                .contains(&limits.dedupe_retention)
        {
            return Err(Error::InvalidLimit);
        }
        let client = pool.get().await.map_err(unavailable)?;
        let rows = client
            .query("SELECT version FROM digitalmate_ingestion.version", &[])
            .await
            .map_err(unavailable)?;
        if rows.len() != 1 || rows[0].get::<_, i32>(0) != 1 {
            return Err(Error::InvalidManifest);
        }
        drop(client);
        Ok(Self { pool, limits })
    }

    /// Resolve identity from this owner's exact persisted ready batch and current
    /// subscription. Caller-supplied scope/binding fields are never consulted.
    /// Refuses wrong digest/size, revoked/replaced subscriptions and terminal work.
    pub async fn delivery_binding(
        &self,
        batch_id: &str,
        digest: &str,
        wire_bytes: usize,
    ) -> Result<SubscriptionBinding, Error> {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || batch_id != format!("batch_{digest}")
            || !(1..=2_097_152).contains(&wire_bytes)
        {
            return Err(Error::Refused);
        }
        let size = i64::try_from(wire_bytes).map_err(|_| Error::Refused)?;
        let client = self.pool.get().await.map_err(unavailable)?;
        let row = client.query_opt(
            "SELECT b.payload,s.binding_json FROM digitalmate_ingestion.batches b JOIN digitalmate_ingestion.subscriptions s ON s.subscription_id=b.subscription_id AND s.workspace_key=b.workspace_key AND s.generation=b.generation WHERE b.batch_id=$1 AND b.digest=$2 AND b.state='ready' AND s.enabled AND octet_length(b.payload)::bigint=$3",
            &[&batch_id, &digest, &size],
        ).await.map_err(unavailable)?.ok_or(Error::Refused)?;
        let bytes: Vec<u8> = row.get(0);
        let batch: DeliveryBatch =
            serde_json::from_slice(&bytes).map_err(|_| Error::InvalidManifest)?;
        batch.validate()?;
        let current = decode(&row.get::<_, String>(1))?;
        current.validate()?;
        if batch.batch_id != batch_id
            || batch.digest != digest
            || batch.binding != current
            || !current.enabled
        {
            return Err(Error::Refused);
        }
        Ok(current)
    }

    #[cfg(test)]
    pub(super) fn pool(&self) -> &Pool {
        &self.pool
    }
    #[cfg(test)]
    pub(super) fn limits(&self) -> &InboxLimits {
        &self.limits
    }

    /// Only call after the real identity/connection owner authorizes this exact
    /// projection. JSON decoding alone is not authorization to register a target.
    /// One logical subscription never changes its tenant/user/workspace.
    pub async fn register(&self, binding: &SubscriptionBinding) -> Result<(), Error> {
        binding.validate()?;
        let json = encode(binding)?;
        let workspace = workspace_key(binding)?;
        let gen = generation(binding.subscription_generation)?;
        let connection_gen = generation(binding.connection_generation)?;
        if gen <= 0
            || connection_gen <= 0
            || binding.subscription_id.is_empty()
            || binding.service_port == 0
        {
            return Err(Error::Refused);
        }
        let mut client = self.pool.get().await.map_err(unavailable)?;
        let tx = client.transaction().await.map_err(unavailable)?;
        tx.execute("INSERT INTO digitalmate_ingestion.workspaces(workspace_key) VALUES($1) ON CONFLICT DO NOTHING",&[&workspace]).await.map_err(unavailable)?;
        tx.query_one("SELECT workspace_key FROM digitalmate_ingestion.workspaces WHERE workspace_key=$1 FOR UPDATE",&[&workspace]).await.map_err(unavailable)?;
        if let Some(row)=tx.query_opt("SELECT workspace_key,generation,connection_generation,binding_json FROM digitalmate_ingestion.subscriptions WHERE subscription_id=$1 FOR UPDATE",&[&binding.subscription_id]).await.map_err(unavailable)? {
            if row.get::<_,String>(0)!=workspace || row.get::<_,i64>(1)>gen || row.get::<_,i64>(2)>connection_gen {return Err(Error::Refused);}
            if row.get::<_,i64>(1)==gen {
                if decode(&row.get::<_,String>(3))?!=*binding {return Err(Error::ConflictingInput);}
                return tx.commit().await.map_err(unavailable);
            }
            let used=tx.query_one("SELECT count(*)::bigint,COALESCE(sum(octet_length(raw)),0)::bigint FROM digitalmate_ingestion.events WHERE subscription_id=$1 AND state IN ('pending','batched')",&[&binding.subscription_id]).await.map_err(unavailable)?;
            tx.execute("UPDATE digitalmate_ingestion.capacity SET pending_events=pending_events-$1,pending_bytes=pending_bytes-$2 WHERE singleton=true",&[&used.get::<_,i64>(0),&used.get::<_,i64>(1)]).await.map_err(unavailable)?;
            tx.execute("UPDATE digitalmate_ingestion.events SET state='revoked',raw=''::bytea,finished_at=clock_timestamp() WHERE subscription_id=$1 AND state IN ('pending','batched')",&[&binding.subscription_id]).await.map_err(unavailable)?;
            tx.execute("UPDATE digitalmate_ingestion.batches SET state='revoked',payload=''::bytea,finished_at=clock_timestamp() WHERE subscription_id=$1 AND state='ready'",&[&binding.subscription_id]).await.map_err(unavailable)?;
            // Fence only this subscription's in-flight batch, not another
            // connection using the same workspace readiness lease.
            tx.execute("UPDATE digitalmate_ingestion.workspaces SET active_batch=NULL,lease_until=NULL,lease_epoch=lease_epoch+1 WHERE workspace_key=$1 AND active_batch IN (SELECT batch_id FROM digitalmate_ingestion.batches WHERE subscription_id=$2)",&[&workspace,&binding.subscription_id]).await.map_err(unavailable)?;
        }
        tx.execute("INSERT INTO digitalmate_ingestion.subscriptions(subscription_id,workspace_key,generation,connection_generation,trigger_id,binding_json,enabled) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(subscription_id) DO UPDATE SET generation=EXCLUDED.generation,connection_generation=EXCLUDED.connection_generation,trigger_id=EXCLUDED.trigger_id,binding_json=EXCLUDED.binding_json,enabled=EXCLUDED.enabled,due_at=NULL",&[&binding.subscription_id,&workspace,&gen,&connection_gen,&binding.trigger_id,&json,&binding.enabled]).await.map_err(unavailable)?;
        refresh_due(&tx, &workspace).await?;
        tx.commit().await.map_err(unavailable)
    }

    /// Success is observable only after commit. On unknown commit the caller
    /// retries the same signed event; the durable uniqueness key reconciles it.
    pub async fn receive(&self, event: VerifiedWebhook) -> Result<ReceiveReceipt, Error> {
        let mut client = self.pool.get().await.map_err(unavailable)?;
        let tx = client.transaction().await.map_err(unavailable)?;
        let first=tx.query_opt("SELECT workspace_key,subscription_id FROM digitalmate_ingestion.subscriptions WHERE trigger_id=$1",&[&event.trigger_id()]).await.map_err(unavailable)?.ok_or(Error::Refused)?;
        let workspace: String = first.get(0);
        let subscription: String = first.get(1);
        tx.query_one("SELECT workspace_key FROM digitalmate_ingestion.workspaces WHERE workspace_key=$1 FOR UPDATE",&[&workspace]).await.map_err(unavailable)?;
        let row=tx.query_one("SELECT binding_json,generation,enabled FROM digitalmate_ingestion.subscriptions WHERE subscription_id=$1 FOR UPDATE",&[&subscription]).await.map_err(unavailable)?;
        let binding = decode(&row.get::<_, String>(0))?;
        if !row.get::<_, bool>(2) {
            return Err(Error::Refused);
        }
        let pending = PendingInput::bind(event, &binding)?;
        let gen: i64 = row.get(1);
        if let Some(existing)=tx.query_opt("SELECT digest FROM digitalmate_ingestion.events WHERE subscription_id=$1 AND generation=$2 AND event_id=$3",&[&subscription,&gen,&pending.source_event_id()]).await.map_err(unavailable)? {
            if existing.get::<_,String>(0)!=pending.digest() {return Err(Error::ConflictingInput);}
            let receipt=ReceiveReceipt{source_event_id:pending.source_id,digest:pending.digest,duplicate:true};
            tx.commit().await.map_err(unavailable)?;return Ok(receipt);
        }
        let used=tx.query_one("SELECT pending_events,pending_bytes FROM digitalmate_ingestion.capacity WHERE singleton=true FOR UPDATE",&[]).await.map_err(unavailable)?;
        let size = i64::try_from(pending.raw.len()).map_err(|_| Error::Capacity)?;
        let per_subscription:i64=tx.query_one("SELECT count(*)::bigint FROM digitalmate_ingestion.events WHERE subscription_id=$1 AND state IN ('pending','batched')",&[&subscription]).await.map_err(unavailable)?.get(0);
        if used.get::<_, i64>(0) >= self.limits.max_pending_events
            || used.get::<_, i64>(1).saturating_add(size) > self.limits.max_pending_bytes
            || per_subscription >= self.limits.per_subscription_events
        {
            return Err(Error::Capacity);
        }
        tx.execute("INSERT INTO digitalmate_ingestion.events(subscription_id,generation,event_id,digest,raw,state) VALUES($1,$2,$3,$4,$5,'pending')",&[&subscription,&gen,&pending.source_id,&pending.digest,&pending.raw]).await.map_err(unavailable)?;
        tx.execute("UPDATE digitalmate_ingestion.capacity SET pending_events=pending_events+1,pending_bytes=pending_bytes+$1 WHERE singleton=true",&[&size]).await.map_err(unavailable)?;
        let delay = millis(self.limits.debounce)?;
        tx.execute("UPDATE digitalmate_ingestion.subscriptions SET due_at=LEAST(COALESCE(due_at,clock_timestamp()+$2::bigint*interval '1 millisecond'),clock_timestamp()+$2::bigint*interval '1 millisecond') WHERE subscription_id=$1",&[&subscription,&delay]).await.map_err(unavailable)?;
        refresh_due(&tx, &workspace).await?;
        let receipt = ReceiveReceipt {
            source_event_id: pending.source_id,
            digest: pending.digest,
            duplicate: false,
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(receipt)
    }

    pub async fn claim(&self, lease_duration: Duration) -> Result<Option<Lease>, Error> {
        let lease_ms = millis(lease_duration)?;
        if !(100..=300_000).contains(&lease_ms) {
            return Err(Error::InvalidLimit);
        }
        let mut client = self.pool.get().await.map_err(unavailable)?;
        let tx = client.transaction().await.map_err(unavailable)?;
        let Some(row)=tx.query_opt("SELECT workspace_key,active_batch FROM digitalmate_ingestion.workspaces WHERE due_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY last_attempt NULLS FIRST,due_at,workspace_key FOR UPDATE SKIP LOCKED LIMIT 1",&[]).await.map_err(unavailable)? else{return Ok(None)};
        let workspace: String = row.get(0);
        let active: Option<String> = row.get(1);
        let batch = if let Some(id) = active {
            let saved=tx.query_opt("SELECT payload FROM digitalmate_ingestion.batches WHERE batch_id=$1 AND state='ready'",&[&id]).await.map_err(unavailable)?.ok_or(Error::Refused)?;
            serde_json::from_slice::<DeliveryBatch>(&saved.get::<_, Vec<u8>>(0))
                .map_err(|_| Error::InvalidManifest)?
        } else {
            let Some(sub)=tx.query_opt("SELECT subscription_id,binding_json,generation FROM digitalmate_ingestion.subscriptions s WHERE workspace_key=$1 AND enabled AND due_at<=clock_timestamp() AND EXISTS(SELECT 1 FROM digitalmate_ingestion.events e WHERE e.subscription_id=s.subscription_id AND e.generation=s.generation AND e.state='pending') ORDER BY due_at,subscription_id LIMIT 1 FOR UPDATE",&[&workspace]).await.map_err(unavailable)? else {
                refresh_due(&tx,&workspace).await?;tx.commit().await.map_err(unavailable)?;return Ok(None)
            };
            let id: String = sub.get(0);
            let binding = decode(&sub.get::<_, String>(1))?;
            let gen: i64 = sub.get(2);
            let rows=tx.query("SELECT event_id,digest,raw FROM digitalmate_ingestion.events WHERE subscription_id=$1 AND generation=$2 AND state='pending' ORDER BY row_id LIMIT 50",&[&id,&gen]).await.map_err(unavailable)?;
            let inputs: Vec<_> = rows
                .iter()
                .map(|r| PendingInput {
                    binding: binding.clone(),
                    source_id: r.get(0),
                    digest: r.get(1),
                    raw: r.get(2),
                })
                .collect();
            let batch = DeliveryBatch::from_inputs(&inputs, &binding)?;
            let payload = serde_json::to_vec(&batch).map_err(|_| Error::InvalidManifest)?;
            tx.execute("INSERT INTO digitalmate_ingestion.batches(batch_id,workspace_key,subscription_id,generation,digest,payload,state) VALUES($1,$2,$3,$4,$5,$6,'ready')",&[&batch.batch_id,&workspace,&id,&gen,&batch.digest,&payload]).await.map_err(unavailable)?;
            let ids: Vec<_> = batch
                .events
                .iter()
                .map(|e| e.source_event_id.clone())
                .collect();
            tx.execute("UPDATE digitalmate_ingestion.events SET state='batched',batch_id=$1 WHERE subscription_id=$2 AND generation=$3 AND event_id=ANY($4) AND state='pending'",&[&batch.batch_id,&id,&gen,&ids]).await.map_err(unavailable)?;
            batch
        };
        batch.validate()?;
        let epoch:i64=tx.query_one("UPDATE digitalmate_ingestion.workspaces SET active_batch=$2,lease_epoch=lease_epoch+1,lease_until=clock_timestamp()+$3::bigint*interval '1 millisecond',last_attempt=clock_timestamp() WHERE workspace_key=$1 RETURNING lease_epoch",&[&workspace,&batch.batch_id,&lease_ms]).await.map_err(unavailable)?.get(0);
        tx.execute(
            "UPDATE digitalmate_ingestion.batches SET attempts=attempts+1 WHERE batch_id=$1",
            &[&batch.batch_id],
        )
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Some(Lease {
            workspace,
            epoch,
            batch,
        }))
    }

    pub async fn renew(&self, lease: &Lease, duration: Duration) -> Result<(), Error> {
        let ms = millis(duration)?;
        if !(100..=300_000).contains(&ms) {
            return Err(Error::InvalidLimit);
        }
        let client = self.pool.get().await.map_err(unavailable)?;
        let count=client.execute("UPDATE digitalmate_ingestion.workspaces SET lease_until=clock_timestamp()+$4::bigint*interval '1 millisecond' WHERE workspace_key=$1 AND lease_epoch=$2 AND active_batch=$3 AND lease_until>clock_timestamp()",&[&lease.workspace,&lease.epoch,&lease.batch.batch_id,&ms]).await.map_err(unavailable)?;
        if count != 1 {
            Err(Error::LeaseLost)
        } else {
            Ok(())
        }
    }

    pub async fn retry(&self, lease: &Lease, delay: Duration, reason: &str) -> Result<(), Error> {
        if ![
            "delivery_unknown",
            "upstream_unavailable",
            "rate_limited",
            "receiver_rejected",
        ]
        .contains(&reason)
            || delay > Duration::from_secs(3600)
        {
            return Err(Error::InvalidLimit);
        }
        let mut client = self.pool.get().await.map_err(unavailable)?;
        let tx = client.transaction().await.map_err(unavailable)?;
        assert_lease(&tx, lease).await?;
        let ms = millis(delay)?;
        tx.execute("UPDATE digitalmate_ingestion.workspaces SET lease_until=NULL,due_at=clock_timestamp()+$2::bigint*interval '1 millisecond' WHERE workspace_key=$1",&[&lease.workspace,&ms]).await.map_err(unavailable)?;
        tx.execute(
            "UPDATE digitalmate_ingestion.batches SET last_error=$2 WHERE batch_id=$1",
            &[&lease.batch.batch_id, &reason],
        )
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }

    pub async fn accept(&self, lease: &Lease, ack: &DurableAck) -> Result<(), Error> {
        if ack.version != 1
            || ack.batch_id != lease.batch.batch_id
            || ack.digest != lease.batch.digest
            || ack.state != "accepted"
        {
            return Err(Error::InvalidManifest);
        }
        let mut client = self.pool.get().await.map_err(unavailable)?;
        let tx = client.transaction().await.map_err(unavailable)?;
        assert_lease(&tx, lease).await?;
        let sub=tx.query_one("SELECT binding_json,enabled FROM digitalmate_ingestion.subscriptions WHERE subscription_id=$1",&[&lease.batch.binding.subscription_id]).await.map_err(unavailable)?;
        if !sub.get::<_, bool>(1) || decode(&sub.get::<_, String>(0))? != lease.batch.binding {
            return Err(Error::Refused);
        }
        let used=tx.query_one("SELECT count(*)::bigint,COALESCE(sum(octet_length(raw)),0)::bigint FROM digitalmate_ingestion.events WHERE batch_id=$1 AND state='batched'",&[&lease.batch.batch_id]).await.map_err(unavailable)?;
        tx.execute("UPDATE digitalmate_ingestion.capacity SET pending_events=pending_events-$1,pending_bytes=pending_bytes-$2 WHERE singleton=true",&[&used.get::<_,i64>(0),&used.get::<_,i64>(1)]).await.map_err(unavailable)?;
        tx.execute("UPDATE digitalmate_ingestion.events SET state='delivered',raw=''::bytea,finished_at=clock_timestamp() WHERE batch_id=$1 AND state='batched'",&[&lease.batch.batch_id]).await.map_err(unavailable)?;
        tx.execute("UPDATE digitalmate_ingestion.batches SET state='accepted',finished_at=clock_timestamp(),payload=''::bytea WHERE batch_id=$1",&[&lease.batch.batch_id]).await.map_err(unavailable)?;
        tx.execute("UPDATE digitalmate_ingestion.workspaces SET active_batch=NULL,lease_until=NULL WHERE workspace_key=$1",&[&lease.workspace]).await.map_err(unavailable)?;
        refresh_due(&tx, &lease.workspace).await?;
        tx.commit().await.map_err(unavailable)
    }

    /// Bounded terminal-metadata retention. Pending/in-flight events and raw
    /// inputs are never expired. Exact-replay guarantees cover the configured
    /// retention horizon; consumers retain their own durable effect receipts.
    pub async fn purge_terminal(&self, limit: i64) -> Result<u64, Error> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::InvalidLimit);
        }
        let mut client = self.pool.get().await.map_err(unavailable)?;
        let tx = client.transaction().await.map_err(unavailable)?;
        let age = millis(self.limits.dedupe_retention)?;
        let events=tx.execute("DELETE FROM digitalmate_ingestion.events WHERE row_id IN (SELECT row_id FROM digitalmate_ingestion.events WHERE state IN ('delivered','revoked') AND finished_at<clock_timestamp()-$1::bigint*interval '1 millisecond' ORDER BY finished_at,row_id FOR UPDATE SKIP LOCKED LIMIT $2)",&[&age,&limit]).await.map_err(unavailable)?;
        let batches=tx.execute("DELETE FROM digitalmate_ingestion.batches b WHERE batch_id IN (SELECT batch_id FROM digitalmate_ingestion.batches WHERE state IN ('accepted','revoked') AND finished_at<clock_timestamp()-$1::bigint*interval '1 millisecond' ORDER BY finished_at,batch_id FOR UPDATE SKIP LOCKED LIMIT $2) AND NOT EXISTS(SELECT 1 FROM digitalmate_ingestion.events e WHERE e.batch_id=b.batch_id)",&[&age,&limit]).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(events + batches)
    }
}

async fn assert_lease(tx: &Transaction<'_>, lease: &Lease) -> Result<(), Error> {
    let row=tx.query_opt("SELECT workspace_key FROM digitalmate_ingestion.workspaces WHERE workspace_key=$1 AND lease_epoch=$2 AND active_batch=$3 AND lease_until>clock_timestamp() FOR UPDATE",&[&lease.workspace,&lease.epoch,&lease.batch.batch_id]).await.map_err(unavailable)?;
    if row.is_some() {
        Ok(())
    } else {
        Err(Error::LeaseLost)
    }
}

async fn refresh_due(tx: &Transaction<'_>, workspace: &str) -> Result<(), Error> {
    tx.execute("UPDATE digitalmate_ingestion.workspaces w SET due_at=(SELECT min(s.due_at) FROM digitalmate_ingestion.subscriptions s WHERE s.workspace_key=w.workspace_key AND s.enabled AND EXISTS(SELECT 1 FROM digitalmate_ingestion.events e WHERE e.subscription_id=s.subscription_id AND e.generation=s.generation AND e.state='pending')) WHERE w.workspace_key=$1 AND w.active_batch IS NULL",&[&workspace]).await.map_err(unavailable)?;
    Ok(())
}
