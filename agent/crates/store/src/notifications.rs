//! Keyspace notifications only wake reconciliation; durable records remain authoritative.
use crate::{Error, RedisRepository, Result};
use adx_agent_core::{Scope, TemplateRef};
use futures_util::StreamExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataChange {
    Alias {
        tenant: String,
        template: String,
        alias: String,
    },
    Binding(Scope),
}

pub struct RedisChanges {
    subscription: redis::aio::PubSub,
    prefix: String,
}
impl RedisChanges {
    /// A disconnected subscription is an error: reconnect then scan, never assume no changes.
    pub async fn recv(&mut self) -> Result<MetadataChange> {
        let channel = {
            let mut messages = self.subscription.on_message();
            let message = messages.next().await.ok_or_else(|| {
                Error::Unavailable("Redis change subscription disconnected".into())
            })?;
            message.get_channel_name().to_owned()
        };
        let key = channel
            .strip_prefix(&self.prefix)
            .ok_or_else(|| Error::Corrupt("unexpected change channel".into()))?;
        let (kind, encoded) = key
            .split_once(':')
            .ok_or_else(|| Error::Corrupt("invalid change key".into()))?;
        let bytes: Vec<u8> = encoded
            .as_bytes()
            .chunks(2)
            .map(|pair| {
                let raw = std::str::from_utf8(pair)
                    .map_err(|_| Error::Corrupt("invalid change key encoding".into()))?;
                if pair.len() != 2 {
                    return Err(Error::Corrupt("invalid change key encoding".into()));
                }
                u8::from_str_radix(raw, 16)
                    .map_err(|_| Error::Corrupt("invalid change key encoding".into()))
            })
            .collect::<Result<_>>()?;
        let parts: Vec<String> = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Corrupt("invalid change key tuple".into()))?;
        match (kind, parts.as_slice()) {
            ("alias", [tenant, template, alias]) => Ok(MetadataChange::Alias {
                tenant: tenant.clone(),
                template: template.clone(),
                alias: alias.clone(),
            }),
            ("binding", [tenant, template, kind, name, binding_id]) => {
                let template_ref = match kind.as_str() {
                    "version" => TemplateRef::Version(name.clone()),
                    "alias" => TemplateRef::Alias(name.clone()),
                    _ => {
                        return Err(Error::Corrupt(
                            "invalid template reference in notification".into(),
                        ))
                    }
                };
                let scope = Scope {
                    tenant: tenant.clone(),
                    template: template.clone(),
                    template_ref,
                    binding_id: binding_id.clone(),
                };
                scope.validate().map_err(Error::Corrupt)?;
                Ok(MetadataChange::Binding(scope))
            }
            _ => Err(Error::Corrupt("invalid metadata notification".into())),
        }
    }
}
impl RedisRepository {
    /// Confirm subscriptions before callers scan current state. Redis must enable K, $, and g.
    pub async fn subscribe_changes(&self) -> Result<RedisChanges> {
        let mut command = redis::cmd("CONFIG");
        command.arg("GET").arg("notify-keyspace-events");
        let values: Vec<String> = self.execute(command, false).await?;
        let flags = values
            .get(1)
            .ok_or_else(|| Error::Unavailable("missing Redis notification configuration".into()))?;
        if !flags.contains('K')
            || (!flags.contains('A') && (!flags.contains('$') || !flags.contains('g')))
        {
            return Err(Error::Invalid(
                "Redis requires notify-keyspace-events K$g for alias reconciliation".into(),
            ));
        }
        let prefix = format!(
            "__keyspace@{}__:{}",
            self.client.get_connection_info().redis_settings().db(),
            self.prefix
        );
        tokio::time::timeout(self.timeout, async {
            let mut subscription =
                self.client.get_async_pubsub().await.map_err(|_| {
                    Error::Unavailable("Redis subscription connection failed".into())
                })?;
            subscription
                .psubscribe(vec![
                    format!("{prefix}alias:*"),
                    format!("{prefix}binding:*"),
                ])
                .await
                .map_err(|_| Error::Unavailable("Redis subscription setup failed".into()))?;
            Ok(RedisChanges {
                subscription,
                prefix,
            })
        })
        .await
        .map_err(|_| Error::Unavailable("Redis subscription setup timed out".into()))?
    }
}
