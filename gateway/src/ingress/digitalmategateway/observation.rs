//! Jiuwen 适配器自有的业务观测组件：体验族指标 + 活跃去重。
//!
//! 业务语义只存在于本目录；通用 Ingress 通过 `MetricsExtension` 渲染，不认识
//! 任何体验/业务类型。PG 不可用时只禁用活跃采集，不阻塞启动。
use super::experience::{ExperienceAnchor, ExperienceMetrics};
use super::observability::{ActivitySink, Error as StorageError, ObservabilityConfig};
use adx_observability::metrics::Text;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// 启动时对暂时性数据库故障的重试次数（退避 1s/2s/3s/4s）。
const CONNECT_ATTEMPTS: u32 = 5;

pub struct Observation {
    metrics: Arc<ExperienceMetrics>,
    activity: Option<Arc<ActivitySink>>,
}

impl Observation {
    /// 从 `ADX_OBSERVABILITY_CONFIG` 构建；未配置时只保留直埋指标。
    pub async fn from_env(jiuwen_enabled: bool) -> Result<Arc<Self>, String> {
        let metrics = Arc::new(ExperienceMetrics::default());
        let Ok(path) = std::env::var("ADX_OBSERVABILITY_CONFIG") else {
            return Ok(Arc::new(Self {
                metrics,
                activity: None,
            }));
        };
        let bytes =
            std::fs::read(&path).map_err(|_| "invalid observability configuration".to_owned())?;
        let settings: ObservabilityConfig = serde_json::from_slice(&bytes)
            .map_err(|_| "invalid observability configuration".to_owned())?;
        settings
            .validate()
            .map_err(|_| "invalid observability configuration".to_owned())?;
        if !jiuwen_enabled {
            tracing::warn!(
                "ADX_OBSERVABILITY_CONFIG is set but the Jiuwen adapter is disabled: \
                 the activity sink has no producer and will stay empty"
            );
        }
        // 配置错误（缺 URL/密钥、schema 版本不符）必须拒绝启动；数据库暂时不可用
        // 先退避重试，仍失败才软降级——活跃族缺数，其余直埋照常。
        let mut attempt = 0;
        let activity = loop {
            match ActivitySink::connect(&settings).await {
                Ok(sink) => break Some(Arc::new(sink)),
                Err(StorageError::Configuration) => {
                    return Err("invalid observability storage configuration".to_owned());
                }
                Err(StorageError::Unavailable) => {
                    attempt += 1;
                    if attempt >= CONNECT_ATTEMPTS {
                        tracing::warn!(
                            attempts = attempt,
                            "activity sink disabled: PostgreSQL unavailable; restart required"
                        );
                        break None;
                    }
                    tracing::warn!(attempt, "activity sink connect failed; retrying");
                    tokio::time::sleep(Duration::from_secs(u64::from(attempt))).await;
                }
            }
        };
        Ok(Arc::new(Self { metrics, activity }))
    }

    /// 体验锚点：挂点从这里取指标与活跃依赖。
    pub fn anchor(
        &self,
        template_release: &str,
        started: Instant,
        tenant_ref: &str,
        user_id: &str,
    ) -> ExperienceAnchor {
        ExperienceAnchor::new(
            Arc::clone(&self.metrics),
            self.activity.clone(),
            template_release,
            started,
            tenant_ref,
            user_id,
        )
    }

    /// 上传成功：记一次无会话的业务活跃。上传不经过 Chat，活跃用户/DAU
    /// 不能只依赖 H3。
    pub fn record_activity(&self, template_release: &str, tenant: &str, user_id: &str) {
        let Some(activity) = &self.activity else {
            return;
        };
        // 上传活跃也走同一个 template_release 基数护栏；否则上传路径会绕过
        // `other` 截断，把无界模板值写进活跃表。
        let template = self.metrics.effective_template(template_release);
        let user_ref = activity.hmac_ref(tenant, user_id);
        activity.record(tenant, &user_ref, None, &template);
    }

    /// 活跃落库任务（服务启动时 spawn）；未启用 PG 时为 None。
    pub fn activity(&self) -> Option<Arc<ActivitySink>> {
        self.activity.clone()
    }
}

impl crate::ingress::server::MetricsExtension for Observation {
    fn render(&self, out: &mut Text) {
        self.metrics.render(out);
        if let Some(activity) = &self.activity {
            activity.render(out);
        }
    }
}
