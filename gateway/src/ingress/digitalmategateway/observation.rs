//! Jiuwen 适配器自有的业务观测组件：体验族指标 + 活跃去重。
//!
//! 业务语义只存在于本目录；通用 Ingress 通过 `MetricsExtension` 渲染，不认识
//! 任何体验/业务类型。PG 不可用时只禁用活跃采集，不阻塞启动。
use super::experience::{ExperienceAnchor, ExperienceMetrics};
use super::observability::{ActivitySink, ObservabilityConfig};
use adx_observability::metrics::Text;
use std::sync::Arc;
use tokio::time::Instant;

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
        let activity = match ActivitySink::connect(&settings).await {
            Ok(sink) => Some(Arc::new(sink)),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "activity sink disabled: PostgreSQL unavailable; startup continues"
                );
                None
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
