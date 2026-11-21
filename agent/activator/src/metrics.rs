//! Optional private Prometheus endpoint for Agent cache counters.
//!
//! This listener has no service authentication, mirroring the other private
//! metrics endpoints: it must bind a deployment-private monitoring interface.
use adx_agent_core::cache::{CacheKind, CacheMetrics, CacheResult};
use adx_observability::metrics::Text;
use axum::{extract::State, http::header, response::IntoResponse, routing::get, Router};
use std::sync::Arc;

/// Serve `GET /metrics`. The caller binds the address at startup so a bad
/// monitoring address fails startup instead of silently disabling collection.
pub async fn serve(
    listener: tokio::net::TcpListener,
    metrics: Arc<CacheMetrics>,
) -> std::io::Result<()> {
    let app = Router::new()
        .route("/metrics", get(render))
        .with_state(metrics);
    axum::serve(listener, app).await
}

async fn render(State(metrics): State<Arc<CacheMetrics>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        render_text(&metrics),
    )
}

/// Dense export: all 2 × 4 combinations exist from process start, so an
/// unobserved branch is a real zero rather than `No data`.
fn render_text(metrics: &CacheMetrics) -> String {
    let mut text = Text::default();
    for cache in CacheKind::ALL {
        for result in CacheResult::ALL {
            text.counter(
                "adx_agent_cache_access_total",
                "Agent cache lookups by cache kind and result; hit ratio uses hit/(hit+miss).",
                &[
                    ("cache", cache.as_str().to_owned()),
                    ("result", result.as_str().to_owned()),
                ],
                metrics.count(cache, result),
            );
        }
    }
    text.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_dense_and_declared_once() {
        let metrics = CacheMetrics::default();
        metrics.record(CacheKind::Binding, CacheResult::Hit);
        let text = render_text(&metrics);
        assert_eq!(
            text.matches("# TYPE adx_agent_cache_access_total counter")
                .count(),
            1
        );
        assert_eq!(
            text.matches("# HELP adx_agent_cache_access_total").count(),
            1
        );
        assert!(text.contains("adx_agent_cache_access_total{cache=\"binding\",result=\"hit\"} 1\n"));
        for cache in ["binding", "template"] {
            for result in ["hit", "miss", "bypass", "error"] {
                assert!(
                    text.contains(&format!(
                        "adx_agent_cache_access_total{{cache=\"{cache}\",result=\"{result}\"}} "
                    )),
                    "missing {cache}/{result}"
                );
            }
        }
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with("adx_agent_cache_access_total{"))
                .count(),
            8
        );
    }

    #[tokio::test]
    async fn endpoint_serves_counters_without_service_authentication() {
        let metrics = Arc::new(CacheMetrics::default());
        metrics.record(CacheKind::Template, CacheResult::Miss);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(serve(listener, metrics));
        let response = reqwest::get(format!("http://{address}/metrics"))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body = response.text().await.unwrap();
        assert!(
            body.contains("adx_agent_cache_access_total{cache=\"template\",result=\"miss\"} 1\n")
        );
        let missing = reqwest::get(format!("http://{address}/other"))
            .await
            .unwrap();
        assert_eq!(missing.status(), 404);
        task.abort();
    }
}
