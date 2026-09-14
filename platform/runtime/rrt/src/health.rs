use crate::pb::health_server::Health;
use crate::pb::{HealthCheckRequest, HealthCheckResponse};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct HealthSvc;

#[tonic::async_trait]
impl Health for HealthSvc {
    async fn check(
        &self,
        _req: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }
}
