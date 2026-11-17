//! Authenticated forwarding of the bounded managed-image domain contract.
use crate::environment_service::EnvironmentService;
use adx_core::managed_images::ImageMutation;
use adx_protocol::control as pb;
use serde_json::Value;
use std::collections::HashMap;
use tonic::Status;

impl EnvironmentService {
    pub async fn managed_image_request(
        &self,
        method: &str,
        id: Option<&str>,
        input: Value,
        query: &HashMap<String, String>,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let tenant_id = if let Some(tenant) = query.get("tenant") {
            if !caller.administrator || tenant.is_empty() {
                return Err(Status::permission_denied(
                    "managed image tenant selection requires an administrator",
                ));
            }
            tenant.clone()
        } else {
            caller.tenant_id.clone()
        };
        let mut client = pb::managed_image_service_client::ManagedImageServiceClient::new(
            self.clients.coordinator().await?,
        );
        let response = match method {
            "POST" => {
                let mutation: ImageMutation = serde_json::from_value(input)
                    .map_err(|_| Status::invalid_argument("invalid managed image mutation"))?;
                self.clients
                    .rpc(
                        "apiserver.managed_image_mutation",
                        client.mutate_managed_image(adx_observability::trace::inject(
                            pb::ManagedImageMutationRequest {
                                tenant_id: tenant_id.clone(),
                                id: id.unwrap_or_default().into(),
                                caller: Some(caller.clone()),
                                mutation_json: serde_json::to_vec(&mutation).map_err(|_| {
                                    Status::invalid_argument("invalid managed image mutation")
                                })?,
                            },
                        )),
                    )
                    .await?
            }
            "GET" => {
                self.clients
                    .rpc(
                        "apiserver.managed_images",
                        client.get_managed_images(adx_observability::trace::inject(
                            pb::ManagedImageQuery {
                                tenant_id,
                                id: id.unwrap_or_default().into(),
                                caller: Some(caller.clone()),
                                after: query.get("after").cloned().unwrap_or_default(),
                                limit: query
                                    .get("limit")
                                    .map(|v| {
                                        v.parse().map_err(|_| {
                                            Status::invalid_argument("invalid image page size")
                                        })
                                    })
                                    .transpose()?
                                    .unwrap_or(100),
                            },
                        )),
                    )
                    .await?
            }
            _ => return Err(Status::unimplemented("unsupported managed image method")),
        };
        serde_json::from_slice(&response.view_json)
            .map_err(|_| Status::data_loss("invalid managed image response"))
    }
}
