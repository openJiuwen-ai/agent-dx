use super::*;
use std::task::{Context, Poll};
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};

/// Reproduce an old node dropping the unknown request field and returning no ID,
/// or a faulty node returning a nonempty inconsistent ID, over the actual RPC.
#[derive(Clone)]
pub(super) struct NodeResponse {
    pub node: NodeRpc,
    pub identity: Arc<Mutex<Option<String>>>,
}

impl tonic::server::NamedService for NodeResponse {
    const NAME: &'static str = "adx.control.v1.NodeService";
}

impl<B> Service<http::Request<B>> for NodeResponse
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let identity = self.identity.lock().unwrap().clone();
        if request.uri().path() != "/adx.control.v1.NodeService/CreateEnvironment"
            || identity.is_none()
        {
            return pb::node_service_server::NodeServiceServer::new(self.node.clone())
                .call(request);
        }
        struct Create(NodeRpc, String);
        impl tonic::server::UnaryService<pb::StartAssignedEnvironmentRequest> for Create {
            type Response = pb::EnvironmentResult;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

            fn call(
                &mut self,
                mut request: Request<pb::StartAssignedEnvironmentRequest>,
            ) -> Self::Future {
                request.get_mut().creation_request_id.clear();
                let node = self.0.clone();
                let identity = self.1.clone();
                Box::pin(async move {
                    let mut response =
                        pb::node_service_server::NodeService::create_environment(&node, request)
                            .await?;
                    response
                        .get_mut()
                        .record
                        .as_mut()
                        .unwrap()
                        .creation_request_id = identity;
                    Ok(response)
                })
            }
        }
        let method = Create(self.node.clone(), identity.unwrap());
        Box::pin(async move {
            Ok(
                tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                    .unary(method, request)
                    .await,
            )
        })
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn central_create_handles_old_node_identity_and_recovers_rejected_results() {
    for identity in ["", "incorrect-request"] {
        let rig = Rig::new().await;
        *rig.node_creation_identity.lock().unwrap() = Some(identity.into());
        let mut client = rig.coordinator.clone();
        let mut request = create("mixed-node");
        request.request_id = "original-request".into();
        let result = client.create_environment(request.clone()).await;
        if identity.is_empty() {
            let response = result.unwrap().into_inner();
            assert!(response.newly_created);
            assert_eq!(
                response.record.unwrap().creation_request_id,
                request.request_id
            );
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert_eq!(error.message(), "node returned inconsistent result");
        }
        let stored = rig.session.get("mixed-node").await.unwrap();
        let running = stored.result.unwrap();
        assert_eq!(running.state, adx_core::EnvironmentState::Running);
        let retry = client
            .create_environment(request)
            .await
            .unwrap()
            .into_inner();
        assert!(!retry.newly_created);
        let record = retry.record.unwrap();
        assert_eq!(record.creation_request_id, "original-request");
        assert_eq!(
            record.assignment.unwrap().generation,
            running.assignment.generation
        );
        assert_eq!(record.runtime.unwrap().id, running.runtime.id);
        assert_eq!(
            rig.backends
                .iter()
                .map(|b| b.started.load(Ordering::SeqCst))
                .sum::<usize>(),
            1
        );
        assert!(rig
            .backends
            .iter()
            .all(|b| b.removed.load(Ordering::SeqCst) == 0));
    }
}
