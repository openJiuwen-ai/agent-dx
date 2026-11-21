//! Fixed-upstream Huawei Push V3 transport. The gateway attaches the signed JWT
//! and sends the gateway-built body; it never selects the URL or
//! project from the request, and never retries a dispatched send.
use super::super::composio::Budget;
use super::jwt::Signer;
use super::{Error, Scope};
use reqwest::header::{HeaderValue, AUTHORIZATION};
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

const ORIGIN: &str = "https://push-api.cloud.huawei.com";
const MAX_RESPONSE: usize = 65_536;

pub(super) struct Client {
    http: reqwest::Client,
    signer: Arc<Signer>,
    project_id: String,
    budget: Arc<dyn Budget>,
}

impl Client {
    pub(super) fn new(
        project_id: String,
        signer: Arc<Signer>,
        budget: Arc<dyn Budget>,
    ) -> Result<Self, Error> {
        if project_id.is_empty() || !project_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Error::NotConfigured);
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(12))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::NotConfigured)?;
        Ok(Self {
            http,
            signer,
            project_id,
            budget,
        })
    }

    fn signed_request(&self, huawei: &Value) -> Result<reqwest::Request, Error> {
        let jwt = self.signer.jwt(SystemTime::now)?;
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", jwt.as_str()))
            .map_err(|_| Error::NotConfigured)?;
        authorization.set_sensitive(true);
        let url = format!("{ORIGIN}/v3/{}/messages:send", self.project_id);
        self.http
            .post(url)
            .header(AUTHORIZATION, authorization)
            .header("push-type", "0")
            .json(huawei)
            .build()
            .map_err(|_| Error::NotConfigured)
    }

    pub(super) async fn prepare(
        &self,
        scope: &Scope,
        huawei: &Value,
    ) -> Result<reqwest::Request, Error> {
        self.budget.acquire(scope).await?;
        scope.check_authentication().await.map_err(|error| {
            if error.status().is_server_error() {
                Error::UpstreamUnavailable
            } else {
                Error::Forbidden
            }
        })?;
        self.signed_request(huawei)
    }

    pub(super) async fn dispatch(
        &self,
        request: reqwest::Request,
        deadline: tokio::time::Instant,
    ) -> Result<Value, Error> {
        delivery_until(deadline, self.http.execute(request)).await
    }
}

async fn delivery_until(
    deadline: tokio::time::Instant,
    dispatch: impl std::future::Future<Output = reqwest::Result<reqwest::Response>>,
) -> Result<Value, Error> {
    tokio::time::timeout_at(deadline, async {
        let mut response = dispatch.await.map_err(|_| Error::OutcomeUnknown)?;
        let status = response.status().as_u16();
        if matches!(status, 401 | 403) {
            tracing::warn!(status, "Huawei Push rejected gateway credentials or project permissions");
            return Err(Error::UpstreamUnavailable);
        }
        if status != 200 && status != 400 {
            return Err(Error::OutcomeUnknown);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::OutcomeUnknown)? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err(Error::OutcomeUnknown);
            }
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&bytes).map_err(|_| Error::OutcomeUnknown)?;
        let code = match body.get("code") {
            Some(Value::String(code)) => code.clone(),
            Some(Value::Number(code)) => code.to_string(),
            _ => return Err(Error::OutcomeUnknown),
        };
        match (status, code.as_str()) {
            (200, "80000000") => Ok(json!({"result":"accepted","provider_code":code})),
            (400, "80100003") | (_, "80300007" | "80300008") => Ok(json!({"result":"rejected","provider_code":code,"http_status":status})),
            (_, "80200005" | "80300002" | "80300036") => {
                tracing::warn!(provider_code = %code, "Huawei Push rejected gateway credentials or project permissions");
                Err(Error::UpstreamUnavailable)
            }
            _ => Err(Error::OutcomeUnknown),
        }
    }).await.map_err(|_| Error::OutcomeUnknown)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    struct TestBudget;

    #[async_trait::async_trait]
    impl Budget for TestBudget {
        async fn acquire(&self, _: &Scope) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn outbound_request_carries_gateway_signed_jwt_and_notification() {
        let signer = Signer::from_pem(
            include_str!("../../../../tests/fixtures/huawei-test-key.pem"),
            "test-key".into(),
            "test-issuer".into(),
        )
        .unwrap();
        let client = Client::new("123".into(), Arc::new(signer), Arc::new(TestBudget)).unwrap();
        let body = json!({"message": {"token": ["test-device"]}});
        let request = client.signed_request(&body).unwrap();

        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(
            request.url().as_str(),
            "https://push-api.cloud.huawei.com/v3/123/messages:send"
        );
        assert_eq!(request.headers()["push-type"], "0");
        let authorization = &request.headers()[AUTHORIZATION];
        assert!(authorization.is_sensitive());
        let token = authorization
            .to_str()
            .unwrap()
            .strip_prefix("Bearer ")
            .unwrap();
        let parts: Vec<_> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let decode = |part| {
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(part)
                .unwrap();
            serde_json::from_slice::<Value>(&bytes).unwrap()
        };
        assert_eq!(decode(parts[0])["alg"], "PS256");
        assert_eq!(decode(parts[1])["iss"], "test-issuer");
        assert!(!parts[2].is_empty());
        let sent: Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(sent, body);
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    use tokio::time::Instant;

    async fn deliver(status: u16, body: Vec<u8>) -> Result<Value, Error> {
        let response = ::http::Response::builder()
            .status(status)
            .body(body)
            .unwrap();
        delivery_until(Instant::now() + Duration::from_secs(20), async {
            Ok(response.into())
        })
        .await
    }

    #[tokio::test]
    async fn classifies_only_definitive_provider_results() {
        assert_eq!(
            deliver(200, br#"{"code":"80000000"}"#.to_vec())
                .await
                .unwrap()["result"],
            "accepted"
        );
        assert_eq!(
            deliver(200, br#"{"code":"80300007"}"#.to_vec())
                .await
                .unwrap()["result"],
            "rejected"
        );
        for status in [401, 403] {
            assert_eq!(
                deliver(status, vec![]).await,
                Err(Error::UpstreamUnavailable)
            );
        }
        assert_eq!(
            deliver(200, br#"{"code":"80200005"}"#.to_vec()).await,
            Err(Error::UpstreamUnavailable)
        );
        for status in [408, 429, 500, 503] {
            assert_eq!(
                deliver(status, br#"{"code":"80000000"}"#.to_vec()).await,
                Err(Error::OutcomeUnknown)
            );
        }
        for body in [
            b"{}".to_vec(),
            b"invalid".to_vec(),
            br#"{"code":"81000001"}"#.to_vec(),
            vec![0; MAX_RESPONSE + 1],
        ] {
            assert_eq!(deliver(200, body).await, Err(Error::OutcomeUnknown));
        }
    }

    #[tokio::test]
    async fn malformed_message_rejection_requires_the_documented_status_and_code() {
        let body = br#"{"code":"80100003"}"#.to_vec();
        assert_eq!(
            deliver(400, body.clone()).await.unwrap(),
            json!({"result":"rejected","provider_code":"80100003","http_status":400})
        );
        assert_eq!(deliver(200, body).await, Err(Error::OutcomeUnknown));
        assert_eq!(
            deliver(400, br#"{"code":"81000001"}"#.to_vec()).await,
            Err(Error::OutcomeUnknown)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_after_dispatch_preserves_unknown() {
        let deadline = Instant::now() + Duration::from_secs(20);
        let delayed = async {
            tokio::time::sleep(Duration::from_secs(21)).await;
            Ok(::http::Response::new(Vec::<u8>::new()).into())
        };
        assert_eq!(
            delivery_until(deadline, delayed).await,
            Err(Error::OutcomeUnknown)
        );
    }
}
