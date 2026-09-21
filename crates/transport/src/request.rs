use http::{header::HeaderName, HeaderMap, HeaderValue};

pub static REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
pub static OPERATION_ID: HeaderName = HeaderName::from_static("x-operation-id");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    pub request_id: String,
    pub operation_id: Option<String>,
}

impl RequestContext {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            request_id: header(headers, &REQUEST_ID)
                .filter(|value| !value.is_empty())
                .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned),
            operation_id: header(headers, &OPERATION_ID)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
        }
    }

    pub fn inject(&self, headers: &mut HeaderMap) -> Result<(), http::header::InvalidHeaderValue> {
        headers.insert(&REQUEST_ID, HeaderValue::from_str(&self.request_id)?);
        if let Some(operation_id) = &self.operation_id {
            headers.insert(&OPERATION_ID, HeaderValue::from_str(operation_id)?);
        }
        Ok(())
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_operation_identity_across_transport_boundaries() {
        let mut headers = HeaderMap::new();
        headers.insert(&REQUEST_ID, HeaderValue::from_static("request-a"));
        headers.insert(&OPERATION_ID, HeaderValue::from_static("operation-a"));
        let context = RequestContext::from_headers(&headers);
        assert_eq!(context.request_id, "request-a");
        assert_eq!(context.operation_id.as_deref(), Some("operation-a"));

        let mut forwarded = HeaderMap::new();
        context.inject(&mut forwarded).unwrap();
        assert_eq!(forwarded[&REQUEST_ID], "request-a");
        assert_eq!(forwarded[&OPERATION_ID], "operation-a");
    }

    #[test]
    fn creates_one_request_identity_when_upstream_has_none() {
        let context = RequestContext::from_headers(&HeaderMap::new());
        assert!(!context.request_id.is_empty());
        assert!(context.operation_id.is_none());
    }
}
