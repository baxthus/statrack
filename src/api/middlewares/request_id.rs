use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use nanoid::nanoid;
use tracing::Instrument;

static X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone)]
pub(crate) struct RequestId(String);

impl RequestId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

pub async fn request_id(mut req: Request, next: Next) -> Response {
    let request_id = RequestId(nanoid!());

    req.extensions_mut().insert(request_id.clone());

    let span = tracing::info_span!("request", request_id = %request_id.as_str());
    let mut res = next.run(req).instrument(span).await;

    if let Ok(value) = HeaderValue::from_str(request_id.as_str()) {
        res.headers_mut().insert(X_REQUEST_ID.clone(), value);
    }

    res
}
