use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::error;

use crate::storage::StorageError;

pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Internal(anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg).into_response(),
            ApiError::Internal(err) => {
                error!(error = %err, "internal server error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response()
            }
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        ApiError::Internal(err.into())
    }
}
