use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use serde::Serialize;

use crate::api::{
    errors::ApiError,
    router::RouterState,
    types::{DISCORD_USER_ID_REGEX, ResponseData},
};

#[derive(Debug, Clone, Serialize)]
pub struct UserResponse {
    pub success: bool,
    pub data: ResponseData,
}

pub async fn get_user(
    State(state): State<Arc<RouterState>>,
    Path(id): Path<String>,
) -> Result<Json<UserResponse>, ApiError> {
    if !DISCORD_USER_ID_REGEX.is_match(&id) {
        return Err(ApiError::BadRequest("invalid user id".to_string()));
    }

    let (user, presence) = tokio::try_join!(
        async {
            state
                .storage
                .get_user(&id)
                .await?
                .ok_or_else(|| ApiError::NotFound("user not found".to_string()))
        },
        async {
            state
                .storage
                .get_presence(&id)
                .await?
                .ok_or_else(|| ApiError::NotFound("presence not found".to_string()))
        },
    )?;

    let data = UserResponse {
        success: true,
        data: ResponseData::build(user, presence),
    };

    Ok(Json(data))
}
