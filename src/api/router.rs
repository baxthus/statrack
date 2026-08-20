use std::sync::Arc;

use axum::{Router, middleware, routing::get};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{api::middlewares, config::AppConfig, storage::Storage};

pub struct RouterState {
    pub storage: Storage,
}

pub async fn new(
    config: AppConfig,
    storage: Storage,
    token: CancellationToken,
) -> Result<(), std::io::Error> {
    let shared_state = Arc::new(RouterState { storage });

    let app = Router::new()
        .route("/", get(|| async { "Hello, World" }))
        .layer(middleware::from_fn(middlewares::request_id))
        .with_state(shared_state);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.api.port)).await?;

    info!(port = %config.api.port, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(token.cancelled_owned())
        .await?;

    Ok(())
}
