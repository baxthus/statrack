use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::fmt;
use tracing_subscriber::{EnvFilter, fmt::time::LocalTime};

use crate::config::AppConfig;
use crate::discord::gateway::Gateway;
use crate::storage::Storage;

pub(crate) mod api;
mod config;
mod discord;
mod storage;

#[tokio::main]
async fn main() {
    setup_logging();

    let config = match AppConfig::new() {
        Ok(config) => config,
        Err(err) => {
            error!(error = %err, "failed to load configuration");
            return;
        }
    };

    let storage = match Storage::connect(config.redis_url.clone()).await {
        Ok(storage) => storage,
        Err(err) => {
            error!(error = %err, "failed to connect to redis");
            return;
        }
    };

    let token = CancellationToken::new();
    {
        let token = token.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("shutting down");
            token.cancel();
        });
    }

    let gateway = {
        let token = token.clone();
        let config = config.clone();
        let storage = storage.clone();
        tokio::spawn(async move {
            Gateway::new(config, storage).run(token).await;
        })
    };

    if let Err(err) = api::new(config, storage, token.clone()).await {
        error!(error = %err, "failed to start api");
    }

    token.cancel();
    if let Err(err) = gateway.await {
        error!(error = %err, "gateway task failed");
    }
}

fn setup_logging() {
    use time::macros::format_description;

    let filter = EnvFilter::builder().try_from_env().unwrap_or(
        EnvFilter::builder()
            .with_default_directive(tracing::Level::WARN.into())
            .parse("statrack=debug")
            .unwrap(),
    );

    let timer = LocalTime::new(format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second]"
    ));

    fmt()
        .with_env_filter(filter)
        .with_timer(timer)
        .with_target(false)
        .pretty()
        .init();
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

    select! {
        _ = ctrl_c => {},
        _ = sigterm.recv() => {},
    }
}
