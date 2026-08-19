use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::fmt;
use tracing_subscriber::{EnvFilter, fmt::time::LocalTime};

use crate::config::AppConfig;
use crate::discord::gateway::Gateway;
use crate::storage::Storage;

mod config;
mod discord;
mod storage;

#[tokio::main]
async fn main() {
    setup_logging();

    let Ok(config) =
        AppConfig::new().inspect_err(|err| error!(error = %err, "failed to load configuration"))
    else {
        return;
    };

    let Ok(storage) = Storage::connect(config.redis_url.clone())
        .await
        .inspect_err(|err| error!(error = %err, "failed to connect to redis"))
    else {
        return;
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

    Gateway::new(config, storage).run(token).await;
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
