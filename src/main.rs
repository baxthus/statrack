use tracing::{error, info};
use tracing_subscriber::fmt;
use tracing_subscriber::{EnvFilter, fmt::time::LocalTime};

use crate::config::AppConfig;

mod config;

fn main() {
    setup_logging();

    let _config = match AppConfig::new() {
        Ok(config) => config,
        Err(err) => {
            error!(error = %err, "failed to load configuration");
            return;
        }
    };

    info!("Hello, World!");
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
