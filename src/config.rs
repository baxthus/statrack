use config::{Config, Environment, File};
use garde::Validate;
use serde::Deserialize;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("validation error: {0}")]
    Validation(#[from] garde::Report),
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub(crate) struct AppConfig {
    #[garde(url)]
    pub redis_url: String,
}

impl AppConfig {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let settings = Config::builder()
            .add_source(File::with_name("config"))
            .add_source(Environment::default().separator("."))
            .build()?;

        let config = settings.try_deserialize::<AppConfig>()?;
        config.validate()?;

        info!("configuration loaded");
        Ok(config)
    }
}
