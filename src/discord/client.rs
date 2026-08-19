use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{config::AppConfig, storage::Storage};

#[derive(Debug, Error)]
enum ClientError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("send error: channel closed")]
    Send(),
}

#[derive(Clone)]
pub struct Client {
    config: AppConfig,
    storage: Storage,
    tx: Sender<WsMessage>,
    token: CancellationToken,
    sequence: Arc<Mutex<Option<i64>>>,
}

impl Client {
    pub fn new(
        config: AppConfig,
        storage: Storage,
        tx: Sender<WsMessage>,
        token: CancellationToken,
    ) -> Self {
        Self {
            config,
            storage,
            tx,
            token,
            sequence: Arc::new(Mutex::new(None)),
        }
    }

    #[instrument(skip_all)]
    pub async fn handle_message(&self, raw: &[u8]) {
        todo!()
    }
}
