use std::time::{Duration, Instant};

use futures_util::SinkExt;
use thiserror::Error;
use tokio::{select, sync::mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{config::AppConfig, discord::client::Client, storage::Storage};

const SOCKET_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("dial: {0}")]
    Dial(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("read: {0}")]
    Read(tokio_tungstenite::tungstenite::Error),
}

struct Backoff {
    min: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max,
            current: min,
        }
    }

    fn next(&mut self) -> Duration {
        let d = self.current;
        self.current = (self.current * 2).min(self.max);
        d
    }

    fn reset(&mut self) {
        self.current = self.min
    }
}

pub struct Gateway {
    config: AppConfig,
    storage: Storage,
}

impl Gateway {
    pub fn new(config: AppConfig, storage: Storage) -> Self {
        Self { config, storage }
    }

    pub async fn run(self, token: CancellationToken) {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(39));

        while !token.is_cancelled() {
            let connected_at = Instant::now();

            match self.connect_and_run(&token).await {
                Ok(()) => info!("connection closed"),
                Err(err) => warn!(error = %err, "connection dropped"),
            }

            if token.is_cancelled() {
                return;
            }

            if connected_at.elapsed() >= Duration::from_secs(10) {
                backoff.reset();
            }

            let wait = backoff.next();
            info!(?wait, "reconnecting to gateway");

            select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn connect_and_run(&self, token: &CancellationToken) -> Result<(), GatewayError> {
        use futures_util::StreamExt;

        let (ws_stream, _) = tokio_tungstenite::connect_async(SOCKET_URL).await?;
        info!("connected to gateway");

        let (mut write, mut read) = ws_stream.split();

        let (tx, mut rx) = mpsc::channel::<Message>(32);
        let writer = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Err(err) = write.send(msg).await {
                    warn!(error = %err, "write failed, closing writer task");
                    break;
                }
            }
        });

        let client = Client::new(self.config.clone(), self.storage.clone(), tx, token.clone());

        let result: Result<(), GatewayError> = loop {
            select! {
                _ = token.cancelled() => {
                    info!("shutting down connection");
                    break Ok(());
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => client.handle_message(text.as_bytes()).await,
                        Some(Ok(_)) => {}, // handled internally
                        Some(Err(err)) => break Err(GatewayError::Read(err)),
                        None => break Ok(()), // stream closed
                    }
                }
            }
        };

        drop(client);
        let _ = writer.await;

        result
    }
}
