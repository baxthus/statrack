use std::time::{Duration, Instant};

use futures_util::SinkExt;
use thiserror::Error;
use tokio::{select, sync::mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    config::AppConfig,
    discord::client::{Client, ClientAction},
    storage::Storage,
};

const SOCKET_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("dial: {0}")]
    Dial(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("read: {0}")]
    Read(tokio_tungstenite::tungstenite::Error),
    #[error("write: {0}")]
    Write(tokio_tungstenite::tungstenite::Error),
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

        let (ws_stream, _) = select! {
            _ = token.cancelled() => return Ok(()),
            result = tokio_tungstenite::connect_async(SOCKET_URL) => result?,
        };
        info!("connected to gateway");

        let connection_token = token.child_token();
        let (mut write, mut read) = ws_stream.split();

        let (tx, mut rx) = mpsc::channel::<Message>(32);
        let mut writer = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                write.send(msg).await?;
            }
            Ok::<_, tokio_tungstenite::tungstenite::Error>(())
        });

        let client = Client::new(
            self.config.clone(),
            self.storage.clone(),
            tx,
            connection_token.clone(),
        );

        let result: Result<(), GatewayError> = loop {
            select! {
                _ = connection_token.cancelled() => {
                    if token.is_cancelled() {
                        info!("shutting down connection");
                    } else {
                        warn!("connection cancelled; reconnecting");
                    }
                    break Ok(());
                }
                writer_result = &mut writer => {
                    match writer_result {
                        Ok(Ok(())) => break Ok(()),
                        Ok(Err(err)) => break Err(GatewayError::Write(err)),
                        Err(err) => {
                            warn!(error = %err, "writer task failed");
                            break Ok(());
                        }
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if client.handle_message(text.as_bytes()).await == ClientAction::Reconnect {
                                break Ok(());
                            }
                        }
                        Some(Ok(_)) => {}, // handled internally
                        Some(Err(err)) => break Err(GatewayError::Read(err)),
                        None => break Ok(()), // stream closed
                    }
                }
            }
        };

        client.shutdown().await;
        writer.abort();
        let _ = writer.await;

        result
    }
}
