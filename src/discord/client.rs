use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;
use tokio::{select, sync::mpsc::Sender, task::JoinHandle, time::MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{config::AppConfig, discord::types::Opcode, storage::Storage};

#[derive(Debug, Error)]
pub(crate) enum ClientError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("send error: {0}")]
    Send(#[from] tokio::sync::mpsc::error::SendError<WsMessage>),
    #[error("gateway protocol error: {0}")]
    Protocol(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientAction {
    Continue,
    Reconnect,
}

#[derive(Debug, Deserialize)]
struct GatewayMessage<'a> {
    op: Opcode,
    #[serde(borrow, default)]
    d: Option<&'a RawValue>,
    #[serde(default)]
    t: Option<String>,
    #[serde(default)]
    s: Option<i64>,
}

#[derive(Debug, Serialize)]
struct OutgoingMessage<T: Serialize> {
    op: Opcode,
    d: T,
}

#[derive(Debug, Deserialize)]
struct HelloData {
    heartbeat_interval: u64,
}

#[derive(Debug, Serialize)]
struct IdentifyProperties {
    os: &'static str,
    browser: &'static str,
    device: &'static str,
}

#[derive(Debug, Serialize)]
struct IdentifyData {
    token: String,
    intents: u32,
    properties: IdentifyProperties,
}

#[derive(Clone)]
pub struct Client {
    config: AppConfig,
    storage: Storage,
    tx: Sender<WsMessage>,
    token: CancellationToken,
    sequence: Arc<Mutex<Option<i64>>>,
    heartbeat_started: Arc<AtomicBool>,
    awaiting_heartbeat_ack: Arc<AtomicBool>,
    heartbeat_task: Arc<Mutex<Option<JoinHandle<()>>>>,
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
            heartbeat_started: Arc::new(AtomicBool::new(false)),
            awaiting_heartbeat_ack: Arc::new(AtomicBool::new(false)),
            heartbeat_task: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn handle_message(&self, raw: &[u8]) -> ClientAction {
        let msg = match serde_json::from_slice::<GatewayMessage>(raw) {
            Ok(msg) => msg,
            Err(err) => {
                error!(error = %err, "failed to decode gateway message");
                return ClientAction::Reconnect;
            }
        };

        if let Some(seq) = msg.s {
            *self.sequence.lock().unwrap() = Some(seq);
        }

        let result = match (msg.op, msg.t.as_deref()) {
            (Opcode::Hello, _) => self.handle_hello(msg.d).await,
            (Opcode::Heartbeat, _) => self.send_heartbeat().await,
            (Opcode::HeartbeatAck, _) => {
                self.awaiting_heartbeat_ack.store(false, Ordering::Release);
                Ok(())
            }
            (Opcode::Reconnect | Opcode::InvalidSession, _) => Ok(()),
            _ => {
                warn!(op = ?msg.op, t = ?msg.t, "received unhandled message");
                return ClientAction::Continue;
            }
        };

        if let Err(err) = result {
            error!(error = %err, "failed to handle gateway message");
            return ClientAction::Reconnect;
        }

        match msg.op {
            Opcode::Reconnect | Opcode::InvalidSession => ClientAction::Reconnect,
            _ => ClientAction::Continue,
        }
    }

    pub async fn shutdown(&self) {
        self.token.cancel();
        let heartbeat_task = self.heartbeat_task.lock().unwrap().take();
        if let Some(heartbeat_task) = heartbeat_task {
            heartbeat_task.abort();
            let _ = heartbeat_task.await;
        }
    }

    async fn write_json<T: Serialize>(&self, op: Opcode, d: T) -> Result<(), ClientError> {
        let text = serde_json::to_string(&OutgoingMessage { op, d })?;
        self.tx.send(WsMessage::text(text)).await?;
        Ok(())
    }

    async fn handle_hello(&self, data: Option<&RawValue>) -> Result<(), ClientError> {
        if self.heartbeat_started.swap(true, Ordering::AcqRel) {
            return Err(ClientError::Protocol("received duplicate hello"));
        }

        let data = data.ok_or(ClientError::Protocol("hello message missing data"))?;
        let hello = serde_json::from_str::<HelloData>(data.get())?;
        if hello.heartbeat_interval == 0 {
            return Err(ClientError::Protocol(
                "hello message has zero heartbeat interval",
            ));
        }

        info!(
            heartbeat_interval = hello.heartbeat_interval,
            "received hello message"
        );

        self.send_identify().await?;

        let client = self.clone();
        let heartbeat_task = tokio::spawn(async move {
            client
                .heartbeat_loop(Duration::from_millis(hello.heartbeat_interval))
                .await;
        });
        *self.heartbeat_task.lock().unwrap() = Some(heartbeat_task);

        Ok(())
    }

    async fn send_identify(&self) -> Result<(), ClientError> {
        let data = IdentifyData {
            token: self.config.discord.bot_token.clone(),
            intents: 1 << 0 | 1 << 1 | 1 << 8, // GUILDS | GUILD_MEMBERS | GUILD_PRESENCES
            properties: IdentifyProperties {
                os: "linux",
                browser: "statrack",
                device: "statrack",
            },
        };

        self.write_json(Opcode::Identify, data).await
    }

    async fn heartbeat_loop(self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            select! {
                _ = self.token.cancelled() => {
                    info!("heartbeat loop cancelled");
                    return;
                }
                _ = ticker.tick() => {
                    if self.awaiting_heartbeat_ack.load(Ordering::Acquire) {
                        warn!("gateway heartbeat acknowledgement missing");
                        self.token.cancel();
                        return;
                    }
                    if let Err(err) = self.send_heartbeat().await {
                        error!(error = %err, "failed to send heartbeat");
                        self.token.cancel();
                        return;
                    }
                }
            }
        }
    }

    async fn send_heartbeat(&self) -> Result<(), ClientError> {
        let seq = *self.sequence.lock().unwrap();
        self.awaiting_heartbeat_ack.store(true, Ordering::Release);
        self.write_json(Opcode::Heartbeat, seq).await?;
        info!("sent heartbeat message");
        Ok(())
    }
}
