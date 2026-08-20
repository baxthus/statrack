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

use crate::{
    config::AppConfig,
    discord::types::{Opcode, Presence, User, event_type},
    storage::Storage,
};

#[derive(Debug, Error)]
pub(crate) enum ClientError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("send error: {0}")]
    Send(#[from] tokio::sync::mpsc::error::SendError<WsMessage>),
    #[error("gateway protocol error: {0}")]
    Protocol(&'static str),
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
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

#[derive(Debug, Deserialize)]
struct ReadyData {
    session_id: String,
    user: User,
}

#[derive(Debug, Deserialize)]
struct GuildMember {
    user: User,
}

#[derive(Debug, Deserialize)]
struct GuildCreateData {
    id: String,
    #[serde(default)]
    members: Vec<GuildMember>,
    #[serde(default)]
    presences: Vec<Presence>,
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
                info!("received heartbeat ack");
                self.awaiting_heartbeat_ack.store(false, Ordering::Release);
                Ok(())
            }
            (Opcode::Reconnect | Opcode::InvalidSession, _) => Ok(()),

            (Opcode::Dispatch, Some(event_type::READY)) => self.handle_ready(msg.d).await,
            (Opcode::Dispatch, Some(event_type::GUILD_CREATE)) => {
                self.handle_guild_create(msg.d).await
            }
            (Opcode::Dispatch, Some(event_type::PRESENCE_UPDATE)) => {
                self.handle_presence_update(msg.d).await
            }

            _ => {
                warn!(op = ?msg.op, t = ?msg.t, "received unhandled message");
                return ClientAction::Continue;
            }
        };

        if let Err(err) = result {
            match err {
                ClientError::Storage(err) => {
                    error!(error = %err, "failed to persist gateway event");
                    return ClientAction::Continue;
                }
                err => {
                    error!(error = %err, "failed to handle gateway message");
                    return ClientAction::Reconnect;
                }
            }
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

    async fn handle_ready(&self, data: Option<&RawValue>) -> Result<(), ClientError> {
        info!("received ready event");

        let data = data.ok_or(ClientError::Protocol("received ready event with no data"))?;
        let ready = serde_json::from_str::<ReadyData>(data.get())?;

        info!(session_id = %ready.session_id, user = format!("{}#{}", ready.user.username, ready.user.discriminator), "ready");
        Ok(())
    }

    async fn handle_guild_create(&self, data: Option<&RawValue>) -> Result<(), ClientError> {
        info!("received guild create event");

        let data = data.ok_or(ClientError::Protocol(
            "received guild create event with no data",
        ))?;
        let guild = serde_json::from_str::<GuildCreateData>(data.get())?;

        if guild.id != self.config.discord.guild_id {
            return Ok(());
        }

        let users = guild
            .members
            .into_iter()
            .map(|m| m.user)
            .collect::<Vec<User>>();

        tokio::try_join!(
            self.storage.set_users(&users),
            self.storage.set_presences(&guild.presences),
        )?;

        Ok(())
    }

    async fn handle_presence_update(&self, data: Option<&RawValue>) -> Result<(), ClientError> {
        info!("received presence update event");

        let data = data.ok_or(ClientError::Protocol(
            "received presence update event with no data",
        ))?;
        let presence = serde_json::from_str::<Presence>(data.get())?;

        let guild_id = presence.guild_id.clone().ok_or(ClientError::Protocol(
            "received presence update event with no guild id",
        ))?;
        if guild_id != self.config.discord.guild_id {
            return Ok(());
        }

        let user_id = presence.user.id.clone();
        self.storage.set_presence(&presence).await?;
        self.storage.publish_update(&user_id).await?;

        Ok(())
    }
}
