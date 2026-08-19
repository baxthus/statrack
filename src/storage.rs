use futures_util::Stream;
use redis::{AsyncTypedCommands, aio::ConnectionManager};
use thiserror::Error;

use crate::{
    config::AppConfig,
    discord::types::{Presence, User},
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("serialization: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct Storage {
    client: redis::Client,
    conn: ConnectionManager,
}

fn user_key(id: &str) -> String {
    format!("user:{id}")
}
fn presence_key(id: &str) -> String {
    format!("presence:{id}")
}
fn updates_channel(id: &str) -> String {
    format!("updates:{id}")
}

impl Storage {
    pub async fn connect(redis_url: String) -> Result<Self, StorageError> {
        let client = redis::Client::open(redis_url)?;
        let conn = client.get_connection_manager().await?;

        redis::cmd("PING")
            .query_async::<String>(&mut conn.clone())
            .await?;

        Ok(Self { client, conn })
    }

    pub async fn set_user(&self, user: &User) -> Result<(), StorageError> {
        self.set_json(user_key(&user.id), user).await
    }

    pub async fn set_users(&self, users: &[User]) -> Result<(), StorageError> {
        let pairs = self.serialize_pairs(users, |u| user_key(&u.id))?;
        self.mset(pairs).await
    }

    pub async fn get_user(&self, id: &str) -> Result<Option<User>, StorageError> {
        self.get_json(user_key(id)).await
    }

    pub async fn set_presence(&self, presence: &Presence) -> Result<(), StorageError> {
        self.set_json(presence_key(&presence.user.id), presence)
            .await
    }

    pub async fn set_presences(&self, presences: &[Presence]) -> Result<(), StorageError> {
        let pairs = self.serialize_pairs(presences, |p| presence_key(&p.user.id))?;
        self.mset(pairs).await
    }

    pub async fn get_presence(&self, user_id: &str) -> Result<Option<Presence>, StorageError> {
        self.get_json(presence_key(user_id)).await
    }

    pub async fn publish_update(&self, id: &str) -> Result<(), StorageError> {
        let mut conn = self.conn.clone();
        conn.publish(updates_channel(id), 0).await?;
        Ok(())
    }

    pub async fn subscribe_updates(
        &self,
        id: &str,
    ) -> Result<impl Stream<Item = String> + 'static, StorageError> {
        use futures_util::StreamExt;

        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.subscribe(updates_channel(id)).await?;

        Ok(pubsub
            .into_on_message()
            .map(|msg| msg.get_payload().unwrap_or_default()))
    }

    // helpers

    async fn set_json<T: serde::Serialize>(
        &self,
        key: String,
        value: &T,
    ) -> Result<(), StorageError> {
        let payload = serde_json::to_string(value)?;

        let mut conn = self.conn.clone();
        conn.set(key, payload).await?;

        Ok(())
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        key: String,
    ) -> Result<Option<T>, StorageError> {
        let mut conn = self.conn.clone();
        let payload = conn.get(key).await?;
        Ok(payload.map(|p| serde_json::from_str(&p)).transpose()?)
    }

    fn serialize_pairs<T: serde::Serialize>(
        &self,
        items: &[T],
        key_fn: impl Fn(&T) -> String,
    ) -> Result<Vec<(String, String)>, StorageError> {
        items
            .iter()
            .map(|item| {
                let payload = serde_json::to_string(item)?;
                Ok((key_fn(item), payload))
            })
            .collect()
    }

    async fn mset(&self, pairs: Vec<(String, String)>) -> Result<(), StorageError> {
        if pairs.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        conn.mset(&pairs).await?;

        Ok(())
    }
}
