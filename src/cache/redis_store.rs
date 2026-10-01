use std::{error::Error, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};

use redis::{AsyncCommands, aio::ConnectionManager};
use serde::{Serialize, de::DeserializeOwned};

use super::{CacheFactory, Store};

type BoxError = Box<dyn Error>;

/// A Redis-backed [`Store`].
///
/// Keys are laid out as:
///
/// ```text
/// <namespace>:v<version>:<json-serialized-key>
/// ```
///
/// `clear` bumps the version counter stored at `<namespace>:__version`, which
/// instantly orphans every existing entry (they expire on their own via TTL)
/// without needing `SCAN`/`DEL`.
pub struct RedisCache<K, V> {
    connection: ConnectionManager,
    /// `"<namespace>:"` — the trailing delimiter prevents `foo` + `"bar"` from
    /// colliding with `fo` + `"obar"`.
    namespace: Vec<u8>,
    version_key: String,
    ttl: Duration,
    _marker: PhantomData<fn(K) -> V>,
}

impl<K, V> RedisCache<K, V> {
    /// Create a Redis-backed store using the supplied connection manager.
    ///
    /// `namespace` isolates this store from all other Redis data. For example,
    /// `authnz:cache` produces keys such as `authnz:cache:v0:<serialized-key>`.
    pub fn new(connection: ConnectionManager, namespace: impl Into<String>, ttl: Duration) -> Self {
        let namespace = namespace.into();

        Self {
            connection,
            version_key: format!("{namespace}:__version"),
            namespace: format!("{namespace}:").into_bytes(),
            ttl,
            _marker: PhantomData,
        }
    }

    /// TTL in milliseconds, clamped to at least 1 (Redis rejects a zero expiry).
    fn ttl_millis(&self) -> u64 {
        u64::try_from(self.ttl.as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
    }

    /// Current cache generation. A missing counter means generation 0.
    async fn current_version(&self) -> Result<u64, BoxError> {
        let mut connection = self.connection.clone();
        let version: Option<u64> = connection.get(&self.version_key).await?;
        Ok(version.unwrap_or(0))
    }

    async fn make_key(&self, key: &K) -> Result<Vec<u8>, BoxError>
    where
        K: Serialize,
    {
        let encoded = serde_json::to_vec(key)?;
        let prefix = format!("v{}:", self.current_version().await?);

        let mut redis_key = Vec::with_capacity(self.namespace.len() + prefix.len() + encoded.len());

        redis_key.extend_from_slice(&self.namespace);
        redis_key.extend_from_slice(prefix.as_bytes());
        redis_key.extend_from_slice(&encoded);

        Ok(redis_key)
    }
}

#[async_trait::async_trait]
impl<K, V> Store<K, V> for RedisCache<K, V>
where
    K: Serialize + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(&self, key: &K) -> Result<Option<V>, BoxError> {
        let redis_key = self.make_key(key).await?;
        let mut connection = self.connection.clone();

        let value: Option<Vec<u8>> = connection.get(redis_key).await?;

        value
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .map_err(Into::into)
    }

    async fn set(&self, key: &K, value: V) -> Result<(), BoxError> {
        let redis_key = self.make_key(key).await?;
        let encoded = serde_json::to_vec(&value)?;
        let mut connection = self.connection.clone();

        connection
            .pset_ex::<_, _, ()>(redis_key, encoded, self.ttl_millis())
            .await?;

        Ok(())
    }

    async fn delete(&self, key: &K) -> Result<(), BoxError> {
        let redis_key = self.make_key(key).await?;
        let mut connection = self.connection.clone();

        connection.unlink::<_, ()>(redis_key).await?;

        Ok(())
    }

    async fn clear(&self) -> Result<(), BoxError> {
        let mut connection = self.connection.clone();

        // INCR is atomic, so concurrent `clear` calls can't lose an increment.
        connection.incr::<_, _, u64>(&self.version_key, 1).await?;

        Ok(())
    }
}

impl CacheFactory for ConnectionManager {
    fn new_cache<K, V>(&self, name: &str, ttl: Duration) -> Arc<dyn Store<K, V>>
    where
        K: Hash + Eq + Clone + Serialize + Send + Sync + 'static,
        V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        Arc::new(RedisCache::new(self.clone(), name, ttl))
    }
}
