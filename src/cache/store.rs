//! General-purpose async key-value store trait.

use std::error::Error;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
/// A general-purpose async key-value storage abstraction.
///
/// Implement this to plug a custom backend (in-memory, Redis, a database
/// table, etc.) into any component that only needs get/set/delete/clear
/// semantics.
///
/// The trait has no TTL concept: expiration, if any, is entirely up to the
/// implementation. The stores built by a [`CacheFactory`] apply the TTL given
/// to [`CacheFactory::new_cache`].
#[async_trait::async_trait]
pub trait Store<K, V>: Send + Sync {
    /// Returns the value stored under `key`, or `None` if absent or expired.
    async fn get(&self, key: &K) -> Result<Option<V>, Box<dyn Error>>;

    /// Stores `value` under `key`, replacing any existing entry.
    async fn set(&self, key: &K, value: V) -> Result<(), Box<dyn Error>>;

    /// Removes the entry under `key`. Removing a missing key is not an error.
    async fn delete(&self, key: &K) -> Result<(), Box<dyn Error>>;

    /// Removes every entry in this store.
    async fn clear(&self) -> Result<(), Box<dyn Error>>;
}

use serde::{Serialize, de::DeserializeOwned};

/// Builds named [`Store`] caches.
///
/// Implementations decide the concrete backend: [`MokaCacheFactory`](super::MokaCacheFactory)
/// keeps entries in process, and a Redis `ConnectionManager` shares them
/// across instances. Tests can supply a no-op or deterministic
/// implementation.
///
/// `name` identifies the logical cache (e.g. `"community_items"`) and `ttl`
/// is the expiry for its entries. Whether asking for the same `name` twice
/// returns the same underlying cache is up to the implementation: the Moka
/// factory shares it, while the Redis one maps the name to a key namespace.
pub trait CacheFactory: Clone {
    /// Returns the cache called `name` with entries that expire after `ttl`.
    fn new_cache<K, V>(&self, name: &str, ttl: Duration) -> Arc<dyn Store<K, V>>
    where
        K: Hash + Eq + Clone + Serialize + Send + Sync + 'static,
        V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static;
}
