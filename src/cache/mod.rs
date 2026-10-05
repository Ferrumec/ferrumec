//! Async key-value caching behind one trait.
//!
//! Code that needs a cache asks a [`CacheFactory`] for a named
//! [`Store<K, V>`] and doesn't care what backs it:
//!
//! - [`MokaCacheFactory`]: in-process caches (default).
//! - `RedisCache` (feature `distributed`): shared across instances. A Redis
//!   `ConnectionManager` is itself a [`CacheFactory`].
//!
//! ```ignore
//! let sessions = infra
//!     .cache_factory()
//!     .new_cache::<String, String>("sessions", Duration::from_secs(300));
//! sessions.set(&"user:1".to_string(), "token".to_string()).await?;
//! ```

mod moka_store;
pub use moka_store::MokaCacheFactory;
#[cfg(feature = "distributed")]
mod redis_store;
#[cfg(feature = "distributed")]
pub use redis_store::RedisCache;
mod store;
pub use store::{CacheFactory, Store};
