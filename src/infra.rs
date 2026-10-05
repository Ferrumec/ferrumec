use crate::cache::CacheFactory;
use crate::cache::MokaCacheFactory;
use crate::event::EventStream;
use crate::event::LocalEventStream;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;

/// The infrastructure a service runs on: a database, a cache factory and an
/// event stream.
///
/// Write services against `Infra` and pick the implementation at startup:
/// [`LocalInfra`] for development and tests, `RemoteInfra` (feature
/// `distributed`) for Postgres + Redis + NATS. Implement it yourself to mix
/// backends, for example Redis caching with an in-process event stream.
#[async_trait]
pub trait Infra {
    /// Probes each dependency and reports its latency.
    async fn health_check(&self) -> Status;
    /// Returns a handle to the Postgres pool. Cloning a pool is cheap and
    /// shares the same connections.
    fn database(&self) -> PgPool;
    /// Pings Postgres with `SELECT 1`. Returns the latency in milliseconds,
    /// or `None` if it failed or took longer than 2 seconds.
    async fn check_postgres(pool: &PgPool) -> Option<u128> {
        let start = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            sqlx::query("SELECT 1").execute(pool),
        )
        .await;
        match result {
            Ok(Ok(_)) => Some(start.elapsed().as_millis()),
            _ => None,
        }
    }
    /// Returns the factory used to create named caches.
    fn cache_factory(&self) -> impl CacheFactory;
    /// Returns the shared event stream.
    fn event_stream(&self) -> Arc<dyn EventStream>;
}

/// Result of [`Infra::health_check`], serializable for a `/health` endpoint.
///
/// Each field is the round-trip latency in milliseconds, or `None` if the
/// check failed or timed out (2 seconds). Backends an implementation doesn't
/// use are reported as `Some(0)`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Status {
    /// Postgres latency (`SELECT 1`).
    pub postgres_latency_ms: Option<u128>,
    /// Redis latency (`PING`).
    pub redis_latency_ms: Option<u128>,
    /// NATS latency (client flush).
    pub nats_latency_ms: Option<u128>,
}

/// [`Infra`] for development and tests: Postgres plus in-process caching and
/// events.
///
/// Uses [`MokaCacheFactory`] and a [`LocalEventStream`] with a queue of 1000
/// messages per subscriber. Cloning shares the same pool, caches and stream.
/// Redis and NATS latencies are reported as `0`.
#[derive(Clone)]
pub struct LocalInfra {
    /// The Postgres connection pool.
    pub postgres: PgPool,
    /// The in-process cache factory.
    pub cache_factory: MokaCacheFactory,
    /// The in-process event stream.
    pub events: Arc<LocalEventStream>,
}

impl LocalInfra {
    /// Connects to Postgres and builds the in-process backends.
    ///
    /// Reads `DATABASE_URL`, defaulting to
    /// `postgres://postgres:postgres@localhost:5432/postgres`. The pool has
    /// up to 20 connections and a 5 second acquire timeout.
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".into());

        // Postgres
        let postgres = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&database_url)
            .await?;
        Ok(Self {
            postgres,
            events: Arc::new(LocalEventStream::new(1000)),
            cache_factory: MokaCacheFactory::default(),
        })
    }
}

#[async_trait]
impl Infra for LocalInfra {
    async fn health_check(&self) -> Status {
        let pg = Self::check_postgres(&self.postgres).await;
        Status {
            postgres_latency_ms: pg,
            redis_latency_ms: Some(0),
            nats_latency_ms: Some(0),
        }
    }
    fn database(&self) -> PgPool {
        self.postgres.clone()
    }
    fn cache_factory(&self) -> impl CacheFactory {
        self.cache_factory.clone()
    }
    fn event_stream(&self) -> Arc<dyn EventStream> {
        self.events.clone()
    }
}

#[cfg(feature = "distributed")]
pub mod dist {
    use crate::event::NatsEventStream;
    use crate::infra::{Arc, CacheFactory, EventStream, Infra, Status};
    use async_nats::Client as NatsClient;
    use redis::aio::ConnectionManager as RedisManager;
    use sqlx::PgPool;
    use sqlx::postgres::PgPoolOptions;
    use std::time::{Duration, Instant};

    /// [`Infra`] for production: Postgres, Redis caching and NATS events.
    ///
    /// Events use [`NatsEventStream`], which is at-most-once. If you need
    /// at-least-once delivery (for example behind an
    /// [`Outbox`](crate::event::Outbox)), implement [`Infra`] with a
    /// [`NatsAloStream`](crate::event::NatsAloStream) instead.
    ///
    /// Requires the `distributed` feature.
    #[derive(Clone)]
    pub struct RemoteInfra {
        /// The Postgres connection pool.
        pub postgres: PgPool,
        /// The Redis connection manager, which acts as the cache factory.
        pub redis: RedisManager,
        /// The core-NATS event stream.
        pub nats: Arc<NatsEventStream>,
    }

    impl RemoteInfra {
        /// Builds the infrastructure from clients you have already created.
        pub fn new(
            postgres: PgPool,
            redis: RedisManager,
            nats: NatsClient,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let nats = Arc::new(NatsEventStream::from_client(nats)?);
            Ok(Self {
                postgres,
                redis,
                nats,
            })
        }

        /// Connects to all three services using environment variables:
        ///
        /// | Variable       | Default                                                |
        /// | -------------- | ------------------------------------------------------ |
        /// | `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/postgres` |
        /// | `REDIS_URL`    | `redis://127.0.0.1:6379`                               |
        /// | `NATS_URL`     | `nats://127.0.0.1:4222`                                |
        ///
        /// The Postgres pool has up to 20 connections and a 5 second acquire
        /// timeout.
        pub async fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".into());
            let redis_url =
                std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
            let nats_url =
                std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());

            // Postgres
            let postgres = PgPoolOptions::new()
                .max_connections(20)
                .acquire_timeout(Duration::from_secs(5))
                .connect(&database_url)
                .await?;

            // Redis
            let redis_client = redis::Client::open(redis_url)?;
            let redis = RedisManager::new(redis_client).await?;

            // Nats
            let nats = Arc::new(NatsEventStream::new(&nats_url).await?);

            Ok(Self {
                postgres,
                redis,
                nats,
            })
        }

        async fn check_redis(mut conn: RedisManager) -> Option<u128> {
            let start = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                redis::cmd("PING").query_async::<String>(&mut conn),
            )
            .await;
            match result {
                Ok(Ok(_)) => Some(start.elapsed().as_millis()),
                _ => None,
            }
        }

        async fn check_nats(client: &NatsClient) -> Option<u128> {
            let start = Instant::now();
            let result = tokio::time::timeout(Duration::from_secs(2), client.flush()).await;
            match result {
                Ok(Ok(_)) => Some(start.elapsed().as_millis()),
                _ => None,
            }
        }
    }

    #[async_trait::async_trait]
    impl Infra for RemoteInfra {
        async fn health_check(&self) -> Status {
            let (pg, rd, nt) = tokio::join!(
                Self::check_postgres(&self.postgres),
                Self::check_redis(self.redis.clone()),
                Self::check_nats(&self.nats.client),
            );
            Status {
                postgres_latency_ms: pg,
                redis_latency_ms: rd,
                nats_latency_ms: nt,
            }
        }
        fn database(&self) -> PgPool {
            self.postgres.clone()
        }
        fn cache_factory(&self) -> impl CacheFactory {
            self.redis.clone()
        }
        fn event_stream(&self) -> Arc<dyn EventStream> {
            self.nats.clone()
        }
    }
}
