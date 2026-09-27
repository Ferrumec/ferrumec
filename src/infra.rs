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

#[async_trait]
pub trait Infra {
    async fn health_check(&self) -> Status;
    fn database(&self) -> PgPool;
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
    fn cache_factory(&self) -> impl CacheFactory;
    fn event_stream(&self) -> Arc<dyn EventStream>;
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct Status {
    pub postgres_latency_ms: Option<u128>,
    pub redis_latency_ms: Option<u128>,
    pub nats_latency_ms: Option<u128>,
}

pub struct LocalInfra {
    pub postgres: PgPool,
    pub cache_factory: MokaCacheFactory,
    pub events: Arc<LocalEventStream>,
}

impl LocalInfra {
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

    #[derive(Clone)]
    pub struct RemoteInfra {
        pub postgres: PgPool,
        pub redis: RedisManager,
        pub nats: Arc<NatsEventStream>,
    }

    impl RemoteInfra {
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

        /// Reads from env:
        /// DATABASE_URL=postgres://user:pass@localhost/db
        /// REDIS_URL=redis://localhost:6379
        /// NATS_URL=nats://localhost:4222
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
