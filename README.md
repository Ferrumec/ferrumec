# ferrumec

Core building blocks for event-driven Rust microservices: pluggable event streams (local or NATS), caching (Moka or Redis), and Postgres-backed infrastructure.

Write your service against a few small traits, then choose the backends at startup. Run everything in-process for development and tests, and switch to Postgres + Redis + NATS in production without touching business logic.

## Features

- **Typed events**: each event type declares its own subject and travels in an envelope with metadata (event ID, version, timestamp, producer, correlation/trace/user/session IDs, audience).
- **Swappable event streams**: in-process, core NATS, or NATS JetStream.
- **Swappable caches**: in-memory (Moka) or Redis, behind one async `Store` trait.
- **Infrastructure bundle**: an `Infra` trait that hands out the database pool, cache factory and event stream, with a built-in health check.

## Installation

```toml
[dependencies]
ferrumec = "0.1"
```

Enable Redis and NATS support with the `distributed` feature:

```toml
ferrumec = { version = "0.1", features = ["distributed"] }
```

| Feature       | Enables                                                                  |
| ------------- | ------------------------------------------------------------------------ |
| *(default)*   | `LocalEventStream`, `MokaCacheFactory`, `LocalInfra`, Postgres via SQLx  |
| `distributed` | `NatsEventStream`, `NatsAloStream`, `RedisCache`, `RemoteInfra`          |

Requires Rust 1.88 or newer (edition 2024, let-chains). The crate enables Tokio's `rt`, `sync`, `macros` and `time` features. Add `rt-multi-thread` in your own `Cargo.toml` if you use `#[tokio::main]`.

## Quick start

Define an event, a subscriber, and publish:

```rust
use ferrumec::event::EventError;
use ferrumec::{Event, EventType, Infra, LocalInfra, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct UserRegistered {
    email: String,
}

impl EventType for UserRegistered {
    const SUBJECT: &'static str = "user.registered";
}

struct Mailer;

#[async_trait::async_trait]
impl Subscriber<UserRegistered> for Mailer {
    async fn on_message(
        &self,
        event: Event<UserRegistered>,
        subject: &str,
    ) -> Result<(), EventError> {
        println!("[{subject}] welcome email to {}", event.payload.email);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Reads DATABASE_URL (defaults to postgres://postgres:postgres@localhost:5432/postgres)
    let infra = LocalInfra::new().await?;
    let events = infra.event_stream();

    Mailer.subscribe(events.clone()).await?;

    Event::new(UserRegistered { email: "ada@example.com".into() })
        .with_producer("auth-service")
        .publish(events)
        .await?;

    Ok(())
}
```

## Events

An event is any `Serialize + DeserializeOwned` type that implements `EventType`, which assigns it a subject.

`Event<T>` wraps your payload with `EventMetaData` and has chainable builders:

```rust
Event::new(payload)
    .with_producer("orders")
    .with_correlation_id(correlation_id)
    .with_trace_id(trace_id)
    .with_user_id(user_id)
    .with_session_id(session_id)
    .with_audience(vec!["admins"])      // or Vec<Uuid>
    .add_audience(user_id)              // append a single entry
    .publish(events)
    .await?;
```

`Identifier` is either a `Uuid` or a free-form `Tag`. Strings that parse as UUIDs become `Identifier::Uuid`, and everything else becomes `Identifier::Tag`.

Implement `Subscriber<T>` and call `.subscribe(event_stream)` to register it. Messages that fail to deserialize are logged and dropped, since redelivery cannot fix them.

### Event stream backends

All backends implement the `EventStream` trait (`publish` / `subscribe`).

| Backend             | Feature       | Delivery                                                                                          |
| ------------------- | ------------- | ------------------------------------------------------------------------------------------------- |
| `LocalEventStream`  | default       | In-process, ordered, with backpressure. A full subscriber queue makes `publish` wait.             |
| `NatsEventStream`   | `distributed` | Core NATS, at-most-once. Handler errors are logged and not retried.                               |
| `NatsAloStream`     | `distributed` | JetStream, at-least-once. Explicit acks, and a handler that returns `Err` is redelivered.         |

Notes:

- `LocalEventStream::new(capacity)` sets the per-subscriber queue size. `LocalEventStream::reliable()` uses 8192. Publishing to a subject with no subscribers is a no-op, not an error.
- Both NATS streams use queue groups. Each instance gets a random group by default, so every instance receives every message. Call `.with_group("name")` on the stream so replicas share the load.
- `NatsAloStream::new(url, stream_name)` creates a JetStream stream that captures subjects matching `<stream_name lowercased>.>`. Give your events subjects that start with that prefix, e.g. stream `EVENTS` with subject `events.user.registered`.

## Caching

The `Store<K, V>` trait is a small async key-value interface (`get`, `set`, `delete`). A `CacheFactory` builds named stores with a TTL:

```rust
use ferrumec::cache::CacheFactory;
use ferrumec::{Infra, LocalInfra, Store};
use std::time::Duration;

async fn demo(infra: &LocalInfra) -> Result<(), Box<dyn std::error::Error>> {
    let sessions = infra
        .cache_factory()
        .new_cache::<String, String>("sessions", Duration::from_secs(300));

    sessions.set(&"user:1".to_string(), "token".to_string()).await?;
    let hit = sessions.get(&"user:1".to_string()).await?;
    sessions.delete(&"user:1".to_string()).await?;
    Ok(())
}
```

- **`MokaCacheFactory`** uses in-process Moka caches with a maximum of 1000 entries each. Requesting the same `name` again returns the same shared cache, and the TTL from the first call wins. Reusing a name with different key/value types logs an error and returns an unshared cache.
- **`RedisCache`** (`distributed`) stores JSON-encoded keys and values under `<namespace>:<key>` with the TTL applied via `SETEX`. A Redis `ConnectionManager` is itself a `CacheFactory`, and the cache name becomes the key namespace.

## Infrastructure

The `Infra` trait bundles what a service needs:

```rust
trait Infra {
    async fn health_check(&self) -> Status;
    fn database(&self) -> PgPool;
    fn cache_factory(&self) -> impl CacheFactory;
    fn event_stream(&self) -> Arc<dyn EventStream>;
}
```

`Status` is serializable and reports `postgres_latency_ms`, `redis_latency_ms` and `nats_latency_ms` (`None` means the check failed or timed out after 2 seconds). This makes it easy to return from a `/health` endpoint.

| Implementation | Feature       | Postgres | Cache | Events                 |
| -------------- | ------------- | -------- | ----- | ---------------------- |
| `LocalInfra`   | default       | yes      | Moka  | `LocalEventStream`     |
| `RemoteInfra`  | `distributed` | yes      | Redis | `NatsEventStream`      |

Environment variables read by the constructors:

| Variable       | Used by                    | Default                                            |
| -------------- | -------------------------- | -------------------------------------------------- |
| `DATABASE_URL` | `LocalInfra`, `RemoteInfra` | `postgres://postgres:postgres@localhost:5432/postgres` |
| `REDIS_URL`    | `RemoteInfra`              | `redis://127.0.0.1:6379`                           |
| `NATS_URL`     | `RemoteInfra`              | `nats://127.0.0.1:4222`                            |

Postgres pools are created with up to 20 connections and a 5 second acquire timeout. `LocalInfra` reports `0` for the Redis and NATS latencies, since it doesn't use them. `RemoteInfra::new(pool, redis, nats)` accepts existing clients if you'd rather build them yourself.

### Using a different setup

Implement `Infra` yourself to mix backends, for example Redis caching with an in-process event stream. Services can also implement the `Module` trait, which constructs a service from any `Infra` and gives it a `configure` hook.

## Project layout

```
src/
├── lib.rs          # public re-exports and the Module trait
├── infra.rs        # Infra, LocalInfra, RemoteInfra, Status
├── event/          # Event envelope, EventStream, local + NATS backends
└── cache/          # Store, CacheFactory, Moka + Redis backends
```

## License

Licensed under the [MIT License](LICENSE).
