# ferrumec

Core building blocks for event-driven Rust microservices: pluggable event streams (local or NATS), a transactional outbox, caching (Moka or Redis), and Postgres-backed infrastructure.

Write your service against a few small traits, then choose the backends at startup. Run everything in-process for development and tests, and switch to Postgres + Redis + NATS in production without touching business logic.

## Features

- **Typed events**: each event type declares its own subject and travels in an envelope with metadata (event ID, version, timestamp, producer, correlation/trace/user/session IDs, audience).
- **Swappable event streams**: in-process, core NATS, or NATS JetStream.
- **Transactional outbox**: write an event in the same database transaction as your data and have it published reliably afterwards.
- **Swappable caches**: in-memory (Moka) or Redis, behind one async `Store` trait.
- **Infrastructure bundle**: an `Infra` trait that hands out the database pool, cache factory and event stream, with a built-in health check.
- **Modules and launcher**: a `Module` trait and `modules!` macro for assembling services, plus an optional Actix Web launcher with OpenTelemetry logs, metrics and traces.

## Installation

```toml
[dependencies]
ferrumec = "0.3"
```

Enable Redis and NATS support with the `distributed` feature, and the HTTP launcher with `launch`:

```toml
ferrumec = { version = "0.3", features = ["distributed", "launch"] }
```

| Feature       | Enables                                                                              |
| ------------- | ------------------------------------------------------------------------------------ |
| *(default)*   | `LocalEventStream`, `Outbox`, `MokaCacheFactory`, `LocalInfra`, Postgres via SQLx    |
| `distributed` | `NatsEventStream`, `NatsAloStream`, `RedisCache`, `RemoteInfra`                      |
| `launch`      | `launch::launch`, `Observability`, `record_request` (OpenTelemetry over OTLP/HTTP)   |

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

On the wire an event is the JSON object `{"metadata": {...}, "payload": {...}}`.

`Identifier` is either a `Uuid` or a free-form `Tag`. Strings that parse as UUIDs become `Identifier::Uuid`, and everything else becomes `Identifier::Tag`.

Implement `Subscriber<T>` and call `.subscribe(event_stream)` to register it. Messages that fail to deserialize are logged and dropped, since redelivery cannot fix them. If you need the raw bytes instead, implement `Handler` and register it with `EventStream::subscribe`.

> **Note:** always publish with `Event::publish` (or the outbox). `EventType::publish` sends the bare payload without the envelope, which a `Subscriber` cannot decode.

### Event stream backends

All backends implement the `EventStream` trait (`publish` / `subscribe`).

| Backend             | Feature       | Delivery                                                                                          |
| ------------------- | ------------- | ------------------------------------------------------------------------------------------------- |
| `LocalEventStream`  | default       | In-process, ordered, with backpressure. A full subscriber queue makes `publish` wait.             |
| `NatsEventStream`   | `distributed` | Core NATS, at-most-once. Handler errors are logged and not retried.                               |
| `NatsAloStream`     | `distributed` | JetStream, at-least-once. Explicit acks, and a handler that returns `Err` is redelivered.         |

Notes:

- `LocalEventStream::new(capacity)` sets the per-subscriber queue size. `LocalEventStream::reliable()` uses 8192. Publishing to a subject with no subscribers is a no-op, not an error.
- `LocalEventStream` accepts NATS-style wildcards when you subscribe through `EventStream::subscribe`: `*` matches one token and `>` matches one or more trailing tokens (`user.*`, `user.>`).
- Both NATS streams use queue groups. Each instance gets a random group by default, so every instance receives every message. Call `.with_group("name")` on the stream so replicas share the load.
- `NatsAloStream::new(url, stream_name)` creates a JetStream stream that captures subjects matching `<stream_name lowercased>.>`. Give your events subjects that start with that prefix, e.g. stream `EVENTS` with subject `events.user.registered`.
- Because `NatsAloStream` redelivers, make handlers idempotent (dedupe on `metadata.event_id`).

## Transactional outbox

Publishing right after `COMMIT` can lose the event if the process dies in between; publishing before the commit can announce a change that is then rolled back. `Outbox` removes that gap: `push` writes the event into an `outbox_events` table inside your transaction, and a background task publishes pending rows to the `EventStream` afterwards.

```rust
use ferrumec::event::Outbox;

let outbox = Outbox::new(infra.database(), infra.event_stream()).await?;

let mut tx = infra.database().begin().await?;
sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
    .bind(id)
    .bind(&email)
    .execute(&mut *tx)
    .await?;
outbox
    .push(&Event::new(UserRegistered { email }), &mut tx)
    .await?;
tx.commit().await?; // the event is published once this succeeds
```

- `Outbox::new(database, event_stream)` creates the table and a partial index if they don't exist, starts the background publisher, and publishes anything left unpublished by a previous run. Create one per process.
- `push(&event, &mut tx)` stores the event with the transaction you pass in. Nothing is published unless it commits, and a rollback discards the event.
- **At-least-once delivery.** A crash after the stream accepts an event but before the row is marked published causes a resend, so subscribers should dedupe on `metadata.event_id`.
- Events are published in insertion order, in batches of up to 100. If a publish fails, the batch stops and is retried after 2 seconds, so a later event never overtakes a failed one.
- Several instances can share the table: pending rows are claimed with `FOR UPDATE SKIP LOCKED`. Order across instances is not guaranteed.
- The publisher wakes through Postgres `LISTEN`/`NOTIFY`, and `push` sends the notification inside your transaction, so it is delivered only on commit. A 5 second poll covers missed notifications. `LISTEN` doesn't work through PgBouncer in transaction-pooling mode; events are then published on the poll.
- Each batch runs in a database transaction that stays open while publishing, so a slow stream holds a pool connection for that long.
- Published rows are kept with `published_at` set. Delete old ones periodically.
- Dropping the `Outbox` stops the publisher; unpublished events stay in the table for the next instance.

## Caching

The `Store<K, V>` trait is a small async key-value interface (`get`, `set`, `delete`, `clear`). A `CacheFactory` builds named stores with a TTL:

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
    sessions.clear().await?;
    Ok(())
}
```

- **`MokaCacheFactory`** uses in-process Moka caches with a maximum of 1000 entries each. Requesting the same `name` again returns the same shared cache, and the TTL from the first call wins. Reusing a name with different key/value types logs an error and returns an unshared cache.
- **`RedisCache`** (`distributed`) JSON-encodes keys and values and stores them under `<namespace>:v<version>:<key>` with the TTL applied in milliseconds. `clear` bumps a version counter at `<namespace>:__version`, which orphans every existing entry at once (they expire on their own) without `SCAN`/`DEL`. Each operation reads that counter first, so it costs two round trips. A Redis `ConnectionManager` is itself a `CacheFactory`, and the cache name becomes the key namespace.

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

| Variable       | Used by                     | Default                                                |
| -------------- | --------------------------- | ------------------------------------------------------ |
| `DATABASE_URL` | `LocalInfra`, `RemoteInfra` | `postgres://postgres:postgres@localhost:5432/postgres` |
| `REDIS_URL`    | `RemoteInfra`               | `redis://127.0.0.1:6379`                               |
| `NATS_URL`     | `RemoteInfra`               | `nats://127.0.0.1:4222`                                |

Postgres pools are created with up to 20 connections and a 5 second acquire timeout. `LocalInfra::new()` uses a `LocalEventStream` with a queue of 1000 messages per subscriber, and reports `0` for the Redis and NATS latencies since it doesn't use them. `RemoteInfra::from_env()` reads the variables above; `RemoteInfra::new(pool, redis, nats)` accepts existing clients if you'd rather build them yourself.

`RemoteInfra` uses `NatsEventStream`, which is at-most-once. If you need at-least-once delivery, for example behind an `Outbox`, implement `Infra` with a `NatsAloStream`.

### Using a different setup

Implement `Infra` yourself to mix backends, for example Redis caching with an in-process event stream.

## Modules and launching

A `Module` is a self-contained slice of a service. It is built from any `Infra` and mounts its routes on an Actix Web app under a namespace:

```rust
use ferrumec::Module;

struct Users { /* repositories, caches, ... */ }

impl Module for Users {
    async fn new(infra: impl ferrumec::Infra) -> Result<Self, Box<dyn std::error::Error>> {
        // take the pool, cache factory and event stream from `infra`
        Ok(Users { /* ... */ })
    }

    fn configure(
        self: std::sync::Arc<Self>,
        cfg: &mut actix_web::web::ServiceConfig,
        namespace: &str,
    ) {
        // register routes under `/{namespace}`
    }
}
```

The `modules!` macro builds several modules from one `Infra` (which must be `Clone`) and returns the list `launch` expects:

```rust
let modules = ferrumec::modules!(
    LocalInfra::new().await?;
    ("users", Users),
    ("orders", Orders),
)?;
```

With the `launch` feature, `launch::launch(modules)` initializes observability and runs the HTTP server, with request tracing and metrics on every route. It is an opinionated bootstrap: telemetry is reported under the service name `mains`, the server binds to `127.0.0.1:8080`, and it panics if observability cannot be initialized. For other settings, build your own `HttpServer` and use `Observability` and `record_request` directly.

### Observability

`Observability::init(service_name, version)` installs OpenTelemetry providers for logs, metrics and traces, exported over OTLP/HTTP, and a `tracing` subscriber that also logs to the console (filtered by `RUST_LOG`, default `info`). Call `shutdown()` before exit to flush. `record_request(method, route, status, duration)` records the `http.server.request.count` counter and `http.server.request.duration` histogram.

| Variable                | Default                                                 |
| ----------------------- | ------------------------------------------------------- |
| `OTEL_LOGS_ENDPOINT`    | `http://127.0.0.1:9428/insert/opentelemetry/v1/logs`    |
| `OTEL_METRICS_ENDPOINT` | `http://127.0.0.1:8428/opentelemetry/v1/metrics`        |
| `OTEL_TRACES_ENDPOINT`  | `http://127.0.0.1:10428/insert/opentelemetry/v1/traces` |

## Project layout

```
src/
├── lib.rs            # crate docs and public re-exports
├── module.rs         # Module trait and modules! macro
├── infra.rs          # Infra, LocalInfra, RemoteInfra, Status
├── event/            # envelope, EventStream, local + NATS backends, Outbox
├── cache/            # Store, CacheFactory, Moka + Redis backends
├── launch.rs         # HTTP server bootstrap (feature `launch`)
└── observability.rs  # OpenTelemetry setup and request metrics (feature `launch`)
```

## License

Licensed under the [MIT License](LICENSE).
