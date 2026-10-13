//! Core building blocks for event-driven Rust microservices.
//!
//! Write a service against a few small traits and choose the backends at
//! startup: everything can run in-process for development and tests, then
//! switch to Postgres + Redis + NATS in production without touching business
//! logic.
//!
//! # Modules
//!
//! - [`event`]: typed events in an [`Event`] envelope, the [`EventStream`]
//!   trait with in-process and NATS backends, [`Subscriber`], and the
//!   transactional `event::Outbox`.
//! - [`cache`]: the async [`Store`] trait and a [`CacheFactory`] with Moka
//!   (in-process) and Redis backends.
//! - [`infra`]: the [`Infra`] trait that bundles the database pool, cache
//!   factory and event stream, with `LocalInfra` (and `RemoteInfra` behind
//!   the `distributed` feature).
//! - `Module` and the `modules!` macro (feature `web`): a small convention for
//!   building services from any [`Infra`] and mounting them on an Actix Web app.
//! - `Authority` and the permission middleware (feature `web`).
//! - `launch` and `observability` (feature `launch`): an HTTP server
//!   bootstrap with OpenTelemetry logs, metrics and traces.
//!
//! # Feature flags
//!
//! | Feature       | Enables                                                              |
//! | ------------- | -------------------------------------------------------------------- |
//! | *(always)*    | The traits (`EventStream`, `Subscriber`, `Store`, `CacheFactory`, `Infra`), `Event` |
//! | `local` (default) | `LocalEventStream`, `MokaCacheFactory`, `LocalInfra`, `Outbox`   |
//! | `distributed` | `NatsEventStream`, `NatsAloStream`, `RedisCache`, `RemoteInfra`      |
//! | `web`         | `Module`, `modules!`, `Authority`, `Infra::configure` (actix-web)    |
//! | `launch`      | `launch::launch`, `Observability`, `record_request` (implies `web`)  |
//!
//! # Example
//!
//! ```ignore
//! // Requires the `local` feature (on by default).
//! use ferrumec::event::EventError;
//! use ferrumec::{Event, EventType, Infra, LocalInfra, Subscriber};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct UserRegistered {
//!     email: String,
//! }
//!
//! impl EventType for UserRegistered {
//!     const SUBJECT: &'static str = "user.registered";
//! }
//!
//! struct Mailer;
//!
//! #[async_trait::async_trait]
//! impl Subscriber<UserRegistered> for Mailer {
//!     async fn on_message(
//!         &self,
//!         event: Event<UserRegistered>,
//!         _subject: &str,
//!     ) -> Result<(), EventError> {
//!         println!("welcome email to {}", event.payload.email);
//!         Ok(())
//!     }
//! }
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let infra = LocalInfra::new().await?; // reads DATABASE_URL
//! let events = infra.event_stream();
//!
//! Mailer.subscribe(events.clone()).await?;
//! Event::new(UserRegistered { email: "ada@example.com".into() })
//!     .publish(events)
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub mod cache;
pub mod event;
#[cfg(feature = "local")]
pub mod infra;
#[cfg(feature = "web")]
mod permission;
#[cfg(feature = "web")]
pub use permission::Authority;
#[cfg(feature = "launch")]
pub mod launch;
#[cfg(feature = "launch")]
pub mod observability;
pub use cache::{CacheFactory, Store};
pub use event::{Event, EventStream, EventType, Subscriber};
#[cfg(feature = "local")]
pub use infra::{Infra, LocalInfra};
#[cfg(feature = "launch")]
pub use observability::{Observability, record_request};
#[cfg(feature = "web")]
mod module;
#[cfg(feature = "web")]
pub use module::Module;
