//! Typed events and the streams that carry them.
//!
//! - Define an event by implementing [`EventType`] for a serializable payload.
//! - Wrap it in an [`Event`] (payload plus [`EventMetaData`]) and publish it
//!   with [`Event::publish`], or write it atomically with a database change
//!   through the [`Outbox`].
//! - Receive events by implementing [`Subscriber`].
//! - Choose a transport by picking an [`EventStream`]: [`LocalEventStream`]
//!   (in-process), or with the `distributed` feature `NatsEventStream`
//!   (at-most-once) and `NatsAloStream` (JetStream, at-least-once).

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::pin::Pin;
use std::{future::Future, marker::PhantomData};
mod envelop;
pub use envelop::{Event, EventMetaData, Identifier};
use std::sync::Arc;
/// Boxed, thread-safe error used throughout the event API.
pub type EventError = Box<dyn std::error::Error + Send + Sync>;
/// A boxed async callback that receives a raw message payload.
pub type EventHandler =
    Box<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Low-level message handler registered with [`EventStream::subscribe`].
///
/// Most code implements [`Subscriber`] instead, which decodes the message
/// into an [`Event`] before calling you. The result matters to at-least-once
/// streams: on `Err`, `NatsAloStream` redelivers the message, while the other
/// streams log the error and move on.
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Processes one message. `subject` is the concrete subject it was
    /// published on and `message` is the raw payload bytes.
    async fn handle(&self, subject: String, message: Vec<u8>) -> Result<(), EventError>;
}

/// A pinned, boxed, `Send` future, used to keep [`EventStream`] object-safe.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A transport that moves raw messages between publishers and subscribers.
///
/// This is the seam that lets a service run in-process during development and
/// on NATS in production. Streams are used as `Arc<dyn EventStream>`.
/// Delivery guarantees depend on the implementation; see
/// [`LocalEventStream`] and the NATS streams.
pub trait EventStream: Send + Sync {
    /// Publishes `payload` on `subject`.
    ///
    /// Typed code usually goes through [`Event::publish`], which serializes
    /// the envelope and picks the subject from [`EventType::SUBJECT`].
    fn publish<'a>(
        &'a self,
        subject: String,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), EventError>>;

    /// Registers `handler` for messages published on `subject`.
    ///
    /// The handler runs on a background task for as long as the stream is
    /// alive. Streams that support wildcards (see [`LocalEventStream`])
    /// accept `*` and `>` tokens in `subject`.
    fn subscribe<'a>(
        &'a self,
        subject: String,
        handler: Arc<dyn Handler>,
    ) -> BoxFuture<'a, Result<(), EventError>>;
}
mod local;
mod outbox;
pub use local::LocalEventStream;
pub use outbox::Outbox;

#[cfg(feature = "distributed")]
mod nats;
#[cfg(feature = "distributed")]
pub use nats::{NatsAloStream, NatsEventStream};

/// Marks a type as an event payload and assigns it a subject.
///
/// ```ignore
/// #[derive(Serialize, Deserialize)]
/// struct UserRegistered { email: String }
///
/// impl EventType for UserRegistered {
///     const SUBJECT: &'static str = "user.registered";
/// }
/// ```
///
/// With `NatsAloStream`, the subject must start with the stream name,
/// lowercased, followed by a dot (stream `EVENTS` captures `events.>`).
pub trait EventType: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The subject this event type is published on and subscribed to.
    const SUBJECT: &'static str;

    /// Publishes the **bare payload** as JSON on [`Self::SUBJECT`], without
    /// an [`Event`] envelope.
    ///
    /// A [`Subscriber`] expects a full `Event<T>`, so it cannot decode this
    /// message and will log an error and drop it. Prefer
    /// [`Event::publish`] (or [`Outbox::push`]) unless the consumer reads
    /// raw payloads.
    fn publish<'a>(&self, es: Arc<dyn EventStream>) -> BoxFuture<'_, Result<(), EventError>> {
        Box::pin(async move {
            es.publish(
                Self::SUBJECT.to_string(),
                serde_json::to_string(self)?.into(),
            )
            .await
            .into()
        })
    }
}

/// Receives typed events of type `T`.
///
/// Implement [`on_message`](Subscriber::on_message) and call
/// [`subscribe`](Subscriber::subscribe) to attach the subscriber to a stream.
/// Incoming messages are decoded as `Event<T>`; messages that fail to decode
/// are logged and dropped, since redelivery cannot fix them. Errors returned
/// from `on_message` are handled by the stream: redelivered on
/// `NatsAloStream`, logged otherwise.
#[async_trait]
pub trait Subscriber<T: EventType>: Send + Sync + Sized + 'static {
    /// Handles one event. `subject` is the concrete subject it arrived on.
    ///
    /// On at-least-once streams the same event can arrive more than once, so
    /// make handling idempotent (dedupe on `event.metadata.event_id`).
    async fn on_message(&self, event: Event<T>, subject: &str) -> Result<(), EventError>;

    /// Subscribes `self` to [`EventType::SUBJECT`] on `es`, consuming it.
    async fn subscribe(self, es: Arc<dyn EventStream>) -> Result<(), EventError> {
        struct MessageHandler<C: Subscriber<T> + Send + Sync + 'static, T: EventType> {
            subscriber: C,
            _marker: PhantomData<T>,
        }

        #[async_trait]
        impl<C: Subscriber<T> + Send + Sync + 'static, T: EventType> Handler for MessageHandler<C, T> {
            async fn handle(&self, subject: String, message: Vec<u8>) -> Result<(), EventError> {
                // Deserialize the full Event<T>
                // Deserialization errors are absorbed since redeliver cannot fix them
                match serde_json::from_slice::<Event<T>>(&message) {
                    Ok(event) => self.subscriber.on_message(event, &subject).await,

                    Err(e) => Ok({
                        tracing::error!("Failed to deserialize event on {}: {}", subject, e);
                    }),
                }
            }
        }

        let handler = Arc::new(MessageHandler::<Self, T> {
            subscriber: self,
            _marker: PhantomData,
        });

        es.subscribe(T::SUBJECT.to_string(), handler).await
    }
}
