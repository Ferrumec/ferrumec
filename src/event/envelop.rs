use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;

/// Who an event is addressed to: a specific entity or a free-form group.
///
/// Parsing a string (via [`FromStr`] or `From<&str>`) yields
/// [`Identifier::Uuid`] when it is a valid UUID and [`Identifier::Tag`]
/// otherwise.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Identifier {
    /// A specific entity, such as a user.
    Uuid(Uuid),
    /// A free-form label, such as a role (`"admins"`) or topic.
    Tag(String),
}

impl From<Uuid> for Identifier {
    fn from(value: Uuid) -> Self {
        Identifier::Uuid(value)
    }
}

impl From<&str> for Identifier {
    fn from(value: &str) -> Identifier {
        if let Ok(v) = value.parse() {
            return v;
        }
        Identifier::Tag(value.to_string())
    }
}

use std::str::FromStr;

impl FromStr for Identifier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try parsing as UUID first
        if let Ok(uuid) = Uuid::parse_str(s) {
            return Ok(Identifier::Uuid(uuid));
        }
        // Otherwise treat as Tag
        Ok(Identifier::Tag(s.to_string()))
    }
}

use super::EventStream;

/// Envelope metadata attached to every [`Event`].
///
/// [`EventMetaData::new`] fills in a fresh `event_id`, version `"v1"` and the
/// current time; the other fields start empty and are set with the `with_*`
/// builders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetaData {
    /// Unique ID of this event. Subscribers can use it to deduplicate
    /// redelivered events.
    pub event_id: Uuid,
    /// Schema version of the payload. Defaults to `"v1"`.
    pub event_version: String,
    /// When the event was created.
    pub occurred_at: SystemTime,
    /// Name of the service that produced the event.
    pub producer: Option<String>,
    /// Links events that belong to the same logical operation.
    pub correlation_id: Option<Uuid>,
    /// Distributed-tracing ID.
    pub trace_id: Option<Uuid>,
    /// The user the event concerns or originated from.
    pub user_id: Option<Uuid>,
    /// Who the event is addressed to.
    pub audience: Vec<Identifier>,
    /// The session the event originated from.
    pub session_id: Option<Uuid>,
}

impl Default for EventMetaData {
    fn default() -> Self {
        Self::new()
    }
}

impl EventMetaData {
    /// Creates metadata with a random `event_id`, version `"v1"` and the
    /// current time.
    pub fn new() -> Self {
        Self {
            event_id: Uuid::new_v4(),
            event_version: "v1".to_string(),
            occurred_at: SystemTime::now(),
            producer: None,
            correlation_id: None,
            trace_id: None,
            user_id: None,
            session_id: None,
            audience: Vec::new(),
        }
    }

    /// Sets the producing service's name.
    pub fn with_producer(mut self, producer: impl Into<String>) -> Self {
        self.producer = Some(producer.into());
        self
    }

    /// Sets the correlation ID.
    pub fn with_correlation_id(mut self, id: Uuid) -> Self {
        self.correlation_id = Some(id);
        self
    }

    /// Sets the trace ID.
    pub fn with_trace_id(mut self, id: Uuid) -> Self {
        self.trace_id = Some(id);
        self
    }

    /// Sets the user ID.
    pub fn with_user_id(mut self, id: Uuid) -> Self {
        self.user_id = Some(id);
        self
    }

    /// Sets the session ID.
    pub fn with_session_id(mut self, id: Uuid) -> Self {
        self.session_id = Some(id);
        self
    }
    /// Replaces the audience.
    pub fn with_audience(mut self, aud: Vec<Identifier>) -> Self {
        self.audience = aud;
        self
    }
}

/// A payload together with its [`EventMetaData`]: the unit that is published
/// and delivered.
///
/// Build one with [`Event::new`], chain the `with_*` builders, then publish it
/// with [`Event::publish`] or hand it to an [`Outbox`](super::Outbox). On the
/// wire it is the JSON object `{"metadata": ..., "payload": ...}`.
///
/// ```ignore
/// Event::new(UserRegistered { email })
///     .with_producer("auth-service")
///     .with_correlation_id(correlation_id)
///     .add_audience("admins")
///     .publish(events)
///     .await?;
/// ```
#[derive(Serialize, Deserialize)]
pub struct Event<T> {
    /// Envelope metadata.
    pub metadata: EventMetaData,
    /// The event payload.
    pub payload: T,
}

impl<T: super::EventType + Sync> Event<T> {
    /// Wraps `payload` with fresh metadata.
    pub fn new(payload: T) -> Self {
        let metadata = EventMetaData::new();
        Self { metadata, payload }
    }
    /// Serializes the event to JSON and publishes it on `T::SUBJECT`.
    ///
    /// This is fire-and-forget with respect to the database: if you need the
    /// event to be published if and only if a transaction commits, use
    /// [`Outbox::push`](super::Outbox::push) instead.
    pub async fn publish(
        &self,
        es: Arc<dyn EventStream>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let payload = serde_json::to_string(self)?.into_bytes();
        es.publish(T::SUBJECT.to_string(), payload).await
    }

    /// Sets the producing service's name. Chainable; see [`EventMetaData`].
    pub fn with_producer(mut self, producer: impl Into<String>) -> Self {
        self.metadata = self.metadata.with_producer(producer);
        self
    }

    /// Sets the correlation ID. Chainable; see [`EventMetaData`].
    pub fn with_correlation_id(mut self, id: Uuid) -> Self {
        self.metadata = self.metadata.with_correlation_id(id);
        self
    }

    /// Sets the trace ID. Chainable; see [`EventMetaData`].
    pub fn with_trace_id(mut self, id: Uuid) -> Self {
        self.metadata = self.metadata.with_trace_id(id);
        self
    }

    /// Sets the user ID. Chainable; see [`EventMetaData`].
    pub fn with_user_id(mut self, id: Uuid) -> Self {
        self.metadata = self.metadata.with_user_id(id);
        self
    }

    /// Sets the session ID. Chainable; see [`EventMetaData`].
    pub fn with_session_id(mut self, id: Uuid) -> Self {
        self.metadata = self.metadata.with_session_id(id);
        self
    }
    /// Replaces the audience with `aud`; strings that parse as UUIDs become
    /// [`Identifier::Uuid`], anything else a [`Identifier::Tag`]. Chainable.
    pub fn with_audience<U>(mut self, aud: Vec<U>) -> Self
    where
        U: Into<Identifier>,
    {
        self.metadata = self
            .metadata
            .with_audience(aud.into_iter().map(Into::into).collect());
        self
    }
    /// Appends one entry to the audience. Chainable.
    pub fn add_audience<U>(mut self, aud: U) -> Self
    where
        U: Into<Identifier>,
    {
        self.metadata.audience.push(aud.into());
        self
    }
}
