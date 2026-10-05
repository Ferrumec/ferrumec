//! Transactional outbox.
//!
//! `push` stores an event in the same database transaction as the caller's
//! business writes, so the event exists if and only if that transaction
//! commits. A background task then publishes unpublished rows to the
//! `EventStream` and marks them as published.
//!
//! Delivery is at-least-once: if the process dies after the stream accepted
//! an event but before the row was marked, the event is published again on
//! the next run. Subscribers should be idempotent (dedupe on
//! `metadata.event_id`).

use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::task::JoinHandle;

use super::{Event, EventError, EventStream, EventType};

const CHANNEL: &str = "outbox_events";
const BATCH_SIZE: i64 = 100;
/// Safety net in case a notification is missed (e.g. listener reconnecting).
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const RETRY_DELAY: Duration = Duration::from_secs(2);

pub struct Outbox {
    worker: JoinHandle<()>,
}

impl Outbox {
    /// Creates the outbox table if needed, then spawns the background
    /// publisher. Events left unpublished by a previous run are published
    /// immediately.
    pub async fn new(database: PgPool, stream: Arc<dyn EventStream>) -> Result<Self, sqlx::Error> {
        migrate(&database).await?;

        // Listen before the first drain so no notification can slip between
        // the drain and the first wait.
        let mut listener = PgListener::connect_with(&database).await?;
        listener.listen(CHANNEL).await?;

        let worker = tokio::spawn(run(database, stream, listener));
        Ok(Self { worker })
    }

    /// Writes `event` to the outbox using the caller's transaction and wakes
    /// the publisher.
    ///
    /// The wake-up is a `pg_notify` issued inside the transaction, which
    /// Postgres only delivers when the transaction commits. The publisher
    /// therefore never wakes up before the row is visible, and a rollback
    /// wakes nobody.
    pub async fn push<T: EventType>(
        &self,
        event: &Event<T>,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), EventError> {
        let payload = serde_json::to_vec(event)?;

        sqlx::query("INSERT INTO outbox_events (event_id, subject, payload) VALUES ($1, $2, $3)")
            .bind(event.metadata.event_id)
            .bind(T::SUBJECT)
            .bind(payload)
            .execute(&mut **tx)
            .await?;

        sqlx::query("SELECT pg_notify($1, '')")
            .bind(CHANNEL)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }
}

impl Drop for Outbox {
    fn drop(&mut self) {
        // Anything in flight stays unpublished and is retried on next start.
        self.worker.abort();
    }
}

async fn migrate(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS outbox_events (
            id           BIGSERIAL PRIMARY KEY,
            event_id     UUID        NOT NULL UNIQUE,
            subject      TEXT        NOT NULL,
            payload      BYTEA       NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at TIMESTAMPTZ
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS outbox_events_unpublished_idx
         ON outbox_events (id) WHERE published_at IS NULL",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn run(pool: PgPool, stream: Arc<dyn EventStream>, mut listener: PgListener) {
    loop {
        // Drain everything currently pending.
        loop {
            match publish_batch(&pool, stream.as_ref()).await {
                Ok(n) if n as i64 == BATCH_SIZE => continue, // probably more
                Ok(_) => break,
                Err(e) => {
                    tracing::warn!("outbox publish failed, will retry: {e}");
                    tokio::time::sleep(RETRY_DELAY).await;
                    break;
                }
            }
        }

        // Sleep until a committed push notifies us, or the poll interval
        // elapses. Notifications that arrive while draining stay queued on
        // the listener, so `recv` returns immediately for them.
        tokio::select! {
            res = listener.recv() => {
                if let Err(e) = res {
                    tracing::warn!("outbox listener error (sqlx reconnects automatically): {e}");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
    }
}

/// Publishes up to `BATCH_SIZE` unpublished events in insertion order and
/// returns how many were published.
///
/// Rows are locked with `FOR UPDATE SKIP LOCKED`, so several service
/// instances can run an outbox on the same table without publishing the same
/// row concurrently. On a publish failure the batch stops there; events
/// already published in it are still marked, and the error is returned.
async fn publish_batch(pool: &PgPool, stream: &dyn EventStream) -> Result<usize, EventError> {
    let mut tx = pool.begin().await?;

    let rows = sqlx::query(
        "SELECT id, subject, payload
         FROM outbox_events
         WHERE published_at IS NULL
         ORDER BY id
         LIMIT $1
         FOR UPDATE SKIP LOCKED",
    )
    .bind(BATCH_SIZE)
    .fetch_all(&mut *tx)
    .await?;

    let mut published: Vec<i64> = Vec::with_capacity(rows.len());
    let mut failure: Option<EventError> = None;

    for row in &rows {
        let id: i64 = row.try_get("id")?;
        let subject: String = row.try_get("subject")?;
        let payload: Vec<u8> = row.try_get("payload")?;

        match stream.publish(subject, payload).await {
            Ok(()) => published.push(id),
            Err(e) => {
                failure = Some(e);
                break; // keep ordering: don't skip past a failed event
            }
        }
    }

    if !published.is_empty() {
        sqlx::query("UPDATE outbox_events SET published_at = now() WHERE id = ANY($1)")
            .bind(&published)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    match failure {
        Some(e) => Err(e),
        None => Ok(published.len()),
    }
}
