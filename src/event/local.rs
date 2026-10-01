use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::{AbortHandle, JoinSet};

use super::{EventError, EventStream, Handler};

pub type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

type Msg = Arc<(String, Bytes)>; // Arc so clones are cheap
type Tx = mpsc::Sender<Msg>;

pub struct LocalEventStream {
    // Exact-subject subscriptions: O(1) lookup on publish.
    exact: DashMap<String, Vec<Tx>>,
    // Patterns containing `*` or `>`: only these are scanned on publish.
    wildcards: DashMap<String, Vec<Tx>>,
    // Handler tasks. Aborted on drop; finished handles are pruned on subscribe.
    tasks: Mutex<Vec<AbortHandle>>,
    channel_capacity: usize,
}

/// One matching subscription entry captured during a publish.
struct Target {
    /// `None` = exact entry (key is the subject); `Some(p)` = wildcard pattern `p`.
    pattern: Option<String>,
    senders: Vec<Tx>,
}

impl LocalEventStream {
    pub fn new(channel_capacity: usize) -> Self {
        Self {
            exact: DashMap::new(),
            wildcards: DashMap::new(),
            tasks: Mutex::new(Vec::new()),
            channel_capacity,
        }
    }

    // 8192 = ~8MB per subscriber if 1KB avg msg. Tune based on RAM.
    pub fn reliable() -> Self {
        Self::new(8192)
    }

    /// Drop closed senders for `key` and remove the entry if nothing is left.
    /// The emptiness check and removal are atomic (`remove_if`), so a concurrent
    /// `subscribe` can never have its fresh sender deleted.
    fn prune(map: &DashMap<String, Vec<Tx>>, key: &str) {
        if let Some(mut senders) = map.get_mut(key) {
            senders.retain(|tx| !tx.is_closed());
        } // guard dropped here, before remove_if touches the same shard
        map.remove_if(key, |_, senders| senders.is_empty());
    }
}

impl EventStream for LocalEventStream {
    fn publish<'a>(
        &'a self,
        subject: String,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), EventError>> {
        Box::pin(async move {
            // Snapshot matching subscriptions. No DashMap guards are held across an await.
            let mut targets: Vec<Target> = Vec::new();

            if let Some(entry) = self.exact.get(subject.as_str()) {
                targets.push(Target {
                    pattern: None,
                    senders: entry.value().clone(),
                });
            }
            for entry in self.wildcards.iter() {
                if subject_matches(entry.key(), &subject) {
                    targets.push(Target {
                        pattern: Some(entry.key().clone()),
                        senders: entry.value().clone(),
                    });
                }
            }

            // No matching subscriptions: drop the event, as before.
            if targets.is_empty() {
                return Ok(());
            }

            let msg: Msg = Arc::new((subject, Bytes::from(payload)));
            let mut dead: HashSet<usize> = HashSet::new();
            let mut pending: Vec<(usize, Tx, Msg)> = Vec::new();

            // Fast path: enqueue without awaiting wherever there is room.
            for (i, target) in targets.iter().enumerate() {
                for tx in &target.senders {
                    match tx.try_send(msg.clone()) {
                        Ok(()) => {}
                        Err(TrySendError::Full(m)) => pending.push((i, tx.clone(), m)),
                        Err(TrySendError::Closed(_)) => {
                            dead.insert(i);
                        }
                    }
                }
            }

            // Slow path: full channels. Await them concurrently so latency is the
            // slowest subscriber, not the sum. Each subscriber still gets lossless,
            // ordered delivery with backpressure, and publish only returns once
            // every send has completed.
            match pending.len() {
                0 => {}
                1 => {
                    let (i, tx, m) = pending.pop().expect("len checked");
                    if tx.send(m).await.is_err() {
                        dead.insert(i);
                    }
                }
                _ => {
                    let mut set = JoinSet::new();
                    for (i, tx, m) in pending {
                        set.spawn(async move { (i, tx.send(m).await.is_err()) });
                    }
                    while let Some(res) = set.join_next().await {
                        if let Ok((i, true)) = res {
                            dead.insert(i);
                        }
                    }
                }
            }

            // Remove closed senders from their respective entries.
            for i in dead {
                match &targets[i].pattern {
                    None => Self::prune(&self.exact, msg.0.as_str()),
                    Some(p) => Self::prune(&self.wildcards, p.as_str()),
                }
            }

            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        subject: String,
        handler: Arc<dyn Handler>,
    ) -> BoxFuture<'a, Result<(), EventError>> {
        Box::pin(async move {
            let (tx, mut rx) = mpsc::channel::<Msg>(self.channel_capacity);

            // Insert sender. Needs a write lock on one shard, only briefly.
            let map = if is_wildcard(&subject) {
                &self.wildcards
            } else {
                &self.exact
            };
            map.entry(subject).or_default().push(tx);

            let join = tokio::spawn(async move {
                // Ordered, lossless consumption. If handler is slow, rx will fill
                // and publishers will await on send(). That's backpressure.
                while let Some(msg) = rx.recv().await {
                    let (subj, payload) = &*msg; // Arc deref
                    if let Err(e) = handler.handle(subj.clone(), payload.to_vec()).await {
                        tracing::warn!(subject = %subj, error = ?e, "event handler failed");
                    }
                }
            });

            // Sync lock, never held across an await.
            let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
            tasks.retain(|h| !h.is_finished());
            tasks.push(join.abort_handle());

            Ok(())
        })
    }
}

/// True if the subscription pattern contains a `*` or `>` token.
fn is_wildcard(pattern: &str) -> bool {
    pattern.split('.').any(|t| t == "*" || t == ">")
}

/// Returns whether a concrete subject matches a NATS-style subscription pattern.
///
/// `*` matches exactly one token.
/// `>` matches one or more trailing tokens and must be the final token.
///
/// Allocation-free: walks both strings token by token.
fn subject_matches(pattern: &str, subject: &str) -> bool {
    let (mut p, mut s) = (pattern.split('.'), subject.split('.'));
    loop {
        match (p.next(), s.next()) {
            // `>` consumes the rest, but only if it is the last pattern token.
            (Some(">"), Some(_)) => return p.next().is_none(),
            (Some("*"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            // Both exhausted together: exact match.
            (None, None) => return true,
            _ => return false,
        }
    }
}

impl Drop for LocalEventStream {
    fn drop(&mut self) {
        // Abort all handler tasks when bus drops. `get_mut` needs no locking,
        // so unlike `try_lock` it can never skip the abort.
        let tasks = self.tasks.get_mut().unwrap_or_else(PoisonError::into_inner);
        for handle in tasks.drain(..) {
            handle.abort();
        }
    }
}
