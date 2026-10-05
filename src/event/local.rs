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

/// An in-process [`EventStream`] for development, tests and single-binary
/// deployments.
///
/// - **Ordered and lossless:** each subscriber has its own bounded queue and
///   handler task, so a subscriber sees messages in publish order. When a
///   queue is full, [`publish`](EventStream::publish) waits (backpressure)
///   rather than dropping the message, and returns once every subscriber has
///   accepted it.
/// - **No subscribers, no error:** publishing to a subject nobody listens to
///   is a no-op.
/// - **Wildcards:** subscription subjects may use NATS-style tokens: `*`
///   matches exactly one token and `>` matches one or more trailing tokens
///   (`user.*`, `user.>`).
/// - **No retries:** a handler error is logged and the message is not
///   redelivered.
///
/// Handler tasks are aborted when the stream is dropped. Not shared across
/// processes; use the NATS streams for that.
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
    /// Creates a stream whose per-subscriber queue holds `channel_capacity`
    /// messages before publishers start to wait.
    pub fn new(channel_capacity: usize) -> Self {
        Self {
            exact: DashMap::new(),
            wildcards: DashMap::new(),
            tasks: Mutex::new(Vec::new()),
            channel_capacity,
        }
    }

    /// Creates a stream with a large queue (8192 messages per subscriber), so
    /// publishers rarely wait.
    ///
    /// 8192 is roughly 8 MB per subscriber at 1 KB per message; use
    /// [`LocalEventStream::new`] to tune this to your memory budget.
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::time::Duration;
    use tokio::sync::{Semaphore, mpsc::UnboundedReceiver, mpsc::UnboundedSender};
    use tokio::time::{sleep, timeout};

    type Got = (String, Vec<u8>);

    /// Forwards every message to the test; optionally blocks on a gate first
    /// so tests can simulate a slow consumer.
    struct Recorder {
        out: UnboundedSender<Got>,
        gate: Option<Arc<Semaphore>>,
    }

    // NOTE: assumes `Handler::handle(&self, String, Vec<u8>) -> BoxFuture<'_, Result<(), EventError>>`.
    #[async_trait]
    impl Handler for Recorder {
        async fn handle(&self, subject: String, payload: Vec<u8>) -> Result<(), EventError> {
            if let Some(gate) = &self.gate {
                gate.acquire().await.unwrap().forget();
            }
            let _ = self.out.send((subject, payload));
            Ok(())
        }
    }

    /// Does nothing; only holds a marker so tests can see when its task is gone.
    struct Marker(#[allow(dead_code)] Arc<()>);

    #[async_trait]
    impl Handler for Marker {
        async fn handle(&self, _: String, _: Vec<u8>) -> Result<(), EventError> {
            Ok(())
        }
    }

    fn recorder() -> (Arc<Recorder>, UnboundedReceiver<Got>) {
        let (out, rx) = mpsc::unbounded_channel();
        (Arc::new(Recorder { out, gate: None }), rx)
    }

    fn gated() -> (Arc<Recorder>, UnboundedReceiver<Got>, Arc<Semaphore>) {
        let (out, rx) = mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));
        let h = Arc::new(Recorder {
            out,
            gate: Some(gate.clone()),
        });
        (h, rx, gate)
    }

    async fn publish(s: &LocalEventStream, subject: &str, body: &str) {
        s.publish(subject.to_string(), body.as_bytes().to_vec())
            .await
            .unwrap();
    }

    async fn subscribe(s: &LocalEventStream, subject: &str, h: Arc<Recorder>) {
        s.subscribe(subject.to_string(), h).await.unwrap();
    }

    async fn next(rx: &mut UnboundedReceiver<Got>) -> Got {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed")
    }

    async fn expect_silence(rx: &mut UnboundedReceiver<Got>) {
        assert!(
            timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
            "unexpected message"
        );
    }

    // ---------- matcher ----------

    #[test]
    fn matcher_table() {
        let cases = [
            ("a.b", "a.b", true),
            ("a.b", "a.c", false),
            ("a.b", "a", false),
            ("a", "a.b", false),
            ("a.*", "a.b", true),
            ("a.*", "a", false),
            ("a.*", "a.b.c", false),
            ("*.b", "a.b", true),
            ("*", "a", true),
            ("*", "a.b", false),
            ("a.*.c", "a.b.c", true),
            ("a.*.c", "a.b.d", false),
            ("a.>", "a.b", true),
            ("a.>", "a.b.c", true),
            ("a.>", "a", false),
            (">", "a", true),
            (">", "a.b.c", true),
            ("a.>.c", "a.b.c", false), // `>` must be last
        ];
        for (pattern, subject, want) in cases {
            assert_eq!(
                subject_matches(pattern, subject),
                want,
                "{pattern} vs {subject}"
            );
        }
    }

    #[test]
    fn wildcard_detection() {
        assert!(!is_wildcard("a.b.c"));
        assert!(is_wildcard("a.*"));
        assert!(is_wildcard("a.>"));
        assert!(is_wildcard(">"));
        assert!(!is_wildcard("a.b*")); // `*` only counts as a whole token
    }

    // ---------- routing ----------

    #[tokio::test]
    async fn subscriptions_land_in_the_right_map() {
        let s = LocalEventStream::new(4);
        let (h, _rx) = recorder();
        subscribe(&s, "a.b", h.clone()).await;
        subscribe(&s, "a.*", h.clone()).await;
        subscribe(&s, "a.>", h).await;
        assert!(s.exact.contains_key("a.b"));
        assert!(!s.exact.contains_key("a.*"));
        assert!(s.wildcards.contains_key("a.*"));
        assert!(s.wildcards.contains_key("a.>"));
    }

    #[tokio::test]
    async fn exact_delivery_and_non_match() {
        let s = LocalEventStream::new(4);
        let (h, mut rx) = recorder();
        subscribe(&s, "orders.created", h).await;

        publish(&s, "orders.created", "x").await;
        assert_eq!(
            next(&mut rx).await,
            ("orders.created".to_string(), b"x".to_vec())
        );

        publish(&s, "orders.deleted", "y").await;
        expect_silence(&mut rx).await;
    }

    #[tokio::test]
    async fn publish_without_subscribers_is_ok() {
        let s = LocalEventStream::new(4);
        s.publish("nobody.home".into(), vec![1, 2, 3])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn overlapping_patterns_each_get_one_copy() {
        let s = LocalEventStream::new(8);
        let (h1, mut exact) = recorder();
        let (h2, mut star) = recorder();
        let (h3, mut tail) = recorder();
        let (h4, mut all) = recorder();
        subscribe(&s, "a.b", h1).await;
        subscribe(&s, "a.*", h2).await;
        subscribe(&s, "a.>", h3).await;
        subscribe(&s, ">", h4).await;

        publish(&s, "a.b", "1").await;
        for rx in [&mut exact, &mut star, &mut tail, &mut all] {
            assert_eq!(next(rx).await.0, "a.b");
            expect_silence(rx).await; // exactly one copy
        }

        publish(&s, "a.b.c", "2").await;
        expect_silence(&mut exact).await;
        expect_silence(&mut star).await;
        assert_eq!(next(&mut tail).await.0, "a.b.c");
        assert_eq!(next(&mut all).await.0, "a.b.c");
    }

    #[tokio::test]
    async fn multiple_subscribers_on_same_subject_all_receive() {
        let s = LocalEventStream::new(4);
        let (h1, mut rx1) = recorder();
        let (h2, mut rx2) = recorder();
        subscribe(&s, "t", h1).await;
        subscribe(&s, "t", h2).await;
        publish(&s, "t", "m").await;
        assert_eq!(next(&mut rx1).await.1, b"m");
        assert_eq!(next(&mut rx2).await.1, b"m");
    }

    #[tokio::test]
    async fn per_subscriber_ordering_is_preserved() {
        let s = LocalEventStream::new(8); // small, so publish hits the slow path too
        let (h, mut rx) = recorder();
        subscribe(&s, "seq", h).await;
        for i in 0..200u32 {
            s.publish("seq".into(), i.to_be_bytes().to_vec())
                .await
                .unwrap();
        }
        for i in 0..200u32 {
            assert_eq!(next(&mut rx).await.1, i.to_be_bytes().to_vec());
        }
    }

    // ---------- backpressure / concurrency ----------

    #[tokio::test]
    async fn full_channel_applies_backpressure_then_delivers_losslessly() {
        let s = Arc::new(LocalEventStream::new(1));
        let (h, mut rx, gate) = gated();
        subscribe(&s, "bp", h).await;

        publish(&s, "bp", "a").await; // taken by the handler, which then blocks on the gate
        sleep(Duration::from_millis(20)).await;
        publish(&s, "bp", "b").await; // fills the single channel slot

        let s2 = s.clone();
        let third = tokio::spawn(async move { s2.publish("bp".into(), b"c".to_vec()).await });
        sleep(Duration::from_millis(50)).await;
        assert!(
            !third.is_finished(),
            "publish should be blocked by backpressure"
        );

        gate.add_permits(3);
        third.await.unwrap().unwrap();
        for want in ["a", "b", "c"] {
            assert_eq!(next(&mut rx).await.1, want.as_bytes());
        }
    }

    #[tokio::test]
    async fn slow_subscriber_does_not_block_delivery_to_others() {
        let s = Arc::new(LocalEventStream::new(1));
        let (slow, mut slow_rx, slow_gate) = gated();
        let (fast, mut fast_rx, fast_gate) = gated();
        subscribe(&s, "fan", slow).await; // first, so sequential sends would stall on it
        subscribe(&s, "fan", fast).await;

        publish(&s, "fan", "a").await;
        sleep(Duration::from_millis(20)).await;
        publish(&s, "fan", "b").await; // both channels now full

        let s2 = s.clone();
        let third = tokio::spawn(async move { s2.publish("fan".into(), b"c".to_vec()).await });
        sleep(Duration::from_millis(20)).await;

        // Free only the fast subscriber: it must get everything while the slow one is stuck.
        fast_gate.add_permits(3);
        for want in ["a", "b", "c"] {
            assert_eq!(next(&mut fast_rx).await.1, want.as_bytes());
        }
        assert!(
            !third.is_finished(),
            "publish waits for the slow subscriber to accept"
        );

        slow_gate.add_permits(3);
        third.await.unwrap().unwrap();
        for want in ["a", "b", "c"] {
            assert_eq!(next(&mut slow_rx).await.1, want.as_bytes());
        }
    }

    // ---------- cleanup (#1) ----------

    #[tokio::test]
    async fn closed_exact_sender_is_removed_on_publish() {
        let s = LocalEventStream::new(4);
        let (tx, rx) = mpsc::channel::<Msg>(4);
        drop(rx);
        s.exact.entry("a.b".into()).or_default().push(tx);

        publish(&s, "a.b", "x").await;
        assert!(s.exact.get("a.b").is_none());
    }

    #[tokio::test]
    async fn closed_wildcard_sender_is_removed_on_publish() {
        let s = LocalEventStream::new(4);
        let (tx, rx) = mpsc::channel::<Msg>(4);
        drop(rx);
        s.wildcards.entry("a.*".into()).or_default().push(tx);

        publish(&s, "a.b", "x").await;
        assert!(s.wildcards.get("a.*").is_none());
    }

    #[tokio::test]
    async fn dead_sender_removal_keeps_live_siblings() {
        let s = LocalEventStream::new(4);
        let (h, mut rx) = recorder();
        subscribe(&s, "a.b", h).await;
        let (dead_tx, dead_rx) = mpsc::channel::<Msg>(4);
        drop(dead_rx);
        s.exact.get_mut("a.b").unwrap().push(dead_tx);

        publish(&s, "a.b", "1").await;
        assert_eq!(next(&mut rx).await.1, b"1");
        assert_eq!(s.exact.get("a.b").unwrap().len(), 1);

        publish(&s, "a.b", "2").await; // still routed to the survivor
        assert_eq!(next(&mut rx).await.1, b"2");
    }

    #[test]
    fn prune_never_removes_an_entry_that_has_a_live_sender() {
        let map: DashMap<String, Vec<Tx>> = DashMap::new();
        let (live, _live_rx) = mpsc::channel::<Msg>(1);
        let (dead, dead_rx) = mpsc::channel::<Msg>(1);
        drop(dead_rx);
        map.insert("k".into(), vec![dead, live]);

        LocalEventStream::prune(&map, "k");
        assert_eq!(map.get("k").unwrap().len(), 1);

        LocalEventStream::prune(&map, "missing"); // no-op, no panic
    }

    // ---------- lifecycle (#6) ----------

    #[tokio::test]
    async fn dropping_the_stream_aborts_handler_tasks() {
        let marker = Arc::new(());
        let s = LocalEventStream::new(4);
        s.subscribe("t".into(), Arc::new(Marker(marker.clone())))
            .await
            .unwrap();
        assert_eq!(Arc::strong_count(&marker), 2); // the task holds the handler

        drop(s);
        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            Arc::strong_count(&marker),
            1,
            "handler task should have been aborted"
        );
    }
}
