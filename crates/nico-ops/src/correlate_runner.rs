//! PRD-007 Slice 2 (#374) — streaming async wrapper around
//! [`nico_correlate::collect_all`].
//!
//! [`run_correlate`] takes a prepared list of named [`Source`]s and
//! returns a [`CorrelateStream`] that yields [`CorrelateUpdate`]s as
//! sources land. The stream's [`Drop`] impl cancels in-flight per-source
//! tasks via a shared [`CancellationToken`] so dismissing the popup
//! mid-fetch shuts everything down cleanly — no errors logged, no task
//! leaks.
//!
//! Wire order is: `Loading { sources }` → per-source landings in
//! *completion* order (`SourceLanded` / `SourceFailed`) →
//! [`CorrelateUpdate::Diagnosis`] → [`CorrelateUpdate::Done`]. The
//! consumer must accept Diagnosis arriving at any point and still
//! render a consistent view — the popup's incremental render relies on
//! that invariant.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use nico_correlate::diagnosis::{Diagnosis, DiagnosisConfig, diagnose};
use nico_correlate::event::Event;
use nico_correlate::id::IdType;
use nico_correlate::source::{Source, SourceResult, StateEntry};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

pub type SourceName = &'static str;

/// One increment of progress from a streaming correlate run.
#[derive(Debug)]
pub enum CorrelateUpdate {
    /// Always the first item. Lists every source the run is waiting on
    /// so the popup can paint the source-availability dots row up
    /// front in `⟳` state.
    Loading { sources: Vec<SourceName> },
    /// A source completed successfully. Carries its events + state so
    /// the consumer can extend its accumulated timeline incrementally.
    SourceLanded {
        source: SourceName,
        events: Vec<Event>,
        state: Vec<StateEntry>,
    },
    /// A source reported [`SourceResult::Unavailable`]. The consumer
    /// renders this as a synthetic `source_error` row.
    SourceFailed { source: SourceName, reason: String },
    /// Diagnosis computed over the accumulated events + state from the
    /// landed sources. `None` when no pattern matched.
    Diagnosis { diagnosis: Option<Diagnosis> },
    /// Terminal item; the consumer can stop polling.
    Done,
}

/// Cancellable async stream of [`CorrelateUpdate`]s.
///
/// Dropping this struct cancels in-flight per-source tasks via the
/// shared [`CancellationToken`]. Cancellation is the happy path — no
/// error is reported back to the caller.
pub struct CorrelateStream {
    inner: ReceiverStream<CorrelateUpdate>,
    cancel: CancellationToken,
}

impl Drop for CorrelateStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Stream for CorrelateStream {
    type Item = CorrelateUpdate;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safety: `inner` is the only field we project. `Drop` only
        // touches `cancel`, never the receiver.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_next(cx)
    }
}

/// Capacity of the per-stream update channel. Sized so a typical 5-source
/// run (Loading + 5 SourceLanded + Diagnosis + Done = 8 items) never
/// blocks a producer task when the consumer is briefly slow.
const CHANNEL_CAPACITY: usize = 32;

/// Spawn a streaming correlate run. The returned [`CorrelateStream`]
/// yields a [`CorrelateUpdate::Loading`] first, then per-source
/// landings in completion order, then [`CorrelateUpdate::Diagnosis`],
/// then [`CorrelateUpdate::Done`]. Dropping the stream cancels in-flight
/// source tasks.
pub fn run_correlate(
    sources: Vec<(SourceName, Box<dyn Source>)>,
    id: String,
    id_type: IdType,
    diag_config: DiagnosisConfig,
) -> CorrelateStream {
    let cancel = CancellationToken::new();
    let (tx, rx) = mpsc::channel::<CorrelateUpdate>(CHANNEL_CAPACITY);

    let cancel_child = cancel.clone();
    tokio::spawn(async move {
        run_streaming(sources, id, id_type, diag_config, cancel_child, tx).await;
    });

    CorrelateStream {
        inner: ReceiverStream::new(rx),
        cancel,
    }
}

async fn run_streaming(
    sources: Vec<(SourceName, Box<dyn Source>)>,
    id: String,
    id_type: IdType,
    diag_config: DiagnosisConfig,
    cancel: CancellationToken,
    tx: mpsc::Sender<CorrelateUpdate>,
) {
    let names: Vec<SourceName> = sources.iter().map(|(n, _)| *n).collect();
    if tx
        .send(CorrelateUpdate::Loading { sources: names })
        .await
        .is_err()
    {
        return;
    }

    let mut set: JoinSet<Option<(SourceName, SourceResult)>> = JoinSet::new();
    for (name, source) in sources {
        let id = id.clone();
        let id_type = id_type.clone();
        let cancel = cancel.clone();
        set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => None,
                result = source.collect(&id, &id_type) => Some((name, result)),
            }
        });
    }

    let mut all_events: Vec<Event> = Vec::new();
    let mut all_state: Vec<StateEntry> = Vec::new();

    while let Some(joined) = set.join_next().await {
        if cancel.is_cancelled() {
            return;
        }
        let Ok(landed) = joined else {
            continue;
        };
        let Some((name, result)) = landed else {
            continue;
        };
        match result {
            SourceResult::Output(out) => {
                all_events.extend(out.events.iter().cloned());
                all_state.extend(out.state.iter().cloned());
                let update = CorrelateUpdate::SourceLanded {
                    source: name,
                    events: out.events,
                    state: out.state,
                };
                if tx.send(update).await.is_err() {
                    return;
                }
            }
            SourceResult::Unavailable(u) => {
                let update = CorrelateUpdate::SourceFailed {
                    source: name,
                    reason: u.reason.clone(),
                };
                if tx.send(update).await.is_err() {
                    return;
                }
            }
        }
    }

    if cancel.is_cancelled() {
        return;
    }

    let diag = diagnose(&all_events, &all_state, &diag_config);
    if tx
        .send(CorrelateUpdate::Diagnosis { diagnosis: diag })
        .await
        .is_err()
    {
        return;
    }
    let _ = tx.send(CorrelateUpdate::Done).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use futures::StreamExt;
    use nico_correlate::event::Severity;
    use nico_correlate::source::{SourceOutput, SourceUnavailable};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    /// Mock source returning a pre-baked result after a controlled
    /// delay. Tracks whether `collect` was awoken (used in the cancel
    /// test to assert a slow source never produces a SourceLanded).
    struct MockSource {
        name: &'static str,
        delay: Duration,
        result: Option<SourceResultBlueprint>,
        collected: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    enum SourceResultBlueprint {
        Ok { events: Vec<Event>, state: Vec<StateEntry> },
        Unavailable { reason: String },
    }

    impl MockSource {
        fn new_ok(name: &'static str, delay_ms: u64, events: Vec<Event>) -> Self {
            Self {
                name,
                delay: Duration::from_millis(delay_ms),
                result: Some(SourceResultBlueprint::Ok {
                    events,
                    state: Vec::new(),
                }),
                collected: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn new_unavailable(name: &'static str, delay_ms: u64, reason: &str) -> Self {
            Self {
                name,
                delay: Duration::from_millis(delay_ms),
                result: Some(SourceResultBlueprint::Unavailable {
                    reason: reason.to_string(),
                }),
                collected: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn collected_handle(&self) -> Arc<AtomicUsize> {
            self.collected.clone()
        }
    }

    #[async_trait]
    impl Source for MockSource {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn collect(&self, _id: &str, _id_type: &IdType) -> SourceResult {
            sleep(self.delay).await;
            self.collected.fetch_add(1, Ordering::SeqCst);
            match self.result.clone().expect("blueprint set") {
                SourceResultBlueprint::Ok { events, state } => {
                    SourceResult::Output(SourceOutput { events, state })
                }
                SourceResultBlueprint::Unavailable { reason } => {
                    SourceResult::Unavailable(SourceUnavailable {
                        name: self.name,
                        reason,
                    })
                }
            }
        }
    }

    fn event(source: &str, kind: &str) -> Event {
        Event {
            ts: Utc::now(),
            source: source.to_string(),
            kind: kind.to_string(),
            message: String::new(),
            severity: Severity::Info,
            tags: HashMap::new(),
        }
    }

    fn diag_config() -> DiagnosisConfig {
        DiagnosisConfig {
            stuck_threshold: Duration::from_secs(30 * 60),
        }
    }

    fn landed_sources(updates: &[CorrelateUpdate]) -> Vec<SourceName> {
        updates
            .iter()
            .filter_map(|u| match u {
                CorrelateUpdate::SourceLanded { source, .. } => Some(*source),
                CorrelateUpdate::SourceFailed { source, .. } => Some(*source),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn single_source_emits_loading_then_landed_then_diagnosis_then_done() {
        let src = MockSource::new_ok("temporal", 0, vec![event("temporal", "Started")]);
        let stream = run_correlate(
            vec![("temporal", Box::new(src))],
            "wf-001".into(),
            IdType::Workflow,
            diag_config(),
        );
        let updates: Vec<_> = stream.collect().await;
        assert_eq!(updates.len(), 4, "expected 4 updates, got {updates:?}");
        assert!(
            matches!(&updates[0], CorrelateUpdate::Loading { sources } if sources == &vec!["temporal"]),
            "first: {:?}",
            updates[0]
        );
        assert!(
            matches!(&updates[1], CorrelateUpdate::SourceLanded { source, events, .. }
                if *source == "temporal" && events.len() == 1),
            "second: {:?}",
            updates[1]
        );
        assert!(
            matches!(&updates[2], CorrelateUpdate::Diagnosis { .. }),
            "third: {:?}",
            updates[2]
        );
        assert!(matches!(updates[3], CorrelateUpdate::Done));
    }

    #[tokio::test]
    async fn sources_land_in_completion_order_not_input_order() {
        // Input order: slow, fast. Expect fast to land first.
        let slow = MockSource::new_ok("slow", 80, vec![event("slow", "X")]);
        let fast = MockSource::new_ok("fast", 5, vec![event("fast", "Y")]);
        let stream = run_correlate(
            vec![("slow", Box::new(slow)), ("fast", Box::new(fast))],
            "wf-001".into(),
            IdType::Workflow,
            diag_config(),
        );
        let updates: Vec<_> = stream.collect().await;
        let order = landed_sources(&updates);
        assert_eq!(order, vec!["fast", "slow"], "raw updates: {updates:?}");
    }

    #[tokio::test]
    async fn unavailable_source_emits_source_failed_with_reason() {
        let ok = MockSource::new_ok("temporal", 0, vec![event("temporal", "Started")]);
        let bad = MockSource::new_unavailable("loki", 5, "LOKI_URL not set");
        let stream = run_correlate(
            vec![("temporal", Box::new(ok)), ("loki", Box::new(bad))],
            "wf-001".into(),
            IdType::Workflow,
            diag_config(),
        );
        let updates: Vec<_> = stream.collect().await;
        let failed: Vec<_> = updates
            .iter()
            .filter_map(|u| match u {
                CorrelateUpdate::SourceFailed { source, reason } => Some((*source, reason.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(failed, vec![("loki", "LOKI_URL not set".to_string())]);

        let landed: Vec<_> = updates
            .iter()
            .filter_map(|u| match u {
                CorrelateUpdate::SourceLanded { source, .. } => Some(*source),
                _ => None,
            })
            .collect();
        assert_eq!(landed, vec!["temporal"]);

        // Diagnosis + Done still arrive even with one source failed.
        assert!(
            updates
                .iter()
                .any(|u| matches!(u, CorrelateUpdate::Diagnosis { .. }))
        );
        assert!(
            updates
                .iter()
                .any(|u| matches!(u, CorrelateUpdate::Done))
        );
    }

    #[tokio::test]
    async fn dropping_stream_mid_fetch_cancels_slow_source_with_no_leaks() {
        let slow = MockSource::new_ok("slow", 5_000, vec![event("slow", "Never")]);
        let collected = slow.collected_handle();
        let stream = run_correlate(
            vec![("slow", Box::new(slow))],
            "wf-001".into(),
            IdType::Workflow,
            diag_config(),
        );
        // Pull Loading; then drop the stream while the slow source is
        // still asleep. The CancellationToken should win the select! and
        // the source's `collected` counter should remain at 0.
        let mut stream = stream;
        let first = stream.next().await;
        assert!(matches!(first, Some(CorrelateUpdate::Loading { .. })));
        drop(stream);
        // Give the spawned tasks a moment to observe cancellation. The
        // mock's `collect` would sleep 5s otherwise — far longer than
        // this window.
        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            collected.load(Ordering::SeqCst),
            0,
            "cancelled source should not complete its work"
        );
    }

    #[tokio::test]
    async fn empty_source_set_still_emits_loading_diagnosis_done() {
        let stream = run_correlate(
            Vec::new(),
            "wf-001".into(),
            IdType::Workflow,
            diag_config(),
        );
        let updates: Vec<_> = stream.collect().await;
        assert_eq!(updates.len(), 3, "{updates:?}");
        assert!(matches!(&updates[0], CorrelateUpdate::Loading { sources } if sources.is_empty()));
        assert!(matches!(&updates[1], CorrelateUpdate::Diagnosis { diagnosis: None }));
        assert!(matches!(updates[2], CorrelateUpdate::Done));
    }
}
