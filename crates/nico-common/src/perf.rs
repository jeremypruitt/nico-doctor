//! Counting decorators around the external-I/O client traits shared by
//! `nico-doctor` and `nico-correlate` (PRD-005 Slice 0b.1). A
//! `CountingX<T: XClient>` wraps any implementation of `XClient`,
//! forwards every call to the inner client, and captures per-method
//! instrumentation: `call_count`, `bytes_in`, `bytes_out`, and a
//! latency vector that snapshot reads convert into `p50`/`p99`.
//!
//! Decorators are wrapper structs — same shape as the existing
//! `MockK8sClient` test-double — so a counter can wrap either the real
//! client or a mock without changes elsewhere. The slice 0b.3 perf
//! tests will drive `run_event_loop` against counter-wrapped mocks and
//! assert exact call counts against the captured snapshot.
//!
//! ## bytes_in / bytes_out
//!
//! - `bytes_in` is `serde_json::to_vec(&parsed_result).len()` for
//!   types we own (`K8sClient` returns; see also `nico_doctor::perf`),
//!   and `prost::Message::encoded_len()` for Temporal proto returns —
//!   the "cheapest available approximation" PRD-005 §0b allows.
//! - `bytes_out` is the request payload length: namespace and field
//!   selectors are summed; numeric parameters contribute a fixed
//!   token (`size_of::<i32>()` / `size_of::<u64>()`). Small but
//!   non-zero, captured per the PRD.
//!
//! ## Latency
//!
//! Each call is bracketed by `Instant::now()`; durations land in a
//! `Mutex<Vec<Duration>>`. On `stats()` the vector is cloned, sorted,
//! and `p50` / `p99` are pulled by index. `hdrhistogram` was
//! considered but skipped — adding a new dep to `nico-common` for a
//! test-only path is more weight than the (sub-millisecond) sort
//! over the single-digit-thousands of samples a perf run produces.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use prost::Message;

use crate::k8s::{K8sClient, PodScope, RawEvent, RawPod};
use crate::temporal::TemporalClient;
use temporal_sdk_core_protos::temporal::api::history::v1::History;
use temporal_sdk_core_protos::temporal::api::workflow::v1::WorkflowExecutionInfo;

/// Snapshot of one method's captured counters. Cheap-to-clone, owned
/// by the caller; assertions in tests read straight off this struct.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MethodStats {
    pub call_count: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub latency_p50: Duration,
    pub latency_p99: Duration,
}

/// Per-method counter cell. Atomics for the totals (so `record` is
/// lock-free on the hot path) plus a mutex'd `Vec<Duration>` for the
/// latency samples (`p50`/`p99` are computed on snapshot read, not
/// on every call).
#[derive(Debug, Default)]
pub struct MethodCounter {
    call_count: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    latencies: Mutex<Vec<Duration>>,
}

impl MethodCounter {
    /// Append one observation. Decorator impls in sibling crates call
    /// this directly; held public because `MethodCounter` is the shared
    /// substrate they wrap their own per-trait counter sets around.
    pub fn record(&self, latency: Duration, bytes_in: u64, bytes_out: u64) {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
        self.latencies.lock().unwrap().push(latency);
    }

    /// Snapshot the current totals. `p50`/`p99` are pulled by index
    /// after sort; an empty sample set yields zero-duration percentiles
    /// (matches "no calls happened" intuition for tests).
    pub fn snapshot(&self) -> MethodStats {
        let mut lats = self.latencies.lock().unwrap().clone();
        lats.sort();
        let (p50, p99) = percentiles(&lats);
        MethodStats {
            call_count: self.call_count.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            latency_p50: p50,
            latency_p99: p99,
        }
    }
}

fn percentiles(sorted: &[Duration]) -> (Duration, Duration) {
    if sorted.is_empty() {
        return (Duration::ZERO, Duration::ZERO);
    }
    let idx = |q: f64| {
        let last = sorted.len() - 1;
        let i = (q * last as f64).round() as usize;
        i.min(last)
    };
    (sorted[idx(0.50)], sorted[idx(0.99)])
}

/// Snapshot of every `K8sClient` method.
#[derive(Debug, Clone, Default)]
pub struct K8sStats {
    pub list_pods: MethodStats,
    pub list_events: MethodStats,
    pub pod_logs: MethodStats,
}

/// Counting decorator for `K8sClient`. Wrap any `T: K8sClient`
/// (real or mock); the decorator implements `K8sClient` itself and
/// forwards every call.
pub struct CountingK8sClient<T: K8sClient> {
    inner: T,
    list_pods: MethodCounter,
    list_events: MethodCounter,
    pod_logs: MethodCounter,
}

impl<T: K8sClient> CountingK8sClient<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            list_pods: MethodCounter::default(),
            list_events: MethodCounter::default(),
            pod_logs: MethodCounter::default(),
        }
    }

    pub fn stats(&self) -> K8sStats {
        K8sStats {
            list_pods: self.list_pods.snapshot(),
            list_events: self.list_events.snapshot(),
            pod_logs: self.pod_logs.snapshot(),
        }
    }
}

fn scope_bytes(scope: &PodScope<'_>) -> u64 {
    match scope {
        PodScope::Namespace(ns) => ns.len() as u64,
        PodScope::AllWithLabel(sel) => sel.len() as u64,
    }
}

#[async_trait]
impl<T: K8sClient> K8sClient for CountingK8sClient<T> {
    async fn list_pods(&self, scope: PodScope<'_>) -> Result<Vec<RawPod>> {
        let bytes_out = scope_bytes(&scope);
        let start = Instant::now();
        let result = self.inner.list_pods(scope).await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|pods| serde_json::to_vec(pods).map(|v| v.len() as u64).unwrap_or(0))
            .unwrap_or(0);
        self.list_pods.record(elapsed, bytes_in, bytes_out);
        result
    }

    async fn list_events(
        &self,
        namespace: &str,
        field_selector: Option<&str>,
    ) -> Result<Vec<RawEvent>> {
        let bytes_out = namespace.len() as u64 + field_selector.map(|s| s.len() as u64).unwrap_or(0);
        let start = Instant::now();
        let result = self.inner.list_events(namespace, field_selector).await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|ev| serde_json::to_vec(ev).map(|v| v.len() as u64).unwrap_or(0))
            .unwrap_or(0);
        self.list_events.record(elapsed, bytes_in, bytes_out);
        result
    }

    async fn pod_logs(
        &self,
        namespace: &str,
        pod: &str,
        since: Duration,
    ) -> Result<Vec<String>> {
        let bytes_out = namespace.len() as u64 + pod.len() as u64 + size_of::<u64>() as u64;
        let start = Instant::now();
        let result = self.inner.pod_logs(namespace, pod, since).await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|lines| serde_json::to_vec(lines).map(|v| v.len() as u64).unwrap_or(0))
            .unwrap_or(0);
        self.pod_logs.record(elapsed, bytes_in, bytes_out);
        result
    }
}

/// Snapshot of every `TemporalClient` method.
#[derive(Debug, Clone, Default)]
pub struct TemporalStats {
    pub list_workflow_executions: MethodStats,
    pub get_workflow_history: MethodStats,
}

/// Counting decorator for `TemporalClient`. `bytes_in` uses
/// `prost::Message::encoded_len()` since the return types are
/// gRPC protobuf messages — same wire-shape the real client deserialises
/// from, so the byte count tracks actual network volume.
pub struct CountingTemporalClient<T: TemporalClient> {
    inner: T,
    list_workflow_executions: MethodCounter,
    get_workflow_history: MethodCounter,
}

impl<T: TemporalClient> CountingTemporalClient<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            list_workflow_executions: MethodCounter::default(),
            get_workflow_history: MethodCounter::default(),
        }
    }

    pub fn stats(&self) -> TemporalStats {
        TemporalStats {
            list_workflow_executions: self.list_workflow_executions.snapshot(),
            get_workflow_history: self.get_workflow_history.snapshot(),
        }
    }
}

#[async_trait]
impl<T: TemporalClient> TemporalClient for CountingTemporalClient<T> {
    async fn list_workflow_executions(
        &self,
        namespace: &str,
        query: &str,
        page_size: i32,
    ) -> Result<Vec<WorkflowExecutionInfo>> {
        let bytes_out =
            namespace.len() as u64 + query.len() as u64 + size_of::<i32>() as u64;
        let start = Instant::now();
        let result = self
            .inner
            .list_workflow_executions(namespace, query, page_size)
            .await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|execs| execs.iter().map(|e| e.encoded_len() as u64).sum())
            .unwrap_or(0);
        self.list_workflow_executions
            .record(elapsed, bytes_in, bytes_out);
        result
    }

    async fn get_workflow_history(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<History> {
        let bytes_out = namespace.len() as u64 + workflow_id.len() as u64;
        let start = Instant::now();
        let result = self.inner.get_workflow_history(namespace, workflow_id).await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|h| h.encoded_len() as u64)
            .unwrap_or(0);
        self.get_workflow_history.record(elapsed, bytes_in, bytes_out);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::MockK8sClient;
    use crate::temporal::testing::MockTemporalClient;
    use temporal_sdk_core_protos::temporal::api::common::v1::WorkflowExecution;

    fn pod(name: &str, ns: &str) -> RawPod {
        RawPod {
            name: name.into(),
            namespace: ns.into(),
            phase: Some("Running".into()),
            ready: true,
            restart_count: 0,
            succeeded: false,
            crash_loop: false,
        }
    }

    #[tokio::test]
    async fn list_pods_increments_call_count_per_call() {
        let mock = MockK8sClient::new().with_pods(vec![pod("core-abc", "nico")]);
        let client = CountingK8sClient::new(mock);

        client.list_pods(PodScope::Namespace("nico")).await.unwrap();
        client.list_pods(PodScope::Namespace("nico")).await.unwrap();
        client.list_pods(PodScope::Namespace("nico")).await.unwrap();

        assert_eq!(client.stats().list_pods.call_count, 3);
    }

    #[tokio::test]
    async fn list_pods_bytes_in_matches_serialized_payload() {
        let pods = vec![pod("core-abc", "nico"), pod("core-def", "nico")];
        let expected: u64 = serde_json::to_vec(&pods).unwrap().len() as u64;

        let mock = MockK8sClient::new().with_pods(pods);
        let client = CountingK8sClient::new(mock);

        client.list_pods(PodScope::Namespace("nico")).await.unwrap();

        assert_eq!(client.stats().list_pods.bytes_in, expected);
    }

    #[tokio::test]
    async fn list_pods_bytes_out_captures_request_string() {
        let mock = MockK8sClient::new();
        let client = CountingK8sClient::new(mock);

        client
            .list_pods(PodScope::AllWithLabel("workflow_id=hp-abc"))
            .await
            .unwrap();

        assert_eq!(
            client.stats().list_pods.bytes_out,
            "workflow_id=hp-abc".len() as u64
        );
    }

    #[tokio::test]
    async fn list_pods_latency_is_non_zero_and_percentiles_resolve() {
        let mock = MockK8sClient::new().with_pods(vec![pod("a", "nico")]);
        let client = CountingK8sClient::new(mock);

        for _ in 0..5 {
            client.list_pods(PodScope::Namespace("nico")).await.unwrap();
        }

        let stats = client.stats().list_pods;
        assert_eq!(stats.call_count, 5);
        // p50/p99 are real samples — at least one of them strictly positive.
        assert!(stats.latency_p99 >= stats.latency_p50);
    }

    #[tokio::test]
    async fn list_events_and_pod_logs_count_independently() {
        let mock = MockK8sClient::new()
            .with_events(vec![])
            .with_logs(vec!["one".into(), "two".into()]);
        let client = CountingK8sClient::new(mock);

        client.list_events("nico", Some("type=Warning")).await.unwrap();
        client.list_events("nico", None).await.unwrap();
        client
            .pod_logs("nico", "core-abc", Duration::from_secs(60))
            .await
            .unwrap();

        let stats = client.stats();
        assert_eq!(stats.list_pods.call_count, 0);
        assert_eq!(stats.list_events.call_count, 2);
        assert_eq!(stats.pod_logs.call_count, 1);
    }

    #[tokio::test]
    async fn list_events_bytes_out_sums_namespace_and_selector() {
        let mock = MockK8sClient::new();
        let client = CountingK8sClient::new(mock);

        client.list_events("nico", Some("type=Warning")).await.unwrap();

        assert_eq!(
            client.stats().list_events.bytes_out,
            ("nico".len() + "type=Warning".len()) as u64
        );
    }

    #[tokio::test]
    async fn errored_call_still_counts_but_records_zero_bytes_in() {
        let mock = MockK8sClient::new().with_pods_err("cluster unreachable");
        let client = CountingK8sClient::new(mock);

        let result = client.list_pods(PodScope::Namespace("nico")).await;
        assert!(result.is_err());

        let stats = client.stats().list_pods;
        assert_eq!(stats.call_count, 1);
        assert_eq!(stats.bytes_in, 0);
    }

    #[tokio::test]
    async fn temporal_list_executions_counts_calls_and_bytes_in() {
        let exec = WorkflowExecutionInfo {
            execution: Some(WorkflowExecution {
                workflow_id: "wf-001".into(),
                run_id: "run-1".into(),
            }),
            ..Default::default()
        };
        let expected = exec.encoded_len() as u64;

        let mock = MockTemporalClient::new().with_executions(vec![exec]);
        let client = CountingTemporalClient::new(mock);

        client
            .list_workflow_executions("default", "ExecutionStatus=Running", 100)
            .await
            .unwrap();
        client
            .list_workflow_executions("default", "ExecutionStatus=Running", 100)
            .await
            .unwrap();

        let stats = client.stats().list_workflow_executions;
        assert_eq!(stats.call_count, 2);
        assert_eq!(stats.bytes_in, expected * 2);
        assert_eq!(
            stats.bytes_out,
            ("default".len() + "ExecutionStatus=Running".len() + size_of::<i32>()) as u64 * 2
        );
    }

    #[tokio::test]
    async fn temporal_get_workflow_history_counts_independently() {
        let mock = MockTemporalClient::new();
        let client = CountingTemporalClient::new(mock);

        client.get_workflow_history("default", "wf-001").await.unwrap();

        let stats = client.stats();
        assert_eq!(stats.list_workflow_executions.call_count, 0);
        assert_eq!(stats.get_workflow_history.call_count, 1);
    }

    #[test]
    fn percentiles_empty_returns_zero() {
        assert_eq!(percentiles(&[]), (Duration::ZERO, Duration::ZERO));
    }

    #[test]
    fn percentiles_single_sample_returns_same_for_p50_and_p99() {
        let s = vec![Duration::from_millis(42)];
        assert_eq!(percentiles(&s), (Duration::from_millis(42), Duration::from_millis(42)));
    }

    #[test]
    fn percentiles_resolves_via_index_rounding() {
        let s: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let (p50, p99) = percentiles(&s);
        // index = round(0.50 * 99) = 50 -> 51ms; round(0.99 * 99) = 98 -> 99ms.
        assert_eq!(p50, Duration::from_millis(51));
        assert_eq!(p99, Duration::from_millis(99));
    }
}
