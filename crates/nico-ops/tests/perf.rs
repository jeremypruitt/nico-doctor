//! PRD-005 regression-guard integration tests for the `nico ops` event loop.
//!
//! Slice 0a.3 (issue #348) seeded three wall-clock / memory tripwires:
//!
//! 1. `cold_start_to_first_paint` — wall-clock from refresh-trigger to
//!    first `Action::Snapshots` reduce stays under a generous bound.
//! 2. `idle_tick_does_not_re_render` — once a refresh has settled,
//!    `Action::Tick` does not flip `app.dirty()` (Finding #1 guard).
//! 3. `memory_bounded_after_n_refreshes` — 1000 reduce cycles do not
//!    grow live heap past a bound (dhat-gated).
//!
//! Slice 0b.3 (issue #351) adds three network/data tripwires on top of
//! the counting decorators landed in Slice 0b.1 (`nico_common::perf`)
//! and Slice 0b.2 (`nico_doctor::perf_source`):
//!
//! 4. `refresh_call_counts` — drive one refresh cycle against a counter-
//!    decorated `K8sClient` and assert EXACT call counts. Catches the
//!    duplicate-fetch regression class that issue #201 originally
//!    fought; PRD-005's Finding #6 confirms the snapshot logs panel
//!    (`spawn_logs_refresh`) still bypasses the shared cache.
//! 5. `refresh_data_volume_under_n_kb` — same harness, sweep
//!    `N ∈ {1, 18, 250, 1000, 10000}`, assert per-N bound on total
//!    `bytes_in`. Tripwire for sudden payload-shape regressions.
//! 6. `deserialize_time_budget` — drive Source-trait decorators against
//!    real `perf_fixtures` parse work, assert total `deserialize_time`
//!    per refresh stays under a bound, and flag decorators whose share
//!    is disproportionate. Feeds Slice 1's audit.
//!
//! ## Scope notes
//!
//! Acceptance criterion #2 of issue #348 (and the same criterion of
//! issue #351) calls for "drive `run_event_loop` against fully stubbed
//! clients". The current `run_event_loop` takes a concrete
//! `Terminal<CrosstermBackend<Stdout>>` and pulls events off
//! `EventStream::new()`, neither of which is testable without a
//! backend-genericisation refactor (out of scope for both slices).
//! These tests instead drive the same behaviors at the `data::collect`,
//! `App::handle`, and `LogSource::collect` layer — the same composable
//! seams `run_event_loop`'s `spawn_refresh` / `spawn_logs_refresh`
//! invoke at refresh-time. Future slices that genericise the event
//! loop can re-target these tests to the wider end-to-end path without
//! changing the assertions.
//!
//! ## Per-decorator summary table
//!
//! Slice 0b.3 tests emit a markdown-style summary table to stdout when
//! run with `--nocapture`. The table is the operator-facing artefact
//! Slice 1's audit consumes; the assertions above are the regression
//! guard. To see it:
//!
//! ```bash
//! cargo test -p nico-ops --test perf -- --nocapture
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use nico_common::k8s::testing::MockK8sClient;
use nico_common::k8s::{K8sClient, RawPod};
use nico_common::perf::CountingK8sClient;
use nico_doctor::dpu::{DpuClient, DpuSnapshot};
use nico_doctor::dpu_cert::{CertSnapshot, DpuCertClient};
use nico_doctor::dpu_health::{DpuHealthClient, HealthSnapshot};
use nico_doctor::dpu_isolation::{DpuIsolationClient, IsolationSnapshot};
use nico_doctor::dpu_services::{DpuServicesClient, ServicesSnapshot};
use nico_doctor::hbn::{HbnClient, HbnSnapshot};
use nico_doctor::layer::{Layer, RunOpts, SkippedLayer};
use nico_doctor::layers::cluster::ClusterLayer;
use nico_doctor::layers::logs::LogsLayer;
use nico_doctor::log_collector::LogCollectorStage;
use nico_doctor::log_source::{K8sLogSource, LogSource};
use nico_doctor::perf_fixtures;
use nico_doctor::perf_source::{
    CountingDpuCertClient, CountingDpuClient, CountingDpuHealthClient, CountingDpuIsolationClient,
    CountingDpuServicesClient, CountingHbnClient, SourceMethodStats,
};
use nico_ops::action::Action;
use nico_ops::app::App;
use nico_ops::data;

// dhat's global allocator must live in the test binary's root file;
// the `dhat-heap` feature is the same opt-in flag the criterion
// benches use (see `benches/README.md`).
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Build N synthetic no-I/O layers. `SkippedLayer` resolves immediately
/// to `LayerOutcome::Skipped` — exactly the right shape for measuring
/// the fan-out + reduce path in isolation from the live cluster.
fn synthetic_layers(n: usize) -> Arc<Vec<Box<dyn Layer>>> {
    // `&'static str` constraint on `Layer::name` forces us to leak the
    // names; tests are short-lived so the leak is harmless.
    let layers: Vec<Box<dyn Layer>> = (0..n)
        .map(|i| {
            let raw = format!("synthetic_layer_{i}");
            let name: &'static str = Box::leak(raw.into_boxed_str());
            SkippedLayer::new(name)
        })
        .collect();
    Arc::new(layers)
}

/// Cold-start regression guard. Bound is intentionally generous (~1s
/// wall-clock) — this is a tripwire for catastrophic regressions, not
/// a tight latency budget. Local baseline on the maintainer's box is
/// O(1ms) for synthetic layers; the bound leaves three orders of
/// magnitude of slack so CI variance does not flake the test.
#[tokio::test]
async fn cold_start_to_first_paint() {
    let layers = synthetic_layers(6);
    let opts = RunOpts::default();
    let mut app = App::with_interval(Duration::from_secs(30));

    let start = Instant::now();
    let snapshots = data::collect(layers, opts, None).await;
    app.handle(Action::Snapshots(snapshots));
    let elapsed = start.elapsed();

    assert!(app.dirty(), "first paint should leave app dirty");
    assert_eq!(
        app.snapshots().len(),
        6,
        "six synthetic layers should produce six snapshots"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "cold-start to first paint took {elapsed:?}; bound is 1s"
    );
}

/// Finding #1 regression guard. The `Tick` reducer at `app.rs:484-487`
/// previously flipped `dirty = true` on every tick while `refreshing`
/// was set, forcing a full re-render at 10Hz even when nothing on
/// screen had changed. After a refresh settles (Snapshots reduce →
/// `refreshing = false`), idle ticks before the next deadline should
/// leave `dirty` alone. This test pins the post-settle steady state.
#[tokio::test]
async fn idle_tick_does_not_re_render() {
    let mut app = App::with_interval(Duration::from_secs(60));
    let t0 = Instant::now();

    // Warm: first Tick seeds boot+now, Snapshots schedules next_refresh
    // for `t0 + 60s` and clears `refreshing`. Drain dirty.
    app.handle(Action::Tick(t0));
    app.handle(Action::Snapshots(vec![]));
    app.clear_dirty();

    // Drive 100 idle ticks at 10Hz, all comfortably before the next
    // refresh deadline. None of them should flip `dirty`.
    let mut dirty_ticks = 0usize;
    for i in 1..=100u64 {
        let now = t0 + Duration::from_millis(i * 100);
        app.handle(Action::Tick(now));
        if app.dirty() {
            dirty_ticks += 1;
            app.clear_dirty();
        }
    }

    assert_eq!(
        dirty_ticks, 0,
        "{dirty_ticks} of 100 idle ticks flipped dirty — Finding #1 regression"
    );
}

/// Memory regression guard. Drives 1000 reduce cycles of fleet-scale
/// snapshots through the `App` and asserts dhat-tracked live heap
/// stays under a ceiling. Catches unbounded growth in the ringbuffer,
/// any future state-sharing stage, or per-cycle leaks. Gated behind
/// `--features dhat-heap` (Slice 0a.1) because the dhat allocator is
/// significantly slower than the system allocator; the default
/// `cargo test` run skips this test entirely.
#[cfg(feature = "dhat-heap")]
#[tokio::test]
async fn memory_bounded_after_n_refreshes() {
    use nico_common::output::Status;
    use nico_ops::model::{Finding, LayerSnapshot};

    let _profiler = dhat::Profiler::builder().testing().build();

    let mut app = App::with_interval(Duration::from_secs(30));
    let t0 = Instant::now();
    app.handle(Action::Tick(t0));

    // Pre-built 50-snapshot fleet, cloned per cycle so each reduce
    // sees a fresh allocation. 1000 cycles is the spec from the
    // acceptance criteria; 50-wide fleet is roughly twice the
    // operator-visible card grid.
    let template = (0..50)
        .map(|i| LayerSnapshot {
            name: format!("layer_{i:03}"),
            status: if i % 5 == 0 { Status::Warn } else { Status::Ok },
            evidence: format!("synthetic evidence row {i}"),
            findings: if i % 5 == 0 {
                vec![Finding {
                    status: Status::Warn,
                    message: format!("synthetic finding {i}"),
                    next_command: None,
                    link: None,
                }]
            } else {
                vec![]
            },
            duration_ms: (i % 100) as u64,
        })
        .collect::<Vec<_>>();

    for _ in 0..1000 {
        app.handle(Action::Snapshots(template.clone()));
    }

    // 50 MiB is generous: the ringbuffer is bounded, snapshots replace
    // (not accumulate), and per-cycle allocations should release on
    // the next reduce. Anything past this ceiling means a regression.
    let stats = dhat::HeapStats::get();
    let bound: u64 = 50 * 1024 * 1024;
    assert!(
        stats.curr_bytes < bound as usize,
        "live heap {} bytes after 1000 reduce cycles exceeds {bound} bound",
        stats.curr_bytes
    );
}

// =====================================================================
// Slice 0b.3 — network/data perf tests (issue #351)
// =====================================================================

const PERF_NAMESPACE: &str = "nico";

/// Build `n` non-`Succeeded` `RawPod`s, so the `LogCollectorStage`
/// fetches `pod_logs` for every one of them. Names are unique so the
/// per-pod cache keys do not collide.
fn fleet_pods(n: usize) -> Vec<RawPod> {
    (0..n)
        .map(|i| RawPod {
            name: format!("dpu-agent-{i:08x}"),
            namespace: PERF_NAMESPACE.into(),
            phase: Some("Running".into()),
            ready: true,
            restart_count: 0,
            succeeded: false,
            crash_loop: false,
        })
        .collect()
}

/// Representative log payload — half error lines, half ok lines — so
/// `bytes_in` is non-zero and the snapshot panel's `entries_from_cache`
/// filter has at least one match per pod. Eight lines × ~40 bytes ≈
/// 320 bytes/pod, in the same ballpark as a real `--tail=500` window
/// would produce after filtering.
fn fleet_log_lines() -> Vec<String> {
    vec![
        "INFO ready=true watcher started".into(),
        "ERROR reconcile loop hit deadline".into(),
        "DEBUG enqueued event q_len=12".into(),
        "WARN backoff increasing to 2s".into(),
        "ERROR failed to dial dpu host=10.0.0.5".into(),
        "INFO tick t=2026-05-11T12:00:00Z".into(),
        "ERROR panic recovered: nil ptr deref".into(),
        "INFO snapshot persisted".into(),
    ]
}

/// Drive one refresh cycle through the same composable seam
/// `run_event_loop` calls at refresh time:
///
/// - `spawn_refresh` →  `data::collect(layers, opts, Some(collector))`
///   which runs `LogCollectorStage::collect` once and then fans the
///   layers out.
/// - `spawn_logs_refresh` → `K8sLogSource::collect(.., &empty_cache)`
///   because the snapshot panel runs outside the doctor cache (see
///   `nico_ops::lib::spawn_logs_refresh`'s comment, and PRD-005's
///   Finding #6).
///
/// Returns the post-refresh `K8sStats` snapshot of the counter-decorated
/// `K8sClient`. Callers assert on exact counts / bytes.
async fn run_one_refresh(
    pods: Vec<RawPod>,
    log_lines: Vec<String>,
) -> nico_common::perf::K8sStats {
    let mock = MockK8sClient::new()
        .with_pods(pods.clone())
        .with_logs(log_lines)
        .with_events(vec![]);
    let counted: Arc<CountingK8sClient<MockK8sClient>> = Arc::new(CountingK8sClient::new(mock));
    let k8s: Arc<dyn K8sClient> = counted.clone();

    // `data::collect` path: cluster + logs layers + LogCollectorStage,
    // exactly the wiring `bootstrap::prepare_layers` produces for these
    // two layers when a kubeconfig is available.
    let layers: Arc<Vec<Box<dyn Layer>>> = Arc::new(vec![
        Box::new(ClusterLayer::new(k8s.clone())) as Box<dyn Layer>,
        Box::new(LogsLayer::new(Arc::new(K8sLogSource::new(k8s.clone()))))
            as Box<dyn Layer>,
    ]);
    let opts = RunOpts {
        namespace: PERF_NAMESPACE.into(),
        since: Duration::from_secs(600),
        timeout: Duration::from_secs(5),
        ..RunOpts::default()
    };
    let collector = Arc::new(LogCollectorStage::new(k8s.clone()));
    let _snapshots = data::collect(layers, opts.clone(), Some(collector)).await;

    // `spawn_logs_refresh` path: the snapshot panel passes an empty
    // prefetched cache, so `K8sLogSource` falls back to its own
    // `list_pods` + per-pod `pod_logs` fetch. This is the duplicate-
    // fetch that Finding #6 calls out.
    let panel_source = K8sLogSource::new(k8s.clone());
    let _ = panel_source
        .collect(
            &opts.namespace,
            opts.since,
            500,
            &HashMap::new(),
        )
        .await
        .expect("K8sLogSource fallback fetch should succeed against mock");

    counted.stats()
}

/// Print a per-method summary table to stdout. Visible under
/// `cargo test -- --nocapture`; silent under default cargo test runs
/// (which capture stdout per test). The format is markdown-ish so the
/// `--nocapture` artefact can be pasted straight into PR comments.
fn print_k8s_summary(label: &str, n: usize, stats: &nico_common::perf::K8sStats) {
    println!();
    println!("== {label} (N={n}) ==");
    println!("| method      | call_count | bytes_in | bytes_out |");
    println!("| ----------- | ---------- | -------- | --------- |");
    let row = |name: &str, m: &nico_common::perf::MethodStats| {
        println!(
            "| {name:11} | {:10} | {:8} | {:9} |",
            m.call_count, m.bytes_in, m.bytes_out
        );
    };
    row("list_pods", &stats.list_pods);
    row("list_events", &stats.list_events);
    row("pod_logs", &stats.pod_logs);
}

/// Acceptance criterion: `refresh_call_counts` asserts EXACT counts (not
/// bounds). Designed to fail loudly the moment a new fetch slips into
/// the refresh path. The exact counts here pin two regression classes:
///
/// 1. **Issue #201 / Finding #6** — duplicate `list_pods` + `pod_logs`
///    fetches between the LogCollectorStage cache (the "shared" path)
///    and `K8sLogSource`'s standalone fallback (the snapshot panel
///    path). Today the panel runs outside the cache, so we see the pod-
///    log fetch twice; closing this gap should change the exact counts
///    below and force the test to be updated.
///
/// 2. **`ClusterLayer` direct `list_pods` call** — independent of the
///    cache, today `ClusterLayer` issues its own `list_pods` for the
///    namespace summary. A future refactor that shares the
///    LogCollectorStage's pod list would drop this count by one.
///
/// Concrete counts for N=3 non-`Succeeded` pods in `PERF_NAMESPACE`:
///
/// | call         | callers                                              | total |
/// | ------------ | ---------------------------------------------------- | ----- |
/// | list_pods    | LogCollectorStage + ClusterLayer + K8sLogSource      | 3     |
/// | list_events  | ClusterLayer (warning events)                        | 1     |
/// | pod_logs     | LogCollectorStage (N) + K8sLogSource panel (N)       | 6     |
#[tokio::test]
async fn refresh_call_counts() {
    let n = 3;
    let stats = run_one_refresh(fleet_pods(n), fleet_log_lines()).await;
    print_k8s_summary("refresh_call_counts", n, &stats);

    assert_eq!(
        stats.list_pods.call_count, 3,
        "list_pods regressed from 3 (LogCollector + ClusterLayer + snapshot panel): \
         the duplicate-fetch class from Finding #6 + issue #201 has shifted"
    );
    assert_eq!(
        stats.list_events.call_count, 1,
        "list_events should be called exactly once per refresh (ClusterLayer's \
         warning-events probe)"
    );
    assert_eq!(
        stats.pod_logs.call_count, (2 * n) as u64,
        "pod_logs regressed from 2*N=6 (one set in LogCollector, one set in \
         snapshot panel): Finding #6's duplicate-fetch class has shifted"
    );
    // `bytes_in` is the cheapest available approximation
    // (`serde_json::to_vec(&parsed_result).len()`); any non-zero value
    // confirms the decorator is actually capturing the wire size of the
    // mocked responses rather than recording a no-op.
    assert!(
        stats.pod_logs.bytes_in > 0,
        "pod_logs.bytes_in should be non-zero on a populated fleet, got 0"
    );
}

/// Per-N upper bound on total `bytes_in` per refresh, in bytes. These
/// values are roughly 1.25× the deterministic ceiling produced by the
/// `MockK8sClient` + `fleet_log_lines` shape used in `run_one_refresh`
/// (which holds 8 log lines × N non-succeeded pods × 2 fetches via the
/// LogCollector + snapshot-panel duplication, plus 3 × N pods worth of
/// `list_pods` payload). They are deliberately loose tripwires for the
/// payload-shape regression class — a noisy log fixture or a new
/// per-pod field would still keep us well under the bound; doubling the
/// number of refresh-path fetches would not.
///
/// The current observed values were captured 2026-05-11 alongside this
/// test landing; see `benches/README.md` for the full table. Update
/// both this table and the README when the fixture shape changes.
fn refresh_bytes_in_bound(n: usize) -> u64 {
    match n {
        1 => 1_500,
        18 => 22_000,
        250 => 305_000,
        1_000 => 1_220_000,
        10_000 => 12_200_000,
        _ => panic!("no documented per-N bound for N={n}"),
    }
}

fn total_bytes_in(s: &nico_common::perf::K8sStats) -> u64 {
    s.list_pods.bytes_in + s.list_events.bytes_in + s.pod_logs.bytes_in
}

/// Acceptance criterion: total `bytes_in` per refresh stays under the
/// per-N bound for every entry in the fixture sweep
/// `N ∈ {1, 18, 250, 1000, 10000}`. The bound is informational — a
/// catastrophic regression tripwire — not a tight budget. Slice 1's
/// audit (issue #352) will rank candidates for tightening it.
///
/// Why the duplicate-fetch shape matters here: the bound is sized
/// against today's pattern of fetching `list_pods` + `pod_logs` twice
/// per refresh (LogCollector + snapshot panel, Finding #6). A future
/// slice that closes that gap will leave headroom; this test will keep
/// passing, but the observed-vs-bound ratio will drop visibly in the
/// summary table.
#[tokio::test]
async fn refresh_data_volume_under_n_kb() {
    for n in [1, 18, 250, 1_000, 10_000] {
        let stats = run_one_refresh(fleet_pods(n), fleet_log_lines()).await;
        print_k8s_summary("refresh_data_volume_under_n_kb", n, &stats);

        let total = total_bytes_in(&stats);
        let bound = refresh_bytes_in_bound(n);
        println!(
            "  → total bytes_in = {total}, per-N bound = {bound} ({:.1}% of bound)",
            (total as f64 / bound as f64) * 100.0,
        );
        assert!(
            total <= bound,
            "N={n}: total bytes_in {total} exceeded per-N bound {bound} — \
             a per-refresh payload-shape regression has landed; see \
             `refresh_bytes_in_bound` and `benches/README.md` for the \
             documented ceilings"
        );
    }
}

// --- Source-trait stubs ------------------------------------------------
//
// Each stub takes the rows synthesised by `perf_fixtures::synthesize_fleet`
// and runs them through the same public parsers the real Sqlx impls use
// (`parse_extension_services`, `parse_bgp_alerts`, etc.). That makes the
// inner-call wall time — which the `CountingX` decorator records as
// `deserialize_time` — track the actual JSON → typed-struct parse cost
// the production code pays per refresh. The stub bodies mirror the test
// stubs inside `perf_source.rs` (which are `cfg(test)`-private), kept in
// step deliberately so this integration test characterises the same
// parser surface.

fn placeholder_fleet_snapshot(dpu_id: &str) -> DpuSnapshot {
    DpuSnapshot {
        dpu_id: dpu_id.into(),
        applied_managed_host_config_version: "v1".into(),
        desired_managed_host_config_version: "v1".into(),
        applied_instance_network_config_version: "v1".into(),
        desired_instance_network_config_version: "v1".into(),
        quarantine_state: None,
        last_seen_at: Utc::now(),
        client_certificate_expiry: None,
        health_alerts: Vec::new(),
        network_config_error: None,
        hbn_version: String::new(),
        bgp_alerts: Vec::new(),
        extension_services_observed_at: None,
        extension_services: Vec::new(),
        infiniband_observed_at: None,
        infiniband_ufm_observable: None,
        infiniband_ports: Vec::new(),
        ib_alerts: Vec::new(),
    }
}

struct PerfStubDpu {
    rows: StdMutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl DpuClient for PerfStubDpu {
    async fn fetch_fleet(&self) -> Result<Vec<DpuSnapshot>> {
        let rows = self.rows.lock().unwrap().clone();
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let id = row.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let services = nico_doctor::dpu::parse_extension_services(
                row.get("network_status_observation")
                    .and_then(|n| n.get("extension_service_observation"))
                    .and_then(|e| e.get("extension_service_statuses")),
            );
            let mut snap = placeholder_fleet_snapshot(&id);
            snap.extension_services = services;
            out.push(snap);
        }
        Ok(out)
    }
}

struct PerfStubHealth {
    rows: StdMutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl DpuHealthClient for PerfStubHealth {
    async fn fetch_snapshot(&self, dpu_id: &str) -> Result<Option<HealthSnapshot>> {
        let rows = self.rows.lock().unwrap().clone();
        let Some(row) = rows.first().cloned() else {
            return Ok(None);
        };
        let _alerts = nico_doctor::dpu::parse_health_alerts(row.get("dpu_agent_health_report"));
        let extension_services = nico_doctor::dpu::parse_extension_services(
            row.get("network_status_observation")
                .and_then(|n| n.get("extension_service_observation"))
                .and_then(|e| e.get("extension_service_statuses")),
        );
        Ok(Some(HealthSnapshot {
            dpu_id: dpu_id.into(),
            agent_version: None,
            agent_version_superseded_at: None,
            alerts: Vec::new(),
            interfaces: Vec::new(),
            client_certificate_expiry: None,
            quarantine_state: None,
            last_seen_at: None,
            registered: true,
            scout_discovery_complete: true,
            hbn_version: String::new(),
            network_config_error: None,
            applied_managed_host_config_version: String::new(),
            desired_managed_host_config_version: String::new(),
            applied_instance_network_config_version: String::new(),
            desired_instance_network_config_version: String::new(),
            bgp_alerts: Vec::new(),
            extension_services_observed_at: None,
            extension_services,
            infiniband_observed_at: None,
            infiniband_ufm_observable: None,
            infiniband_ports: Vec::new(),
            ib_alerts: Vec::new(),
        }))
    }
}

struct PerfStubServices {
    rows: StdMutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl DpuServicesClient for PerfStubServices {
    async fn fetch_snapshot(&self, dpu_id: &str) -> Result<Option<ServicesSnapshot>> {
        let rows = self.rows.lock().unwrap().clone();
        let Some(row) = rows.first().cloned() else {
            return Ok(None);
        };
        let services = nico_doctor::dpu::parse_extension_services(
            row.get("network_status_observation")
                .and_then(|n| n.get("extension_service_observation"))
                .and_then(|e| e.get("extension_service_statuses")),
        );
        Ok(Some(ServicesSnapshot {
            dpu_id: dpu_id.into(),
            observed_at: None,
            services,
        }))
    }
}

struct PerfStubIsolation {
    rows: StdMutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl DpuIsolationClient for PerfStubIsolation {
    async fn fetch_snapshot(&self, machine_id: &str) -> Result<IsolationSnapshot> {
        let rows = self.rows.lock().unwrap().clone();
        // Drill the JSON the same way `SqlxDpuIsolationClient` does so
        // the inner call's wall time tracks the real parse cost.
        let quarantine = rows
            .first()
            .and_then(|r| r.get("network_config"))
            .and_then(|n| n.get("quarantine_state"))
            .and_then(|q| q.get("mode"))
            .and_then(|m| m.as_str())
            .map(str::to_owned);
        Ok(IsolationSnapshot {
            machine_id: machine_id.into(),
            registered: true,
            scout_discovery_complete: true,
            quarantine_state: quarantine,
            last_seen_at: None,
        })
    }
}

struct PerfStubCert {
    rows: StdMutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl DpuCertClient for PerfStubCert {
    async fn fetch_snapshot(&self, dpu_id: &str) -> Result<CertSnapshot> {
        let rows = self.rows.lock().unwrap().clone();
        let expiry = rows
            .first()
            .and_then(|r| r.get("network_status_observation"))
            .and_then(|n| n.get("client_certificate_expiry"))
            .and_then(|v| v.as_i64())
            .and_then(|s| chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0));
        Ok(CertSnapshot {
            dpu_id: dpu_id.into(),
            client_certificate_expiry: expiry,
        })
    }
}

struct PerfStubHbn {
    rows: StdMutex<Vec<serde_json::Value>>,
}

impl PerfStubHbn {
    fn build_snapshot(row: &serde_json::Value, dpu_id: &str) -> HbnSnapshot {
        let nco = row.get("network_status_observation");
        let bgp_alerts = nico_doctor::hbn::parse_bgp_alerts(row.get("dpu_agent_health_report"));
        HbnSnapshot {
            dpu_id: dpu_id.into(),
            hbn_version: String::new(),
            applied_managed_host_config_version: nco
                .and_then(|v| v.get("network_config_version"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .into(),
            desired_managed_host_config_version: String::new(),
            applied_instance_network_config_version: String::new(),
            desired_instance_network_config_version: String::new(),
            network_config_error: None,
            bgp_alerts,
            quarantine_state: None,
            last_seen_at: Utc::now(),
        }
    }
}

#[async_trait]
impl HbnClient for PerfStubHbn {
    async fn fetch_snapshot(&self, dpu_id: &str) -> Result<Option<HbnSnapshot>> {
        let rows = self.rows.lock().unwrap().clone();
        Ok(rows.first().map(|r| Self::build_snapshot(r, dpu_id)))
    }

    async fn fetch_all_snapshots(&self) -> Result<Vec<HbnSnapshot>> {
        let rows = self.rows.lock().unwrap().clone();
        Ok(rows
            .iter()
            .enumerate()
            .map(|(i, r)| Self::build_snapshot(r, &format!("dpu-{i:08x}")))
            .collect())
    }
}

/// Drive each Source-trait counting decorator through one refresh-shaped
/// call sequence against the perf-fixture rows and return the per-
/// decorator deserialize-time totals. Mirrors the call pattern the
/// production refresh path produces: one fleet fetch + one per-DPU
/// fetch through every per-DPU Source client.
struct DeserializeBreakdown {
    rows: Vec<(&'static str, SourceMethodStats)>,
}

impl DeserializeBreakdown {
    fn total(&self) -> Duration {
        self.rows.iter().map(|(_, s)| s.deserialize_time_total).sum()
    }

    fn print(&self, label: &str, n: usize) {
        println!();
        println!("== {label} (N={n}) ==");
        println!("| decorator                     | calls | total (µs) | p50 (µs) | p99 (µs) | % of total |");
        println!("| ----------------------------- | ----- | ---------- | -------- | -------- | ---------- |");
        let total = self.total().as_micros().max(1);
        for (name, s) in &self.rows {
            let pct = (s.deserialize_time_total.as_micros() as f64 / total as f64) * 100.0;
            println!(
                "| {name:<29} | {:5} | {:10} | {:8} | {:8} | {:9.1}% |",
                s.call_count,
                s.deserialize_time_total.as_micros(),
                s.deserialize_time_p50.as_micros(),
                s.deserialize_time_p99.as_micros(),
                pct,
            );
        }
        println!("|                               |       | {:10} |          |          |    100.0%  |", total);
    }
}

async fn run_source_decorators(n: usize) -> DeserializeBreakdown {
    let rows = perf_fixtures::synthesize_fleet(n);

    // Each Source decorator gets its own copy of the rows so per-call
    // parse work is independent — mirrors the production fan-out where
    // every layer drives its own client.
    let dpu = CountingDpuClient::new(PerfStubDpu {
        rows: StdMutex::new(rows.clone()),
    });
    dpu.fetch_fleet().await.expect("PerfStubDpu fetch_fleet");

    let health = CountingDpuHealthClient::new(PerfStubHealth {
        rows: StdMutex::new(rows.clone()),
    });
    let services = CountingDpuServicesClient::new(PerfStubServices {
        rows: StdMutex::new(rows.clone()),
    });
    let isolation = CountingDpuIsolationClient::new(PerfStubIsolation {
        rows: StdMutex::new(rows.clone()),
    });
    let cert = CountingDpuCertClient::new(PerfStubCert {
        rows: StdMutex::new(rows.clone()),
    });
    let hbn = CountingHbnClient::new(PerfStubHbn {
        rows: StdMutex::new(rows.clone()),
    });

    // Per-DPU layers (`dpu_health`, `dpu_services`, `dpu_isolation`,
    // `dpu_cert`) fan out one call per DPU per refresh. `hbn` runs the
    // fleet-wide variant once per refresh (matches `SqlxHbnClient`'s
    // fleet path used by `HbnLayer`).
    for i in 0..n {
        let id = format!("dpu-seed-{i:08x}");
        health.fetch_snapshot(&id).await.expect("health fetch");
        services.fetch_snapshot(&id).await.expect("services fetch");
        isolation
            .fetch_snapshot(&id)
            .await
            .expect("isolation fetch");
        cert.fetch_snapshot(&id).await.expect("cert fetch");
    }
    hbn.fetch_all_snapshots().await.expect("hbn fetch_all");

    DeserializeBreakdown {
        rows: vec![
            ("DpuClient::fetch_fleet", dpu.stats().fetch_fleet),
            (
                "DpuHealthClient::fetch_snapshot",
                health.stats().fetch_snapshot,
            ),
            (
                "DpuServicesClient::fetch_snapshot",
                services.stats().fetch_snapshot,
            ),
            (
                "DpuIsolationClient::fetch_snapshot",
                isolation.stats().fetch_snapshot,
            ),
            ("DpuCertClient::fetch_snapshot", cert.stats().fetch_snapshot),
            (
                "HbnClient::fetch_all_snapshots",
                hbn.stats().fetch_all_snapshots,
            ),
        ],
    }
}

/// Acceptance criterion: total `deserialize_time` across all Source-
/// trait decorators per refresh stays under a (deliberately loose)
/// budget. The budget is a tripwire for the "parse cost ballooned"
/// regression class; Slice 1's audit (issue #352) will tighten it after
/// the discovery-driven ranking lands.
///
/// The test also prints a per-decorator breakdown table with the
/// percentage each decorator contributes to the total. Operators
/// reading the `--nocapture` output get a flag-by-eye view of which
/// Source decorator is disproportionately hot — that is the artefact
/// Slice 1 consumes.
///
/// The default 1 s budget is sized for an Apple-Silicon Mac with the
/// system allocator, single-digit-µs-per-call parse work, fleet size
/// N=50 (observed ~35 ms, ~3.5% of budget). When the `dhat-heap`
/// feature is on, the dhat allocator amplifies wall time by ~370× on
/// the same workload (observed ~12.7 s), so the budget switches to
/// 60 s — still a meaningful "catastrophic blow-up" tripwire, just
/// scaled to the allocator overhead. Tighten when CI stability lets
/// us pick tighter numbers.
#[tokio::test]
async fn deserialize_time_budget() {
    let n = 50;
    let breakdown = run_source_decorators(n).await;
    breakdown.print("deserialize_time_budget", n);

    let total = breakdown.total();
    // dhat tracking amplifies the parse path's wall time by ~2 orders
    // of magnitude; see the doc comment above for the observed ratio.
    let budget = if cfg!(feature = "dhat-heap") {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(1)
    };
    println!(
        "  → total deserialize_time = {:?}, budget = {:?} ({:.1}% of budget)",
        total,
        budget,
        (total.as_micros() as f64 / budget.as_micros() as f64) * 100.0,
    );
    assert!(
        total < budget,
        "total deserialize_time across all Source decorators ({total:?}) \
         exceeded the per-refresh budget ({budget:?}); see the table \
         above for the per-decorator breakdown"
    );

    // Sanity check: at least one decorator must record non-zero
    // deserialize_time. If every entry is zero, the perf-fixtures
    // are not exercising the parse path (the test is no-op).
    let any_nonzero = breakdown
        .rows
        .iter()
        .any(|(_, s)| s.deserialize_time_total > Duration::ZERO);
    assert!(
        any_nonzero,
        "every Source decorator recorded zero deserialize_time — the \
         perf-fixture parse path is not being exercised"
    );
}
