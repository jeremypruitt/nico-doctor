# nico-ops benches — PRD-005 baseline (Slice 0a.2)

Four criterion benches that quantify the wall-clock + CPU cost of the
`nico ops` hot path. Each one targets a specific PRD-005 finding so
later slices have a numeric baseline to rank improvements against.

| Bench | What it measures | Target finding |
| --- | --- | --- |
| `idle_tick_bench` | 1000 ticks through `App::handle(Action::Tick(now))` with no in-flight refresh | Finding #1 (always-dirty tick re-render while refreshing) |
| `fan_out_bench` | `data::collect` against synthetic layers, sweep `N ∈ {1, 18, 250, 1000, 10000}` | Fan-out scaling baseline for `prepare_layers` + `runner::run` |
| `reducer_bench` | One bench per `Action` variant: Snapshots, NamespaceEvents, LogLines, Tick-while-refreshing, Focus, Refresh | Reducer microbench surface |
| `render_bench` | `view::render` through `ratatui::backend::TestBackend`, sweep `N ∈ {1, 18, 250}` | Findings #2 + #3 (sparkline recompute + per-card `evidence.clone()`) |

## How to run

```bash
# All four benches:
cargo bench -p nico-ops

# One bench:
cargo bench -p nico-ops --bench fan_out_bench

# Quick smoke pass (smaller sample counts):
cargo bench -p nico-ops -- --quick

# With dhat heap profiling (writes dhat-heap.json next to the binary):
cargo bench -p nico-ops --features dhat-heap --bench render_bench
```

Output lives under `target/criterion/`; the HTML report at
`target/criterion/report/index.html` lets you compare runs.

## Fixtures

Slice 0a.1 (#346) shipped `nico_doctor::perf_fixtures::synthesize_*(n)`
which multiplies KB-scale seed rows under
`crates/nico-doctor/tests/fixtures/perf/` up to fleet-scale `N` (1, 18,
250, 1000, 10000) at bench startup. These benches operate one layer
above that — at the `LayerSnapshot` / `App` reducer / `view::render`
level — so they keep their own synthetic generators in
`benches/common/mod.rs` (`fleet_snapshots`, `fleet_layers`,
`warmed_app`, …). When a later slice grows benches that consume raw
DPU/pod/Temporal/Loki JSON, those should switch to
`nico_doctor::perf_fixtures` directly.

To refresh the seed rows against a live cluster:

```bash
./scripts/capture-fixtures.sh
```

## Baseline numbers — 2026-05-10

Captured locally on an Apple Silicon Mac (M-series, optimized
`bench` profile). These are the regression-guard baselines that
later PRD-005 slices will rank against; they are **not** budgets and
are not portable across machines. Re-capture on the same host after
landing any change that should move the needle.

### idle_tick

| Bench | Median |
| --- | --- |
| `1000_ticks_no_refresh` | **4.34 µs** (~4.3 ns/tick) |

The 1000-tick batch sits well under the 100 ms tick cadence
(`nico_ops::TICK`), so idle-tick wall-clock is not the bottleneck —
which makes it a useful flat baseline against which Slice 0a.3's
`idle_tick_does_not_re_render` integration test will catch the
"always-dirty while refreshing" path (Finding #1).

### fan_out

| N (layers) | Median |
| --- | --- |
| 1 | 8.1 µs |
| 18 | 25.5 µs |
| 250 | 252 µs |
| 1000 | 989 µs |
| 10000 | 10.99 ms |

Roughly linear in N (≈ 1 µs/layer once N ≥ 18). The synthetic
`BenchLayer` does no I/O — this baseline isolates the cost of
`run_streaming`'s `FuturesUnordered` + per-result mpsc roundtrip from
the cost of the layer bodies themselves.

### reducer

| Action variant | Median |
| --- | --- |
| `Snapshots(18)` | 8.6 µs |
| `NamespaceEvents(18)` | 4.1 µs |
| `LogLines(18)` | 3.9 µs |
| `Tick` (while refreshing) | 3.4 µs |
| `Focus(Right)` | 3.4 µs |
| `Refresh` | 2.3 µs |

All six are O(N=18) one-shot calls, so the relative ordering reflects
how much work each arm does. `Snapshots` is the heaviest (recomputes
deltas, pulses, prev-status, history push); `Refresh` is the cheapest
(it flips two flags and emits `Effect::StartRefresh`).

### render

| N (fleet snapshots) | Terminal | Median |
| --- | --- | --- |
| 1 | 160×48 | 196 µs |
| 18 | 160×48 | 261 µs |
| 250 | 160×48 | 351 µs |

Render scales sub-linearly with N because off-screen cards are not
painted — but the per-card work (sparkline recompute + the
`evidence.clone()` from Finding #3) still runs for every snapshot.
The N=250 row is the leading indicator for Findings #2 + #3.

## Slice 0a.3 — integration tests

Sibling to the criterion benches above, Slice 0a.3 ships three
regression-guard integration tests in `crates/nico-ops/tests/perf.rs`.
They exercise the same composable seams (`data::collect` +
`App::handle`) but assert end-to-end behavior at the level the
operator notices on screen, rather than measuring it in isolation.

| Test | What it pins | Bound |
| --- | --- | --- |
| `cold_start_to_first_paint` | wall-clock from `data::collect` to first `Action::Snapshots` reduce | < 1 s (3 OOMs slack over local baseline) |
| `idle_tick_does_not_re_render` | `Action::Tick` after a settled refresh does not flip `app.dirty()` | exactly 0 of 100 ticks |
| `memory_bounded_after_n_refreshes` | live heap (dhat) after 1000 reduce cycles stays bounded | `dhat-heap` feature gated; 50 MiB ceiling |

Run them with:

```bash
# Two of three (memory test no-ops without the feature):
cargo test -p nico-ops --test perf

# Includes the dhat-gated memory regression guard:
cargo test -p nico-ops --test perf --features dhat-heap
```

The bounds are deliberately loose — they are tripwires for catastrophic
regressions, not tight latency budgets. Tighten them only after a
stable per-host baseline has been established and the tests have been
running clean across enough CI runs to characterize variance.

## Slice 0b.3 — network/data perf tests

Three additional regression-guard integration tests live in
`crates/nico-ops/tests/perf.rs`, exercising the counting decorators
from Slice 0b.1 (`nico_common::perf`) and Slice 0b.2
(`nico_doctor::perf_source`). They drive the same composable seam
`run_event_loop` calls at refresh time (`data::collect` plus the
snapshot-panel `K8sLogSource::collect`) — backend genericisation is
still out of scope, see the file header for why.

| Test | What it pins | Bound |
| --- | --- | --- |
| `refresh_call_counts` | EXACT per-`K8sClient` method counts on one refresh, N=3 pods | `list_pods=3`, `list_events=1`, `pod_logs=6` |
| `refresh_data_volume_under_n_kb` | total `bytes_in` per refresh across the fixture sweep | per-N table below |
| `deserialize_time_budget` | total Source-trait `deserialize_time` per refresh | 1 s default; 60 s under `dhat-heap` |

Run them with:

```bash
# All five default tests (Slice 0a.3 plus the three 0b.3 tests):
cargo test -p nico-ops --test perf

# Same, with the dhat-gated memory test added and the deserialize
# budget switched to its dhat-heap variant:
cargo test -p nico-ops --test perf --features dhat-heap

# To see the per-method / per-decorator summary tables:
cargo test -p nico-ops --test perf -- --nocapture
```

The exact counts in `refresh_call_counts` are designed to fail loudly:
the test pins the duplicate-fetch shape today's refresh path produces
(`LogCollectorStage` + `ClusterLayer` + snapshot-panel `K8sLogSource`),
which Finding #6 calls out. Closing that gap will need this test
updated alongside the fix.

### Per-N bytes_in bounds — 2026-05-11

Captured locally with the `MockK8sClient` fixture used in
`run_one_refresh` (eight log lines × N non-`Succeeded` pods, fetched
twice per refresh; 3× `list_pods` per refresh; empty `list_events`).
Values are deterministic for a given fixture shape, so the bounds are
~1.25× the observed ceiling. Update both `refresh_bytes_in_bound` in
`tests/perf.rs` and this table when the fixture shape changes.

| N (pods) | observed `bytes_in` | per-N bound | % of bound |
| -------- | ------------------- | ----------- | ---------- |
| 1        | 962                 | 1 500       | 64.1%      |
| 18       | 17 231              | 22 000      | 78.3%      |
| 250      | 239 255             | 305 000     | 78.4%      |
| 1 000    | 957 005             | 1 220 000   | 78.4%      |
| 10 000   | 9 570 005           | 12 200 000  | 78.4%      |

### Per-decorator deserialize_time breakdown — 2026-05-11

Sample output of the `deserialize_time_budget` table at N=50 (system
allocator, Apple Silicon). The per-DPU layers each carry ~24-25 % of
the per-refresh parse time and are the obvious starting point for
Slice 1's audit; `DpuClient::fetch_fleet` and `HbnClient::fetch_all_snapshots`
run once per refresh and contribute < 1 % each.

| decorator                            | calls | total (µs) | % of total |
| ------------------------------------ | ----- | ---------- | ---------- |
| `DpuClient::fetch_fleet`             | 1     | 263        | 0.8 %      |
| `DpuHealthClient::fetch_snapshot`    | 50    | 8 642      | 24.9 %     |
| `DpuServicesClient::fetch_snapshot`  | 50    | 8 715      | 25.1 %     |
| `DpuIsolationClient::fetch_snapshot` | 50    | 8 400      | 24.2 %     |
| `DpuCertClient::fetch_snapshot`      | 50    | 8 416      | 24.3 %     |
| `HbnClient::fetch_all_snapshots`     | 1     | 221        | 0.6 %      |
| **total**                            |       | **34 660** | 100 %      |

dhat-heap mode amplifies these totals by ~370× on the same workload —
the 60 s `dhat-heap` budget is sized against that observation, not
against the 1 s default budget.
