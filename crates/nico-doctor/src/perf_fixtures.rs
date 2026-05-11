//! Synthetic fixture generators for the PRD-005 performance harness.
//!
//! Each `synthesize_*` helper takes a single canonical seed row checked
//! into `tests/fixtures/perf/`, parses it once, then clones the seed N
//! times with id permutation so a small KB-scale fixture multiplies up
//! to fleet-scale (1, 18, 250, 1000, 10000) at bench startup. No large
//! fixtures are committed to the repo; the multiplication happens
//! in-process.
//!
//! The id permutation pattern per fixture:
//! - machine: replaces the top-level `id` field
//! - instance: replaces the top-level `machine_id` field
//! - pod: replaces `metadata.name` (and the `dpu-id` label / `nodeName`)
//! - temporal event: replaces the top-level `event_id`
//! - loki line: replaces `labels.pod` and `labels.dpu_id`
//!
//! See `scripts/capture-fixtures.sh` for how the seed rows were captured
//! from a live cluster.

use serde_json::Value;

const MACHINE_SEED: &str = include_str!("../tests/fixtures/perf/machine.json");
const INSTANCE_SEED: &str = include_str!("../tests/fixtures/perf/instance.json");
const POD_SEED: &str = include_str!("../tests/fixtures/perf/pod.json");
const TEMPORAL_EVENT_SEED: &str =
    include_str!("../tests/fixtures/perf/temporal_event.json");
const LOKI_LINE_SEED: &str = include_str!("../tests/fixtures/perf/loki_line.json");

fn parse_seed(label: &str, raw: &str) -> Value {
    serde_json::from_str(raw)
        .unwrap_or_else(|e| panic!("perf seed {label} is not valid JSON: {e}"))
}

fn dpu_id(i: usize) -> String {
    format!("dpu-seed-{i:08x}")
}

fn pod_name(i: usize) -> String {
    format!("dpu-agent-seed-{i:08x}")
}

fn node_name(i: usize) -> String {
    format!("worker-seed-{i:08x}")
}

/// Synthesize `n` `machines`-shaped rows. Slice 0a's `fan_out_bench` and
/// the doctor-side fleet collector consume this to drive fleet-scale
/// load against mocked clients.
pub fn synthesize_fleet(n: usize) -> Vec<Value> {
    let seed = parse_seed("machine.json", MACHINE_SEED);
    (0..n)
        .map(|i| {
            let mut row = seed.clone();
            row["id"] = Value::String(dpu_id(i));
            row
        })
        .collect()
}

/// Synthesize `n` `instances` rows joined by `machine_id` to the fleet
/// produced by [`synthesize_fleet`].
pub fn synthesize_instances(n: usize) -> Vec<Value> {
    let seed = parse_seed("instance.json", INSTANCE_SEED);
    (0..n)
        .map(|i| {
            let mut row = seed.clone();
            row["machine_id"] = Value::String(dpu_id(i));
            row
        })
        .collect()
}

/// Synthesize `n` Kubernetes Pod listings, one per synthetic DPU.
pub fn synthesize_pods(n: usize) -> Vec<Value> {
    let seed = parse_seed("pod.json", POD_SEED);
    (0..n)
        .map(|i| {
            let mut pod = seed.clone();
            pod["metadata"]["name"] = Value::String(pod_name(i));
            pod["metadata"]["labels"]["dpu-id"] = Value::String(dpu_id(i));
            pod["spec"]["nodeName"] = Value::String(node_name(i));
            pod
        })
        .collect()
}

/// Synthesize `n` Temporal history events, each with a unique
/// `event_id` and workflow input pointing at a distinct DPU.
pub fn synthesize_temporal_events(n: usize) -> Vec<Value> {
    let seed = parse_seed("temporal_event.json", TEMPORAL_EVENT_SEED);
    (0..n)
        .map(|i| {
            let mut event = seed.clone();
            event["event_id"] = Value::String(format!("evt-{i:08x}"));
            event["workflow_execution"]["workflow_id"] =
                Value::String(format!("dpu-provision-seed-{i:08x}"));
            event["attributes"]["input"]["dpu_id"] = Value::String(dpu_id(i));
            event
        })
        .collect()
}

/// Synthesize `n` Loki log lines, each tagged with a distinct pod /
/// DPU label so per-pod aggregation stays representative.
pub fn synthesize_loki_lines(n: usize) -> Vec<Value> {
    let seed = parse_seed("loki_line.json", LOKI_LINE_SEED);
    (0..n)
        .map(|i| {
            let mut line = seed.clone();
            line["labels"]["pod"] = Value::String(pod_name(i));
            line["labels"]["dpu_id"] = Value::String(dpu_id(i));
            line
        })
        .collect()
}
