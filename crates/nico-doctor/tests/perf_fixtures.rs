//! Tests for the perf-fixture synthesizers in `nico_doctor::perf_fixtures`.
//!
//! These pin the contract that PRD-005 Slice 0a.2/0a.3 benches consume:
//! given KB-scale seed rows, `synthesize_*(n)` returns N rows for any
//! N in `{1, 18, 250, 1000, 10000}` with stable, unique ids.

use nico_doctor::perf_fixtures::{
    synthesize_fleet, synthesize_instances, synthesize_loki_lines, synthesize_pods,
    synthesize_temporal_events,
};

const N_SWEEP: &[usize] = &[1, 18, 250, 1000, 10000];

#[test]
fn synthesize_fleet_returns_n_machine_rows_for_each_canonical_n() {
    for &n in N_SWEEP {
        let rows = synthesize_fleet(n);
        assert_eq!(rows.len(), n, "synthesize_fleet({n}) returned wrong count");
    }
}

#[test]
fn synthesize_fleet_assigns_unique_ids() {
    let rows = synthesize_fleet(250);
    let mut ids: Vec<&str> = rows
        .iter()
        .map(|r| r["id"].as_str().expect("machine row has string id"))
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 250, "machine ids must be unique across the fleet");
}

#[test]
fn synthesize_fleet_preserves_seed_shape() {
    let rows = synthesize_fleet(1);
    let row = &rows[0];
    assert!(row["network_config_version"].is_string());
    assert!(row["network_status_observation"].is_object());
    assert!(row["network_config"].is_object());
    assert!(row["dpu_agent_health_report"].is_object());
}

#[test]
fn synthesize_instances_returns_n_rows_with_unique_machine_ids() {
    for &n in N_SWEEP {
        let rows = synthesize_instances(n);
        assert_eq!(rows.len(), n);
    }
    let rows = synthesize_instances(250);
    let mut ids: Vec<&str> = rows
        .iter()
        .map(|r| r["machine_id"].as_str().expect("instance has machine_id"))
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 250);
}

#[test]
fn synthesize_pods_returns_n_pods_with_unique_names() {
    for &n in N_SWEEP {
        let rows = synthesize_pods(n);
        assert_eq!(rows.len(), n);
    }
    let rows = synthesize_pods(250);
    let mut names: Vec<&str> = rows
        .iter()
        .map(|r| {
            r["metadata"]["name"]
                .as_str()
                .expect("pod has metadata.name")
        })
        .collect();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 250);
}

#[test]
fn synthesize_temporal_events_returns_n_events_with_unique_event_ids() {
    for &n in N_SWEEP {
        let rows = synthesize_temporal_events(n);
        assert_eq!(rows.len(), n);
    }
    let rows = synthesize_temporal_events(250);
    let mut ids: Vec<&str> = rows
        .iter()
        .map(|r| r["event_id"].as_str().expect("event has string event_id"))
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 250);
}

#[test]
fn synthesize_loki_lines_returns_n_lines_with_unique_pods() {
    for &n in N_SWEEP {
        let rows = synthesize_loki_lines(n);
        assert_eq!(rows.len(), n);
    }
    let rows = synthesize_loki_lines(250);
    let mut pods: Vec<&str> = rows
        .iter()
        .map(|r| {
            r["labels"]["pod"]
                .as_str()
                .expect("loki line has labels.pod")
        })
        .collect();
    pods.sort();
    pods.dedup();
    assert_eq!(pods.len(), 250);
}

#[test]
fn synthesize_with_zero_returns_empty() {
    assert!(synthesize_fleet(0).is_empty());
    assert!(synthesize_instances(0).is_empty());
    assert!(synthesize_pods(0).is_empty());
    assert!(synthesize_temporal_events(0).is_empty());
    assert!(synthesize_loki_lines(0).is_empty());
}
