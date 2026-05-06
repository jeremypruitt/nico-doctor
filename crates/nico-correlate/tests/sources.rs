use anyhow::Result;
use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use nico_common::k8s::testing::MockK8sClient;
use nico_common::k8s::{RawEvent, RawPod};
use nico_common::temporal::testing::MockTemporalClient;
use nico_correlate::id::IdType;
use nico_correlate::source::{Source, SourceResult};
use nico_correlate::sources::k8s::K8sSource;
use nico_correlate::sources::loki::{
    K8sLogLine, K8sLogStreamClient, LokiClient, LokiLogLine, LokiSource,
};
use nico_correlate::sources::postgres::{
    PgAuditEvent, PgEntityData, PgRow, PostgresClient, PostgresSource,
};
use nico_correlate::sources::redfish::{
    RedfishClient, RedfishData, RedfishRawEvent, RedfishSource, RedfishSystemState,
};
use nico_correlate::sources::temporal::TemporalSource;
use std::sync::Arc;
use temporal_sdk_core_protos::temporal::api::enums::v1::EventType;
use temporal_sdk_core_protos::temporal::api::history::v1::{History, HistoryEvent};

fn ts(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

// ─── Loki mocks ──────────────────────────────────────────────────────────────

struct MockLokiOk {
    lines: Vec<LokiLogLine>,
}

#[async_trait]
impl LokiClient for MockLokiOk {
    async fn query_range(
        &self,
        _id: &str,
        _id_type: &IdType,
        _since: Duration,
        _pod_pattern: Option<&str>,
    ) -> Result<Vec<LokiLogLine>> {
        Ok(self
            .lines
            .iter()
            .map(|l| LokiLogLine {
                ts: l.ts,
                message: l.message.clone(),
                pod: l.pod.clone(),
                is_serial_console: l.is_serial_console,
            })
            .collect())
    }
}

struct MockLokiErr;

#[async_trait]
impl LokiClient for MockLokiErr {
    async fn query_range(
        &self,
        _id: &str,
        _id_type: &IdType,
        _since: Duration,
        _pod_pattern: Option<&str>,
    ) -> Result<Vec<LokiLogLine>> {
        Err(anyhow::anyhow!("loki unavailable"))
    }
}

struct MockK8sLogOk {
    lines: Vec<K8sLogLine>,
}

#[async_trait]
impl K8sLogStreamClient for MockK8sLogOk {
    async fn stream_logs(
        &self,
        _id: &str,
        _id_type: &IdType,
        _since: Duration,
        _pod_pattern: Option<&str>,
    ) -> Result<Vec<K8sLogLine>> {
        Ok(self
            .lines
            .iter()
            .map(|l| K8sLogLine {
                ts: l.ts,
                message: l.message.clone(),
                pod: l.pod.clone(),
            })
            .collect())
    }
}

// ─── LokiSource: two log lines, one serial-console, one plain ────────────────

#[tokio::test]
async fn loki_source_two_lines_serial_console_and_plain() {
    let loki = MockLokiOk {
        lines: vec![
            LokiLogLine {
                ts: ts(1000),
                message: "BIOS POST complete".into(),
                pod: Some("serial-console-pod".into()),
                is_serial_console: true,
            },
            LokiLogLine {
                ts: ts(2000),
                message: "container started".into(),
                pod: Some("hp-worker-xyz".into()),
                is_serial_console: false,
            },
        ],
    };
    let source = LokiSource::new(
        Box::new(loki),
        None::<Box<dyn K8sLogStreamClient>>,
        None,
        Duration::hours(1),
    );
    let output = match source.collect("hp-abc", &IdType::Workflow).await {
        SourceResult::Output(o) => o,
        _ => panic!("expected Output"),
    };
    assert_eq!(output.events.len(), 2);
    let kinds: Vec<&str> = output.events.iter().map(|e| e.kind.as_str()).collect();
    assert!(kinds.contains(&"SerialConsoleLog"), "expected SerialConsoleLog in {kinds:?}");
    assert!(kinds.contains(&"Log"), "expected Log in {kinds:?}");
}

// ─── LokiSource fallback: Loki errors, k8s returns one line ──────────────────

#[tokio::test]
async fn loki_source_falls_back_to_k8s_on_error() {
    let k8s = MockK8sLogOk {
        lines: vec![K8sLogLine {
            ts: ts(3000),
            message: "starting container".into(),
            pod: "hp-worker-xyz".into(),
        }],
    };
    let source = LokiSource::new(
        Box::new(MockLokiErr),
        Some(Box::new(k8s) as Box<dyn K8sLogStreamClient>),
        None,
        Duration::hours(1),
    );
    let output = match source.collect("hp-abc", &IdType::Workflow).await {
        SourceResult::Output(o) => o,
        _ => panic!("expected Output"),
    };
    assert_eq!(output.events.len(), 1);
    assert_eq!(output.events[0].source, "k8s-logs");
}

// ─── PostgresSource: one row → state non-empty; one audit event → events non-empty ──

struct MockPostgresOk {
    data: PgEntityData,
}

#[async_trait]
impl PostgresClient for MockPostgresOk {
    async fn query_entity(&self, _id: &str, _id_type: &IdType) -> Result<PgEntityData> {
        Ok(self.data.clone())
    }
}

#[tokio::test]
async fn postgres_source_state_and_events_non_empty() {
    let data = PgEntityData {
        rows: vec![PgRow {
            table: "hosts".into(),
            columns: vec![
                ("id".into(), "host-r12u5".into()),
                ("status".into(), "ready".into()),
            ],
        }],
        audit_events: vec![PgAuditEvent {
            ts: ts(1000),
            action: "provision_start".into(),
            detail: "provisioning initiated".into(),
        }],
    };
    let source = PostgresSource::new(Box::new(MockPostgresOk { data }));
    let output = match source.collect("host-r12u5", &IdType::Host).await {
        SourceResult::Output(o) => o,
        _ => panic!("expected Output"),
    };
    assert!(!output.state.is_empty(), "expected non-empty state");
    assert!(!output.events.is_empty(), "expected non-empty events");
    assert_eq!(output.events[0].source, "postgres");
}

// ─── TemporalSource: two events of different types ───────────────────────────

#[tokio::test]
async fn temporal_source_two_events_correct_kinds() {
    let history = History {
        events: vec![
            HistoryEvent {
                event_id: 1,
                event_type: EventType::WorkflowExecutionStarted as i32,
                ..Default::default()
            },
            HistoryEvent {
                event_id: 2,
                event_type: EventType::WorkflowExecutionFailed as i32,
                ..Default::default()
            },
        ],
    };
    let client = MockTemporalClient::new().with_history(history);
    let source = TemporalSource::new(Arc::new(client), "default".into());
    let output = match source.collect("hp-abc", &IdType::Workflow).await {
        SourceResult::Output(o) => o,
        _ => panic!("expected Output"),
    };
    assert_eq!(output.events.len(), 2);
    assert_eq!(output.events[0].kind, "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED");
    assert_eq!(output.events[1].kind, "EVENT_TYPE_WORKFLOW_EXECUTION_FAILED");
    assert!(output.events.iter().all(|e| e.source == "temporal"));
}

// ─── K8sSource: one pod warning event → source is "k8s" ─────────────────────

#[tokio::test]
async fn k8s_source_one_pod_event_has_k8s_source() {
    let pods = vec![RawPod {
        name: "hp-worker-xyz".into(),
        namespace: "nico".into(),
        phase: Some("Running".into()),
        ready: true,
        restart_count: 0,
        succeeded: false,
        crash_loop: false,
    }];
    let events = vec![RawEvent {
        ts: Some(ts(1000)),
        event_type: Some("Warning".into()),
        reason: Some("OOMKilled".into()),
        message: Some("container ran out of memory".into()),
    }];
    let mut client = MockK8sClient::new().with_pods(pods).with_events(events);
    client.require_label_selector = Some("workflow_id=hp-abc".into());
    let source = K8sSource::new(Arc::new(client));
    let output = match source.collect("hp-abc", &IdType::Workflow).await {
        SourceResult::Output(o) => o,
        _ => panic!("expected Output"),
    };
    assert!(!output.events.is_empty(), "expected non-empty events");
    assert_eq!(output.events[0].source, "k8s");
}

// ─── RedfishSource: one hardware event → events non-empty ───────────────────

struct MockRedfishOk {
    data: RedfishData,
}

#[async_trait]
impl RedfishClient for MockRedfishOk {
    async fn query(&self, _id: &str, _id_type: &IdType) -> Result<RedfishData> {
        Ok(self.data.clone())
    }
}

#[tokio::test]
async fn redfish_source_one_hardware_event_non_empty() {
    let data = RedfishData {
        system_state: RedfishSystemState {
            host_id: "host-r12u5".into(),
            power_state: "On".into(),
            boot_source: "Hdd".into(),
            health: "Critical".into(),
        },
        events: vec![RedfishRawEvent {
            ts: ts(1000),
            event_type: "DriveFault".into(),
            detail: "NVMe slot 2 failed".into(),
        }],
    };
    let source = RedfishSource::new(Box::new(MockRedfishOk { data }));
    let output = match source.collect("host-r12u5", &IdType::Host).await {
        SourceResult::Output(o) => o,
        _ => panic!("expected Output"),
    };
    assert!(!output.events.is_empty(), "expected non-empty events");
    assert_eq!(output.events[0].source, "redfish");
}
