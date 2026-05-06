use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nico_common::k8s::{K8sClient, PodScope, RawEvent, RawPod};

use crate::event::{Event, Severity};
use crate::id::IdType;
use crate::source::{Source, SourceOutput, SourceResult, SourceUnavailable, StateEntry};

#[derive(Clone)]
pub struct K8sPod {
    pub name: String,
    pub status: String,
    pub restart_count: u32,
    pub crash_loop: bool,
}

#[derive(Clone)]
pub struct K8sWarningEvent {
    pub ts: DateTime<Utc>,
    pub pod_name: String,
    pub reason: String,
    pub message: String,
}

#[derive(Clone)]
pub struct K8sPodData {
    pub pod: K8sPod,
    pub warning_events: Vec<K8sWarningEvent>,
}

pub struct K8sSource {
    client: Arc<dyn K8sClient>,
}

impl K8sSource {
    pub fn new(client: Arc<dyn K8sClient>) -> Self {
        Self { client }
    }
}

/// Build correlate's `K8sPodData` view by composing the common low-level
/// `K8sClient` primitives: list pods by label, then fetch warning events
/// per pod via a field selector scoped to that pod.
async fn find_pods_with_events(
    client: &dyn K8sClient,
    id: &str,
    id_type: &IdType,
) -> Result<Vec<K8sPodData>> {
    let label_selector = format!("{}={id}", id_type.label_key());
    let pods = client.list_pods(PodScope::AllWithLabel(&label_selector)).await?;

    let mut results = Vec::new();
    for raw in pods {
        let namespace = if raw.namespace.is_empty() {
            "default".to_string()
        } else {
            raw.namespace.clone()
        };
        let field_selector = format!(
            "involvedObject.kind=Pod,involvedObject.name={},type=Warning",
            raw.name
        );
        let raw_events = client
            .list_events(&namespace, Some(&field_selector))
            .await
            .unwrap_or_default();

        let warning_events = raw_events
            .into_iter()
            .filter_map(|e| event_to_warning(&raw.name, e))
            .collect();

        results.push(K8sPodData {
            pod: pod_view(&raw),
            warning_events,
        });
    }

    Ok(results)
}

fn pod_view(raw: &RawPod) -> K8sPod {
    let status = raw.phase.clone().unwrap_or_else(|| "Unknown".to_string());
    K8sPod {
        name: raw.name.clone(),
        status,
        restart_count: raw.restart_count,
        crash_loop: raw.crash_loop,
    }
}

fn event_to_warning(pod_name: &str, raw: RawEvent) -> Option<K8sWarningEvent> {
    let reason = raw.reason?;
    Some(K8sWarningEvent {
        ts: raw.ts.unwrap_or_else(Utc::now),
        pod_name: pod_name.to_string(),
        reason,
        message: raw.message.unwrap_or_default(),
    })
}

fn pod_state_entry(pod: &K8sPod) -> StateEntry {
    let restart_word = if pod.restart_count == 1 { "restart" } else { "restarts" };
    let display_status = if pod.crash_loop { "CrashLoopBackOff" } else { pod.status.as_str() };
    StateEntry {
        source: "k8s",
        key: pod.name.clone(),
        value: format!("{display_status} ({} {})", pod.restart_count, restart_word),
    }
}

fn warning_event_to_event(e: K8sWarningEvent) -> Event {
    Event {
        ts: e.ts,
        source: "k8s".into(),
        kind: e.reason.clone(),
        message: format!("{}: {}", e.pod_name, e.message),
        severity: Severity::classify("k8s", &e.reason, &e.message),
        tags: Default::default(),
    }
}

#[async_trait]
impl Source for K8sSource {
    fn name(&self) -> &'static str {
        "k8s"
    }

    async fn collect(&self, id: &str, id_type: &IdType) -> SourceResult {
        match find_pods_with_events(&*self.client, id, id_type).await {
            Ok(pod_data) => {
                let state = pod_data.iter().map(|pd| pod_state_entry(&pd.pod)).collect();
                let events = pod_data
                    .into_iter()
                    .flat_map(|pd| pd.warning_events.into_iter().map(warning_event_to_event))
                    .collect();
                SourceResult::Output(SourceOutput { events, state })
            }
            Err(e) => SourceResult::Unavailable(SourceUnavailable {
                name: "k8s",
                reason: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use nico_common::k8s::testing::MockK8sClient;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn raw_pod(name: &str, phase: &str, restart_count: u32, crash_loop: bool) -> RawPod {
        RawPod {
            name: name.into(),
            namespace: "nico".into(),
            phase: Some(phase.into()),
            ready: false,
            restart_count,
            succeeded: false,
            crash_loop,
        }
    }

    #[tokio::test]
    async fn pods_become_state_entries() {
        let mut client = MockK8sClient::new()
            .with_pods(vec![raw_pod("hp-worker-xyz", "Running", 3, false)]);
        client.require_label_selector = Some("workflow_id=hp-abc".into());
        let source = K8sSource::new(Arc::new(client));

        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.state.len(), 1);
        assert_eq!(output.state[0].source, "k8s");
        assert_eq!(output.state[0].key, "hp-worker-xyz");
        assert_eq!(output.state[0].value, "Running (3 restarts)");
        assert!(output.events.is_empty());
    }

    #[tokio::test]
    async fn singular_restart_word() {
        let mut client = MockK8sClient::new()
            .with_pods(vec![raw_pod("hp-worker-xyz", "Running", 1, false)]);
        client.require_label_selector = Some("workflow_id=hp-abc".into());
        let source = K8sSource::new(Arc::new(client));

        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.state[0].value, "Running (1 restart)");
    }

    #[tokio::test]
    async fn warning_events_map_to_warning_severity_events() {
        let mut client = MockK8sClient::new()
            .with_pods(vec![raw_pod("hp-worker-xyz", "Running", 2, false)])
            .with_events(vec![RawEvent {
                ts: Some(ts(1000)),
                event_type: Some("Warning".into()),
                reason: Some("OOMKilled".into()),
                message: Some("container ran out of memory".into()),
            }]);
        client.require_label_selector = Some("workflow_id=hp-abc".into());
        let source = K8sSource::new(Arc::new(client));

        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events.len(), 1);
        assert_eq!(output.events[0].severity, Severity::Warning);
        assert_eq!(output.events[0].source, "k8s");
        assert_eq!(output.events[0].kind, "OOMKilled");
        assert_eq!(output.events[0].message, "hp-worker-xyz: container ran out of memory");
    }

    #[tokio::test]
    async fn crash_loop_pod_shows_crash_loop_back_off_status() {
        let mut client = MockK8sClient::new()
            .with_pods(vec![raw_pod("hp-worker-xyz", "Running", 5, true)]);
        client.require_label_selector = Some("workflow_id=hp-abc".into());
        let source = K8sSource::new(Arc::new(client));

        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.state[0].value, "CrashLoopBackOff (5 restarts)");
    }

    #[tokio::test]
    async fn unavailable_client_returns_unavailable() {
        let client = MockK8sClient::new().with_pods_err("cluster unreachable");
        let source = K8sSource::new(Arc::new(client));
        let result = source.collect("hp-abc", &IdType::Workflow).await;
        match result {
            SourceResult::Unavailable(u) => {
                assert_eq!(u.name, "k8s");
                assert!(u.reason.contains("cluster unreachable"));
            }
            _ => panic!("expected Unavailable"),
        }
    }
}
