use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::event::{Event, Severity};
use crate::id::IdType;
use crate::source::{Source, SourceResult, SourceOutput, SourceUnavailable, StateEntry};

#[derive(Clone)]
pub struct K8sPod {
    pub name: String,
    pub status: String,
    pub restart_count: u32,
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

#[async_trait]
pub trait K8sClient: Send + Sync {
    async fn find_pods_with_events(&self, id: &str, id_type: &IdType) -> Result<Vec<K8sPodData>>;
}

pub struct K8sSource {
    client: Box<dyn K8sClient>,
}

impl K8sSource {
    pub fn new(client: Box<dyn K8sClient>) -> Self {
        Self { client }
    }
}

fn pod_state_entry(pod: &K8sPod) -> StateEntry {
    let restart_word = if pod.restart_count == 1 { "restart" } else { "restarts" };
    StateEntry {
        source: "k8s",
        key: pod.name.clone(),
        value: format!("{} ({} {})", pod.status, pod.restart_count, restart_word),
    }
}

fn warning_event_to_event(e: K8sWarningEvent) -> Event {
    Event {
        ts: e.ts,
        source: "k8s".into(),
        kind: e.reason.clone(),
        message: format!("{}: {}", e.pod_name, e.message),
        severity: Severity::Warning,
    }
}

#[async_trait]
impl Source for K8sSource {
    fn name(&self) -> &'static str {
        "k8s"
    }

    async fn collect(&self, id: &str, id_type: &IdType) -> SourceResult {
        match self.client.find_pods_with_events(id, id_type).await {
            Ok(pod_data) => {
                let state = pod_data.iter().map(|pd| pod_state_entry(&pd.pod)).collect();
                let events = pod_data.into_iter()
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

    struct FakeK8sClient {
        result: Result<Vec<K8sPodData>>,
    }

    impl FakeK8sClient {
        fn ok(data: Vec<K8sPodData>) -> Self {
            Self { result: Ok(data) }
        }
        fn err(msg: &str) -> Self {
            Self { result: Err(anyhow::anyhow!(msg.to_string())) }
        }
    }

    #[async_trait]
    impl K8sClient for FakeK8sClient {
        async fn find_pods_with_events(&self, _id: &str, _id_type: &IdType) -> Result<Vec<K8sPodData>> {
            match &self.result {
                Ok(data) => Ok(data.clone()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }
    }

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[tokio::test]
    async fn pods_become_state_entries() {
        let data = vec![K8sPodData {
            pod: K8sPod { name: "hp-worker-xyz".into(), status: "Running".into(), restart_count: 3 },
            warning_events: vec![],
        }];
        let source = K8sSource::new(Box::new(FakeK8sClient::ok(data)));
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
        let data = vec![K8sPodData {
            pod: K8sPod { name: "hp-worker-xyz".into(), status: "Running".into(), restart_count: 1 },
            warning_events: vec![],
        }];
        let source = K8sSource::new(Box::new(FakeK8sClient::ok(data)));
        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.state[0].value, "Running (1 restart)");
    }

    #[tokio::test]
    async fn warning_events_map_to_warning_severity_events() {
        let data = vec![K8sPodData {
            pod: K8sPod { name: "hp-worker-xyz".into(), status: "Running".into(), restart_count: 2 },
            warning_events: vec![K8sWarningEvent {
                ts: ts(1000),
                pod_name: "hp-worker-xyz".into(),
                reason: "OOMKilled".into(),
                message: "container ran out of memory".into(),
            }],
        }];
        let source = K8sSource::new(Box::new(FakeK8sClient::ok(data)));
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
    async fn unavailable_client_returns_unavailable() {
        let source = K8sSource::new(Box::new(FakeK8sClient::err("cluster unreachable")));
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
