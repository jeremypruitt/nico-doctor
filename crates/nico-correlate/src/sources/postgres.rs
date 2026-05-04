use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::event::{Event, Severity};
use crate::id::IdType;
use crate::source::{Source, SourceResult, SourceOutput, SourceUnavailable, StateEntry};

#[derive(Clone)]
pub struct PgRow {
    pub table: String,
    pub columns: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct PgAuditEvent {
    pub ts: DateTime<Utc>,
    pub action: String,
    pub detail: String,
}

#[derive(Clone)]
pub struct PgEntityData {
    pub rows: Vec<PgRow>,
    pub audit_events: Vec<PgAuditEvent>,
}

#[async_trait]
pub trait PostgresClient: Send + Sync {
    async fn query_entity(&self, id: &str, id_type: &IdType) -> Result<PgEntityData>;
}

pub struct PostgresSource {
    client: Box<dyn PostgresClient>,
}

impl PostgresSource {
    pub fn new(client: Box<dyn PostgresClient>) -> Self {
        Self { client }
    }
}

fn audit_event_to_event(e: PgAuditEvent) -> Event {
    let severity = if e.action.contains("fail") || e.action.contains("error") || e.action.contains("delete") {
        Severity::Warning
    } else {
        Severity::Info
    };
    let message = if e.detail.is_empty() { e.action.clone() } else { e.detail };
    Event {
        ts: e.ts,
        source: "postgres".into(),
        kind: e.action,
        message,
        severity,
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn collect(&self, id: &str, id_type: &IdType) -> SourceResult {
        match self.client.query_entity(id, id_type).await {
            Ok(data) => {
                let events = data.audit_events.into_iter().map(audit_event_to_event).collect();
                let state = data.rows.into_iter().flat_map(|row| {
                    let table = row.table;
                    row.columns.into_iter().map(move |(k, v)| StateEntry {
                        source: "postgres",
                        key: format!("{}.{}", table, k),
                        value: v,
                    })
                }).collect();
                SourceResult::Output(SourceOutput { events, state })
            }
            Err(e) => SourceResult::Unavailable(SourceUnavailable {
                name: "postgres",
                reason: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    struct FakePostgresClient {
        result: Result<PgEntityData>,
    }

    impl FakePostgresClient {
        fn ok(data: PgEntityData) -> Self {
            Self { result: Ok(data) }
        }
        fn err(msg: &str) -> Self {
            Self { result: Err(anyhow::anyhow!(msg.to_string())) }
        }
    }

    #[async_trait]
    impl PostgresClient for FakePostgresClient {
        async fn query_entity(&self, _id: &str, _id_type: &IdType) -> Result<PgEntityData> {
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
    async fn host_row_becomes_state_entries() {
        let data = PgEntityData {
            rows: vec![PgRow {
                table: "hosts".into(),
                columns: vec![
                    ("id".into(), "host-r12u5".into()),
                    ("status".into(), "ready".into()),
                ],
            }],
            audit_events: vec![],
        };
        let source = PostgresSource::new(Box::new(FakePostgresClient::ok(data)));
        let result = source.collect("host-r12u5", &IdType::Host).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.state.len(), 2);
        assert_eq!(output.state[0].key, "hosts.id");
        assert_eq!(output.state[0].value, "host-r12u5");
        assert_eq!(output.state[1].key, "hosts.status");
        assert_eq!(output.state[1].value, "ready");
        assert!(output.events.is_empty());
    }

    #[tokio::test]
    async fn audit_log_failure_maps_to_warning_event() {
        let data = PgEntityData {
            rows: vec![],
            audit_events: vec![PgAuditEvent {
                ts: ts(1000),
                action: "provision_fail".into(),
                detail: "timeout".into(),
            }],
        };
        let source = PostgresSource::new(Box::new(FakePostgresClient::ok(data)));
        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events.len(), 1);
        assert_eq!(output.events[0].severity, Severity::Warning);
        assert_eq!(output.events[0].source, "postgres");
        assert_eq!(output.events[0].kind, "provision_fail");
        assert_eq!(output.events[0].message, "timeout");
    }

    #[tokio::test]
    async fn audit_log_success_maps_to_info_event() {
        let data = PgEntityData {
            rows: vec![],
            audit_events: vec![PgAuditEvent {
                ts: ts(1000),
                action: "create_host".into(),
                detail: "".into(),
            }],
        };
        let source = PostgresSource::new(Box::new(FakePostgresClient::ok(data)));
        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events[0].severity, Severity::Info);
        assert_eq!(output.events[0].message, "create_host");
    }

    #[tokio::test]
    async fn unavailable_client_returns_unavailable() {
        let source = PostgresSource::new(Box::new(FakePostgresClient::err("connection refused")));
        let result = source.collect("hp-abc", &IdType::Workflow).await;
        match result {
            SourceResult::Unavailable(u) => {
                assert_eq!(u.name, "postgres");
                assert!(u.reason.contains("connection refused"));
            }
            _ => panic!("expected Unavailable"),
        }
    }
}
