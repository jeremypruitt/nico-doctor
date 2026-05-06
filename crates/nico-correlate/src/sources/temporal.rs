use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nico_common::temporal::TemporalClient;
use temporal_sdk_core_protos::temporal::api::enums::v1::{EventType, RetryState};
use temporal_sdk_core_protos::temporal::api::history::v1::{
    history_event::Attributes, History, HistoryEvent,
};

use crate::event::{Event, Severity};
use crate::id::IdType;
use crate::source::{Source, SourceOutput, SourceResult, SourceUnavailable};

#[derive(Clone, Default, Debug)]
pub struct RawTemporalEvent {
    pub event_type: String,
    pub ts: DateTime<Utc>,
    pub activity_name: Option<String>,
    pub error_message: Option<String>,
    pub at_max_retries: bool,
}

pub struct TemporalSource {
    client: Arc<dyn TemporalClient>,
    namespace: String,
}

impl TemporalSource {
    pub fn new(client: Arc<dyn TemporalClient>, namespace: String) -> Self {
        Self { client, namespace }
    }
}

fn map_event(raw: RawTemporalEvent) -> Event {
    let severity = Severity::classify("temporal", &raw.event_type, "");
    let mut tags = HashMap::new();
    if let Some(name) = raw.activity_name {
        tags.insert("activity_name".into(), name);
    }
    if let Some(err) = raw.error_message {
        tags.insert("error_signature".into(), err);
    }
    if raw.at_max_retries {
        tags.insert("at_max_retries".into(), "true".into());
    }
    Event {
        ts: raw.ts,
        source: "temporal".into(),
        kind: raw.event_type.clone(),
        message: raw.event_type,
        severity,
        tags,
    }
}

fn proto_ts_to_chrono(ts: prost_wkt_types::Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32).unwrap_or_else(Utc::now)
}

pub(crate) fn event_type_name(n: i32) -> String {
    EventType::try_from(n)
        .map(|et| et.as_str_name().to_string())
        .unwrap_or_else(|_| format!("UnknownEventType({})", n))
}

pub(crate) fn history_event_to_raw(
    e: &HistoryEvent,
    activity_by_event_id: &HashMap<i64, String>,
) -> RawTemporalEvent {
    let (activity_name, error_message, at_max_retries) =
        if let Some(Attributes::ActivityTaskFailedEventAttributes(failed)) = &e.attributes {
            let name = activity_by_event_id.get(&failed.scheduled_event_id).cloned();
            let err = failed
                .failure
                .as_ref()
                .map(|f| f.message.clone())
                .filter(|s| !s.is_empty());
            let at_max = RetryState::try_from(failed.retry_state)
                .ok()
                .map(|s| s == RetryState::MaximumAttemptsReached)
                .unwrap_or(false);
            (name, err, at_max)
        } else {
            (None, None, false)
        };

    let ts = e.event_time.map(proto_ts_to_chrono).unwrap_or_else(Utc::now);
    let event_type = event_type_name(e.event_type);
    RawTemporalEvent {
        event_type,
        ts,
        activity_name,
        error_message,
        at_max_retries,
    }
}

/// Translate a Temporal `History` proto into correlate's flat
/// `RawTemporalEvent` list, joining each `ActivityTaskFailed` to the name
/// of the activity it scheduled.
pub(crate) fn convert_history(history: History) -> Vec<RawTemporalEvent> {
    let mut activity_by_event_id: HashMap<i64, String> = HashMap::new();
    for e in &history.events {
        if let Some(Attributes::ActivityTaskScheduledEventAttributes(attrs)) = &e.attributes {
            let name = attrs
                .activity_type
                .as_ref()
                .map(|t| t.name.clone())
                .unwrap_or_default();
            if !name.is_empty() {
                activity_by_event_id.insert(e.event_id, name);
            }
        }
    }
    history
        .events
        .into_iter()
        .map(|e| history_event_to_raw(&e, &activity_by_event_id))
        .collect()
}

fn output_from_raw(raw_events: Vec<RawTemporalEvent>) -> SourceOutput {
    SourceOutput {
        events: raw_events.into_iter().map(map_event).collect(),
        state: vec![],
    }
}

#[async_trait]
impl Source for TemporalSource {
    fn name(&self) -> &'static str {
        "temporal"
    }

    async fn collect(&self, id: &str, _id_type: &IdType) -> SourceResult {
        match self.client.get_workflow_history(&self.namespace, id).await {
            Ok(history) => SourceResult::Output(output_from_raw(convert_history(history))),
            Err(e) => SourceResult::Unavailable(SourceUnavailable {
                name: "temporal",
                reason: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use nico_common::temporal::testing::MockTemporalClient;
    use temporal_sdk_core_protos::temporal::api::common::v1::ActivityType;
    use temporal_sdk_core_protos::temporal::api::failure::v1::Failure;
    use temporal_sdk_core_protos::temporal::api::history::v1::{
        ActivityTaskFailedEventAttributes, ActivityTaskScheduledEventAttributes, HistoryEvent,
    };

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    // --- Severity classification via map_event (sync, no protobuf). ---

    #[test]
    fn workflow_started_maps_to_info_event() {
        let event = map_event(RawTemporalEvent {
            event_type: "WorkflowExecutionStarted".into(),
            ts: ts(1000),
            ..Default::default()
        });
        assert_eq!(event.kind, "WorkflowExecutionStarted");
        assert_eq!(event.severity, Severity::Info);
        assert_eq!(event.source, "temporal");
        assert_eq!(event.ts, ts(1000));
    }

    #[test]
    fn workflow_failed_maps_to_error_event() {
        let event = map_event(RawTemporalEvent {
            event_type: "WorkflowExecutionFailed".into(),
            ts: ts(2000),
            ..Default::default()
        });
        assert_eq!(event.severity, Severity::Error);
    }

    #[test]
    fn workflow_timed_out_maps_to_error_severity() {
        let event = map_event(RawTemporalEvent {
            event_type: "WorkflowExecutionTimedOut".into(),
            ts: ts(3000),
            ..Default::default()
        });
        assert_eq!(event.severity, Severity::Error);
        assert_eq!(event.kind, "WorkflowExecutionTimedOut");
    }

    #[test]
    fn activity_task_failed_maps_to_error_severity() {
        let event = map_event(RawTemporalEvent {
            event_type: "ActivityTaskFailed".into(),
            ts: ts(4000),
            ..Default::default()
        });
        assert_eq!(event.severity, Severity::Error);
    }

    // --- event_type_name tests. ---

    #[test]
    fn workflow_execution_started_maps_to_proto_name() {
        let name = event_type_name(EventType::WorkflowExecutionStarted as i32);
        assert_eq!(name, "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED");
    }

    #[test]
    fn workflow_execution_failed_maps_to_proto_name() {
        let name = event_type_name(EventType::WorkflowExecutionFailed as i32);
        assert_eq!(name, "EVENT_TYPE_WORKFLOW_EXECUTION_FAILED");
    }

    #[test]
    fn activity_task_failed_maps_to_proto_name() {
        let name = event_type_name(EventType::ActivityTaskFailed as i32);
        assert_eq!(name, "EVENT_TYPE_ACTIVITY_TASK_FAILED");
    }

    #[test]
    fn unknown_event_type_uses_fallback_format() {
        let name = event_type_name(99999);
        assert_eq!(name, "UnknownEventType(99999)");
    }

    // --- history_event_to_raw and convert_history. ---

    #[test]
    fn activity_failed_event_extracts_error_message_and_max_retries() {
        let scheduled_event_id = 5_i64;
        let mut activity_by_event_id = HashMap::new();
        activity_by_event_id.insert(scheduled_event_id, "my-activity".to_string());

        let event = HistoryEvent {
            event_id: 8,
            event_type: EventType::ActivityTaskFailed as i32,
            attributes: Some(Attributes::ActivityTaskFailedEventAttributes(
                ActivityTaskFailedEventAttributes {
                    scheduled_event_id,
                    retry_state: RetryState::MaximumAttemptsReached as i32,
                    failure: Some(Failure {
                        message: "disk full".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let raw = history_event_to_raw(&event, &activity_by_event_id);
        assert_eq!(raw.activity_name.as_deref(), Some("my-activity"));
        assert_eq!(raw.error_message.as_deref(), Some("disk full"));
        assert!(raw.at_max_retries);
    }

    #[test]
    fn convert_history_joins_activity_scheduled_to_failed() {
        let history = History {
            events: vec![
                HistoryEvent {
                    event_id: 10,
                    event_type: EventType::ActivityTaskScheduled as i32,
                    attributes: Some(Attributes::ActivityTaskScheduledEventAttributes(
                        ActivityTaskScheduledEventAttributes {
                            activity_type: Some(ActivityType {
                                name: "provision-host".to_string(),
                            }),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
                HistoryEvent {
                    event_id: 13,
                    event_type: EventType::ActivityTaskFailed as i32,
                    attributes: Some(Attributes::ActivityTaskFailedEventAttributes(
                        ActivityTaskFailedEventAttributes {
                            scheduled_event_id: 10,
                            retry_state: RetryState::InProgress as i32,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            ],
        };

        let raw = convert_history(history);
        assert_eq!(raw.len(), 2);
        assert_eq!(raw[1].activity_name.as_deref(), Some("provision-host"));
        assert!(!raw[1].at_max_retries);
    }

    // --- Source-level integration with common MockTemporalClient. ---

    #[tokio::test]
    async fn empty_history_returns_empty_output() {
        let client = MockTemporalClient::new();
        let source = TemporalSource::new(Arc::new(client), "default".into());
        let output = match source.collect("hp-abc", &IdType::Workflow).await {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert!(output.events.is_empty());
        assert!(output.state.is_empty());
    }

    #[tokio::test]
    async fn unavailable_client_returns_unavailable() {
        let client = MockTemporalClient::new().with_history_err("connection refused");
        let source = TemporalSource::new(Arc::new(client), "default".into());
        let result = source.collect("hp-abc", &IdType::Workflow).await;
        match result {
            SourceResult::Unavailable(u) => {
                assert_eq!(u.name, "temporal");
                assert!(u.reason.contains("connection refused"));
            }
            _ => panic!("expected Unavailable"),
        }
    }

    #[tokio::test]
    async fn history_events_map_through_source_to_event_kinds() {
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
}
