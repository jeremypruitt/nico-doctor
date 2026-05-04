use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::event::{Event, Severity};
use crate::id::IdType;
use crate::source::{Source, SourceResult, SourceOutput, SourceUnavailable, StateEntry};

/// Current system state read from a BMC Redfish endpoint (GET-only, ADR-002).
#[derive(Clone)]
pub struct RedfishSystemState {
    /// Resolved host ID — differs from the entity ID when the entity is a DPU.
    pub host_id: String,
    pub power_state: String,
    pub boot_source: String,
    pub health: String,
}

#[derive(Clone)]
pub struct RedfishRawEvent {
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub detail: String,
}

#[derive(Clone)]
pub struct RedfishData {
    pub system_state: RedfishSystemState,
    pub events: Vec<RedfishRawEvent>,
}

/// All Redfish calls are read-only GETs (ADR-002).
/// For DPU entities the client resolves the associated host via Postgres
/// `hosts.dpu_id` and queries that host's BMC address.
#[async_trait]
pub trait RedfishClient: Send + Sync {
    async fn query(&self, id: &str, id_type: &IdType) -> Result<RedfishData>;
}

pub struct RedfishSource {
    client: Box<dyn RedfishClient>,
}

impl RedfishSource {
    pub fn new(client: Box<dyn RedfishClient>) -> Self {
        Self { client }
    }
}

fn map_event(raw: RedfishRawEvent) -> Event {
    let severity = if raw.event_type.contains("Fault")
        || raw.event_type.contains("Critical")
        || raw.event_type.contains("Failed")
    {
        Severity::Error
    } else if raw.event_type.contains("Warning") || raw.event_type.contains("Degraded") {
        Severity::Warning
    } else {
        Severity::Info
    };
    Event {
        ts: raw.ts,
        source: "redfish".into(),
        kind: raw.event_type.clone(),
        message: if raw.detail.is_empty() { raw.event_type } else { raw.detail },
        severity,
    }
}

#[async_trait]
impl Source for RedfishSource {
    fn name(&self) -> &'static str {
        "redfish"
    }

    async fn collect(&self, id: &str, id_type: &IdType) -> SourceResult {
        if !matches!(id_type, IdType::Host | IdType::Dpu) {
            return SourceResult::Output(SourceOutput { events: vec![], state: vec![] });
        }

        match self.client.query(id, id_type).await {
            Ok(data) => {
                let events = data.events.into_iter().map(map_event).collect();
                let host = &data.system_state.host_id;
                let state = vec![
                    StateEntry { source: "redfish", key: format!("{host}.power_state"), value: data.system_state.power_state },
                    StateEntry { source: "redfish", key: format!("{host}.boot_source"), value: data.system_state.boot_source },
                    StateEntry { source: "redfish", key: format!("{host}.health"),      value: data.system_state.health },
                ];
                SourceResult::Output(SourceOutput { events, state })
            }
            Err(e) => SourceResult::Unavailable(SourceUnavailable {
                name: "redfish",
                reason: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    struct FakeRedfishClient {
        result: Result<RedfishData>,
    }

    impl FakeRedfishClient {
        fn ok(data: RedfishData) -> Self {
            Self { result: Ok(data) }
        }
        fn err(msg: &str) -> Self {
            Self { result: Err(anyhow::anyhow!(msg.to_string())) }
        }
    }

    #[async_trait]
    impl RedfishClient for FakeRedfishClient {
        async fn query(&self, _id: &str, _id_type: &IdType) -> Result<RedfishData> {
            match &self.result {
                Ok(d) => Ok(d.clone()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }
    }

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn host_data(host_id: &str) -> RedfishData {
        RedfishData {
            system_state: RedfishSystemState {
                host_id: host_id.into(),
                power_state: "On".into(),
                boot_source: "Hdd".into(),
                health: "OK".into(),
            },
            events: vec![],
        }
    }

    #[tokio::test]
    async fn host_power_state_becomes_state_entries() {
        let source = RedfishSource::new(Box::new(FakeRedfishClient::ok(host_data("host-r12u5"))));
        let result = source.collect("host-r12u5", &IdType::Host).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.state.len(), 3);
        assert_eq!(output.state[0].key, "host-r12u5.power_state");
        assert_eq!(output.state[0].value, "On");
        assert_eq!(output.state[1].key, "host-r12u5.boot_source");
        assert_eq!(output.state[2].key, "host-r12u5.health");
        assert_eq!(output.state[2].value, "OK");
        assert!(output.events.is_empty());
    }

    #[tokio::test]
    async fn dpu_resolves_to_host_state_entries() {
        // When entity is a DPU the client resolves the host; state keys carry the host ID.
        let source = RedfishSource::new(Box::new(FakeRedfishClient::ok(host_data("host-r12u5"))));
        let result = source.collect("dpu-bf3-r12u5", &IdType::Dpu).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.state[0].key, "host-r12u5.power_state");
    }

    #[tokio::test]
    async fn fault_event_maps_to_error_severity() {
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
                detail: "NVMe slot 2".into(),
            }],
        };
        let source = RedfishSource::new(Box::new(FakeRedfishClient::ok(data)));
        let result = source.collect("host-r12u5", &IdType::Host).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events[0].severity, Severity::Error);
        assert_eq!(output.events[0].source, "redfish");
        assert_eq!(output.events[0].kind, "DriveFault");
        assert_eq!(output.events[0].message, "NVMe slot 2");
    }

    #[tokio::test]
    async fn power_on_event_maps_to_info_severity() {
        let data = RedfishData {
            system_state: RedfishSystemState {
                host_id: "host-r12u5".into(),
                power_state: "On".into(),
                boot_source: "Hdd".into(),
                health: "OK".into(),
            },
            events: vec![RedfishRawEvent {
                ts: ts(2000),
                event_type: "SystemPowerOn".into(),
                detail: "".into(),
            }],
        };
        let source = RedfishSource::new(Box::new(FakeRedfishClient::ok(data)));
        let result = source.collect("host-r12u5", &IdType::Host).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events[0].severity, Severity::Info);
        assert_eq!(output.events[0].message, "SystemPowerOn");
    }

    #[tokio::test]
    async fn non_host_dpu_type_returns_empty_output() {
        let source = RedfishSource::new(Box::new(FakeRedfishClient::ok(host_data("host-r12u5"))));
        let result = source.collect("hp-abc", &IdType::Workflow).await;
        let output = match result {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert!(output.events.is_empty());
        assert!(output.state.is_empty());
    }

    #[tokio::test]
    async fn unreachable_bmc_returns_unavailable() {
        let source = RedfishSource::new(Box::new(FakeRedfishClient::err("connection refused")));
        let result = source.collect("host-r12u5", &IdType::Host).await;
        match result {
            SourceResult::Unavailable(u) => {
                assert_eq!(u.name, "redfish");
                assert!(u.reason.contains("connection refused"));
            }
            _ => panic!("expected Unavailable"),
        }
    }
}
