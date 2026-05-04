use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use crate::event::{Event, Severity};
use crate::id::IdType;
use crate::source::{Source, SourceResult, SourceOutput, SourceUnavailable, StateEntry};

pub struct LokiLogLine {
    pub ts: DateTime<Utc>,
    pub message: String,
    #[allow(dead_code)]
    pub pod: Option<String>,
    pub is_serial_console: bool,
}

#[async_trait]
pub trait LokiClient: Send + Sync {
    async fn query_range(
        &self,
        id: &str,
        id_type: &IdType,
        since: Duration,
        pod_pattern: Option<&str>,
    ) -> Result<Vec<LokiLogLine>>;
}

pub struct K8sLogLine {
    pub ts: DateTime<Utc>,
    pub message: String,
    pub pod: String,
}

#[async_trait]
pub trait K8sLogStreamClient: Send + Sync {
    async fn stream_logs(
        &self,
        id: &str,
        id_type: &IdType,
        since: Duration,
        pod_pattern: Option<&str>,
    ) -> Result<Vec<K8sLogLine>>;
}

pub struct LokiSource {
    loki: Box<dyn LokiClient>,
    k8s_fallback: Box<dyn K8sLogStreamClient>,
    pub pod_pattern: Option<String>,
    pub since: Duration,
}

impl LokiSource {
    pub fn new(
        loki: Box<dyn LokiClient>,
        k8s_fallback: Box<dyn K8sLogStreamClient>,
        pod_pattern: Option<String>,
        since: Duration,
    ) -> Self {
        Self { loki, k8s_fallback, pod_pattern, since }
    }
}

fn loki_line_to_event(line: LokiLogLine) -> Event {
    let kind = if line.is_serial_console { "SerialConsoleLog" } else { "Log" };
    Event {
        ts: line.ts,
        source: "loki".into(),
        kind: kind.into(),
        message: line.message,
        severity: Severity::Info,
    }
}

fn k8s_line_to_event(line: K8sLogLine) -> Event {
    Event {
        ts: line.ts,
        source: "k8s-logs".into(),
        kind: "Log".into(),
        message: format!("[{}] {}", line.pod, line.message),
        severity: Severity::Info,
    }
}

#[async_trait]
impl Source for LokiSource {
    fn name(&self) -> &'static str {
        "loki"
    }

    async fn collect(&self, id: &str, id_type: &IdType) -> SourceResult {
        match self.loki.query_range(id, id_type, self.since, self.pod_pattern.as_deref()).await {
            Ok(lines) => {
                let events = lines.into_iter().map(loki_line_to_event).collect();
                SourceResult::Output(SourceOutput { events, state: vec![] })
            }
            Err(loki_err) => {
                match self.k8s_fallback.stream_logs(id, id_type, self.since, self.pod_pattern.as_deref()).await {
                    Ok(lines) => {
                        let events = lines.into_iter().map(k8s_line_to_event).collect();
                        let state = vec![StateEntry {
                            source: "loki",
                            key: "fallback".into(),
                            value: "[loki unavailable, using k8s streaming]".into(),
                        }];
                        SourceResult::Output(SourceOutput { events, state })
                    }
                    Err(_) => SourceResult::Unavailable(SourceUnavailable {
                        name: "loki",
                        reason: loki_err.to_string(),
                    }),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::{Arc, Mutex};

    struct FakeLokiClient {
        result: Result<Vec<LokiLogLine>>,
    }

    impl FakeLokiClient {
        fn ok(lines: Vec<LokiLogLine>) -> Self {
            Self { result: Ok(lines) }
        }
        fn err(msg: &str) -> Self {
            Self { result: Err(anyhow::anyhow!(msg.to_string())) }
        }
    }

    #[async_trait]
    impl LokiClient for FakeLokiClient {
        async fn query_range(
            &self,
            _id: &str,
            _id_type: &IdType,
            _since: Duration,
            _pod_pattern: Option<&str>,
        ) -> Result<Vec<LokiLogLine>> {
            match &self.result {
                Ok(lines) => Ok(lines.iter().map(|l| LokiLogLine {
                    ts: l.ts,
                    message: l.message.clone(),
                    pod: l.pod.clone(),
                    is_serial_console: l.is_serial_console,
                }).collect()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }
    }

    struct FakeK8sLogStreamClient {
        result: Result<Vec<K8sLogLine>>,
    }

    impl FakeK8sLogStreamClient {
        fn ok(lines: Vec<K8sLogLine>) -> Self {
            Self { result: Ok(lines) }
        }
        fn err(msg: &str) -> Self {
            Self { result: Err(anyhow::anyhow!(msg.to_string())) }
        }
    }

    #[async_trait]
    impl K8sLogStreamClient for FakeK8sLogStreamClient {
        async fn stream_logs(
            &self,
            _id: &str,
            _id_type: &IdType,
            _since: Duration,
            _pod_pattern: Option<&str>,
        ) -> Result<Vec<K8sLogLine>> {
            match &self.result {
                Ok(lines) => Ok(lines.iter().map(|l| K8sLogLine {
                    ts: l.ts,
                    message: l.message.clone(),
                    pod: l.pod.clone(),
                }).collect()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }
    }

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn make_source(loki: impl LokiClient + 'static, k8s: impl K8sLogStreamClient + 'static) -> LokiSource {
        LokiSource::new(Box::new(loki), Box::new(k8s), None, Duration::hours(1))
    }

    #[tokio::test]
    async fn loki_lines_appear_with_loki_source_and_correct_ts() {
        let loki = FakeLokiClient::ok(vec![LokiLogLine {
            ts: ts(1000),
            message: "provisioning started".into(),
            pod: Some("hp-worker-xyz".into()),
            is_serial_console: false,
        }]);
        let source = make_source(loki, FakeK8sLogStreamClient::err("not needed"));
        let output = match source.collect("hp-abc", &IdType::Workflow).await {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events.len(), 1);
        assert_eq!(output.events[0].source, "loki");
        assert_eq!(output.events[0].ts, ts(1000));
        assert_eq!(output.events[0].kind, "Log");
    }

    #[tokio::test]
    async fn serial_console_lines_appear_in_timeline() {
        let loki = FakeLokiClient::ok(vec![LokiLogLine {
            ts: ts(2000),
            message: "BIOS POST complete".into(),
            pod: Some("serial-console".into()),
            is_serial_console: true,
        }]);
        let source = make_source(loki, FakeK8sLogStreamClient::err("not needed"));
        let output = match source.collect("host-r12u5", &IdType::Host).await {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events.len(), 1);
        assert_eq!(output.events[0].kind, "SerialConsoleLog");
        assert_eq!(output.events[0].source, "loki");
    }

    #[tokio::test]
    async fn loki_unavailable_falls_back_to_k8s_with_annotation() {
        let k8s = FakeK8sLogStreamClient::ok(vec![K8sLogLine {
            ts: ts(3000),
            message: "starting container".into(),
            pod: "hp-worker-xyz".into(),
        }]);
        let source = make_source(FakeLokiClient::err("connection refused"), k8s);
        let output = match source.collect("hp-abc", &IdType::Workflow).await {
            SourceResult::Output(o) => o,
            _ => panic!("expected Output"),
        };
        assert_eq!(output.events.len(), 1);
        assert_eq!(output.events[0].source, "k8s-logs");
        assert_eq!(output.state.len(), 1);
        assert!(output.state[0].value.contains("[loki unavailable, using k8s streaming]"));
    }

    #[tokio::test]
    async fn both_unavailable_returns_unavailable() {
        let source = make_source(
            FakeLokiClient::err("loki down"),
            FakeK8sLogStreamClient::err("k8s down"),
        );
        match source.collect("hp-abc", &IdType::Workflow).await {
            SourceResult::Unavailable(u) => {
                assert_eq!(u.name, "loki");
                assert!(u.reason.contains("loki down"));
            }
            _ => panic!("expected Unavailable"),
        }
    }

    #[tokio::test]
    async fn pod_pattern_forwarded_to_loki_client() {
        struct CaptureLoki {
            captured: Arc<Mutex<Option<String>>>,
        }

        #[async_trait]
        impl LokiClient for CaptureLoki {
            async fn query_range(
                &self,
                _id: &str,
                _id_type: &IdType,
                _since: Duration,
                pod_pattern: Option<&str>,
            ) -> Result<Vec<LokiLogLine>> {
                *self.captured.lock().unwrap() = pod_pattern.map(str::to_string);
                Ok(vec![])
            }
        }

        let captured = Arc::new(Mutex::new(None));
        let source = LokiSource::new(
            Box::new(CaptureLoki { captured: captured.clone() }),
            Box::new(FakeK8sLogStreamClient::ok(vec![])),
            Some("hp-worker-*".into()),
            Duration::hours(1),
        );
        source.collect("hp-abc", &IdType::Workflow).await;
        assert_eq!(*captured.lock().unwrap(), Some("hp-worker-*".to_string()));
    }

    #[tokio::test]
    async fn source_is_independently_optional() {
        let source = make_source(
            FakeLokiClient::err("loki unavailable"),
            FakeK8sLogStreamClient::err("k8s unavailable"),
        );
        match source.collect("hp-abc", &IdType::Workflow).await {
            SourceResult::Unavailable(u) => assert_eq!(u.name, "loki"),
            SourceResult::Output(_) => panic!("expected Unavailable, not Output"),
        }
    }
}
