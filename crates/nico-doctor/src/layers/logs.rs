use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use nico_common::output::Status;
use crate::log_source::LogSource;
use crate::layer::{aggregate_status, Check, Layer, LayerResult, RunOpts};

const LOG_LINE_LIMIT: usize = 500;

pub struct LogsLayer {
    source: Arc<dyn LogSource>,
}

impl LogsLayer {
    pub fn new(source: Arc<dyn LogSource>) -> Self {
        Self { source }
    }
}

#[async_trait]
impl Layer for LogsLayer {
    fn name(&self) -> &'static str { "logs" }

    async fn run(&self, opts: &RunOpts) -> LayerResult {
        let start = Instant::now();

        let (pod_errors, source_label, source_ok) =
            match self.source.collect(&opts.namespace, opts.since, LOG_LINE_LIMIT).await {
                Ok(c) => (c.entries, c.label, c.primary_ok),
                Err(_) => (Vec::new(), "unavailable".to_string(), false),
            };

        let checks = checks_from(&pod_errors, &source_label, source_ok, &opts.namespace);
        let overall = aggregate_status(&checks);

        LayerResult {
            name: "logs",
            status: overall,
            checks,
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }
}

fn checks_from(
    pod_errors: &[(String, String)],
    source_label: &str,
    source_ok: bool,
    namespace: &str,
) -> Vec<Check> {
    let error_count = pod_errors.len();
    let mut checks = vec![
        Check {
            name: "error_lines",
            status: if error_count == 0 { Status::Ok } else { Status::Warn },
            value: format!("{error_count} errors"),
            next_command: None,
        },
        Check {
            name: "source",
            status: if source_ok { Status::Ok } else { Status::Warn },
            value: source_label.to_string(),
            next_command: None,
        },
    ];

    for (pod, line) in pod_errors {
        let excerpt = if line.len() > 80 {
            format!("{}…", &line[..79])
        } else {
            line.clone()
        };
        checks.push(Check {
            name: "pod_error",
            status: Status::Warn,
            value: format!("{pod}: {excerpt}"),
            next_command: Some(format!("kubectl logs {pod} -n {namespace}")),
        });
    }

    checks
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use anyhow::Result;
    use async_trait::async_trait;
    use crate::log_source::{LogCollection, LogSource};

    fn opts() -> RunOpts {
        RunOpts { namespace: "nico".into(), since: Duration::from_secs(600), timeout: Duration::from_secs(5) }
    }

    struct FakeLogSource {
        label: String,
        primary_ok: bool,
        entries: Vec<(String, String)>,
    }

    impl FakeLogSource {
        fn new(label: &str, primary_ok: bool, entries: Vec<(&str, &str)>) -> Self {
            Self {
                label: label.to_string(),
                primary_ok,
                entries: entries.into_iter().map(|(p, t)| (p.to_string(), t.to_string())).collect(),
            }
        }
    }

    #[async_trait]
    impl LogSource for FakeLogSource {
        fn name(&self) -> &str { &self.label }

        async fn collect(&self, _: &str, _: Duration, _: usize) -> Result<LogCollection> {
            Ok(LogCollection {
                label: self.label.clone(),
                primary_ok: self.primary_ok,
                entries: self.entries.clone(),
            })
        }
    }

    #[test]
    fn checks_from_no_errors_reports_ok() {
        let checks = checks_from(&[], "loki", true, "nico");
        assert_eq!(aggregate_status(&checks), Status::Ok);
        assert_eq!(checks.iter().filter(|c| c.name == "pod_error").count(), 0);
    }

    #[test]
    fn checks_from_errors_present_reports_warn_with_one_pod_error_per_entry() {
        let pod_errors = vec![
            ("core-abc".to_string(), "ERROR: disk full".to_string()),
            ("rest-xyz".to_string(), "FATAL: oom".to_string()),
        ];
        let checks = checks_from(&pod_errors, "loki", true, "nico");

        assert_eq!(aggregate_status(&checks), Status::Warn);
        assert_eq!(checks.iter().filter(|c| c.name == "pod_error").count(), 2);
        let err_check = checks.iter().find(|c| c.name == "error_lines").unwrap();
        assert_eq!(err_check.status, Status::Warn);
    }

    #[test]
    fn checks_from_source_unavailable_marks_source_warn() {
        let checks = checks_from(&[], "k8s (loki unavailable)", false, "nico");
        let src = checks.iter().find(|c| c.name == "source").unwrap();
        assert_eq!(src.status, Status::Warn);
        assert_eq!(checks.iter().filter(|c| c.name == "source").count(), 1);
    }

    #[tokio::test]
    async fn primary_source_with_errors_reports_warn_with_kubectl_hints() {
        let source: Arc<dyn LogSource> = Arc::new(FakeLogSource::new(
            "loki", true,
            vec![("core-abc", "ERROR: disk full"), ("rest-xyz", "FATAL: oom")],
        ));
        let result = LogsLayer::new(source).run(&opts()).await;

        assert_eq!(result.status, Status::Warn);
        let err_check = result.checks.iter().find(|c| c.name == "error_lines").unwrap();
        assert_eq!(err_check.value, "2 errors");
        let src = result.checks.iter().find(|c| c.name == "source").unwrap();
        assert_eq!(src.value, "loki");
        assert_eq!(src.status, Status::Ok);
        let pod_errors: Vec<_> = result.checks.iter().filter(|c| c.name == "pod_error").collect();
        assert_eq!(pod_errors.len(), 2);
        assert!(pod_errors[0].next_command.as_deref().unwrap().starts_with("kubectl logs"));
    }

    #[tokio::test]
    async fn fallback_source_marks_source_warn_and_keeps_label() {
        let source: Arc<dyn LogSource> = Arc::new(FakeLogSource::new(
            "k8s (loki unavailable)", false,
            vec![("core-abc", "ERROR: connection refused")],
        ));
        let result = LogsLayer::new(source).run(&opts()).await;

        assert_eq!(result.status, Status::Warn);
        let src = result.checks.iter().find(|c| c.name == "source").unwrap();
        assert!(src.value.contains("loki unavailable"));
        assert_eq!(src.status, Status::Warn);
    }

    #[tokio::test]
    async fn empty_source_reports_ok() {
        let source: Arc<dyn LogSource> = Arc::new(FakeLogSource::new("loki", true, vec![]));
        let result = LogsLayer::new(source).run(&opts()).await;
        assert_eq!(result.status, Status::Ok);
        let err_check = result.checks.iter().find(|c| c.name == "error_lines").unwrap();
        assert_eq!(err_check.value, "0 errors");
        assert!(result.checks.iter().filter(|c| c.name == "pod_error").count() == 0);
    }
}
