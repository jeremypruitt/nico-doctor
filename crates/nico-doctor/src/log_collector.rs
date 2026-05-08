use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nico_common::k8s::{K8sClient, PodScope};

/// One round of pod-log collection, run **once per refresh** before the
/// per-layer `runner::run` fan-out. Both `ClusterLayer` (for `pod_log_tail`
/// detail checks) and `K8sLogSource` (logs layer) consume from the
/// resulting `HashMap` rather than issuing their own `pod_logs` calls.
/// See issue #201 for why this lives outside the parallel layer set.
pub struct LogCollectorStage {
    k8s: Arc<dyn K8sClient>,
}

impl LogCollectorStage {
    pub fn new(k8s: Arc<dyn K8sClient>) -> Self {
        Self { k8s }
    }

    /// Fetch recent log lines for every non-`Succeeded` pod in `namespace`,
    /// at most one `pod_logs` call per pod. Pods whose `pod_logs` call
    /// fails contribute an empty `Vec` so callers can still distinguish
    /// "pod existed but had no errors" from "pod was never seen". A
    /// failure listing pods yields an empty cache (cluster + logs layers
    /// will then emit no per-pod detail checks for this refresh).
    pub async fn collect(
        &self,
        namespace: &str,
        since: Duration,
    ) -> HashMap<String, Vec<String>> {
        let pods = match self.k8s.list_pods(PodScope::Namespace(namespace)).await {
            Ok(p) => p,
            Err(_) => return HashMap::new(),
        };
        let mut out = HashMap::new();
        for pod in &pods {
            if pod.succeeded {
                continue;
            }
            let lines = self
                .k8s
                .pod_logs(namespace, &pod.name, since)
                .await
                .unwrap_or_default();
            out.insert(pod.name.clone(), lines);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use nico_common::k8s::{RawEvent, RawPod};
    use std::sync::Mutex;

    struct CountingK8s {
        pods: Vec<RawPod>,
        logs_per_pod: HashMap<String, Vec<String>>,
        pod_logs_calls: Mutex<Vec<String>>,
        list_pods_err: Option<String>,
    }

    impl CountingK8s {
        fn new(pods: Vec<RawPod>, logs_per_pod: HashMap<String, Vec<String>>) -> Self {
            Self {
                pods,
                logs_per_pod,
                pod_logs_calls: Mutex::new(Vec::new()),
                list_pods_err: None,
            }
        }

        fn with_list_pods_err(mut self, msg: &str) -> Self {
            self.list_pods_err = Some(msg.to_string());
            self
        }

        fn calls(&self) -> Vec<String> {
            self.pod_logs_calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl K8sClient for CountingK8s {
        async fn list_pods(&self, _scope: PodScope<'_>) -> Result<Vec<RawPod>> {
            if let Some(err) = &self.list_pods_err {
                anyhow::bail!("{}", err);
            }
            Ok(self.pods.clone())
        }
        async fn list_events(
            &self,
            _ns: &str,
            _field_selector: Option<&str>,
        ) -> Result<Vec<RawEvent>> {
            Ok(vec![])
        }
        async fn pod_logs(
            &self,
            _ns: &str,
            pod: &str,
            _since: Duration,
        ) -> Result<Vec<String>> {
            self.pod_logs_calls.lock().unwrap().push(pod.to_string());
            Ok(self.logs_per_pod.get(pod).cloned().unwrap_or_default())
        }
    }

    fn pod(name: &str, succeeded: bool) -> RawPod {
        RawPod {
            name: name.into(),
            namespace: "nico".into(),
            phase: None,
            ready: true,
            restart_count: 0,
            succeeded,
            crash_loop: false,
        }
    }

    #[tokio::test]
    async fn empty_namespace_yields_empty_cache() {
        let client = Arc::new(CountingK8s::new(vec![], HashMap::new()));
        let stage = LogCollectorStage::new(client.clone());
        let cache = stage.collect("nico", Duration::from_secs(60)).await;
        assert!(cache.is_empty());
        assert!(client.calls().is_empty());
    }

    #[tokio::test]
    async fn cache_contains_one_entry_per_non_succeeded_pod() {
        let mut logs = HashMap::new();
        logs.insert("p1".into(), vec!["INFO ok".into()]);
        logs.insert("p2".into(), vec!["ERROR boom".into()]);
        let client = Arc::new(CountingK8s::new(
            vec![pod("p1", false), pod("p2", false)],
            logs,
        ));
        let stage = LogCollectorStage::new(client.clone());
        let cache = stage.collect("nico", Duration::from_secs(60)).await;

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get("p1").unwrap(), &vec!["INFO ok".to_string()]);
        assert_eq!(cache.get("p2").unwrap(), &vec!["ERROR boom".to_string()]);
    }

    #[tokio::test]
    async fn pod_logs_called_at_most_once_per_pod() {
        let mut logs = HashMap::new();
        logs.insert("p1".into(), vec!["x".into()]);
        logs.insert("p2".into(), vec!["y".into()]);
        let client = Arc::new(CountingK8s::new(
            vec![pod("p1", false), pod("p2", false)],
            logs,
        ));
        let stage = LogCollectorStage::new(client.clone());
        let _ = stage.collect("nico", Duration::from_secs(60)).await;

        let calls = client.calls();
        assert_eq!(calls.len(), 2);
        let mut sorted = calls;
        sorted.sort();
        assert_eq!(sorted, vec!["p1".to_string(), "p2".to_string()]);
    }

    #[tokio::test]
    async fn succeeded_pods_skipped() {
        let client = Arc::new(CountingK8s::new(
            vec![pod("p1", false), pod("migrate-job", true)],
            HashMap::new(),
        ));
        let stage = LogCollectorStage::new(client.clone());
        let cache = stage.collect("nico", Duration::from_secs(60)).await;

        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key("p1"));
        assert_eq!(client.calls(), vec!["p1".to_string()]);
    }

    #[tokio::test]
    async fn list_pods_failure_yields_empty_cache() {
        let client = Arc::new(
            CountingK8s::new(vec![], HashMap::new())
                .with_list_pods_err("api server unreachable"),
        );
        let stage = LogCollectorStage::new(client.clone());
        let cache = stage.collect("nico", Duration::from_secs(60)).await;
        assert!(cache.is_empty());
        assert!(client.calls().is_empty());
    }
}
