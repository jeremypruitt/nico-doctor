use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use async_trait::async_trait;

use nico_common::k8s::{K8sClient, PodScope};

/// Pre-fetched per-pod log lines, populated once per refresh by
/// [`crate::log_collector::LogCollectorStage`]. Consumers (`K8sLogSource`,
/// `ClusterLayer`) read from this in preference to issuing their own
/// `pod_logs` calls. An empty map signals "no cache available" — sources
/// that have a fallback (e.g. the standalone snapshot logs panel) may
/// fetch directly. Issue #201.
pub type PodLogsCache = HashMap<String, Vec<String>>;

/// One round of log collection: the entries gathered, the human-readable
/// label of the source that produced them, and whether the primary
/// (preferred) source was used.
pub struct LogCollection {
    pub label: String,
    pub primary_ok: bool,
    pub entries: Vec<(String, String)>,
}

#[async_trait]
pub trait LogSource: Send + Sync {
    /// Human-readable name used when this source is the one that produced a
    /// `LogCollection`. The chain adapter uses this when annotating fallback
    /// labels (e.g. "k8s (loki unavailable)").
    fn name(&self) -> &str;

    /// Collect a round of log entries from this source. `prefetched` is
    /// the per-refresh `PodLogsCache` populated by `LogCollectorStage` —
    /// sources that can satisfy the request from cache should do so;
    /// otherwise they fetch directly. Loki ignores it (Loki has no
    /// per-pod-log analogue in the cache).
    async fn collect(
        &self,
        namespace: &str,
        since: Duration,
        limit: usize,
        prefetched: &PodLogsCache,
    ) -> Result<LogCollection>;
}

pub fn best_effort_chain(sources: Vec<Arc<dyn LogSource>>) -> Arc<dyn LogSource> {
    Arc::new(BestEffortChain { sources })
}

struct BestEffortChain {
    sources: Vec<Arc<dyn LogSource>>,
}

#[async_trait]
impl LogSource for BestEffortChain {
    fn name(&self) -> &str {
        self.sources.first().map(|s| s.name()).unwrap_or("none")
    }

    async fn collect(
        &self,
        namespace: &str,
        since: Duration,
        limit: usize,
        prefetched: &PodLogsCache,
    ) -> Result<LogCollection> {
        let mut failed: Vec<String> = Vec::new();
        for (idx, source) in self.sources.iter().enumerate() {
            match source.collect(namespace, since, limit, prefetched).await {
                Ok(mut c) => {
                    if idx == 0 {
                        return Ok(c);
                    }
                    c.primary_ok = false;
                    c.label = format!("{} ({} unavailable)", c.label, failed.join(", "));
                    return Ok(c);
                }
                Err(_) => {
                    failed.push(source.name().to_string());
                }
            }
        }
        anyhow::bail!("no log source available: tried {}", failed.join(", "));
    }
}

pub(crate) fn is_error_line(s: &str) -> bool {
    let l = s.to_lowercase();
    l.contains("error") || l.contains("panic") || l.contains("fatal")
}

/// Like [`is_error_line`] but also matches `warn`-severity lines. Mirrors the
/// classification done by `nico_ops::model::log_level_from_text` so the
/// cluster layer's `pod_log_tail` filtering aligns with operator-facing UI.
pub(crate) fn is_error_or_warn_line(s: &str) -> bool {
    if is_error_line(s) {
        return true;
    }
    s.to_lowercase().contains("warn")
}

pub struct K8sLogSource {
    k8s: Arc<dyn K8sClient>,
}

impl K8sLogSource {
    pub fn new(k8s: Arc<dyn K8sClient>) -> Self {
        Self { k8s }
    }
}

#[async_trait]
impl LogSource for K8sLogSource {
    fn name(&self) -> &str { "k8s" }

    async fn collect(
        &self,
        namespace: &str,
        since: Duration,
        limit: usize,
        prefetched: &PodLogsCache,
    ) -> Result<LogCollection> {
        let entries = if prefetched.is_empty() {
            // Standalone path (e.g. the snapshot logs panel) — no
            // per-refresh cache populated, so we fetch directly. The
            // `LogCollectorStage` populates the cache in the doctor
            // refresh path, where this branch is never taken.
            let pods = self.k8s.list_pods(PodScope::Namespace(namespace)).await?;
            let mut owned: PodLogsCache = HashMap::with_capacity(pods.len());
            for pod in pods {
                let lines = self
                    .k8s
                    .pod_logs(namespace, &pod.name, since)
                    .await
                    .unwrap_or_default();
                owned.insert(pod.name, lines);
            }
            entries_from_cache(&owned, limit)
        } else {
            entries_from_cache(prefetched, limit)
        };
        Ok(LogCollection {
            label: "k8s".to_string(),
            primary_ok: true,
            entries,
        })
    }
}

fn entries_from_cache(cache: &PodLogsCache, limit: usize) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (pod_name, lines) in cache {
        for line in lines.iter().take(limit) {
            if is_error_line(line) {
                out.push((pod_name.clone(), line.clone()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    struct OkSource {
        name: &'static str,
        entries: Vec<(String, String)>,
    }

    #[async_trait]
    impl LogSource for OkSource {
        fn name(&self) -> &str { self.name }
        async fn collect(
            &self,
            _: &str,
            _: Duration,
            _: usize,
            _: &PodLogsCache,
        ) -> Result<LogCollection> {
            Ok(LogCollection {
                label: self.name.to_string(),
                primary_ok: true,
                entries: self.entries.clone(),
            })
        }
    }

    struct FailSource { name: &'static str }

    #[async_trait]
    impl LogSource for FailSource {
        fn name(&self) -> &str { self.name }
        async fn collect(
            &self,
            _: &str,
            _: Duration,
            _: usize,
            _: &PodLogsCache,
        ) -> Result<LogCollection> {
            Err(anyhow!("{} broken", self.name))
        }
    }

    fn d() -> Duration { Duration::from_secs(1) }
    fn empty_cache() -> PodLogsCache { HashMap::new() }

    #[tokio::test]
    async fn chain_returns_first_source_when_it_succeeds() {
        let chain = best_effort_chain(vec![
            Arc::new(OkSource { name: "loki", entries: vec![("p".into(), "x".into())] }),
            Arc::new(OkSource { name: "k8s", entries: vec![] }),
        ]);
        let c = chain.collect("ns", d(), 10, &empty_cache()).await.unwrap();
        assert_eq!(c.label, "loki");
        assert!(c.primary_ok);
        assert_eq!(c.entries.len(), 1);
    }

    #[tokio::test]
    async fn chain_falls_back_and_annotates_when_first_fails() {
        let chain = best_effort_chain(vec![
            Arc::new(FailSource { name: "loki" }),
            Arc::new(OkSource { name: "k8s", entries: vec![("p".into(), "x".into())] }),
        ]);
        let c = chain.collect("ns", d(), 10, &empty_cache()).await.unwrap();
        assert_eq!(c.label, "k8s (loki unavailable)");
        assert!(!c.primary_ok);
        assert_eq!(c.entries.len(), 1);
    }

    #[tokio::test]
    async fn chain_returns_error_when_all_sources_fail() {
        let chain = best_effort_chain(vec![
            Arc::new(FailSource { name: "loki" }),
            Arc::new(FailSource { name: "k8s" }),
        ]);
        assert!(chain.collect("ns", d(), 10, &empty_cache()).await.is_err());
    }

    fn pod(name: &str) -> nico_common::k8s::RawPod {
        nico_common::k8s::RawPod {
            name: name.into(),
            namespace: "ns".into(),
            phase: None,
            ready: true,
            restart_count: 0,
            succeeded: false,
            crash_loop: false,
        }
    }

    #[tokio::test]
    async fn k8s_source_reads_from_prefetched_cache_when_present() {
        // A k8s client whose pod_logs would panic if hit — proves we
        // never call it when the cache is populated.
        struct PanicLogs;
        #[async_trait]
        impl K8sClient for PanicLogs {
            async fn list_pods(&self, _: PodScope<'_>) -> Result<Vec<nico_common::k8s::RawPod>> {
                panic!("list_pods must not be called when cache is non-empty");
            }
            async fn list_events(
                &self,
                _: &str,
                _: Option<&str>,
            ) -> Result<Vec<nico_common::k8s::RawEvent>> {
                Ok(vec![])
            }
            async fn pod_logs(&self, _: &str, _: &str, _: Duration) -> Result<Vec<String>> {
                panic!("pod_logs must not be called when cache is non-empty");
            }
        }
        let source = K8sLogSource::new(Arc::new(PanicLogs));
        let mut cache: PodLogsCache = HashMap::new();
        cache.insert("p1".into(), vec!["INFO ok".into(), "ERROR boom".into()]);
        cache.insert("p2".into(), vec!["INFO heartbeat".into()]);

        let c = source.collect("ns", d(), 100, &cache).await.unwrap();
        assert_eq!(c.label, "k8s");
        assert!(c.primary_ok);
        assert_eq!(c.entries.len(), 1);
        assert_eq!(c.entries[0].0, "p1");
        assert!(c.entries[0].1.contains("ERROR boom"));
    }

    #[tokio::test]
    async fn k8s_source_falls_back_to_direct_fetch_when_cache_empty() {
        use nico_common::k8s::testing::MockK8sClient;
        let client = Arc::new(
            MockK8sClient::new()
                .with_pods(vec![pod("p1")])
                .with_logs(vec!["ERROR direct fetch".into()]),
        );
        let source = K8sLogSource::new(client);

        let c = source.collect("ns", d(), 100, &empty_cache()).await.unwrap();
        assert_eq!(c.entries.len(), 1);
        assert_eq!(c.entries[0].0, "p1");
        assert!(c.entries[0].1.contains("ERROR direct fetch"));
    }
}
