use std::time::Duration;
use async_trait::async_trait;
use anyhow::Result;
use chrono::Utc;
use kube::{Client, Api};
use kube::api::{ListParams, LogParams};
use k8s_openapi::api::core::v1::{Pod, Event as CoreEvent};

pub struct PodInfo {
    pub name: String,
    pub ready: bool,
    pub restart_count: u32,
    pub succeeded: bool,
}

pub struct EventInfo {
    #[allow(dead_code)]
    pub message: String,
    #[allow(dead_code)]
    pub reason: String,
}

#[async_trait]
pub trait K8sClient: Send + Sync {
    async fn list_pods(&self, namespace: &str) -> Result<Vec<PodInfo>>;
    async fn list_events(&self, namespace: &str, since: Duration) -> Result<Vec<EventInfo>>;
    async fn pod_logs(&self, namespace: &str, pod: &str, since: Duration) -> Result<Vec<String>>;
}

pub(crate) fn map_pod(pod: Pod) -> PodInfo {
    let status = pod.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref());
    let succeeded = matches!(phase, Some("Succeeded"));
    let name = pod.metadata.name.unwrap_or_default();
    let container_statuses = status.and_then(|s| s.container_statuses.as_ref());
    let ready = container_statuses
        .map(|cs| !cs.is_empty() && cs.iter().all(|c| c.ready))
        .unwrap_or(false);
    let restart_count: u32 = container_statuses
        .map(|cs| cs.iter().map(|c| c.restart_count.max(0) as u32).sum())
        .unwrap_or(0);
    PodInfo { name, ready, restart_count, succeeded }
}

pub struct KubeRsK8sClient {
    client: Client,
}

impl KubeRsK8sClient {
    pub async fn try_new(context: Option<&str>) -> Result<Self> {
        let client = if let Some(ctx) = context {
            use kube::config::KubeConfigOptions;
            let config = kube::Config::from_kubeconfig(&KubeConfigOptions {
                context: Some(ctx.to_string()),
                ..Default::default()
            }).await?;
            Client::try_from(config)?
        } else {
            Client::try_default().await?
        };
        Ok(Self { client })
    }

    pub fn raw_client(&self) -> &Client {
        &self.client
    }
}

#[async_trait]
impl K8sClient for KubeRsK8sClient {
    async fn list_pods(&self, namespace: &str) -> Result<Vec<PodInfo>> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod_list = api.list(&ListParams::default()).await
            .map_err(|e| anyhow::anyhow!("k8s list_pods failed: {e}"))?;
        Ok(pod_list.items.into_iter().map(map_pod).collect())
    }

    async fn list_events(&self, namespace: &str, since: Duration) -> Result<Vec<EventInfo>> {
        let api: Api<CoreEvent> = Api::namespaced(self.client.clone(), namespace);
        let event_list = api.list(&ListParams::default()).await
            .map_err(|e| anyhow::anyhow!("k8s list_events failed: {e}"))?;

        let since_chrono = chrono::Duration::from_std(since)
            .unwrap_or_else(|_| chrono::Duration::hours(24));
        let cutoff = Utc::now() - since_chrono;

        let infos = event_list.items.into_iter().filter_map(|e| {
            if e.type_.as_deref() != Some("Warning") {
                return None;
            }
            let ts = e.last_timestamp.as_ref().map(|t| t.0)
                .or_else(|| e.first_timestamp.as_ref().map(|t| t.0))?;
            if ts < cutoff {
                return None;
            }
            let message = e.message.unwrap_or_default();
            let reason = e.reason.unwrap_or_default();
            Some(EventInfo { message, reason })
        }).collect();

        Ok(infos)
    }

    async fn pod_logs(&self, namespace: &str, pod: &str, since: Duration) -> Result<Vec<String>> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let lp = LogParams {
            since_seconds: Some(since.as_secs() as i64),
            ..Default::default()
        };
        let log_data = api.logs(pod, &lp).await
            .map_err(|e| anyhow::anyhow!("k8s pod_logs failed: {e}"))?;
        Ok(log_data.lines().map(|l| l.to_string()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{ContainerStatus, PodStatus};

    fn pod_with_phase(phase: &str) -> Pod {
        Pod {
            status: Some(PodStatus {
                phase: Some(phase.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn pod_with_ready_containers(ready: bool) -> Pod {
        let cs = ContainerStatus {
            ready,
            restart_count: 0,
            ..Default::default()
        };
        Pod {
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                container_statuses: Some(vec![cs]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn map_pod_succeeded_phase_sets_succeeded_true() {
        let info = map_pod(pod_with_phase("Succeeded"));
        assert!(info.succeeded);
        assert!(!info.ready);
    }

    #[test]
    fn map_pod_running_phase_sets_succeeded_false() {
        let info = map_pod(pod_with_phase("Running"));
        assert!(!info.succeeded);
    }

    #[test]
    fn map_pod_ready_container_sets_ready_true() {
        let info = map_pod(pod_with_ready_containers(true));
        assert!(info.ready);
        assert!(!info.succeeded);
    }

    #[test]
    fn map_pod_not_ready_container_sets_ready_false() {
        let info = map_pod(pod_with_ready_containers(false));
        assert!(!info.ready);
        assert!(!info.succeeded);
    }
}
