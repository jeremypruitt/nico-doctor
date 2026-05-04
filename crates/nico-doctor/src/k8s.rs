use std::time::Duration;
use async_trait::async_trait;
use anyhow::Result;

pub struct PodInfo {
    pub name: String,
    pub ready: bool,
    pub restart_count: u32,
}

pub struct EventInfo {
    pub message: String,
    pub reason: String,
}

#[async_trait]
pub trait K8sClient: Send + Sync {
    async fn list_pods(&self, namespace: &str) -> Result<Vec<PodInfo>>;
    async fn list_events(&self, namespace: &str, since: Duration) -> Result<Vec<EventInfo>>;
    async fn pod_logs(&self, namespace: &str, pod: &str, since: Duration) -> Result<Vec<String>>;
}
