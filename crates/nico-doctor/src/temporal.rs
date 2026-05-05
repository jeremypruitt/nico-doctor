use std::time::SystemTime;
use async_trait::async_trait;
use anyhow::Result;

pub struct RunningWorkflow {
    pub workflow_id: String,
    pub workflow_type: String,
    pub start_time: SystemTime,
    pub last_event: String,
}

pub struct FailedWorkflow {
    pub workflow_id: String,
    pub workflow_type: String,
    #[allow(dead_code)]
    pub close_time: SystemTime,
}

#[async_trait]
pub trait TemporalClient: Send + Sync {
    /// Running workflows whose start_time is earlier than stuck_before.
    async fn list_stuck(&self, namespace: &str, stuck_before: SystemTime) -> Result<Vec<RunningWorkflow>>;
    /// Workflows that failed after the given since time.
    async fn list_failed(&self, namespace: &str, since: SystemTime) -> Result<Vec<FailedWorkflow>>;
}
