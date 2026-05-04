use std::time::Duration;
use async_trait::async_trait;
use anyhow::Result;

pub struct LokiLine {
    pub pod: String,
    pub text: String,
}

pub enum LokiQueryResult {
    Lines(Vec<LokiLine>),
    Unreachable,
}

#[async_trait]
pub trait LokiClient: Send + Sync {
    /// Query Loki for lines matching error|panic|fatal in the given namespace.
    /// Returns Unreachable when Loki is not reachable; caller falls back to k8s streaming.
    async fn query_errors(&self, namespace: &str, since: Duration, limit: usize) -> Result<LokiQueryResult>;
}
