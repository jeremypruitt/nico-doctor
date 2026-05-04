use std::time::Duration;
use async_trait::async_trait;
use nico_common::output::Status;

pub struct RunOpts {
    pub namespace: String,
    pub since: Duration,
    pub timeout: Duration,
}

pub struct Check {
    pub name: &'static str,
    pub status: Status,
    pub value: String,
    pub next_command: Option<String>,
}

pub struct LayerResult {
    pub name: &'static str,
    pub status: Status,
    pub checks: Vec<Check>,
    pub duration_ms: u64,
}

#[async_trait]
pub trait Layer: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run(&self, opts: &RunOpts) -> LayerResult;
}
