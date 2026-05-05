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

pub struct SkippedLayer {
    name: &'static str,
}

impl SkippedLayer {
    pub fn new(name: &'static str) -> Box<dyn Layer> {
        Box::new(Self { name })
    }
}

#[async_trait]
impl Layer for SkippedLayer {
    fn name(&self) -> &'static str { self.name }
    async fn run(&self, _opts: &RunOpts) -> LayerResult {
        LayerResult {
            name: self.name,
            status: Status::Skipped,
            checks: vec![],
            duration_ms: 0,
        }
    }
}

pub struct UnconfiguredLayer {
    name: &'static str,
    reason: String,
}

impl UnconfiguredLayer {
    pub fn new(name: &'static str, reason: impl Into<String>) -> Box<dyn Layer> {
        Box::new(Self { name, reason: reason.into() })
    }
}

#[async_trait]
impl Layer for UnconfiguredLayer {
    fn name(&self) -> &'static str { self.name }
    async fn run(&self, _opts: &RunOpts) -> LayerResult {
        LayerResult {
            name: self.name,
            status: Status::Unknown,
            checks: vec![Check {
                name: "config",
                status: Status::Unknown,
                value: self.reason.clone(),
                next_command: None,
            }],
            duration_ms: 0,
        }
    }
}
