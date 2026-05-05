use async_trait::async_trait;
use anyhow::Result;

pub struct GrpcServiceInfo {
    #[allow(dead_code)]
    pub name: String,
    pub method_count: usize,
}

#[allow(dead_code)]
pub enum GrpcInspectResult {
    Reachable { services: Vec<GrpcServiceInfo> },
    Unreachable,
}

#[async_trait]
pub trait GrpcInspector: Send + Sync {
    async fn inspect(&self, addr: &str) -> Result<GrpcInspectResult>;
}
