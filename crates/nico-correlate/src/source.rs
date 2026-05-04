use async_trait::async_trait;
use crate::event::Event;
use crate::id::IdType;

#[derive(Debug, Clone)]
pub struct StateEntry {
    pub source: &'static str,
    pub key: String,
    pub value: String,
}

pub struct SourceOutput {
    pub events: Vec<Event>,
    pub state: Vec<StateEntry>,
}

pub struct SourceUnavailable {
    pub name: &'static str,
    pub reason: String,
}

pub enum SourceResult {
    Output(SourceOutput),
    Unavailable(SourceUnavailable),
}

#[async_trait]
pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    async fn collect(&self, id: &str, id_type: &IdType) -> SourceResult;
}
