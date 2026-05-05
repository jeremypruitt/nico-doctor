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
    #[allow(dead_code)]
    pub reason: String,
}

pub enum SourceResult {
    Output(SourceOutput),
    Unavailable(SourceUnavailable),
}

#[async_trait]
pub trait Source: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    async fn collect(&self, id: &str, id_type: &IdType) -> SourceResult;
}

pub struct UnavailableSource {
    name: &'static str,
    reason: String,
}

impl UnavailableSource {
    pub fn new(name: &'static str, reason: impl Into<String>) -> Self {
        Self { name, reason: reason.into() }
    }
}

#[async_trait]
impl Source for UnavailableSource {
    fn name(&self) -> &'static str { self.name }
    async fn collect(&self, _id: &str, _id_type: &IdType) -> SourceResult {
        SourceResult::Unavailable(SourceUnavailable { name: self.name, reason: self.reason.clone() })
    }
}
