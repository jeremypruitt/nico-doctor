//! Low-level Temporal Workflow Service client primitives shared by
//! `nico-doctor` and `nico-correlate`.
//!
//! The trait exposes the two RPCs both binaries actually issue against the
//! Temporal frontend: `ListWorkflowExecutions` (used by doctor's workflows
//! layer for stuck/failed queries) and `GetWorkflowExecutionHistory` (used
//! by correlate to assemble a workflow timeline). Higher-level domain
//! views (e.g. "list stuck" or "history events with error tags") are
//! built by each crate on top of this primitive interface.

use anyhow::Result;
use async_trait::async_trait;
use temporal_sdk_core_protos::temporal::api::common::v1::WorkflowExecution;
use temporal_sdk_core_protos::temporal::api::history::v1::History;
use temporal_sdk_core_protos::temporal::api::workflow::v1::WorkflowExecutionInfo;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    workflow_service_client::WorkflowServiceClient, GetWorkflowExecutionHistoryRequest,
    ListWorkflowExecutionsRequest,
};
use tonic::transport::Channel;

#[async_trait]
pub trait TemporalClient: Send + Sync {
    /// Run a Visibility query against the namespace. Returns the matched
    /// `WorkflowExecutionInfo`s. Caller composes the query string.
    async fn list_workflow_executions(
        &self,
        namespace: &str,
        query: &str,
        page_size: i32,
    ) -> Result<Vec<WorkflowExecutionInfo>>;

    /// Fetch the full event history for a workflow.
    async fn get_workflow_history(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<History>;
}

/// Real gRPC-backed implementation. Connects on each call; callers that
/// care about connection reuse can wrap this in their own pool.
pub struct GrpcTemporalClient {
    address: String,
}

impl GrpcTemporalClient {
    pub fn new(address: String) -> Self {
        Self { address }
    }

    async fn connect(&self) -> Result<WorkflowServiceClient<Channel>> {
        let channel = Channel::from_shared(self.address.clone())
            .map_err(|e| anyhow::anyhow!("invalid Temporal address: {e}"))?
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("connect to Temporal failed: {e}"))?;
        Ok(WorkflowServiceClient::new(channel))
    }
}

#[async_trait]
impl TemporalClient for GrpcTemporalClient {
    async fn list_workflow_executions(
        &self,
        namespace: &str,
        query: &str,
        page_size: i32,
    ) -> Result<Vec<WorkflowExecutionInfo>> {
        let mut client = self.connect().await?;
        let response = client
            .list_workflow_executions(ListWorkflowExecutionsRequest {
                namespace: namespace.to_string(),
                query: query.to_string(),
                page_size,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("ListWorkflowExecutions RPC failed: {e}"))?;
        Ok(response.into_inner().executions)
    }

    async fn get_workflow_history(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<History> {
        let mut client = self.connect().await?;
        let response = client
            .get_workflow_execution_history(GetWorkflowExecutionHistoryRequest {
                namespace: namespace.to_string(),
                execution: Some(WorkflowExecution {
                    workflow_id: workflow_id.to_string(),
                    run_id: String::new(),
                }),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("GetWorkflowExecutionHistory RPC failed: {e}"))?;
        Ok(response.into_inner().history.unwrap_or_default())
    }
}

/// Test fakes shared across both crates' test suites.
pub mod testing {
    use super::*;
    use std::sync::Mutex;

    /// In-memory fake driven entirely by what tests pre-populate. The
    /// fake stores raw protobuf types; helpers below build them concisely.
    pub struct MockTemporalClient {
        pub executions: Mutex<Result<Vec<WorkflowExecutionInfo>>>,
        pub history: Mutex<Result<History>>,
    }

    impl Default for MockTemporalClient {
        fn default() -> Self {
            Self {
                executions: Mutex::new(Ok(vec![])),
                history: Mutex::new(Ok(History::default())),
            }
        }
    }

    impl MockTemporalClient {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_executions(mut self, execs: Vec<WorkflowExecutionInfo>) -> Self {
            self.executions = Mutex::new(Ok(execs));
            self
        }

        pub fn with_executions_err(mut self, err: impl std::fmt::Display) -> Self {
            self.executions = Mutex::new(Err(anyhow::anyhow!("{err}")));
            self
        }

        pub fn with_history(mut self, history: History) -> Self {
            self.history = Mutex::new(Ok(history));
            self
        }

        pub fn with_history_err(mut self, err: impl std::fmt::Display) -> Self {
            self.history = Mutex::new(Err(anyhow::anyhow!("{err}")));
            self
        }
    }

    #[async_trait]
    impl TemporalClient for MockTemporalClient {
        async fn list_workflow_executions(
            &self,
            _namespace: &str,
            _query: &str,
            _page_size: i32,
        ) -> Result<Vec<WorkflowExecutionInfo>> {
            let guard = self.executions.lock().unwrap();
            match &*guard {
                Ok(v) => Ok(v.clone()),
                Err(e) => Err(anyhow::anyhow!("{e}")),
            }
        }

        async fn get_workflow_history(
            &self,
            _namespace: &str,
            _workflow_id: &str,
        ) -> Result<History> {
            let guard = self.history.lock().unwrap();
            match &*guard {
                Ok(v) => Ok(v.clone()),
                Err(e) => Err(anyhow::anyhow!("{e}")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporal_sdk_core_protos::temporal::api::common::v1::{
        WorkflowExecution, WorkflowType,
    };

    #[tokio::test]
    async fn mock_returns_configured_executions() {
        use testing::MockTemporalClient;
        let exec = WorkflowExecutionInfo {
            execution: Some(WorkflowExecution {
                workflow_id: "wf-001".into(),
                run_id: "run-1".into(),
            }),
            r#type: Some(WorkflowType {
                name: "HostProvisioning".into(),
            }),
            ..Default::default()
        };
        let client = MockTemporalClient::new().with_executions(vec![exec]);
        let out = client
            .list_workflow_executions("default", "ExecutionStatus=Running", 100)
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].execution.as_ref().unwrap().workflow_id, "wf-001");
    }

    #[tokio::test]
    async fn mock_returns_configured_history_err() {
        use testing::MockTemporalClient;
        let client = MockTemporalClient::new().with_history_err("not found");
        let result = client.get_workflow_history("default", "wf-x").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}
