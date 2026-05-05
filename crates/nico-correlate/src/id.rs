#[derive(Debug, Clone, PartialEq)]
pub enum IdType {
    Workflow,
    Host,
    Dpu,
    Request,
}

impl IdType {
    pub fn label_key(&self) -> &'static str {
        match self {
            IdType::Workflow => "workflow_id",
            IdType::Host => "host_id",
            IdType::Dpu => "dpu_id",
            IdType::Request => "request_id",
        }
    }

    pub fn cli_name(&self) -> &'static str {
        match self {
            IdType::Workflow => "workflow",
            IdType::Host => "host",
            IdType::Dpu => "dpu",
            IdType::Request => "request",
        }
    }

    pub fn from_cli_name(s: &str) -> Option<Self> {
        match s {
            "workflow" => Some(IdType::Workflow),
            "host" => Some(IdType::Host),
            "dpu" => Some(IdType::Dpu),
            "request" => Some(IdType::Request),
            _ => None,
        }
    }
}

pub fn detect_id_type(id: &str) -> Option<IdType> {
    if id.starts_with("hp-") || id.starts_with("wf-") {
        return Some(IdType::Workflow);
    }
    if id.starts_with("host-") {
        return Some(IdType::Host);
    }
    if id.starts_with("dpu-") {
        return Some(IdType::Dpu);
    }
    if id.starts_with("req-") {
        return Some(IdType::Request);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_heuristics() {
        let cases = [
            ("hp-7f3a2c",       Some(IdType::Workflow)),
            ("wf-abc123",       Some(IdType::Workflow)),
            ("host-r12u5",      Some(IdType::Host)),
            ("host-prov-r12u5", Some(IdType::Host)),
            ("dpu-bf3-r12u5",   Some(IdType::Dpu)),
            ("req-a83b",        Some(IdType::Request)),
            ("unknown-xyz",     None),
            ("",                None),
        ];
        for (input, expected) in cases {
            assert_eq!(detect_id_type(input), expected, "input: {input:?}");
        }
    }

    #[test]
    fn label_keys() {
        assert_eq!(IdType::Workflow.label_key(), "workflow_id");
        assert_eq!(IdType::Host.label_key(), "host_id");
        assert_eq!(IdType::Dpu.label_key(), "dpu_id");
        assert_eq!(IdType::Request.label_key(), "request_id");
    }

    #[test]
    fn cli_names() {
        assert_eq!(IdType::Workflow.cli_name(), "workflow");
        assert_eq!(IdType::Host.cli_name(), "host");
        assert_eq!(IdType::Dpu.cli_name(), "dpu");
        assert_eq!(IdType::Request.cli_name(), "request");
    }

    #[test]
    fn from_cli_name_roundtrips() {
        for variant in [IdType::Workflow, IdType::Host, IdType::Dpu, IdType::Request] {
            assert_eq!(IdType::from_cli_name(variant.cli_name()), Some(variant));
        }
        assert_eq!(IdType::from_cli_name("unknown"), None);
        assert_eq!(IdType::from_cli_name(""), None);
    }
}
