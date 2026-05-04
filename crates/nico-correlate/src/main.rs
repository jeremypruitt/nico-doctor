mod correlate;
mod event;
mod id;
mod source;
mod sources;
mod timeline;

use clap::Parser;
use serde::Serialize;
use crate::id::{IdType, detect_id_type};
use crate::source::{Source, SourceResult, StateEntry};
use crate::sources::temporal::{TemporalSource, TemporalClient, RawTemporalEvent};
use crate::sources::postgres::{PostgresSource, PostgresClient, PgEntityData};
use crate::sources::k8s::{K8sSource, K8sClient, K8sPodData};
use crate::timeline::filter_timeline;
use crate::correlate::exit_code;
use crate::event::Event;
use anyhow::Result;
use async_trait::async_trait;

#[derive(Parser)]
#[command(name = "nico-correlate", about = "Correlate all events for a given entity ID")]
struct Cli {
    /// Entity ID to correlate (workflow, host, DPU, or request ID)
    id: String,

    /// Override auto-detected ID type (workflow|host|dpu|request)
    #[arg(short = 't', long)]
    r#type: Option<String>,

    /// Restrict to specific sources (comma-separated: temporal,postgres,k8s)
    #[arg(short = 's', long, value_delimiter = ',')]
    sources: Vec<String>,

    /// Output JSON
    #[arg(short = 'j', long)]
    json: bool,
}

// Real Temporal client is wired in issue #14.
struct TodoTemporalClient;

#[async_trait]
impl TemporalClient for TodoTemporalClient {
    async fn get_history(&self, _workflow_id: &str) -> Result<Vec<RawTemporalEvent>> {
        todo!("real Temporal gRPC client — see issue #14")
    }
}

// Real Postgres client is wired when sqlx is added.
struct TodoPostgresClient;

#[async_trait]
impl PostgresClient for TodoPostgresClient {
    async fn query_entity(&self, _id: &str, _id_type: &IdType) -> Result<PgEntityData> {
        todo!("real Postgres client — connect via NICO_POSTGRES_URL")
    }
}

// Real k8s client is wired when kube-rs is added.
struct TodoK8sClient;

#[async_trait]
impl K8sClient for TodoK8sClient {
    async fn find_pods_with_events(&self, _id: &str, _id_type: &IdType) -> Result<Vec<K8sPodData>> {
        todo!("real k8s client — uses in-cluster or kubeconfig")
    }
}

#[derive(Serialize)]
struct JsonOutput<'a> {
    version: u32,
    id: &'a str,
    id_type: &'a str,
    events: Vec<JsonEvent<'a>>,
    sources_unavailable: Vec<&'a str>,
    state: Vec<JsonStateEntry<'a>>,
}

#[derive(Serialize)]
struct JsonEvent<'a> {
    ts: String,
    source: &'a str,
    kind: &'a str,
    severity: &'a str,
}

#[derive(Serialize)]
struct JsonStateEntry<'a> {
    source: &'a str,
    key: &'a str,
    value: &'a str,
}

fn id_type_str(t: &IdType) -> &'static str {
    match t {
        IdType::Workflow => "workflow",
        IdType::Host => "host",
        IdType::Dpu => "dpu",
        IdType::Request => "request",
    }
}

fn parse_id_type(s: &str) -> Option<IdType> {
    match s {
        "workflow" => Some(IdType::Workflow),
        "host" => Some(IdType::Host),
        "dpu" => Some(IdType::Dpu),
        "request" => Some(IdType::Request),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let id_type = if let Some(ref t) = cli.r#type {
        match parse_id_type(t) {
            Some(it) => Some(it),
            None => {
                eprintln!("error: unknown --type {t:?}; use workflow|host|dpu|request");
                std::process::exit(1);
            }
        }
    } else {
        detect_id_type(&cli.id)
    };

    if id_type.is_none() {
        eprintln!(
            "error: could not detect ID type for {:?}\nHint: re-run with --type workflow|host|dpu|request",
            cli.id
        );
        std::process::exit(1);
    }
    let id_type = id_type.unwrap();

    println!("detected type: {}", id_type_str(&id_type));

    let all_sources: Vec<(&str, Box<dyn Source>)> = vec![
        ("temporal", Box::new(TemporalSource::new(Box::new(TodoTemporalClient)))),
        ("postgres", Box::new(PostgresSource::new(Box::new(TodoPostgresClient)))),
        ("k8s", Box::new(K8sSource::new(Box::new(TodoK8sClient)))),
    ];

    let sources: Vec<Box<dyn Source>> = if cli.sources.is_empty() {
        all_sources.into_iter().map(|(_, s)| s).collect()
    } else {
        all_sources.into_iter()
            .filter(|(name, _)| cli.sources.iter().any(|s| s == name))
            .map(|(_, s)| s)
            .collect()
    };

    let mut all_results: Vec<SourceResult> = Vec::new();
    for source in &sources {
        all_results.push(source.collect(&cli.id, &id_type).await);
    }

    let events: Vec<Event> = all_results.iter()
        .filter_map(|r| if let SourceResult::Output(o) = r { Some(o.events.clone()) } else { None })
        .flatten()
        .collect();

    let state: Vec<StateEntry> = all_results.iter()
        .filter_map(|r| if let SourceResult::Output(o) = r { Some(o.state.clone()) } else { None })
        .flatten()
        .collect();

    let unavailable: Vec<&str> = all_results.iter()
        .filter_map(|r| if let SourceResult::Unavailable(u) = r { Some(u.name) } else { None })
        .collect();

    let filtered = filter_timeline(events, 5, 10);

    let code = exit_code(Some(&id_type), &all_results);

    if cli.json {
        let out = JsonOutput {
            version: 1,
            id: &cli.id,
            id_type: id_type_str(&id_type),
            events: filtered.iter().map(|e| JsonEvent {
                ts: e.ts.to_rfc3339(),
                source: &e.source,
                kind: &e.kind,
                severity: match e.severity {
                    crate::event::Severity::Info => "info",
                    crate::event::Severity::Warning => "warning",
                    crate::event::Severity::Error => "error",
                },
            }).collect(),
            sources_unavailable: unavailable.clone(),
            state: state.iter().map(|s| JsonStateEntry {
                source: s.source,
                key: &s.key,
                value: &s.value,
            }).collect(),
        };
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        println!("Timeline ({} events):", filtered.len());
        for e in &filtered {
            println!("  {}  {}  {}", e.ts.format("%H:%M:%S"), e.source, e.kind);
        }

        let pg_state: Vec<&StateEntry> = state.iter().filter(|s| s.source == "postgres").collect();
        if !pg_state.is_empty() {
            println!("\nPostgres state (current):");
            for s in &pg_state {
                println!("  {}: {}", s.key, s.value);
            }
        }

        let k8s_state: Vec<&StateEntry> = state.iter().filter(|s| s.source == "k8s").collect();
        if !k8s_state.is_empty() {
            println!("\nK8s pods touched:");
            for s in &k8s_state {
                println!("  {}  {}", s.key, s.value);
            }
        }

        for name in &unavailable {
            println!("[source unavailable: {name}]");
        }
    }

    std::process::exit(code);
}
