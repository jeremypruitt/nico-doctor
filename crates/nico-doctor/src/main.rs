use std::process;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use clap::Parser;
use nico_common::config::{Config, ConfigOverrides, ColorMode, OutputFormat};
use nico_common::output::{OutputMode, Status};

mod formatter;
mod grpc;
mod http;
mod k8s;
mod layer;
mod layers;
mod loki;
mod postgres;
mod runner;
mod temporal;

use layer::RunOpts;

const LAYER_ORDER: &[&str] = &["cluster", "logs", "workflows", "health", "grpc", "postgres"];

#[derive(Parser)]
#[command(name = "nico-doctor", about = "Read-only health check for nico/ncx clusters")]
struct Cli {
    #[arg(short, long, help = "Kubernetes namespace")]
    namespace: Option<String>,

    #[arg(long, help = "Kubernetes context")]
    context: Option<String>,

    #[arg(long, value_delimiter = ',', help = "Layers to skip")]
    skip: Vec<String>,

    #[arg(long, default_value = "10m", help = "Look-back window for logs/events")]
    since: String,

    #[arg(long, default_value = "5s", help = "Per-check timeout")]
    timeout: String,

    #[arg(short, long, help = "Output JSON")]
    json: bool,

    #[arg(short, long, help = "Show details for passing checks")]
    verbose: bool,

    #[arg(long, help = "ASCII-only output")]
    ascii: bool,

    #[arg(long, help = "Disable color output")]
    no_color: bool,

    #[arg(long, help = "Postgres connection URL")]
    postgres_url: Option<String>,

    #[arg(long, value_name = "PATH", help = "Config file path (default: ~/.config/nico-tools/config.toml)")]
    config: Option<String>,
}

// --- Inactive client stubs ---
// Used when the backing service is absent or unconfigured.

struct InactiveK8sClient { reason: &'static str }

#[async_trait]
impl k8s::K8sClient for InactiveK8sClient {
    async fn list_pods(&self, _ns: &str) -> anyhow::Result<Vec<k8s::PodInfo>> {
        Err(anyhow::anyhow!("{}", self.reason))
    }
    async fn list_events(&self, _ns: &str, _since: Duration) -> anyhow::Result<Vec<k8s::EventInfo>> {
        Err(anyhow::anyhow!("{}", self.reason))
    }
    async fn pod_logs(&self, _ns: &str, _pod: &str, _since: Duration) -> anyhow::Result<Vec<String>> {
        Err(anyhow::anyhow!("{}", self.reason))
    }
}

struct InactiveLokiClient { reason: &'static str }

#[async_trait]
impl loki::LokiClient for InactiveLokiClient {
    async fn query_errors(&self, _ns: &str, _since: Duration, _limit: usize) -> anyhow::Result<loki::LokiQueryResult> {
        Err(anyhow::anyhow!("{}", self.reason))
    }
}

struct InactivePostgresClient { reason: &'static str }

#[async_trait]
impl postgres::PostgresClient for InactivePostgresClient {
    async fn pool_stats(&self) -> anyhow::Result<postgres::PoolStats> {
        Err(anyhow::anyhow!("{}", self.reason))
    }
    async fn lock_waits(&self) -> anyhow::Result<Vec<postgres::LockWait>> {
        Err(anyhow::anyhow!("{}", self.reason))
    }
}

fn exit_code(report: &runner::Report) -> i32 {
    match report.summary_status() {
        Status::Ok | Status::Skipped => 0,
        Status::Warn => 1,
        Status::Fail => 2,
        Status::Unknown => 3,
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Load config file from --config or the default path.
    let config_path = cli.config.as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
            std::path::PathBuf::from(home).join(".config/nico-tools/config.toml")
        });
    let file_toml = std::fs::read_to_string(&config_path).ok();

    // CLI flags are highest precedence; env and file layers are handled by Config::load.
    let overrides = ConfigOverrides {
        namespace: cli.namespace.clone(),
        context: cli.context.clone(),
        postgres_url: cli.postgres_url.clone(),
        color: if cli.no_color { Some(ColorMode::Never) } else { None },
        format: if cli.json { Some(OutputFormat::Json) } else { None },
        ..Default::default()
    };

    let env: std::collections::HashMap<String, String> = std::env::vars().collect();
    let config = match Config::load(file_toml.as_deref(), &env, &overrides) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error loading config: {e}");
            process::exit(1);
        }
    };

    let mode = OutputMode {
        color: match config.output.color {
            ColorMode::Always => true,
            ColorMode::Never => false,
            ColorMode::Auto => std::env::var("NO_COLOR").is_err(),
        },
        ascii: cli.ascii,
    };

    let since = humantime::parse_duration(&cli.since).unwrap_or(Duration::from_secs(600));
    let timeout = humantime::parse_duration(&cli.timeout).unwrap_or(Duration::from_secs(5));

    let opts = RunOpts { namespace: config.cluster.namespace.clone(), since, timeout };

    // Build k8s client using context from Config (flag > env > file > default).
    let k8s_client: Option<Arc<dyn k8s::K8sClient>> =
        match k8s::KubeRsK8sClient::try_new(config.cluster.context.as_deref()).await {
            Ok(c) => Some(Arc::new(c) as Arc<dyn k8s::K8sClient>),
            Err(_) => None,
        };

    // Loki URL is not a Config field — read from env directly.
    let loki_url = std::env::var("LOKI_URL").ok();
    let loki_client: Arc<dyn loki::LokiClient> = match loki_url.as_deref() {
        Some(url) => Arc::new(loki::RealLokiClient::new(url.to_string())) as Arc<dyn loki::LokiClient>,
        None => Arc::new(InactiveLokiClient { reason: "LOKI_URL not set" }),
    };

    let mut layers: Vec<Box<dyn layer::Layer>> = vec![];

    for &name in LAYER_ORDER {
        if cli.skip.iter().any(|s| s.as_str() == name) {
            layers.push(layer::SkippedLayer::new(name));
            continue;
        }
        match name {
            "cluster" => {
                match k8s_client.as_ref() {
                    Some(k8s) => layers.push(Box::new(layers::cluster::ClusterLayer::new(k8s.clone()))),
                    None => layers.push(layer::UnconfiguredLayer::new(
                        "cluster",
                        "kubeconfig not found; set --context or cluster.context in config",
                    )),
                }
            }
            "logs" => {
                match (k8s_client.as_ref(), loki_url.is_some()) {
                    (Some(k8s), _) => {
                        layers.push(Box::new(layers::logs::LogsLayer::new(
                            loki_client.clone(),
                            k8s.clone(),
                        )));
                    }
                    (None, true) => {
                        layers.push(Box::new(layers::logs::LogsLayer::new(
                            loki_client.clone(),
                            Arc::new(InactiveK8sClient { reason: "kubeconfig not found" }),
                        )));
                    }
                    (None, false) => {
                        layers.push(layer::UnconfiguredLayer::new(
                            "logs", "set LOKI_URL or ensure kubeconfig is accessible",
                        ));
                    }
                }
            }
            "workflows" => {
                layers.push(Box::new(layers::workflows::WorkflowsLayer::new(
                    Arc::new(temporal::RealTemporalClient::new(
                        config.temporal.address.clone(),
                        config.temporal.namespace.clone(),
                    )),
                    config.temporal.stuck_threshold,
                )));
            }
            "health" => {
                let endpoints_str = std::env::var("NICO_HEALTH_ENDPOINTS").ok();
                match endpoints_str.as_deref() {
                    Some(s) if !s.is_empty() => {
                        let services: Vec<http::ServiceEndpoint> = s.split(',')
                            .map(|entry| entry.trim())
                            .filter(|entry| !entry.is_empty())
                            .map(|entry| {
                                if let Some((name, url)) = entry.split_once('=') {
                                    http::ServiceEndpoint {
                                        name: name.trim().to_string(),
                                        base_url: url.trim().to_string(),
                                    }
                                } else {
                                    http::ServiceEndpoint {
                                        name: entry.to_string(),
                                        base_url: entry.to_string(),
                                    }
                                }
                            })
                            .collect();
                        layers.push(Box::new(layers::health::HealthLayer::new(
                            Arc::new(http::ReqwestHttpClient::new()),
                            services,
                        )));
                    }
                    _ => layers.push(layer::UnconfiguredLayer::new(
                        "health", "set NICO_HEALTH_ENDPOINTS=name=http://host:port to enable",
                    )),
                }
            }
            "grpc" => {
                // Prefer explicit NICO_GRPC_ADDRESS; fall back to temporal address from Config.
                let grpc_addr = std::env::var("NICO_GRPC_ADDRESS")
                    .unwrap_or_else(|_| config.temporal.address.clone());
                layers.push(Box::new(layers::grpc::GrpcLayer::new(
                    Arc::new(grpc::TonicGrpcInspector),
                    grpc_addr,
                )));
            }
            "postgres" => {
                match postgres::SqlxPostgresClient::new(&config.postgres.url) {
                    Ok(pg) => layers.push(Box::new(layers::postgres::PostgresLayer::new(Arc::new(pg)))),
                    Err(e) => {
                        eprintln!("warning: postgres URL invalid: {e}");
                        eprintln!("  hint: set postgres.url in ~/.config/nico-tools/config.toml or use --postgres-url");
                        layers.push(Box::new(layers::postgres::PostgresLayer::new(
                            Arc::new(InactivePostgresClient { reason: "invalid postgres URL" }),
                        )));
                    }
                }
            }
            _ => {}
        }
    }

    let report = runner::run(&layers, &opts).await;

    if matches!(config.output.format, OutputFormat::Json) {
        println!("{}", formatter::format_json(&report, &config.cluster.namespace));
    } else {
        print!("{}", formatter::format_report(&report, &mode, cli.verbose));
    }

    process::exit(exit_code(&report));
}
