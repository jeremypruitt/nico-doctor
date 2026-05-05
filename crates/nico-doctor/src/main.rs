#![allow(dead_code)]
use std::process;
use std::sync::Arc;
use std::time::Duration;
use clap::Parser;
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
    #[arg(short, long, help = "Kubernetes namespace", default_value = "nico")]
    namespace: String,

    #[arg(long, help = "Kubernetes context [env: NICO_CONTEXT]")]
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

    #[arg(long, env = "NICO_POSTGRES_URL", help = "Postgres connection URL")]
    postgres_url: Option<String>,
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

    let mode = OutputMode {
        color: !cli.no_color && std::env::var("NO_COLOR").is_err(),
        ascii: cli.ascii,
    };

    let since = humantime::parse_duration(&cli.since).unwrap_or(Duration::from_secs(600));
    let timeout = humantime::parse_duration(&cli.timeout).unwrap_or(Duration::from_secs(5));

    let opts = RunOpts { namespace: cli.namespace.clone(), since, timeout };

    let mut layers: Vec<Box<dyn layer::Layer>> = vec![];

    for &name in LAYER_ORDER {
        if cli.skip.iter().any(|s| s.as_str() == name) {
            layers.push(layer::SkippedLayer::new(name));
            continue;
        }
        match name {
            "postgres" => {
                if let Some(ref url) = cli.postgres_url {
                    match postgres::SqlxPostgresClient::new(url) {
                        Ok(pg) => layers.push(Box::new(layers::postgres::PostgresLayer::new(Arc::new(pg)))),
                        Err(e) => eprintln!("warning: postgres layer disabled: {e}"),
                    }
                }
            }
            _ => {}
        }
    }

    let report = runner::run(&layers, &opts).await;

    if cli.json {
        println!("{}", formatter::format_json(&report, &cli.namespace));
    } else {
        print!("{}", formatter::format_report(&report, &mode, cli.verbose));
    }

    process::exit(exit_code(&report));
}
