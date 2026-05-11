#!/usr/bin/env bash
# capture-fixtures.sh — refresh PRD-005 perf seed rows from a live cluster.
#
# Performance benches (Slice 0a.2+) multiply these single canonical rows
# by N at startup, so we never check in fleet-scale fixtures. Re-run this
# whenever any of the upstream schemas drift.
#
# Output: one JSON row per source under
#   crates/nico-doctor/tests/fixtures/perf/
#
# Required env (load from .env.local — same file used by smoke.sh):
#   POSTGRES_URL  — psql DSN with read access to forgedb (machines, instances)
#   KUBECONFIG    — kubeconfig for the target cluster
#   LOKI_URL      — base URL for the cluster's Loki gateway (e.g. http://loki:3100)
#   TEMPORAL_ADDR — host:port for the temporal-frontend gRPC service
#
# Required tools: psql, kubectl, jq, curl, temporal (CLI v1.x)
#
# Usage:
#   ./scripts/capture-fixtures.sh [--env <path>]   # defaults to .env.local

set -euo pipefail

ENV_FILE=".env.local"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env) ENV_FILE="$2"; shift 2 ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) echo "usage: $0 [--env <path>]" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$ENV_FILE" ]]; then
    echo "error: env file not found: $ENV_FILE" >&2
    echo "  hint: cp .env.example .env.local  then fill in your values" >&2
    exit 1
fi

# shellcheck source=/dev/null
source "$ENV_FILE"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${REPO_ROOT}/crates/nico-doctor/tests/fixtures/perf"
mkdir -p "$OUT_DIR"

require() {
    command -v "$1" >/dev/null 2>&1 || { echo "error: missing required tool: $1" >&2; exit 1; }
}
require psql
require kubectl
require jq
require curl

# ── 1. machines row (postgres) ─────────────────────────────────────────────
echo "→ capturing machine.json from forgedb.machines"
psql "$POSTGRES_URL" -At -c "
    SELECT json_build_object(
        'id',                          id,
        'network_config_version',      network_config_version,
        'network_config',              network_config,
        'network_status_observation',  network_status_observation,
        'dpu_agent_health_report',     dpu_agent_health_report
    )
    FROM machines
    WHERE network_status_observation IS NOT NULL
    LIMIT 1
" | jq '.' > "${OUT_DIR}/machine.json"

# ── 2. instances row (postgres) ────────────────────────────────────────────
echo "→ capturing instance.json from forgedb.instances"
psql "$POSTGRES_URL" -At -c "
    SELECT json_build_object(
        'machine_id',              machine_id,
        'network_config_version',  network_config_version
    )
    FROM instances
    LIMIT 1
" | jq '.' > "${OUT_DIR}/instance.json"

# ── 3. pod listing (kubectl) ───────────────────────────────────────────────
echo "→ capturing pod.json from infra-controller pods"
kubectl get pods -n infra-controller -l app=dpu-agent -o json \
    | jq '.items[0]' > "${OUT_DIR}/pod.json"

# ── 4. temporal event (temporal CLI) ──────────────────────────────────────
echo "→ capturing temporal_event.json (first event of a recent workflow)"
require temporal
WORKFLOW_ID="$(
    temporal --address "$TEMPORAL_ADDR" workflow list \
        --query 'WorkflowType="DpuProvisionWorkflow"' --limit 1 \
        --output json | jq -r '.[0].execution.workflowId'
)"
temporal --address "$TEMPORAL_ADDR" workflow show \
    --workflow-id "$WORKFLOW_ID" --output json \
    | jq '.events[0]' > "${OUT_DIR}/temporal_event.json"

# ── 5. loki line (loki query) ─────────────────────────────────────────────
echo "→ capturing loki_line.json (single recent dpu-agent log line)"
LOKI_RAW="$(
    curl -fsS --get "${LOKI_URL%/}/loki/api/v1/query_range" \
        --data-urlencode 'query={app="dpu-agent"}' \
        --data-urlencode 'limit=1'
)"
echo "$LOKI_RAW" | jq '
    .data.result[0] as $r
    | {
        timestamp: ($r.values[0][0] | tonumber / 1e9 | todateiso8601),
        labels: $r.stream,
        line: $r.values[0][1]
    }
' > "${OUT_DIR}/loki_line.json"

echo "✓ wrote 5 seed fixtures to ${OUT_DIR}"
ls -lh "${OUT_DIR}"
