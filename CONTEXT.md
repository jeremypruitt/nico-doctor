# nico-doctor — Domain Context

## Purpose
Diagnostic CLI for nico/carbide/ncx installations. Read-only. Compresses the
operator diagnostic ladder into seconds. Never hides the underlying tools —
every output line points at where to dig deeper.

## Ubiquitous language

- **NICo** — NVIDIA Infrastructure Controller. gRPC service. Source of truth
  for desired host state. This is the external/marketing name for carbide.
- **Carbide** — This is the internal name for NICO
  packages NICo + dependencies into a deployable form.
- **NCX** — Umbrella term for the NICO/carbide components
- **Host** — physical machine managed by NICo. Has a BMC and (usually) a DPU.
- **DPU** — data processing unit attached to a host.
- **Workflow** — Temporal workflow orchestrating host lifecycle (e.g.
  HostProvisioning).
- **Activity** — atomic step inside a workflow, retried independently.
- **Site Agent** — the in-cluster worker that executes activities.
- **Layer** (nico-doctor specific) — one of the six categories the doctor
  checks: cluster, logs, workflows, health, grpc, postgres.
- **Finding** — a single warning or failure produced by a layer.

## What "diagnostic" means here
Read-only. No remediation. Output is human-readable by default and JSON under
`--json`. Exit codes are 0 / 1 / 2 / 3 (see ADR-001).

## Operator's diagnostic ladder
1. Is the cluster healthy?     → `cluster` layer
2. Are pods logging errors?    → `logs` layer
3. Are workflows stuck?        → `workflows` layer
4. Are services healthy?       → `health` layer
5. Is gRPC reachable?          → `grpc` layer
6. Is Postgres pressured?      → `postgres` layer

## Out of scope (explicit)
- Remediation actions
- Persistent state (no SQLite, no daemons)
- Web UI / TUI
- Alerting (Datadog already does that)
- Self-update or telemetry
