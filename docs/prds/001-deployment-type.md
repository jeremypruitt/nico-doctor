# PRD-001 — `nico --deployment-type`: capability-based detection

- **Status:** Specced (2026-05-09); awaiting `/to-issues` breakdown.
- **Epic:** #245 (carries `prd-001` label; tracks slice progress).
- **Touches:** ADR-0013 (boot probe, to be amended).
- **Deferred follow-up:** #242 (capabilities object in JSON).

## Problem

`nico` (ops, doctor) hard-fails (exit 3) when the configured controller namespace doesn't exist on the active cluster. NICo has at least three documented dev shapes — full (core+rest), core-only (carbide-kind), and the rest repo's documented quick-setup (`kind-nico-rest-local`, with mock-core stand-in for the real gRPC core). The shapes use *different* controller namespaces (`forge-system` vs `nico-rest`), different gRPC services (`carbide-api:1079` vs `nico-rest-mock-core:11079`), and different postgres schemas. The tool has zero awareness of which shape is in front of it, so the rest contributor's documented quick-setup path errors out at boot.

## Personas

- **Rest-repo contributor** following the documented quick-setup. Primary unblock target.
- **Core / full-stack operator** running against a co-located full or core-only kind cluster. Behavior must remain identical to today (no regressions).

## Goals

- `nico ops` and `nico doctor` correctly classify the active cluster as one of three deployment-types and behave appropriately for each.
- Auto-detect by default; explicit override available; safe escape hatch when detection is wrong or absent.
- Read-only; no remediation; no new external dependencies.

## Non-goals

- Multi-cluster / cross-cluster correlation.
- Bringing up clusters (kind setup, helm install, etc.).
- A signature-catalog DSL or external sig file. Detection rules stay hardcoded.
- A `capabilities` object in JSON output (deferred — see #242).

## High-level design

### Three deployment-types (hardcoded labels)

| Type             | Controller ns | gRPC address                          | forgedb |
|------------------|---------------|---------------------------------------|---------|
| `full`           | `forge-system`| `carbide-api.forge-system:1079`       | yes     |
| `core-only`      | `forge-system`| `carbide-api.forge-system:1079`       | yes     |
| `rest-only-mock` | `nico-rest`   | `nico-rest-mock-core.nico-rest:11079` | no      |

### Detection (capability-based; signals 2 + 3 + 4)

Architecture: detection produces a capability bundle; layers gate on capabilities, not on the type-name label. Type label is a human-friendly summary of the bundle. **Capability vocabulary is deliberately not finalized here** (see #242 + auto-memory `project_deployment_type_capability_vocab.md`); a follow-up grilling session must settle the names before any sub-issue touches capability internals.

Signal ladder (first match wins):

1. **Signature workload probe** — `Service nico-rest-mock-core@nico-rest` definitively → `rest-only-mock`. `Service carbide-api@forge-system` + `nico-rest-api@nico-rest` → `full`. `Service carbide-api@forge-system`, no `nico-rest` ns → `core-only`.
2. **Namespace inventory** — fallback when (1) is inconclusive. Combination of `forge-system` / `nico-rest` presence/absence.
3. **CRD inventory** — fallback when (1) and (2) are inconclusive. `sites.nico.nvidia.io` present → rest deployed; core CRDs present → core deployed.

If all three signals fail to match a known type → exit 3 with diagnostic data (observed namespaces, observed services). Recovery: pass `--deployment-type` explicitly or use `--deployment-type=force`.

### Hybrid trust model

- `--deployment-type=<full|core-only|rest-only-mock>` → trust it, skip detection.
- `--deployment-type=force` → trust nothing, skip detection, run with raw config; banner shows `deployment-type: force (no enforcement)`.
- `[cluster] deployment_type = "..."` in `config.toml` or `NICO_DEPLOYMENT_TYPE` env → trust it, skip detection.
- Otherwise → run the detection ladder above.

### Per-layer behavior

| Layer       | full / core-only                      | rest-only-mock                                  |
|-------------|---------------------------------------|-------------------------------------------------|
| `cluster`   | runs                                  | runs                                            |
| `logs`      | runs                                  | runs                                            |
| `workflows` | runs                                  | runs (Temporal real)                            |
| `health`    | runs (per-layer endpoint detail TBD)  | runs (per-layer endpoint detail TBD)            |
| `grpc`      | dials `carbide-api:1079`              | dials `nico-rest-mock-core:11079`               |
| `postgres`  | runs                                  | runs                                            |
| `dpu`       | runs                                  | **n/a — no forgedb**                            |

`dpu`-in-`rest-only-mock` is the only layer that "skips" by deployment-type. All other type-dependent variation is address re-pointing via the capability bundle.

### Status semantics for "n/a in this deployment-type"

Extend `LayerOutcome::Skipped { reason: Option<String> }`. Status priority is unchanged (`Fail > Warn > Unknown > Ok`; `Skipped` sits independently). Formatter renders the reason when present (`. dpu (skipped — n/a in rest-only-mock: no forgedb)`). JSON gains `skipped_reason` field on layer entries.

`Status::Unknown` (the existing `UnconfiguredLayer` path) is *not* reused — that's a soft-fail meaning "your config is broken"; n/a-by-design must not look like a fail.

## UX

### Boot banner

```
  ◐ booting nico  ·  reach: port-forward (auto-detected)  ·  type: rest-only-mock (auto)

    connecting
      ✓  load kubeconfig
      ✓  reach API server

    validating
      ✓  credentials
      ✓  detect deployment-type: rest-only-mock              ← NEW step
      ✓  namespace 'nico-rest' exists                         ← capability-resolved
      ✓  list-pods permission

    serving
      ✓  port-forward: workflows
      ✓  port-forward: grpc → nico-rest-mock-core:11079       ← resolved addr shown
      ✓  port-forward: postgres
      ✓  reach postgres
```

Source tag values for the top-line indicator: `auto | flag | config | force`.

### Config precedence

Capability bundle slots in as a new defaults layer:

```
hardcoded defaults < deployment-type capability bundle < file < env < CLI
```

When a per-key file/env/CLI override contradicts the active deployment-type's bundle (e.g., `cluster.namespace=forge-system` with `deployment-type=rest-only-mock`), emit a one-line warning at boot. `--deployment-type=force` silences this warning.

### JSON output additions

- New top-level `deployment_type: { name: "...", source: "auto|flag|config|force" }`.
- New `skipped_reason: "..."` field on layer entries when `Skipped` carries a reason.
- *Not* shipping `capabilities` object — deferred to #242.

## ADR work

Amend ADR-0013 (boot probe) to document the new `detect_deployment_type` step in the `validating` section, its placement (after `credentials`, before `namespace_exists`, because the latter needs the resolved namespace), and the failure semantics (timeout vs no-match-with-diagnostic-data).

## Domain language to add (CONTEXT.md)

`Deployment-type` and `Force mode` are already part of the ubiquitous-language section (added when this PRD was specced).

## Open question deliberately deferred

Capability vocabulary (`controller_namespace`, `grpc_address`, `forgedb_present`, `mock_core` were sketched but not finalized). Must be re-grilled in a follow-up session before any sub-issue that touches capability internals is implemented. See auto-memory: `project_deployment_type_capability_vocab.md`.

## Implementation tracking

The slice breakdown lives in epic #245 as a tasklist. Sub-issues are created via `/to-issues` against the epic, all carrying `prd-001` label and `Parent: #245` per the conventions in `docs/agents/issue-tracker.md`.
