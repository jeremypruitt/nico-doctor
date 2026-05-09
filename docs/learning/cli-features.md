# CLI Feature Candidates

Surfaced from the learning plan. **Candidates, not commitments.** Real specs land here as topics finish; the `nico` CLI's roadmap is its own project.

Rule of thumb (from MASTER.md):
- Same multi-step diagnostic run more than once → `nico doctor` check.
- Mentally joining two data sources → `nico correlate` rule.
- Eyeballing a sequence of states for health → `nico ops` panel.

## From topic 01-hbn (2026-05-07)

| Command | Kind | Seed source | Why |
|---|---|---|---|
| `nico doctor hbn <dpu-id>` | doctor | 01-hbn §3 (most rows) | Single-DPU verdict: HBN container running, version ≥ minimums, applied vs desired config version, BGP peer up, quarantine state, last-seen timestamp. |
| `nico doctor dpu-cert <dpu-id>` | doctor | 01-hbn §3 (cert-expiry row) | Days-to-expiry on dpu-agent client cert via last `DpuNetworkStatus`. Rolls into `nico doctor certs`. |
| `nico correlate hbn-config-drift <machine-id>` | correlate | 01-hbn §3 (stuck-state guidance) | Joins desired config (forgedb) ↔ last reported status; shows version drift age + relevant `health.rs` probes. |
| `nico ops hbn` | ops panel | 01-hbn §5 | Per-DPU table: HBN ver / NVUE ver / applied managed_host_ver / applied instance_ver / drift / quarantine / cert days. |
| `nico doctor dpu-isolation <machine-id>` | doctor | 01-hbn §3 (isolation row) | Distinguishes "not yet known" vs "deliberately quarantined" vs "lost connection." |

## Pre-existing sketches (from MASTER.md)

These predate the topic work; keeping them here so the file is the single backlog.

- `nico doctor pxe` — walk the PXE chain for a given host MAC, report exactly where it broke.
- `nico doctor certs` — list every cert in the trust mesh with days-to-expiry. (`nico doctor dpu-cert` rolls up into this.)
- `nico correlate host-allocation <id>` — full lifecycle of a host allocation across REST, core, site-agent, dpu-agent, Temporal logs, into one timeline.
- `nico ops fabric` — IB fabric health from UFM (once UFM access exists).
- `nico ops dpus` — DPU inventory: firmware, last-checkin, current tenant, HBN status. (Overlaps with `nico ops hbn`; merge candidate.)
