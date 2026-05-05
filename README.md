# nico-tools

Diagnostic CLI tooling for NICO/carbide/NCX installations. Read-only — never modifies cluster state.

## Crates

| Crate | Binary | Purpose |
|-------|--------|---------|
| `nico-doctor` | `nico-doctor` | Six-layer cluster health check |
| `nico-correlate` | `nico-correlate` | Cross-source event correlation for a single entity |
| `nico-common` | — | Shared types |

## nico-correlate

Aggregates events and current state from every available source (Temporal, Postgres, k8s, Loki, Redfish) into a unified timeline for a given entity ID. Each source is independently optional — if one is unreachable the rest still run.

### Build

```bash
cargo build --release -p nico-correlate
# Binary at: target/release/nico-correlate
```

### Quick start

```bash
# Host entity
NICO_POSTGRES_URL=postgres://nico:secret@localhost:5432/nico nico-correlate host-r12u5

# DPU entity (resolves via hosts.dpu_id)
NICO_POSTGRES_URL=postgres://nico:secret@localhost:5432/nico nico-correlate dpu-bf3-r12u5

# Postgres only
NICO_POSTGRES_URL=... nico-correlate --sources postgres host-r12u5

# JSON output for scripting
NICO_POSTGRES_URL=... nico-correlate --json host-r12u5
```

---

## Using nico-correlate against a NICO/carbide Postgres database

### Constructing NICO_POSTGRES_URL

`NICO_POSTGRES_URL` is a standard libpq connection string. The database runs inside the carbide cluster; use `kubectl port-forward` to reach it from your workstation.

**Step 1 — find the Postgres credentials secret**

```bash
kubectl get secret -n nico-system -l app.kubernetes.io/component=postgresql -o name
# e.g. secret/nico-postgresql
```

**Step 2 — extract the DSN fields**

```bash
kubectl get secret nico-postgresql -n nico-system -o jsonpath='{.data.postgres-password}' | base64 -d
# prints the password; default user is usually "nico" and database "nico"
```

Alternatively, if the secret stores a full DSN key:

```bash
kubectl get secret nico-postgresql -n nico-system -o jsonpath='{.data.database-url}' | base64 -d
```

**Step 3 — port-forward Postgres, then run the tool**

```bash
# Terminal 1: forward the Postgres port
kubectl port-forward -n nico-system svc/nico-postgresql 5432:5432

# Terminal 2: run nico-correlate against it
NICO_POSTGRES_URL="postgres://nico:<password>@localhost:5432/nico" \
  nico-correlate host-r12u5
```

One-liner (background port-forward, run tool, then clean up):

```bash
kubectl port-forward -n nico-system svc/nico-postgresql 5432:5432 &
PF_PID=$!
NICO_POSTGRES_URL="postgres://nico:<password>@localhost:5432/nico" \
  nico-correlate host-r12u5
kill $PF_PID
```

> `NICO_POSTGRES_URL` is the only required input. No config file is needed.

---

### Typical operator queries

| Goal | Command |
|------|---------|
| Look up a host by ID | `NICO_POSTGRES_URL=... nico-correlate host-r12u5` |
| Look up a DPU (resolves via `hosts.dpu_id`) | `NICO_POSTGRES_URL=... nico-correlate dpu-bf3-r12u5` |
| Scope to Postgres only | `NICO_POSTGRES_URL=... nico-correlate --sources postgres host-r12u5` |
| JSON output for scripting | `NICO_POSTGRES_URL=... nico-correlate --json host-r12u5` |
| Workflow correlation | `NICO_POSTGRES_URL=... nico-correlate <workflow-id>` |
| Limit look-back window | `NICO_POSTGRES_URL=... nico-correlate --since 30m host-r12u5` |

---

### Tables queried per entity type

| Entity type | ID prefix example | Tables queried |
|-------------|------------------|---------------|
| Host | `host-r12u5` | `hosts` (WHERE `id = ?`), `audit_log` |
| DPU | `dpu-bf3-r12u5` | `hosts` (WHERE `dpu_id = ?`), `audit_log` |
| Workflow | `<uuid>` or `hp-*` | `workflows` (WHERE `id = ?`), `audit_log` |
| Request | `req-*` | `audit_log` only |

The `audit_log` table is queried for all entity types using `entity_id = ?`, returning the 100 most recent events ordered by `ts DESC`.

---

### Expected output shape

**Human-readable (default):**

```
detected type: host
Timeline (3 events):
  14:02:11  postgres  create_host
  14:03:45  postgres  provision_start
  14:08:22  postgres  provision_complete

Postgres state (current):
  hosts.id: host-r12u5
  hosts.status: ready
  hosts.dpu_id: dpu-bf3-r12u5
  hosts.created_at: 2026-05-04T14:02:11Z
[source unavailable: temporal]
[source unavailable: loki]
```

**JSON (`--json`):**

```json
{
  "version": 1,
  "id": "host-r12u5",
  "id_type": "host",
  "events": [
    {
      "ts": "2026-05-04T14:02:11Z",
      "source": "postgres",
      "kind": "create_host",
      "severity": "info"
    }
  ],
  "sources_unavailable": ["temporal", "loki"],
  "state": [
    { "source": "postgres", "key": "hosts.id",     "value": "host-r12u5" },
    { "source": "postgres", "key": "hosts.status", "value": "ready" }
  ]
}
```

---

### When the database is unreachable

If `NICO_POSTGRES_URL` is not set or the connection fails, the Postgres source reports itself unavailable and the tool continues with the remaining sources. The output line looks like:

```
[source unavailable: postgres]
```

No crash. Exit code is 1 (partial data) rather than 2 (no data) if at least one other source returned results.

To confirm connectivity before running:

```bash
psql "$NICO_POSTGRES_URL" -c "SELECT 1"
```
