use async_trait::async_trait;
use anyhow::Result;

#[derive(serde::Serialize)]
pub struct PoolStats {
    pub active: i64,
    pub max_conn: i64,
}

#[derive(serde::Serialize)]
pub struct LockWait {
    pub waiting_pid: i32,
    pub blocking_pid: Option<i32>,
    pub relation: Option<String>,
    pub wait_secs: f64,
}

#[async_trait]
pub trait PostgresClient: Send + Sync {
    async fn pool_stats(&self) -> Result<PoolStats>;
    async fn lock_waits(&self) -> Result<Vec<LockWait>>;
}

pub struct SqlxPostgresClient {
    pool: sqlx::PgPool,
}

impl SqlxPostgresClient {
    pub fn new(url: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect_lazy(url)
            .map_err(|e| anyhow::anyhow!("invalid postgres URL: {e}"))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl PostgresClient for SqlxPostgresClient {
    async fn pool_stats(&self) -> Result<PoolStats> {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT \
             (SELECT count(*) FROM pg_stat_activity WHERE state != 'idle')::bigint AS active, \
             (SELECT setting::bigint FROM pg_settings WHERE name = 'max_connections') AS max_conn",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("pool_stats query failed: {e}"))?;
        Ok(PoolStats { active: row.0, max_conn: row.1 })
    }

    async fn lock_waits(&self) -> Result<Vec<LockWait>> {
        let rows: Vec<(i32, Option<i32>, Option<String>, f64)> = sqlx::query_as(
            "SELECT \
             waiting.pid AS waiting_pid, \
             blocking.pid AS blocking_pid, \
             pgc.relname AS relation, \
             EXTRACT(EPOCH FROM (now() - waiting.query_start))::float8 AS wait_secs \
             FROM pg_stat_activity waiting \
             JOIN pg_locks wl ON wl.pid = waiting.pid AND NOT wl.granted \
             LEFT JOIN pg_locks bl ON bl.relation = wl.relation AND bl.granted \
             LEFT JOIN pg_stat_activity blocking ON blocking.pid = bl.pid \
             LEFT JOIN pg_class pgc ON pgc.oid = wl.relation \
             WHERE waiting.state = 'active'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("lock_waits query failed: {e}"))?;
        Ok(rows
            .into_iter()
            .map(|(waiting_pid, blocking_pid, relation, wait_secs)| LockWait {
                waiting_pid,
                blocking_pid,
                relation,
                wait_secs,
            })
            .collect())
    }
}
