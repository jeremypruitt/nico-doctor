//! Counting decorators around `nico-doctor`'s external-I/O client traits
//! (PRD-005 Slice 0b.1). Companion of `nico_common::perf` — same
//! wrapper-struct pattern, same `MethodCounter` substrate. Lives in
//! `nico-doctor` because the `LokiClient` and `PostgresClient` traits
//! are local to this crate.

use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use nico_common::perf::{MethodCounter, MethodStats};

use crate::loki::{LokiClient, LokiQueryResult};
use crate::postgres::{LockWait, PoolStats, PostgresClient};

/// Approximate bytes-out for any Postgres query. The decorator does not
/// see the SQL string (`pool_stats` / `lock_waits` build it internally),
/// so a small constant token is captured — non-zero per PRD-005 §0b.
const PG_QUERY_BYTES_APPROX: u64 = 256;

#[derive(Debug, Clone, Default)]
pub struct LokiStats {
    pub query_errors: MethodStats,
}

pub struct CountingLokiClient<T: LokiClient> {
    inner: T,
    query_errors: MethodCounter,
}

impl<T: LokiClient> CountingLokiClient<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            query_errors: MethodCounter::default(),
        }
    }

    pub fn stats(&self) -> LokiStats {
        LokiStats {
            query_errors: self.query_errors.snapshot(),
        }
    }
}

#[async_trait]
impl<T: LokiClient> LokiClient for CountingLokiClient<T> {
    async fn query_errors(
        &self,
        namespace: &str,
        since: Duration,
        limit: usize,
    ) -> Result<LokiQueryResult> {
        let bytes_out =
            namespace.len() as u64 + size_of::<u64>() as u64 + size_of::<u64>() as u64;
        let start = Instant::now();
        let result = self.inner.query_errors(namespace, since, limit).await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|r| serde_json::to_vec(r).map(|v| v.len() as u64).unwrap_or(0))
            .unwrap_or(0);
        self.query_errors.record(elapsed, bytes_in, bytes_out);
        result
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostgresStats {
    pub pool_stats: MethodStats,
    pub lock_waits: MethodStats,
}

pub struct CountingPostgresClient<T: PostgresClient> {
    inner: T,
    pool_stats: MethodCounter,
    lock_waits: MethodCounter,
}

impl<T: PostgresClient> CountingPostgresClient<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            pool_stats: MethodCounter::default(),
            lock_waits: MethodCounter::default(),
        }
    }

    pub fn stats(&self) -> PostgresStats {
        PostgresStats {
            pool_stats: self.pool_stats.snapshot(),
            lock_waits: self.lock_waits.snapshot(),
        }
    }
}

#[async_trait]
impl<T: PostgresClient> PostgresClient for CountingPostgresClient<T> {
    async fn pool_stats(&self) -> Result<PoolStats> {
        let start = Instant::now();
        let result = self.inner.pool_stats().await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|s| serde_json::to_vec(s).map(|v| v.len() as u64).unwrap_or(0))
            .unwrap_or(0);
        self.pool_stats
            .record(elapsed, bytes_in, PG_QUERY_BYTES_APPROX);
        result
    }

    async fn lock_waits(&self) -> Result<Vec<LockWait>> {
        let start = Instant::now();
        let result = self.inner.lock_waits().await;
        let elapsed = start.elapsed();
        let bytes_in = result
            .as_ref()
            .map(|w| serde_json::to_vec(w).map(|v| v.len() as u64).unwrap_or(0))
            .unwrap_or(0);
        self.lock_waits
            .record(elapsed, bytes_in, PG_QUERY_BYTES_APPROX);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loki::LokiLine;
    use crate::loki::testing::MockLokiClient;
    use std::sync::Mutex;

    type LockWaitSeed = (i32, Option<i32>, Option<String>, f64);

    struct MockPostgresClient {
        pool: std::result::Result<(i64, i64), String>,
        waits: Mutex<Vec<LockWaitSeed>>,
    }

    #[async_trait]
    impl PostgresClient for MockPostgresClient {
        async fn pool_stats(&self) -> Result<PoolStats> {
            match &self.pool {
                Ok((a, m)) => Ok(PoolStats {
                    active: *a,
                    max_conn: *m,
                }),
                Err(e) => Err(anyhow::anyhow!("{e}")),
            }
        }

        async fn lock_waits(&self) -> Result<Vec<LockWait>> {
            Ok(self
                .waits
                .lock()
                .unwrap()
                .iter()
                .map(|(w, b, r, s)| LockWait {
                    waiting_pid: *w,
                    blocking_pid: *b,
                    relation: r.clone(),
                    wait_secs: *s,
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn loki_query_errors_counts_calls() {
        let mock = MockLokiClient::new().with_lines(vec![]);
        let client = CountingLokiClient::new(mock);

        client.query_errors("nico", Duration::from_secs(60), 100).await.unwrap();
        client.query_errors("nico", Duration::from_secs(60), 100).await.unwrap();

        assert_eq!(client.stats().query_errors.call_count, 2);
    }

    #[tokio::test]
    async fn loki_bytes_in_matches_serialized_payload() {
        let rows = vec![
            ("core-abc".to_string(), "ERROR: boom".to_string()),
            ("core-def".to_string(), "panic: stack overflow".to_string()),
        ];
        let expected = serde_json::to_vec(&LokiQueryResult::Lines(
            rows.iter()
                .map(|(p, t)| LokiLine {
                    pod: p.clone(),
                    text: t.clone(),
                })
                .collect(),
        ))
        .unwrap()
        .len() as u64;

        let mock = MockLokiClient::new().with_lines(rows);
        let client = CountingLokiClient::new(mock);

        client.query_errors("nico", Duration::from_secs(60), 100).await.unwrap();

        assert_eq!(client.stats().query_errors.bytes_in, expected);
    }

    #[tokio::test]
    async fn loki_unreachable_still_counts_and_records_bytes() {
        let mock = MockLokiClient::new();
        let client = CountingLokiClient::new(mock);

        client.query_errors("nico", Duration::from_secs(60), 100).await.unwrap();

        let expected = serde_json::to_vec(&LokiQueryResult::Unreachable).unwrap().len() as u64;
        let stats = client.stats().query_errors;
        assert_eq!(stats.call_count, 1);
        assert_eq!(stats.bytes_in, expected);
    }

    #[tokio::test]
    async fn postgres_pool_stats_counts_and_records_bytes() {
        let mock = MockPostgresClient {
            pool: Ok((42, 100)),
            waits: Mutex::new(vec![]),
        };
        let client = CountingPostgresClient::new(mock);

        let stats = client.pool_stats().await.unwrap();
        assert_eq!(stats.active, 42);

        let expected_bytes = serde_json::to_vec(&PoolStats {
            active: 42,
            max_conn: 100,
        })
        .unwrap()
        .len() as u64;

        let snap = client.stats().pool_stats;
        assert_eq!(snap.call_count, 1);
        assert_eq!(snap.bytes_in, expected_bytes);
        assert_eq!(snap.bytes_out, PG_QUERY_BYTES_APPROX);
    }

    #[tokio::test]
    async fn postgres_lock_waits_counts_independently_of_pool_stats() {
        let mock = MockPostgresClient {
            pool: Ok((1, 10)),
            waits: Mutex::new(vec![(42, Some(13), Some("machines".into()), 0.5)]),
        };
        let client = CountingPostgresClient::new(mock);

        client.lock_waits().await.unwrap();
        client.lock_waits().await.unwrap();
        client.pool_stats().await.unwrap();

        let stats = client.stats();
        assert_eq!(stats.pool_stats.call_count, 1);
        assert_eq!(stats.lock_waits.call_count, 2);
    }

    #[tokio::test]
    async fn postgres_errored_call_counts_with_zero_bytes_in() {
        let mock = MockPostgresClient {
            pool: Err("connection refused".into()),
            waits: Mutex::new(vec![]),
        };
        let client = CountingPostgresClient::new(mock);

        let result = client.pool_stats().await;
        assert!(result.is_err());

        let stats = client.stats().pool_stats;
        assert_eq!(stats.call_count, 1);
        assert_eq!(stats.bytes_in, 0);
    }
}
