/*
 * Common utilities for benchmarking redis-async vs redis crates
 */

use redis_async::client as redis_async_client;
use std::sync::atomic::{AtomicUsize, Ordering};

// Benchmark configuration
pub const NUM_OPERATIONS: usize = 1000;
pub const NUM_CONCURRENT_TASKS: usize = 10;
pub const OPS_PER_TASK: usize = NUM_OPERATIONS / NUM_CONCURRENT_TASKS;

// Unique key prefix generator to avoid conflicts between benchmark runs
static KEY_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn unique_prefix() -> String {
    let count = KEY_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("bench_{}_{}", std::process::id(), count)
}

/// Create a redis-async PairedConnection
pub async fn create_redis_async_connection(
) -> Result<redis_async_client::PairedConnection, redis_async::error::Error> {
    redis_async_client::paired_connect("127.0.0.1", 6379).await
}

/// Create a redis crate MultiplexedConnection
pub async fn create_redis_connection(
) -> Result<redis::aio::MultiplexedConnection, redis::RedisError> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    client.get_multiplexed_async_connection().await
}

/// Cleanup keys with a given prefix (for after benchmarks)
pub async fn cleanup_keys(prefix: &str) {
    // Use redis crate for cleanup as it's simpler
    if let Ok(mut con) = create_redis_connection().await {
        let pattern = format!("{}*", prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut con)
            .await
            .unwrap_or_default();

        if !keys.is_empty() {
            let _: Result<(), _> = redis::cmd("DEL").arg(&keys).query_async(&mut con).await;
        }
    }
}

/// Generate a key name for benchmarks
pub fn make_key(prefix: &str, index: usize) -> String {
    format!("{}:{}", prefix, index)
}

/// Generate test value
pub fn make_value(index: usize) -> String {
    format!("value_{}", index)
}
