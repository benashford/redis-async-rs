/*
 * Single-threaded benchmarks comparing redis-async vs redis crate
 *
 * Tests:
 * 1. Batch operations - fire-and-forget style SET operations
 * 2. Individual operations - sequential dependent operations (INCR -> SET chain)
 */

mod common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_util::future;
use redis::AsyncCommands;
use redis_async::client::PairedConnection;
use redis_async::resp_array;

use common::{
    cleanup_keys, create_redis_async_connection, create_redis_connection, make_key, make_value,
    unique_prefix, NUM_OPERATIONS,
};

/// Benchmark batch SET operations with redis-async
async fn redis_async_batch_set(conn: &PairedConnection, prefix: &str) {
    // Send all SET commands without waiting for each response
    let futures: Vec<_> = (0..NUM_OPERATIONS)
        .map(|i| {
            let key = make_key(prefix, i);
            let value = make_value(i);
            conn.send::<String>(resp_array!["SET", key, value])
        })
        .collect();

    // Wait for all to complete
    let _results = future::join_all(futures).await;
}

/// Benchmark batch SET operations with redis crate
async fn redis_batch_set(mut conn: redis::aio::MultiplexedConnection, prefix: &str) {
    // Send all SET commands
    for i in 0..NUM_OPERATIONS {
        let key = make_key(prefix, i);
        let value = make_value(i);
        let _: () = conn.set(&key, &value).await.expect("SET failed");
    }
}

/// Benchmark individual dependent operations with redis-async
/// Each INCR result is used to create the next key
async fn redis_async_individual_dependent(conn: &PairedConnection, prefix: &str) {
    let counter_key = format!("{}_ctr", prefix);

    for i in 0..NUM_OPERATIONS {
        // INCR and wait for result
        let ctr: i64 = conn
            .send(resp_array!["INCR", &counter_key])
            .await
            .expect("INCR failed");

        // Use the counter to create a dependent key
        let key = format!("{}:dep_{}", prefix, ctr);
        let value = make_value(i);
        let _: String = conn
            .send(resp_array!["SET", &key, &value])
            .await
            .expect("SET failed");
    }
}

/// Benchmark individual dependent operations with redis crate
async fn redis_individual_dependent(mut conn: redis::aio::MultiplexedConnection, prefix: &str) {
    let counter_key = format!("{}_ctr", prefix);

    for i in 0..NUM_OPERATIONS {
        // INCR and wait for result
        let ctr: i64 = conn.incr(&counter_key, 1).await.expect("INCR failed");

        // Use the counter to create a dependent key
        let key = format!("{}:dep_{}", prefix, ctr);
        let value = make_value(i);
        let _: () = conn.set(&key, &value).await.expect("SET failed");
    }
}

fn bench_batch_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("single_threaded_batch");

    // Create connections once, outside the iteration loop
    let redis_async_conn = rt
        .block_on(create_redis_async_connection())
        .expect("Failed to connect redis-async");
    let redis_conn = rt
        .block_on(create_redis_connection())
        .expect("Failed to connect redis");

    group.bench_function(BenchmarkId::new("redis-async", NUM_OPERATIONS), |b| {
        b.to_async(&rt).iter(|| async {
            let prefix = unique_prefix();
            redis_async_batch_set(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| {
            let conn = conn.clone();
            async move {
                let prefix = unique_prefix();
                redis_batch_set(conn, &prefix).await;
                cleanup_keys(&prefix).await;
            }
        });
    });

    group.finish();
}

fn bench_individual_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("single_threaded_individual");

    // Create connections once, outside the iteration loop
    let redis_async_conn = rt
        .block_on(create_redis_async_connection())
        .expect("Failed to connect redis-async");
    let redis_conn = rt
        .block_on(create_redis_connection())
        .expect("Failed to connect redis");

    group.bench_function(BenchmarkId::new("redis-async", NUM_OPERATIONS), |b| {
        b.to_async(&rt).iter(|| async {
            let prefix = unique_prefix();
            redis_async_individual_dependent(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| {
            let conn = conn.clone();
            async move {
                let prefix = unique_prefix();
                redis_individual_dependent(conn, &prefix).await;
                cleanup_keys(&prefix).await;
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_batch_operations, bench_individual_operations);
criterion_main!(benches);
