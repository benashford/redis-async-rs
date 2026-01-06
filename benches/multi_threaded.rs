/*
 * Multi-threaded/concurrent benchmarks comparing redis-async vs redis crate
 *
 * Tests operations spread across multiple Tokio tasks:
 * 1. Batch operations - independent operations across concurrent tasks
 * 2. Individual operations - dependent operations within each task, parallel across tasks
 */

mod common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_util::future;
use redis::AsyncCommands;
use redis_async::client::PairedConnection;
use redis_async::resp_array;

use common::{
    cleanup_keys, create_redis_async_connection, create_redis_connection, make_key, make_value,
    unique_prefix, NUM_CONCURRENT_TASKS, NUM_OPERATIONS, OPS_PER_TASK,
};

/// Benchmark concurrent batch SET operations with redis-async
async fn redis_async_concurrent_batch(conn: &PairedConnection, prefix: &str) {
    // Spawn multiple tasks, each doing a portion of the work
    let tasks: Vec<_> = (0..NUM_CONCURRENT_TASKS)
        .map(|task_id| {
            let conn = conn.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                let futures: Vec<_> = (0..OPS_PER_TASK)
                    .map(|i| {
                        let key = make_key(&prefix, task_id * OPS_PER_TASK + i);
                        let value = make_value(i);
                        conn.send::<String>(resp_array!["SET", key, value])
                    })
                    .collect();
                future::join_all(futures).await
            })
        })
        .collect();

    // Wait for all tasks to complete
    for task in tasks {
        let _ = task.await;
    }
}

/// Benchmark concurrent batch SET operations with redis crate
async fn redis_concurrent_batch(conn: redis::aio::MultiplexedConnection, prefix: &str) {
    // Spawn multiple tasks, each doing a portion of the work
    let tasks: Vec<_> = (0..NUM_CONCURRENT_TASKS)
        .map(|task_id| {
            let mut conn = conn.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                for i in 0..OPS_PER_TASK {
                    let key = make_key(&prefix, task_id * OPS_PER_TASK + i);
                    let value = make_value(i);
                    let _: () = conn.set(&key, &value).await.expect("SET failed");
                }
            })
        })
        .collect();

    // Wait for all tasks to complete
    for task in tasks {
        let _ = task.await;
    }
}

/// Benchmark concurrent dependent operations with redis-async
/// Each task runs its own chain of dependent INCR -> SET operations
async fn redis_async_concurrent_dependent(conn: &PairedConnection, prefix: &str) {
    let tasks: Vec<_> = (0..NUM_CONCURRENT_TASKS)
        .map(|task_id| {
            let conn = conn.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                let counter_key = format!("{}_ctr_{}", prefix, task_id);

                for i in 0..OPS_PER_TASK {
                    // INCR and wait for result
                    let ctr: i64 = conn
                        .send(resp_array!["INCR", &counter_key])
                        .await
                        .expect("INCR failed");

                    // Use the counter to create a dependent key
                    let key = format!("{}:t{}:dep_{}", prefix, task_id, ctr);
                    let value = make_value(i);
                    let _: String = conn
                        .send(resp_array!["SET", &key, &value])
                        .await
                        .expect("SET failed");
                }
            })
        })
        .collect();

    for task in tasks {
        let _ = task.await;
    }
}

/// Benchmark concurrent dependent operations with redis crate
async fn redis_concurrent_dependent(conn: redis::aio::MultiplexedConnection, prefix: &str) {
    let tasks: Vec<_> = (0..NUM_CONCURRENT_TASKS)
        .map(|task_id| {
            let mut conn = conn.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                let counter_key = format!("{}_ctr_{}", prefix, task_id);

                for i in 0..OPS_PER_TASK {
                    // INCR and wait for result
                    let ctr: i64 = conn.incr(&counter_key, 1).await.expect("INCR failed");

                    // Use the counter to create a dependent key
                    let key = format!("{}:t{}:dep_{}", prefix, task_id, ctr);
                    let value = make_value(i);
                    let _: () = conn.set(&key, &value).await.expect("SET failed");
                }
            })
        })
        .collect();

    for task in tasks {
        let _ = task.await;
    }
}

fn bench_concurrent_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_threaded_batch");

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
            redis_async_concurrent_batch(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| async {
            let prefix = unique_prefix();
            redis_concurrent_batch(conn.clone(), &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.finish();
}

fn bench_concurrent_individual(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_threaded_individual");

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
            redis_async_concurrent_dependent(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| async {
            let prefix = unique_prefix();
            redis_concurrent_dependent(conn.clone(), &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_batch, bench_concurrent_individual);
criterion_main!(benches);
