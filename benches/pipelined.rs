/*
 * Pipelined benchmarks comparing redis-async vs redis crate
 *
 * Tests different pipelining strategies:
 *
 * For redis-async: Implicit pipelining - commands sent via send() are
 *   automatically batched and pipelined by the library
 *
 * For redis crate: Explicit Pipeline - commands are added to a Pipeline
 *   object and executed as a batch
 *
 * Tests:
 * 1. Single-threaded pipelined batch - all ops in one pipeline
 * 2. Single-threaded pipelined individual - dependent operations with pipeline batches
 * 3. Multi-threaded pipelined batch - concurrent tasks with pipelines
 * 4. Multi-threaded pipelined individual - concurrent dependent pipelines
 */

mod common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_util::future;
use redis_async::client::PairedConnection;
use redis_async::resp_array;

use common::{
    cleanup_keys, create_redis_async_connection, create_redis_connection, make_key, make_value,
    unique_prefix, NUM_CONCURRENT_TASKS, NUM_OPERATIONS, OPS_PER_TASK,
};

// ============================================================================
// Single-threaded pipelined benchmarks
// ============================================================================

/// redis-async: Send all commands and collect futures, then await all at once
/// This leverages redis-async's implicit pipelining - all sends go out before
/// awaiting any response
async fn redis_async_pipeline_batch(conn: &PairedConnection, prefix: &str) {
    // Collect all futures first (commands are pipelined)
    let futures: Vec<_> = (0..NUM_OPERATIONS)
        .map(|i| {
            let key = make_key(prefix, i);
            let value = make_value(i);
            conn.send::<String>(resp_array!["SET", key, value])
        })
        .collect();

    // Wait for all responses at once
    let _results = future::join_all(futures).await;
}

/// redis crate: Use explicit Pipeline object
async fn redis_pipeline_batch(mut conn: redis::aio::MultiplexedConnection, prefix: &str) {
    let mut pipe = redis::pipe();
    for i in 0..NUM_OPERATIONS {
        let key = make_key(prefix, i);
        let value = make_value(i);
        pipe.set(&key, &value).ignore();
    }

    let _: () = pipe.query_async(&mut conn).await.expect("Pipeline failed");
}

/// redis-async: Batch setup, then read all, then write all - demonstrates staged pipelining
async fn redis_async_pipeline_staged(conn: &PairedConnection, prefix: &str) {
    // Stage 1: Set up initial values (pipelined)
    let setup_futures: Vec<_> = (0..NUM_OPERATIONS)
        .map(|i| {
            let key = make_key(prefix, i);
            conn.send::<String>(resp_array!["SET", key, i.to_string()])
        })
        .collect();
    let _ = future::join_all(setup_futures).await;

    // Stage 2: Increment all counters (pipelined)
    let incr_futures: Vec<_> = (0..NUM_OPERATIONS)
        .map(|i| {
            let key = format!("{}_ctr:{}", prefix, i);
            conn.send::<i64>(resp_array!["INCR", key])
        })
        .collect();
    let counts: Vec<Result<i64, _>> = future::join_all(incr_futures).await;

    // Stage 3: Use counter values - write back (pipelined)
    let write_futures: Vec<_> = counts
        .into_iter()
        .enumerate()
        .map(|(i, count)| {
            let ctr = count.unwrap_or(0);
            let key = format!("{}_processed:{}", prefix, i);
            conn.send::<String>(resp_array!["SET", key, format!("final_{}", ctr)])
        })
        .collect();
    let _ = future::join_all(write_futures).await;
}

/// redis crate: Same staged pipeline pattern
async fn redis_pipeline_staged(mut conn: redis::aio::MultiplexedConnection, prefix: &str) {
    // Stage 1: Setup with pipeline
    let mut setup_pipe = redis::pipe();
    for i in 0..NUM_OPERATIONS {
        let key = make_key(prefix, i);
        setup_pipe.set(&key, i).ignore();
    }
    let _: () = setup_pipe
        .query_async(&mut conn)
        .await
        .expect("Setup failed");

    // Stage 2: Increment counters with pipeline
    let mut incr_pipe = redis::pipe();
    for i in 0..NUM_OPERATIONS {
        let key = format!("{}_ctr:{}", prefix, i);
        incr_pipe.incr(&key, 1);
    }
    let counts: Vec<i64> = incr_pipe.query_async(&mut conn).await.expect("Incr failed");

    // Stage 3: Write back with pipeline
    let mut write_pipe = redis::pipe();
    for (i, ctr) in counts.into_iter().enumerate() {
        let key = format!("{}_processed:{}", prefix, i);
        write_pipe.set(&key, format!("final_{}", ctr)).ignore();
    }
    let _: () = write_pipe
        .query_async(&mut conn)
        .await
        .expect("Write failed");
}

// ============================================================================
// Multi-threaded pipelined benchmarks
// ============================================================================

/// redis-async: Concurrent tasks each with their own pipelined batch
async fn redis_async_concurrent_pipeline_batch(conn: &PairedConnection, prefix: &str) {
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

    for task in tasks {
        let _ = task.await;
    }
}

/// redis crate: Concurrent tasks each with their own pipeline
async fn redis_concurrent_pipeline_batch(conn: redis::aio::MultiplexedConnection, prefix: &str) {
    let tasks: Vec<_> = (0..NUM_CONCURRENT_TASKS)
        .map(|task_id| {
            let mut conn = conn.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                let mut pipe = redis::pipe();
                for i in 0..OPS_PER_TASK {
                    let key = make_key(&prefix, task_id * OPS_PER_TASK + i);
                    let value = make_value(i);
                    pipe.set(&key, &value).ignore();
                }
                let _: () = pipe.query_async(&mut conn).await.expect("Pipeline failed");
            })
        })
        .collect();

    for task in tasks {
        let _ = task.await;
    }
}

/// redis-async: Concurrent tasks with dependent staged pipelines
async fn redis_async_concurrent_pipeline_staged(conn: &PairedConnection, prefix: &str) {
    let tasks: Vec<_> = (0..NUM_CONCURRENT_TASKS)
        .map(|task_id| {
            let conn = conn.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                // Stage 1: setup values
                let setup_futures: Vec<_> = (0..OPS_PER_TASK)
                    .map(|i| {
                        let key = format!("{}:t{}:setup_{}", prefix, task_id, i);
                        conn.send::<String>(resp_array!["SET", key, i.to_string()])
                    })
                    .collect();
                let _ = future::join_all(setup_futures).await;

                // Stage 2: increment counters
                let incr_futures: Vec<_> = (0..OPS_PER_TASK)
                    .map(|i| {
                        let key = format!("{}:t{}:ctr_{}", prefix, task_id, i);
                        conn.send::<i64>(resp_array!["INCR", key])
                    })
                    .collect();
                let counts: Vec<_> = future::join_all(incr_futures).await;

                // Stage 3: use counter values
                let dep_futures: Vec<_> = counts
                    .into_iter()
                    .enumerate()
                    .map(|(i, count)| {
                        let ctr = count.unwrap_or(0);
                        let key = format!("{}:t{}:dep_{}_{}", prefix, task_id, i, ctr);
                        conn.send::<String>(resp_array!["SET", key, format!("final_{}", ctr)])
                    })
                    .collect();
                let _ = future::join_all(dep_futures).await;
            })
        })
        .collect();

    for task in tasks {
        let _ = task.await;
    }
}

/// redis crate: Concurrent tasks with dependent staged pipelines
async fn redis_concurrent_pipeline_staged(conn: redis::aio::MultiplexedConnection, prefix: &str) {
    let tasks: Vec<_> = (0..NUM_CONCURRENT_TASKS)
        .map(|task_id| {
            let mut conn = conn.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                // Stage 1: setup values
                let mut setup_pipe = redis::pipe();
                for i in 0..OPS_PER_TASK {
                    let key = format!("{}:t{}:setup_{}", prefix, task_id, i);
                    setup_pipe.set(&key, i).ignore();
                }
                let _: () = setup_pipe
                    .query_async(&mut conn)
                    .await
                    .expect("Setup failed");

                // Stage 2: increment counters
                let mut incr_pipe = redis::pipe();
                for i in 0..OPS_PER_TASK {
                    let key = format!("{}:t{}:ctr_{}", prefix, task_id, i);
                    incr_pipe.incr(&key, 1);
                }
                let counts: Vec<i64> = incr_pipe.query_async(&mut conn).await.expect("Incr failed");

                // Stage 3: use counter values
                let mut dep_pipe = redis::pipe();
                for (i, ctr) in counts.into_iter().enumerate() {
                    let key = format!("{}:t{}:dep_{}_{}", prefix, task_id, i, ctr);
                    dep_pipe.set(&key, format!("final_{}", ctr)).ignore();
                }
                let _: () = dep_pipe.query_async(&mut conn).await.expect("Dep failed");
            })
        })
        .collect();

    for task in tasks {
        let _ = task.await;
    }
}

// ============================================================================
// Benchmark groups
// ============================================================================

fn bench_single_threaded_pipeline_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipelined_st_batch");

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
            redis_async_pipeline_batch(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| {
            let conn = conn.clone();
            async move {
                let prefix = unique_prefix();
                redis_pipeline_batch(conn, &prefix).await;
                cleanup_keys(&prefix).await;
            }
        });
    });

    group.finish();
}

fn bench_single_threaded_pipeline_staged(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipelined_st_staged");

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
            redis_async_pipeline_staged(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| {
            let conn = conn.clone();
            async move {
                let prefix = unique_prefix();
                redis_pipeline_staged(conn, &prefix).await;
                cleanup_keys(&prefix).await;
            }
        });
    });

    group.finish();
}

fn bench_multi_threaded_pipeline_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipelined_mt_batch");

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
            redis_async_concurrent_pipeline_batch(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| {
            let conn = conn.clone();
            async move {
                let prefix = unique_prefix();
                redis_concurrent_pipeline_batch(conn, &prefix).await;
                cleanup_keys(&prefix).await;
            }
        });
    });

    group.finish();
}

fn bench_multi_threaded_pipeline_staged(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipelined_mt_staged");

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
            redis_async_concurrent_pipeline_staged(&redis_async_conn, &prefix).await;
            cleanup_keys(&prefix).await;
        });
    });

    group.bench_function(BenchmarkId::new("redis", NUM_OPERATIONS), |b| {
        let conn = redis_conn.clone();
        b.to_async(&rt).iter(|| {
            let conn = conn.clone();
            async move {
                let prefix = unique_prefix();
                redis_concurrent_pipeline_staged(conn, &prefix).await;
                cleanup_keys(&prefix).await;
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_threaded_pipeline_batch,
    bench_single_threaded_pipeline_staged,
    bench_multi_threaded_pipeline_batch,
    bench_multi_threaded_pipeline_staged
);
criterion_main!(benches);
