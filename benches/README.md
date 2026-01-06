# Performance Benchmarks

Benchmarks comparing `redis-async` with the `redis` crate from crates.io.

## Prerequisites

- A Redis server running on `127.0.0.1:6379`

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark file
cargo bench --bench single_threaded
cargo bench --bench multi_threaded
cargo bench --bench pipelined

# Run a specific group
cargo bench -- single_threaded_batch
```

## Benchmark Categories

### Single-Threaded (`single_threaded.rs`)
- **Batch**: Fire-and-forget SET operations (1000 ops)
- **Individual**: Dependent INCR → SET chains (1000 ops)

### Multi-Threaded (`multi_threaded.rs`)
- **Batch**: 10 concurrent Tokio tasks, each doing 100 SET operations
- **Individual**: 10 concurrent tasks with dependent operation chains

### Pipelined (`pipelined.rs`)
Compares implicit pipelining (redis-async) vs explicit Pipeline (redis crate):
- **ST Batch**: Single-threaded pipelined batch operations
- **ST Dependent**: Single-threaded with MGET→process→MSET chains
- **MT Batch**: Multi-threaded pipelined batches
- **MT Dependent**: Multi-threaded dependent pipeline chains

## Interpreting Results

Criterion outputs timing statistics with confidence intervals. Lower times are better.

Results are saved to `target/criterion/` with HTML reports you can view in a browser.

## Notes

- All benchmarks clean up their keys after each iteration
- Keys use unique prefixes to avoid conflicts between concurrent runs
- The `redis` crate uses `MultiplexedConnection` for fair async comparison
