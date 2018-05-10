# redis-async

[![Build Status](https://travis-ci.org/benashford/redis-async-rs.svg?branch=master)](https://travis-ci.org/benashford/redis-async-rs)
[![](http://meritbadge.herokuapp.com/redis-async)](https://crates.io/crates/redis-async)
[![](https://img.shields.io/crates/d/redis-async.svg)](https://crates.io/crates/redis-async)
[![](https://img.shields.io/crates/dv/redis-async.svg)](https://crates.io/crates/redis-async)
[![](https://docs.rs/redis-async/badge.svg)](https://docs.rs/redis-async/)
[![Dependency Status](https://dependencyci.com/github/benashford/redis-async-rs/badge)](https://dependencyci.com/github/benashford/redis-async-rs)

Using Tokio and Rust's futures to create an asynchronous Redis client.  [Documentation](https://docs.rs/redis-async/)

## Releases

The API is currently low-level and still subject to change.

Initially I'm focussing on single-server Redis instances, another long-term goal is to support Redis clusters.  This would make the implementation more complex as it requires routing, and handling error conditions such as `MOVED`.

## Other clients

There are a number of pre-existing Redis clients for Rust, two of particular interest are:

* Tokio-Redis - https://github.com/tokio-rs/tokio-redis - written as a demo of Tokio, by Tokio developers
* Redis-RS - https://github.com/mitsuhiko/redis-rs - the most popular, but uses blocking I/O and isn't compatible with Tokio (yet)

## Usage

There are three functions in `redis_async::client` which provide functionality.  One is a low-level interface, a second is a high-level interface, the third is dedicated to PUBSUB functionality.

### Low-level interface

The function `client::connect` returns a future that resolves to a connection which implements both `Sink` and `Stream`.  These work independently of one another to allow pipelining.  It is the responsibility of the caller to match responses to requests.  It is also the responsibility of the client to convert application data into instances of `resp::RespValue` and back (there are conversion traits available for common examples).

This is a very low-level API compared to most Redis clients, but is done so intentionally, for two reasons: 1) it is the common demoniator between a functional Redis client (i.e. is able to support all types of requests, including those that block and have streaming responses), and 2) it results in clean `Sink`s and `Stream`s which will be composable with other Tokio-based libraries.

For most practical purposes this low-level interface will not be used, the only exception possibly being the [`MONITOR`](https://redis.io/commands/monitor) command.

#### Example

An example of this low-level interface is in [`examples/monitor.rs`](examples/monitor.rs).  This can be run with `cargo run --example monitor`, it will run until it is `Ctrl-C`'d and will show every command run against the Redis server.

### High-level interface

`client::paired_connect` is used for most Redis commands (those for which one command returns one response, it's not suitable for PUBSUB, `MONITOR` or other similar commands).  It allows a Redis command to be sent and a Future returned for each command.

Commands will be sent in the order that `send` is called, regardless of how the future is realised.  This is to allow us to take advantage of Redis's features by implicitly pipelining commands where appropriate.  One side-effect of this is that for many commands, e.g. `SET` we don't need to realise the future at all, it can be assumed to be fire-and-forget; but, the final future of the final command does need to be realised (at least) to ensure that the correct behaviour is observed.

#### Example

See [`examples/realistic.rs`](examples/realistic.rs) for an example using completely artificial test data, it is realistic in the sense that it simulates a real-world pattern where certain operations depend on the results of others.

This shows that the code can be written in a straight line fashion - iterate through the outer-loop, for each make a call to `INCR` a value and use the result to write the data to a unique key.  But when run, the various calls will be pipelined.

In order to test this, a tool like ngrep can be used to monitor the data sent to Redis, so running `cargo run --release --example realistic` (the `--release` flag needs to be set for the buffers to fill faster than packets can be sent to the Redis server) shows the data flowing:

```
interface: lo0 (127.0.0.0/255.0.0.0)
filter: (ip or ip6) and ( port 6379 )
#####
T 127.0.0.1:61112 -> 127.0.0.1:6379 [AP]
  *2..$4..INCR..$18..realistic_test_ctr..*2..$4..INCR..$18..realistic_test_ctr..*2..$4..INCR..$18..
  realistic_test_ctr..*2..$4..INCR..$18..realistic_test_ctr..*2..$4..INCR..$18..realistic_test_ctr.
  .*2..$4..INCR..$18..realistic_test_ctr..*2..$4..INCR..$18..realistic_test_ctr..*2..$4..INCR..$18.
  .realistic_test_ctr..*2..$4..INCR..$18..realistic_test_ctr..*2..$4..INCR..$18..realistic_test_ctr
  ..
##
T 127.0.0.1:6379 -> 127.0.0.1:61112 [AP]
  :1..:2..:3..:4..:5..:6..:7..:8..:9..:10..
##
T 127.0.0.1:61112 -> 127.0.0.1:6379 [AP]
  *3..$3..SET..$4..rt_1..$1..0..*3..$3..SET..$1..0..$4..rt_1..*3..$3..SET..$4..rt_2..$1..1..*3..$3.
  .SET..$1..1..$4..rt_2..*3..$3..SET..$4..rt_3..$1..2..*3..$3..SET..$1..2..$4..rt_3..*3..$3..SET..$
  4..rt_4..$1..3..*3..$3..SET..$1..3..$4..rt_4..*3..$3..SET..$4..rt_5..$1..4..*3..$3..SET..$1..4..$
  4..rt_5..*3..$3..SET..$4..rt_6..$1..5..*3..$3..SET..$1..5..$4..rt_6..*3..$3..SET..$4..rt_7..$1..6
  ..*3..$3..SET..$1..6..$4..rt_7..*3..$3..SET..$4..rt_8..$1..7..*3..$3..SET..$1..7..$4..rt_8..*3..$
  3..SET..$4..rt_9..$1..8..*3..$3..SET..$1..8..$4..rt_9..*3..$3..SET..$5..rt_10..$1..9..*3..$3..SET
  ..$1..9..$5..rt_10..
##
T 127.0.0.1:6379 -> 127.0.0.1:61112 [AP]
  +OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+OK..+O
  K..
```

See note on 'Performance' for what impact this has.

### PUBSUB

PUBSUB in Redis works differently.  A connection will subscribe to one or more topics, then receive all messages that are published to that topic.  As such the single-request/single-response model of `paired_connect` will not work.  A specific `client::pubsub_connect` is provided for this purpose.

It returns a future which resolves to a `PubsubConnection`, this provides a `subscribe` function that takes a topic as a parameter and returns a future which, once the subscription is confirmed, resolves to a stream that contains all messages published to that topic.

#### Example

See an [`examples/pubsub.rs`](examples/pubsub.rs).  This will listen on a topic (by default: `test-topic`) and print each message as it arrives.  To run this example: `cargo run --example pubsub` then in a separate terminal open `redis-cli` to the same server and publish some messages (e.g. `PUBLISH test-topic TESTING`).

## Performance

This project is still in its early stages, as such there is plenty of scope for change that could improve (or worsen) performance characteristics.  There are however a number of benchmarks in [`benches/benchmarks.rs`](benches/benchmarks.rs).

The benchmarks are intended to be compatible with those of [redis-rs](https://github.com/mitsuhiko/redis-rs), I've added several more to cover more scenarios.

### Results

In most cases the difference is small.

| Benchmark        | redis-rs (the control)                                            | redis-async-rs  |
| ---------------- | ----------------------------------------------------------------- | --------------- |
| simple_getsetdel | 131,107 ns/iter (not pipelined)<br>53,826 ns/iter (pipelined)     | 113,738 ns/iter |
| complex          | 9,286,682 ns/iter (non pipelined)<br>565,834 ns/iter (pipelined)  | 610,752 ns/iter |

For `redis-rs` each benchmark has a pipelined and a non-pipelined version.  For `redis-async-rs` there is only one version as pipelining is handled implicitely.

The results clearly show the effects of the implicit pipelining, however explicit pipelining of `redis-rs` still wins.

## Next steps

* Better documentation
* Support for `PSUBSCRIBE` as well as `SUBSCRIBE`
* Test all Redis commands
* Decide on best way of supporting [Redis transactions](https://redis.io/topics/transactions)
* Decide on best way of supporting blocking Redis commands
* Ensure all edge-cases are complete (e.g. Redis commands that return sets, nil, etc.)

## History

Originally this was intended to be a Rust port of [my Clojure/Java Redis client](https://github.com/benashford/redis-async), however significant differences between that model and the Tokio model of doing things, and a desire to remain within idiomatic Rust, meant the design evolved to be quite different.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
