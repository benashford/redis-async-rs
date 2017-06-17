# redis-async

[![Build Status](https://travis-ci.org/benashford/redis-async-rs.svg?branch=master)](https://travis-ci.org/benashford/redis-async-rs)

An exercise in learning Tokio and Rust's futures by creating a Redis client.

## Releases

None as yet.  Coming soon...

## Other clients

* Tokio-Redis - https://github.com/tokio-rs/tokio-redis
* Redis-RS - https://github.com/mitsuhiko/redis-rs (synchronous, doesn't use Tokio)

## Why a new Redis client?

The primary goal is to teach myself Tokio.  With a longer-term goal of achieving more than the two existing Redis clients listed above.  For example, Tokio-Redis assumes that a Redis client receives one response to one request, which is true in most cases; however some Redis commands (e.g. the PUBSUB commands) result in infinite streams of data.  A fully-functional Redis client needs to handle such things.

Initially I'm focussing on single-server Redis instances, another long-term goal is to support Redis clusters.  This would make the implementation more complex as it requires routing, and handling error conditions such as `MOVED`.

## Implementation

### Low-level interface

The function `client::connect` returns a pair of `Sink` and `Stream` which both transport `resp::RespValue`s between client and Redis, these work independently of one another to allow pipelining.  It is the responsibility of the caller to match responses to requests.  It is also the responsibility of the client to convert application data into instances of `resp::RespValue` and back (there are conversion traits available for common examples).  Working examples of this can be found in the tests in [`client.rs`](src/client.rs).

This is a very low-level API compared to most Redis clients, but is done so intentionally, for two reasons: 1) it is the common demoniator between a functional Redis client (i.e. is able to support all types of requests, including those that block and have streaming responses), and 2) it results in clean `Sink`s and `Stream`s which will be composable with other Tokio-based libraries.

### High-level interface

Working exclusively with such a low-level interface would be time-consuming and error prone.  So we also provide a higher-level API to work with.  `client::paired_connect` is used for most Redis commands (those for which one command returns one response, it's not suitable for PUBSUB or other similar commands).  It allows a Redis command to be sent and a Future returned for each command.

Commands will be sent in the order that `send` is called, regardless of how the future is realised.  This is to allow us to take advantage of Redis's features by implicitly pipelining commands where appropriate.  One side-effect of this is that for many commands, e.g. `SET` we don't need to realise the future at all, it can be assumed to be fire-and-forget; but, the final future of the final command does need to be realised (at least) to ensure that the correct behaviour is observed.

### PUBSUB

PUBSUB in Redis works differently.  A connection will subscribe to one or more topics, then receive all messages that are published to that topic.  As such the single-request/single-response model of `paired_connect` will not work.  A specific `client::pubsub_connect` is provided for this purpose.

It returns a future which resolves to a `PubsubConnection`, this provides a `subscribe` function that takes a topic as a parameter and returns a future which, once the subscription is confirmed, resolves to a stream that contains all messages published to that topic.

See an [example](examples/pubsub.rs).

## Next steps

* Support for `PSUBSCRIBE` as well as `SUBSCRIBE`

## Questions

* The exact exposed API, will be subject to change.
* Would it be possible, or would it even make any sense, to wrap and hide the reactor so that this library can be called from applications based on blocking I/O but still take advantage of Tokio for the internals (e.g. implicit pipelining)?

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
