# redis-async

An exercise in learning Tokio and Rust's futures by trying to port [my Clojure/Java Redis client](https://github.com/benashford/redis-async) to rust.

## Releases

None as yet, there might not ever be, it depends if this exercise turns into a useful tool or not.

## Progress

Nothing of any significance, more complete examples can be found at:

* Tokio-Redis - https://github.com/tokio-rs/tokio-redis
* Redis-RS - https://github.com/mitsuhiko/redis-rs (synchronous, doesn't use Tokio)

## Goals

### Why a new Redis client?

The primary goal is to teach myself Tokio.  With a longer-term goal of acheiving more than the two existing Redis clients listed above.  For example, Tokio-Redis assumes that a Redis client receives one response to one request, which is true in most cases; however some Redis commands (e.g. the PUBSUB commands) result in infinite streams of data.  A fully-functional Redis client needs to handle such things.

Initially I'm focussing on single-server Redis instances, another long-term goal is to support Redis clusters.  This would make the implementation more complex as it requires routing, and handling error conditions such as `MOVED`.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
