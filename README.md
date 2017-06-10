# redis-async

An exercise in learning Tokio and Rust's futures by creating a Redis client.

## Releases

None as yet.  Coming soon...

## Other clients

* Tokio-Redis - https://github.com/tokio-rs/tokio-redis
* Redis-RS - https://github.com/mitsuhiko/redis-rs (synchronous, doesn't use Tokio)

## Why a new Redis client?

The primary goal is to teach myself Tokio.  With a longer-term goal of achieving more than the two existing Redis clients listed above.  For example, Tokio-Redis assumes that a Redis client receives one response to one request, which is true in most cases; however some Redis commands (e.g. the PUBSUB commands) result in infinite streams of data.  A fully-functional Redis client needs to handle such things.

Initially I'm focussing on single-server Redis instances, another long-term goal is to support Redis clusters.  This would make the implementation more complex as it requires routing, and handling error conditions such as `MOVED`.

## Progress

At present the function `client::connect` returns a pair of `Sink` and `Stream` which both transport `resp::RespValue`s between client and Redis, these work independently of one another to allow pipelining.  It is the responsibility of the caller to match responses to requests.  It is also the responsibility of the client to convert application data into instances of `resp::RespValue` and back (there are conversion traits available for common examples).  Working examples of this can be found in the tests in [`client.rs`](src/client.rs).

This is a very low-level API compared to most Redis clients, but is done so intentionally, for two reasons: 1) it is the common demoniator between a functional Redis client (i.e. is able to support all types of requests, including those that block and have streaming responses), and 2) it results in clean `Sink`s and `Stream`s which will be composable with other Tokio-based libraries.

## Next steps

### A higher-level API

The low-level interface presented by independent `Sink` and `Stream`s is not particularly friendly, although it is very flexible.  I intend to create several higher-level "clients", one for standard one-request-one-response commands (most Redis commands fall into this category); one for one-request-multiple-responses (e.g. the PUBSUB commands); and potentially more as there are a number of commands that do unique things or change the nature of a connection and make it unsuitable for other purposes (e.g. MONITOR).

These higher-level clients will be built upon the existing low-level interface.  But as far as the consuming application is concerned, the details will be hidden.  For example, this should be possible:

```rust
let set_future = hl_client.set("X", "123");
let get_future = hl_client.get("X");
let result = get_future.wait().unwrap(); // Will equal "123"
```

### Goals

Any future higher-level client should take advantage of the asynchonous nature of the underlying interface.  In practice this means supporting implicit pipelining rather than requiring code to explitely be aware of such things.  Also, the same physical Redis connection should be sharable across threads.

## Questions

There are some unresolved questions/design issues/etc. at present:

1. What types should the high-level client support?  The low-level interface transports `resp::RespValue` which represents Redis's serialisation protocol, but hiding this would have value in high-level examples.  However, Redis's "strings" are actually byte arrays, so judicious use of conversion traits might be required to reduce the boilerplate while still allowing the full set of functionality.

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
