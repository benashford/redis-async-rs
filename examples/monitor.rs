/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

extern crate futures;
#[macro_use]
extern crate redis_async;
extern crate tokio;

use std::env;

use futures::{future, Future, Sink, Stream};

use redis_async::client;

fn main() {
    let addr = env::args()
        .nth(1)
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse()
        .unwrap();

    let monitor = client::connect(&addr)
        .map_err(|e| e.into())
        .and_then(|connection| {
            connection
                .send(resp_array!["MONITOR"])
                .map_err(|e| e.into())
        })
        .and_then(|connection| {
            connection.skip(1).for_each(|incoming| {
                println!("{:?}", incoming);
                future::ok(())
            })
        });

    tokio::run(monitor.map_err(|e| println!("ERROR: {:?}", e)));
}
