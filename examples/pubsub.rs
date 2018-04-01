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
extern crate redis_async;
extern crate tokio;

use std::env;

use futures::{Future, Stream};
use futures::future;

use redis_async::client;
use redis_async::resp::FromResp;

fn main() {
    let topic = env::args().nth(1).unwrap_or("test-topic".to_string());
    let addr = env::args()
        .nth(2)
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse()
        .unwrap();

    let msgs =
        client::pubsub_connect(&addr).and_then(move |connection| connection.subscribe(topic));
    let the_loop = msgs.map_err(|_| ()).and_then(|msgs| {
        msgs.for_each(|message| {
            println!("{}", String::from_resp(message).unwrap());
            future::ok(())
        })
    });

    tokio::run(the_loop);
}
