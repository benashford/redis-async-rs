/*
 * Copyright 2017 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

extern crate futures;
extern crate tokio_core;
extern crate redis_async;

use std::env;

use futures::{future, Future, Sink, Stream};

use tokio_core::reactor::Core;

use redis_async::client;

fn main() {
    let mut core = Core::new().unwrap();

    let addr = env::args()
        .nth(1)
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse()
        .unwrap();

    let monitor = client::connect(&addr, &core.handle()).map_err(|e| e.into()).and_then(|connection| {
        let client::ClientConnection { sender, receiver } = connection;
        sender.send(["MONITOR"].as_ref().into())
            .map_err(|e| e.into())
            .and_then(move |_| {
                          receiver.for_each(|incoming| {
                                              println!("{:?}", incoming);
                                              future::ok(())
                                            })
                      })
    });

    core.run(monitor).unwrap();
}