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

use std::sync::Arc;
use std::env;

use futures::{future, Future};

use tokio_core::reactor::Core;

use redis_async::client;

// An artificial "realistic" non-trivial example to demonstrate usage
fn main() {
    // Create some completely arbitrary "test data"
    let test_data_size = 10;
    let test_data: Vec<_> = (0..test_data_size).map(|x| (x, x.to_string())).collect();

    let mut core = Core::new().unwrap();

    let addr = env::args()
        .nth(1)
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse()
        .unwrap();

    let test_f = client::paired_connect(&addr, &core.handle());

    let send_data = test_f.and_then(|connection| {
        let connection = Arc::new(connection);
        let futures: Vec<_> = test_data
            .into_iter()
            .map(move |data| {
                let connection_inner = connection.clone();
                connection
                    .send(vec!["INCR", "realistic_test_ctr"])
                    .and_then(move |ctr| {
                                  let key = format!("rt_{}", ctr.into_string().unwrap());
                                  let d_val = data.0.to_string();
                                  connection_inner.send(vec!["SET", &key, &d_val]);
                                  connection_inner.send(vec!["SET", &data.1, &key])
                              })
            })
            .collect();
        future::join_all(futures)
    });

    let result = core.run(send_data).unwrap();
    assert_eq!(result.len(), test_data_size);
}