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
use std::sync::Arc;

use futures::sync::oneshot;
use futures::{future, Future};

use redis_async::client;

// An artificial "realistic" non-trivial example to demonstrate usage
fn main() {
    // Create some completely arbitrary "test data"
    let test_data_size = 10;
    let test_data: Vec<_> = (0..test_data_size).map(|x| (x, x.to_string())).collect();

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string())
        .parse()
        .unwrap();

    let test_f = client::paired_connect(&addr);
    let (tx, rx) = oneshot::channel();

    let send_data = test_f.and_then(|connection| {
        let connection = Arc::new(connection);
        let futures = test_data.into_iter().map(move |data| {
            let connection_inner = connection.clone();
            connection
                .send(resp_array!["INCR", "realistic_test_ctr"])
                .and_then(move |ctr: String| {
                    let key = format!("rt_{}", ctr);
                    let d_val = data.0.to_string();
                    connection_inner.send_and_forget(resp_array!["SET", &key, d_val]);
                    connection_inner.send(resp_array!["SET", data.1, key])
                })
        });
        future::join_all(futures)
    });
    let deliver = send_data.then(|result| match result {
        Ok(result) => match tx.send(result) {
            Ok(_) => future::ok(()),
            Err(e) => {
                println!("Unexpected error: {:?}", e);
                future::err(())
            }
        },
        Err(e) => {
            println!("Unexpected error: {:?}", e);
            future::err(())
        }
    });

    tokio::run(deliver);

    let result: Vec<String> = rx.wait().expect("Waiting for delivery");
    println!("RESULT: {:?}", result);
    assert_eq!(result.len(), test_data_size);
}
