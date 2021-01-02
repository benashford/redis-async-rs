/*
 * Copyright 2017-2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

#[cfg(feature = "tokio02")]
extern crate tokio_02 as tokio;

#[cfg(feature = "tokio10")]
extern crate tokio_10 as tokio;

use std::env;

use futures_util::future;

// use futures::{future, Future};

use redis_async::{client::ConnectionBuilder, resp_array};

#[cfg(feature = "with_tokio")]
#[tokio::main]
async fn main() {
    do_main().await;
}

#[cfg(feature = "with_async_std")]
#[async_std::main]
async fn main() {
    do_main().await;
}

/// An artificial "realistic" non-trivial example to demonstrate usage
async fn do_main() {
    // Create some completely arbitrary "test data"
    let test_data_size = 10;
    let test_data: Vec<_> = (0..test_data_size).map(|x| (x, x.to_string())).collect();

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string());

    let connection_builder = ConnectionBuilder::new(addr).expect("Cannot parse address");

    let connection = connection_builder
        .paired_connect()
        .await
        .expect("Cannot open connection");

    let futures = test_data.into_iter().map(|data| {
        let connection_inner = connection.clone();
        let incr_f = connection.send(resp_array!["INCR", "realistic_test_ctr"]);
        async move {
            let ctr: String = incr_f.await.expect("Cannot increment");

            let key = format!("rt_{}", ctr);
            let d_val = data.0.to_string();
            connection_inner.send_and_forget(resp_array!["SET", &key, d_val]);
            connection_inner
                .send(resp_array!["SET", data.1, key])
                .await
                .expect("Cannot set")
        }
    });
    let result: Vec<String> = future::join_all(futures).await;
    println!("RESULT: {:?}", result);
    assert_eq!(result.len(), test_data_size);
}
