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

use futures::{sink::SinkExt, stream::StreamExt};

use redis_async::{client, resp_array};

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

async fn do_main() {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string())
        .parse()
        .expect("Cannot parse Redis connection string");

    let mut connection = client::connect(&addr)
        .await
        .expect("Cannot connect to Redis");
    connection
        .send(resp_array!["MONITOR"])
        .await
        .expect("Cannot send MONITOR command");

    let mut skip_one = connection.skip(1);

    while let Some(incoming) = skip_one.next().await {
        println!("{:?}", incoming.expect("Cannot read incoming value"));
    }
}
