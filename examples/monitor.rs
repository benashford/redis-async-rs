/*
 * Copyright 2017-2022 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::env;

use futures::{sink::SinkExt, stream::StreamExt};

use redis_async::{client, resp_array};

#[tokio::main]
async fn main() {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1".to_string());

    #[cfg(not(feature = "tls"))]
    let mut connection = client::connect(&addr, 6379)
        .await
        .expect("Cannot connect to Redis");

    #[cfg(feature = "tls")]
    let mut connection = client::connect_tls(&addr, 6379)
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
