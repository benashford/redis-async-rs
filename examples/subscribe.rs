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

use futures::StreamExt;

use redis_async::{client, resp::FromResp};

#[tokio::main]
async fn main() {
    env_logger::init();
    let topic = env::args()
        .nth(1)
        .unwrap_or_else(|| "test-topic".to_string());
    let addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let pubsub_con = client::pubsub_connect(addr, 6379)
        .await
        .expect("Cannot connect to Redis");
    let mut msgs = pubsub_con
        .subscribe(&topic)
        .await
        .expect("Cannot subscribe to topic");

    while let Some(message) = msgs.next().await {
        match message {
            Ok(message) => println!("{}", String::from_resp(message).unwrap()),
            Err(e) => {
                eprintln!("ERROR: {}", e);
                break;
            }
        }
    }

    println!("The end");
}
