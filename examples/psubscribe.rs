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

use futures::StreamExt;

use redis_async::{client::ConnectionBuilder, protocol::FromResp};

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
    env_logger::init();
    let topic = env::args().nth(1).unwrap_or_else(|| "test.*".to_string());

    let addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string());

    let pubsub_con = ConnectionBuilder::new(addr)
        .expect("Cannot parse address")
        .pubsub_connect()
        .await
        .expect("Cannot open connection");

    let mut msgs = pubsub_con
        .psubscribe(&topic)
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
