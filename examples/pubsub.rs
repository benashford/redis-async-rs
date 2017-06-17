extern crate futures;
extern crate tokio_core;
extern crate redis_async;

use std::env;

use futures::{Future, Stream};
use futures::future;

use tokio_core::reactor::Core;

use redis_async::client;

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let topic = env::args().nth(1).unwrap_or("test-topic".to_string());
    let addr = env::args().nth(2).unwrap_or("127.0.0.1:6379".to_string()).parse().unwrap();

    let msgs = client::pubsub_connect(&addr, &handle).and_then(move |connection| connection.subscribe(topic));
    let the_loop = msgs.map_err(|_| ()).and_then(|msgs| msgs.for_each(|message| {
        println!("{}", message.into_string().unwrap());
        future::ok(())
    }));

    core.run(the_loop).unwrap();
}