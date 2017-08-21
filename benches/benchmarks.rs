/*
 * Copyright 2017 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

#![feature(test)]

extern crate test;

extern crate futures;
extern crate tokio_core;
extern crate redis_async;

use test::Bencher;

use futures::Future;

use tokio_core::reactor::Core;

use redis_async::client;

#[bench]
fn bench_simple_getsetdel(b: &mut Bencher) {
    let mut core = Core::new().unwrap();
    let addr = "127.0.0.1:6379".parse().unwrap();

    let connection = client::paired_connect(&addr, &core.handle());
    let connection = core.run(connection).unwrap();

    b.iter(|| {
               let set = connection.send(["SET", "test_key", "42"].as_ref());
               let get = connection.send(["GET", "test_key"].as_ref());
               let del = connection.send(["DEL", "test_key"].as_ref());
               let get_set = get.join(del);
               let (_, _) = core.run(get_set).unwrap();
           });
}

#[bench]
fn bench_big_pipeline(b: &mut Bencher) {
    let mut core = Core::new().unwrap();
    let addr = "127.0.0.1:6379".parse().unwrap();

    let connection = client::paired_connect(&addr, &core.handle());
    let connection = core.run(connection).unwrap();

    let data_size = 1000;

    b.iter(|| {
        for x in 0..data_size {
            let test_key = format!("test_{}", x);
            connection.send(["SET", &test_key, &x.to_string()].as_ref());
        }
        let mut gets = Vec::with_capacity(data_size);
        for x in 0..data_size {
            let test_key = format!("test_{}", x);
            gets.push(connection.send(["GET", &test_key].as_ref()));
        }
        let last_get = gets.remove(999);
        let _ = core.run(last_get);
    });
}