/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

#![feature(test)]

extern crate futures;
#[macro_use]
extern crate redis_async;
extern crate test;
extern crate tokio;

use std::net::SocketAddr;
use std::sync::Arc;

use test::Bencher;

use futures::sync::oneshot;
use futures::Future;

use tokio::runtime::Runtime;

use redis_async::client;

fn spawn_and_wait<R, E, F>(runtime: &mut Runtime, f: F) -> Result<R, E>
where
    R: Send + 'static,
    E: Send + 'static,
    F: Future<Item = R, Error = E> + Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    runtime.spawn(f.then(|r| tx.send(r).map_err(|_| panic!("Cannot send result"))));
    rx.wait().expect("Cannot wait")
}

fn open_paired_connection(runtime: &mut Runtime, addr: &SocketAddr) -> client::PairedConnection {
    spawn_and_wait(runtime, client::paired_connect(addr)).expect("No connection")
}

#[bench]
fn bench_simple_getsetdel(b: &mut Bencher) {
    let addr = "127.0.0.1:6379".parse().unwrap();

    let mut runtime = Runtime::new().expect("Runtime");

    let connection = open_paired_connection(&mut runtime, &addr);

    b.iter(|| {
        connection.send_and_forget(resp_array!["SET", "test_key", "42"]);
        let get = connection.send(resp_array!["GET", "test_key"]);
        let del = connection.send(resp_array!["DEL", "test_key"]);
        let get_set = get.join(del);
        let _: (String, String) = spawn_and_wait(&mut runtime, get_set).unwrap();
    });

    runtime.shutdown_now().wait().unwrap();
}

#[bench]
fn bench_big_pipeline(b: &mut Bencher) {
    let addr = "127.0.0.1:6379".parse().unwrap();

    let mut runtime = Runtime::new().expect("Runtime");

    let connection = open_paired_connection(&mut runtime, &addr);

    let data_size = 100;

    b.iter(|| {
        for x in 0..data_size {
            let test_key = format!("test_{}", x);
            connection.send_and_forget(resp_array!["SET", test_key, x.to_string()]);
        }
        let mut gets = Vec::with_capacity(data_size);
        for x in 0..data_size {
            let test_key = format!("test_{}", x);
            gets.push(connection.send(resp_array!["GET", test_key]));
        }
        let last_get = gets.remove(data_size - 1);
        let _: String = spawn_and_wait(&mut runtime, last_get).unwrap();
    });

    runtime.shutdown_now().wait().unwrap();
}

#[bench]
fn bench_complex_pipeline(b: &mut Bencher) {
    let addr = "127.0.0.1:6379".parse().unwrap();

    let mut runtime = Runtime::new().expect("Runtime");

    let connection_outer = Arc::new(open_paired_connection(&mut runtime, &addr));

    let data_size = 100;

    b.iter(|| {
        let all_sets = {
            let connection = connection_outer.clone();
            let sets = (0..data_size).map(move |x| {
                let connection_inner = connection.clone();
                connection_inner
                    .send(resp_array!["INCR", "id_gen"])
                    .and_then(move |id: String| {
                        let id = format!("id_{}", id);
                        connection_inner.send(resp_array!["SET", &id, &x.to_string()])
                    })
            });
            futures::future::join_all(sets)
        };

        let outcome: Vec<String> = spawn_and_wait(&mut runtime, all_sets).expect("Answers");
        assert_eq!(outcome.len(), 100);
    });

    runtime.shutdown_now().wait().unwrap();
}
