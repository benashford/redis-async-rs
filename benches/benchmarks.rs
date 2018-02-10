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

extern crate env_logger;
extern crate futures;
extern crate futures_cpupool;

#[macro_use]
extern crate log;

extern crate tokio;

#[macro_use]
extern crate redis_async;

use std::net::SocketAddr;
use std::sync::Arc;

use test::Bencher;

use futures::Future;
use futures::future;
use futures::future::Executor;
use futures::sync::oneshot;

use futures_cpupool::CpuPool;

use tokio::executor::current_thread;

use redis_async::client;
use redis_async::resp;

fn open_paired_connection(addr: &SocketAddr, cpu_pool: CpuPool) -> client::PairedConnection {
    let rx = current_thread::run(move |_| {
        let (tx, rx) = oneshot::channel();
        let connection = client::paired_connect(addr, cpu_pool)
            .map_err(|e| println!("Error opening connection: {}", e))
            .and_then(|con| match tx.send(con) {
                Ok(_) => future::ok(()),
                Err(e) => future::err(()),
            });
        current_thread::spawn(connection);
        rx
    });
    rx.wait().expect("Connection")
}

#[bench]
fn bench_simple_getsetdel(b: &mut Bencher) {
    let cpu_pool = CpuPool::new_num_cpus();
    let addr = "127.0.0.1:6379".parse().unwrap();

    let connection = open_paired_connection(&addr, cpu_pool.clone());

    b.iter(|| {
        faf!(connection.send(resp_array!["SET", "test_key", "42"]));
        let get = connection.send(resp_array!["GET", "test_key"]);
        let del = connection.send(resp_array!["DEL", "test_key"]);
        let get_set = get.join(del);
        let cpu_f = cpu_pool.spawn(get_set);
        let result: (String, String) = cpu_f.wait().expect("No result");
    });
}

// #[bench]
// fn bench_big_pipeline(b: &mut Bencher) {
//     let mut core = Core::new().unwrap();
//     let addr = "127.0.0.1:6379".parse().unwrap();

//     let connection = client::paired_connect(&addr, &core.handle());
//     let connection = core.run(connection).unwrap();

//     let data_size = 100;

//     b.iter(|| {
//         for x in 0..data_size {
//             let test_key = format!("test_{}", x);
//             faf!(connection.send(resp_array!["SET", test_key, x.to_string()]));
//         }
//         let mut gets = Vec::with_capacity(data_size);
//         for x in 0..data_size {
//             let test_key = format!("test_{}", x);
//             gets.push(connection.send(resp_array!["GET", test_key]));
//         }
//         let last_get = gets.remove(data_size - 1);
//         let _: String = core.run(last_get).unwrap();
//     });
// }

#[bench]
fn bench_complex_pipeline(b: &mut Bencher) {
    let cpu_pool = CpuPool::new_num_cpus();
    let addr = "127.0.0.1:6379".parse().unwrap();

    let connection_outer = Arc::new(open_paired_connection(&addr, cpu_pool.clone()));

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

        let cpu_f = cpu_pool.spawn(all_sets);
        let result: Vec<String> = cpu_f.wait().expect("No result");
        assert_eq!(result.len(), data_size);
    });

    println!("{:?}", cpu_pool);
}
