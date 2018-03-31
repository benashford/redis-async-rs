/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::io;
use std::net::SocketAddr;

use futures::{Future, Stream};

use tokio_tcp::TcpStream;

use tokio_io::{AsyncRead, codec::Framed};

use resp;

pub type RespConnection = Framed<TcpStream, resp::RespCodec>;

/// Connect to a Redis server and return paired Sink and Stream for reading and writing
/// asynchronously.
pub fn connect(addr: &SocketAddr) -> Box<Future<Item = RespConnection, Error = io::Error>> {
    Box::new(TcpStream::connect(addr).map(move |socket| socket.framed(resp::RespCodec)))
}
