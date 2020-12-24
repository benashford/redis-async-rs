/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::io;

use bytes::BytesMut;

use tokio_util::codec::Encoder;

use crate::protocol::{codec::encode::encode, resp::RespValue};

use super::RespCodec;

impl Encoder<RespValue> for RespCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: RespValue, buf: &mut BytesMut) -> Result<(), Self::Error> {
        encode(msg, buf);
        Ok(())
    }
}
