/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use bytes::{Buf, BytesMut};

use tokio_util::codec::Decoder;

use crate::{
    error::Error,
    protocol::{codec::decode::decode, resp::RespValue},
};

use super::RespCodec;

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match decode(buf, 0) {
            Ok(None) => Ok(None),
            Ok(Some((pos, item))) => {
                buf.advance(pos);
                Ok(Some(item))
            }
            Err(e) => Err(e),
        }
    }
}
