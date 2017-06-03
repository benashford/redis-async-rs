use std::io;

use bytes::BytesMut;

use tokio_io::codec::{Decoder, Encoder};

// TODO - this stuff - all options
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RespValue {
    Array(Vec<RespValue>),
    BulkString(Vec<u8>)
}

pub trait ToResp {
    fn to_resp(&self) -> RespValue;
}

impl<'a> ToResp for &'a str {
    fn to_resp(&self) -> RespValue {
        RespValue::BulkString(self.as_bytes().into())
    }
}

impl<'a> ToResp for &'a [&'a str] {
    fn to_resp(&self) -> RespValue {
        RespValue::Array(self.as_ref().iter().map(|x| x.to_resp()).collect())
    }
}

impl<T: ToResp> From<T> for RespValue {
    fn from(from: T) -> RespValue {
        from.to_resp()
    }
}

/// Codec to read frames
pub struct RespCodec;

impl Encoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn encode(&mut self, msg: RespValue, buf: &mut BytesMut) -> io::Result<()> {
        unimplemented!()
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
        unimplemented!()
    }
}