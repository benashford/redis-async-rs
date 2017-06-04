use std::io;

use bytes::{BufMut, BytesMut};

use tokio_io::codec::{Decoder, Encoder};

// TODO - this stuff - all Resp types
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

#[inline]
fn write_rn(buf: &mut BytesMut) {
    buf.put_u8(b'\r');
    buf.put_u8(b'\n');
}

#[inline]
fn check_and_reserve(buf: &mut BytesMut, amt: usize) {
    let remaining_bytes = buf.remaining_mut();
    if remaining_bytes < amt {
        buf.reserve(amt);
    }
}

#[inline]
fn write_header(symb: u8, len: usize, buf: &mut BytesMut) {
    let len_as_string = len.to_string();
    let len_as_bytes = len_as_string.as_bytes();
    let header_bytes = 1 + len_as_bytes.len() + 2;
    check_and_reserve(buf, header_bytes);
    buf.put_u8(symb);
    buf.extend(len_as_bytes);
    write_rn(buf);
}

impl Encoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn encode(&mut self, msg: RespValue, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match msg {
            RespValue::Array(ary) => {
                write_header(b'*', ary.len(), buf);
                for v in ary {
                    self.encode(v, buf)?;
                }
            }
            RespValue::BulkString(bstr) => {
                let len = bstr.len();
                write_header(b'$', len, buf);
                check_and_reserve(buf, len + 2);
                buf.extend(bstr);
                write_rn(buf);
            }
        }
        Ok(())
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}