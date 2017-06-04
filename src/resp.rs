use std::io;
use std::str;

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

fn write_rn(buf: &mut BytesMut) {
    buf.put_u8(b'\r');
    buf.put_u8(b'\n');
}

fn check_and_reserve(buf: &mut BytesMut, amt: usize) {
    let remaining_bytes = buf.remaining_mut();
    if remaining_bytes < amt {
        buf.reserve(amt);
    }
}

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

#[inline]
fn parse_error(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message)
}

/// Many RESP types have their length (which is either bytes or "number of elements", depending on context)
/// encoded as a string, terminated by "\r\n", this looks for them.
///
/// Only return the string if the whole sequence is complete, including the terminator bytes (but those final
/// two bytes will not be returned)
fn decode_size_string<'a>(buf: &'a mut BytesMut, idx: usize) -> Result<Option<&'a [u8]>, io::Error> {
    let length = buf.len();
    let mut at_end = false;
    let mut pos = idx;
    loop {
        if length <= pos {
            return Ok(None)
        }
        match (at_end, buf[pos]) {
            (true, b'\n') => return Ok(Some(&buf[idx..pos - 1])),
            (false, b'\r') => { at_end = true },
            (false, b'0'...b'9') => (),
            _ => return Err(parse_error(format!("Unexpected byte in size_string: {}", buf[pos])))
        }
        pos += 1;
    }
}

type DecodeResult = Result<Option<(usize, RespValue)>, io::Error>;

fn decode_bulk_string(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    let (size_of_size, size) = {
        let size_string = match decode_size_string(buf, idx) {
            Ok(None) => return Ok(None),
            Ok(Some(size_str)) => size_str,
            Err(e) => return Err(e)
        };

        // Using `expect` rather then propagating an error as the validity of the size_string was tested by an
        // earlier step.
        let size:usize = str::from_utf8(size_string).expect("Valid UTF-8 string").parse().expect("Size as a string");
        (size_string.len(), size)
    };
    let pos = idx + size_of_size + 2;
    let remaining = buf.len() - pos;
    let required_bytes = size + 2;

    if remaining < required_bytes {
        return Ok(None)
    }

    let bulk_string = RespValue::BulkString(buf[pos..(pos + size)].to_vec());
    Ok(Some((pos + required_bytes, bulk_string)))
}

fn decode(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    let length = buf.len();
    if length <= idx {
        return Ok(None)
    }

    let first_byte = buf[idx];
    match first_byte {
        b'$' => decode_bulk_string(buf, idx + 1),
        _ => Err(parse_error(format!("Unexpected byte: {}", first_byte)))
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match decode(buf, 0) {
            Ok(None) => Ok(None),
            Ok(Some((pos, item))) => {
                buf.split_to(pos);
                Ok(Some(item))
            },
            Err(e) => Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use tokio_io::codec::{Decoder, Encoder};

    use super::{RespCodec, RespValue};

    #[test]
    fn test_bulk_string() {
        let resp_object = RespValue::BulkString("THISISATEST".as_bytes().to_vec());
        let mut bytes = BytesMut::new();
        let mut codec = RespCodec;
        codec.encode(resp_object.clone(), &mut bytes).unwrap();
        assert_eq!(b"$11\r\nTHISISATEST\r\n".to_vec(), bytes.to_vec());

        let deserialized = codec.decode(&mut bytes).unwrap().unwrap();
        assert_eq!(deserialized, resp_object);
    }
}