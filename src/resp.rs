/*
 * Copyright 2017 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! An implementation of the RESP protocol

use std::io;
use std::str;

use bytes::{BufMut, BytesMut};

use tokio_io::codec::{Decoder, Encoder};

use super::error::Error;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RespValue {
    Array(Vec<RespValue>),
    BulkString(Vec<u8>),
    Error(String),
    Integer(usize),
    SimpleString(String),
}

impl RespValue {
    pub fn into_string(self) -> Result<String, Error> {
        match self {
            RespValue::Array(_) => Err(Error::RESP("Not stringable".into())),
            RespValue::BulkString(ref bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
            RespValue::Error(string) => Ok(string),
            RespValue::Integer(i) => Ok(i.to_string()),
            RespValue::SimpleString(string) => Ok(string),
        }
    }
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

impl<'a> ToResp for String {
    fn to_resp(&self) -> RespValue {
        RespValue::BulkString(self.as_bytes().into())
    }
}

impl<'a, T> ToResp for Vec<T>
    where T: ToResp
{
    fn to_resp(&self) -> RespValue {
        RespValue::Array(self.into_iter().map(|v| v.to_resp()).collect())
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

fn write_simple_string(symb: u8, string: &str, buf: &mut BytesMut) {
    let bytes = string.as_bytes();
    let size = 1 + bytes.len() + 2;
    check_and_reserve(buf, size);
    buf.put_u8(symb);
    buf.extend(bytes);
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
            RespValue::Error(ref string) => {
                write_simple_string(b'-', string, buf);
            }
            RespValue::Integer(val) => {
                // Simple integer are just the header
                write_header(b':', val, buf);
            }
            RespValue::SimpleString(ref string) => {
                write_simple_string(b'+', string, buf);
            }
        }
        Ok(())
    }
}

#[inline]
fn parse_error(message: String) -> Error {
    Error::RESP(message)
}

/// Many RESP types have their length (which is either bytes or "number of elements", depending on context)
/// encoded as a string, terminated by "\r\n", this looks for them.
///
/// Only return the string if the whole sequence is complete, including the terminator bytes (but those final
/// two bytes will not be returned)
///
/// TODO - rename this function potentially, it's used for simple integers too
fn scan_integer<'a>(buf: &'a mut BytesMut, idx: usize) -> Result<Option<(usize, &'a [u8])>, Error> {
    let length = buf.len();
    let mut at_end = false;
    let mut pos = idx;
    loop {
        if length <= pos {
            return Ok(None);
        }
        match (at_end, buf[pos]) {
            (true, b'\n') => return Ok(Some((pos + 1, &buf[idx..pos - 1]))),
            (false, b'\r') => at_end = true,
            (false, b'0'...b'9') => (),
            (_, val) => return Err(parse_error(format!("Unexpected byte in size_string: {}", val))),
        }
        pos += 1;
    }
}

fn scan_string(buf: &mut BytesMut, idx: usize) -> Option<(usize, String)> {
    let length = buf.len();
    let mut at_end = false;
    let mut pos = idx;
    loop {
        if length <= pos {
            return None;
        }
        match (at_end, buf[pos]) {
            (true, b'\n') => {
                let value = String::from_utf8_lossy(&buf[idx..pos - 1]).into_owned();
                return Some((pos + 1, value));
            }
            (true, _) => at_end = false,
            (false, b'\r') => at_end = true,
            (false, _) => (),
        }
        pos += 1;
    }
}

fn decode_raw_integer(buf: &mut BytesMut, idx: usize) -> Result<Option<(usize, usize)>, Error> {
    match scan_integer(buf, idx) {
        Ok(None) => Ok(None),
        Ok(Some((pos, int_str))) => {
            let int: usize = str::from_utf8(int_str)
                .expect("Not a string")
                .parse()
                .expect("Not an integer");
            Ok(Some((pos, int)))
        }
        Err(e) => Err(e),
    }
}

type DecodeResult = Result<Option<(usize, RespValue)>, Error>;

fn decode_bulk_string(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match decode_raw_integer(buf, idx) {
        Ok(None) => Ok(None),
        Ok(Some((pos, size))) => {
            let remaining = buf.len() - pos;
            let required_bytes = size + 2;

            if remaining < required_bytes {
                return Ok(None);
            }

            let bulk_string = RespValue::BulkString(buf[pos..(pos + size)].to_vec());
            Ok(Some((pos + required_bytes, bulk_string)))
        }
        Err(e) => Err(e),
    }
}

fn decode_array(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match decode_raw_integer(buf, idx) {
        Ok(None) => Ok(None),
        Ok(Some((pos, size))) => {
            let mut pos = pos;
            let mut values = Vec::with_capacity(size);
            for _ in 0..size {
                match decode(buf, pos) {
                    Ok(None) => return Ok(None),
                    Ok(Some((new_pos, value))) => {
                        values.push(value);
                        pos = new_pos;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(Some((pos, RespValue::Array(values))))
        }
        Err(e) => Err(e),
    }
}

fn decode_integer(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match decode_raw_integer(buf, idx) {
        Ok(None) => Ok(None),
        Ok(Some((pos, int))) => Ok(Some((pos, RespValue::Integer(int)))),
        Err(e) => Err(e),
    }
}

/// A simple string is any series of bytes that ends with `\r\n`
fn decode_simple_string(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match scan_string(buf, idx) {
        None => Ok(None),
        Some((pos, string)) => Ok(Some((pos, RespValue::SimpleString(string)))),
    }
}

fn decode_error(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match scan_string(buf, idx) {
        None => Ok(None),
        Some((pos, string)) => Ok(Some((pos, RespValue::Error(string)))),
    }
}

fn decode(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    let length = buf.len();
    if length <= idx {
        return Ok(None);
    }

    let first_byte = buf[idx];
    match first_byte {
        b'$' => decode_bulk_string(buf, idx + 1),
        b'*' => decode_array(buf, idx + 1),
        b':' => decode_integer(buf, idx + 1),
        b'+' => decode_simple_string(buf, idx + 1),
        b'-' => decode_error(buf, idx + 1),
        _ => Err(parse_error(format!("Unexpected byte: {}", first_byte))),
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match decode(buf, 0) {
            Ok(None) => Ok(None),
            Ok(Some((pos, item))) => {
                buf.split_to(pos);
                Ok(Some(item))
            }
            Err(e) => Err(e),
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

    #[test]
    fn test_array() {
        let resp_object = RespValue::Array(vec!["TEST1".into(), "TEST2".into()]);
        let mut bytes = BytesMut::new();
        let mut codec = RespCodec;
        codec.encode(resp_object.clone(), &mut bytes).unwrap();
        assert_eq!(b"*2\r\n$5\r\nTEST1\r\n$5\r\nTEST2\r\n".to_vec(),
                   bytes.to_vec());

        let deserialized = codec.decode(&mut bytes).unwrap().unwrap();
        assert_eq!(deserialized, resp_object);
    }
}