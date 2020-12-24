/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::str;

use bytes::BytesMut;

use crate::{error::Error, protocol::resp::RespValue};

type DecodeResult = Result<Option<(usize, RespValue)>, Error>;

#[inline]
fn parse_error(message: String) -> Error {
    Error::RESP(message, None)
}

/// Many RESP types have their length (which is either bytes or "number of elements", depending on context)
/// encoded as a string, terminated by "\r\n", this looks for them.
///
/// Only return the string if the whole sequence is complete, including the terminator bytes (but those final
/// two bytes will not be returned)
///
/// TODO - rename this function potentially, it's used for simple integers too
fn scan_integer(buf: &mut BytesMut, idx: usize) -> Result<Option<(usize, &[u8])>, Error> {
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
            (false, b'0'..=b'9') => (),
            (false, b'-') => (),
            (_, val) => {
                return Err(parse_error(format!(
                    "Unexpected byte in size_string: {}",
                    val
                )));
            }
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

fn decode_raw_integer(buf: &mut BytesMut, idx: usize) -> Result<Option<(usize, i64)>, Error> {
    match scan_integer(buf, idx) {
        Ok(None) => Ok(None),
        Ok(Some((pos, int_str))) => {
            // Redis integers are transmitted as strings, so we first convert the raw bytes into a string...
            match str::from_utf8(int_str) {
                Ok(string) => {
                    // ...and then parse the string.
                    match string.parse() {
                        Ok(int) => Ok(Some((pos, int))),
                        Err(_) => Err(parse_error(format!("Not an integer: {}", string))),
                    }
                }
                Err(_) => Err(parse_error(format!("Not a valid string: {:?}", int_str))),
            }
        }
        Err(e) => Err(e),
    }
}

fn decode_bulk_string(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match decode_raw_integer(buf, idx) {
        Ok(None) => Ok(None),
        Ok(Some((pos, -1))) => Ok(Some((pos, RespValue::Nil))),
        Ok(Some((pos, size))) if size >= 0 => {
            let size = size as usize;
            let remaining = buf.len() - pos;
            let required_bytes = size + 2;

            if remaining < required_bytes {
                return Ok(None);
            }

            let bulk_string = RespValue::BulkString(buf[pos..(pos + size)].to_vec());
            Ok(Some((pos + required_bytes, bulk_string)))
        }
        Ok(Some((_, size))) => Err(parse_error(format!("Invalid string size: {}", size))),
        Err(e) => Err(e),
    }
}

fn decode_array(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match decode_raw_integer(buf, idx) {
        Ok(None) => Ok(None),
        Ok(Some((pos, -1))) => Ok(Some((pos, RespValue::Nil))),
        Ok(Some((pos, size))) if size >= 0 => {
            let size = size as usize;
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
        Ok(Some((_, size))) => Err(parse_error(format!("Invalid array size: {}", size))),
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
#[allow(clippy::unknown_clippy_lints, clippy::unnecessary_wraps)]
fn decode_simple_string(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match scan_string(buf, idx) {
        None => Ok(None),
        Some((pos, string)) => Ok(Some((pos, RespValue::SimpleString(string)))),
    }
}

#[allow(clippy::unknown_clippy_lints, clippy::unnecessary_wraps)]
fn decode_error(buf: &mut BytesMut, idx: usize) -> DecodeResult {
    match scan_string(buf, idx) {
        None => Ok(None),
        Some((pos, string)) => Ok(Some((pos, RespValue::Error(string)))),
    }
}

pub(crate) fn decode(buf: &mut BytesMut, idx: usize) -> DecodeResult {
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
