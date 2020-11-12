/*
 * Copyright 2017-2019 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! An implementation of the RESP protocol

use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash},
    io, str,
};

use bytes::{Buf, BufMut, BytesMut};

use tokio_util::codec::{Decoder, Encoder};

use super::error::{self, Error};

/// A single RESP value, this owns the data that is read/to-be written to Redis.
///
/// It is cloneable to allow multiple copies to be delivered in certain circumstances, e.g. multiple
/// subscribers to the same topic.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RespValue {
    Nil,

    /// Zero, one or more other `RespValue`s.
    Array(Vec<RespValue>),

    /// A bulk string.  In Redis terminology a string is a byte-array, so this is stored as a
    /// vector of `u8`s to allow clients to interpret the bytes as appropriate.
    BulkString(Vec<u8>),

    /// An error from the Redis server
    Error(String),

    /// Redis documentation defines an integer as being a signed 64-bit integer:
    /// https://redis.io/topics/protocol#resp-integers
    Integer(i64),

    SimpleString(String),
}

impl RespValue {
    fn into_result(self) -> Result<RespValue, Error> {
        match self {
            RespValue::Error(string) => Err(Error::Remote(string)),
            x => Ok(x),
        }
    }

    /// Convenience function for building dynamic Redis commands with variable numbers of
    /// arguments, e.g. RPUSH
    ///
    /// This will panic if called for anything other than arrays
    pub fn append<T>(mut self, other: impl IntoIterator<Item = T>) -> Self
    where
        T: Into<RespValue>,
    {
        match self {
            RespValue::Array(ref mut vals) => {
                vals.extend(other.into_iter().map(|t| t.into()));
            }
            _ => panic!("Can only append to arrays"),
        }
        self
    }
}

/// A trait to be implemented for every time which can be read from a RESP value.
///
/// Implementing this trait on a type means that type becomes a valid return type for calls such as `send` on
/// `client::PairedConnection`
pub trait FromResp: Sized {
    /// Return a `Result` containing either `Self` or `Error`.  Errors can occur due to either: a) the particular
    /// `RespValue` being incompatible with the required type, or b) a remote Redis error occuring.
    fn from_resp(resp: RespValue) -> Result<Self, Error> {
        Self::from_resp_int(resp.into_result()?)
    }

    fn from_resp_int(resp: RespValue) -> Result<Self, Error>;
}

impl FromResp for RespValue {
    fn from_resp_int(resp: RespValue) -> Result<RespValue, Error> {
        Ok(resp)
    }
}

impl FromResp for String {
    fn from_resp_int(resp: RespValue) -> Result<String, Error> {
        match resp {
            RespValue::BulkString(ref bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
            RespValue::Integer(i) => Ok(i.to_string()),
            RespValue::SimpleString(string) => Ok(string),
            _ => Err(error::resp("Cannot convert into a string", resp)),
        }
    }
}

impl FromResp for Vec<u8> {
    fn from_resp_int(resp: RespValue) -> Result<Vec<u8>, Error> {
        match resp {
            RespValue::BulkString(bytes) => Ok(bytes),
            _ => Err(error::resp("Not a bulk string", resp)),
        }
    }
}

impl FromResp for i64 {
    fn from_resp_int(resp: RespValue) -> Result<i64, Error> {
        match resp {
            RespValue::Integer(i) => Ok(i),
            _ => Err(error::resp("Cannot be converted into an i64", resp)),
        }
    }
}

macro_rules! impl_fromresp_integers {
    ($($int_ty:ident),* $(,)*) => {
        $(
            #[allow(clippy::cast_lossless)]
            impl FromResp for $int_ty {
                fn from_resp_int(resp: RespValue) -> Result<Self, Error> {
                    i64::from_resp_int(resp).and_then(|x| {
                        // $int_ty::max_value() as i64 > 0 should be optimized out. It tests if
                        // the target integer type needs an "upper bounds" check
                        if x < ($int_ty::min_value() as i64)
                            || ($int_ty::max_value() as i64 > 0
                                && x > ($int_ty::max_value() as i64))
                        {
                            Err(error::resp(
                                concat!(
                                    "i64 value cannot be represented as {}",
                                    stringify!($int_ty),
                                ),
                                RespValue::Integer(x),
                            ))
                        } else {
                            Ok(x as $int_ty)
                        }
                    })
                }
            }
        )*
    };
}

impl_fromresp_integers!(isize, usize, i32, u32, u64);

impl FromResp for bool {
    fn from_resp_int(resp: RespValue) -> Result<bool, Error> {
        i64::from_resp_int(resp).and_then(|x| match x {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(error::resp(
                "i64 value cannot be represented as bool",
                RespValue::Integer(x),
            )),
        })
    }
}

impl<T: FromResp> FromResp for Option<T> {
    fn from_resp_int(resp: RespValue) -> Result<Option<T>, Error> {
        match resp {
            RespValue::Nil => Ok(None),
            x => Ok(Some(T::from_resp_int(x)?)),
        }
    }
}

impl<T: FromResp> FromResp for Vec<T> {
    fn from_resp_int(resp: RespValue) -> Result<Vec<T>, Error> {
        match resp {
            RespValue::Array(ary) => {
                let mut ar = Vec::with_capacity(ary.len());
                for value in ary {
                    ar.push(T::from_resp(value)?);
                }
                Ok(ar)
            }
            _ => Err(error::resp("Cannot be converted into a vector", resp)),
        }
    }
}

impl<K: FromResp + Hash + Eq, T: FromResp, S: BuildHasher + Default> FromResp for HashMap<K, T, S> {
    fn from_resp_int(resp: RespValue) -> Result<HashMap<K, T, S>, Error> {
        match resp {
            RespValue::Array(ary) => {
                let mut map = HashMap::with_capacity_and_hasher(ary.len(), S::default());
                let mut items = ary.into_iter();

                while let Some(k) = items.next() {
                    let key = K::from_resp(k)?;
                    let value = T::from_resp(items.next().ok_or_else(|| {
                        error::resp(
                            "Cannot convert an odd number of elements into a hashmap",
                            "".into(),
                        )
                    })?)?;

                    map.insert(key, value);
                }

                Ok(map)
            }
            _ => Err(error::resp("Cannot be converted into a hashmap", resp)),
        }
    }
}

impl FromResp for () {
    fn from_resp_int(resp: RespValue) -> Result<(), Error> {
        match resp {
            RespValue::SimpleString(string) => match string.as_ref() {
                "OK" => Ok(()),
                _ => Err(Error::RESP(
                    format!("Unexpected value within SimpleString: {}", string),
                    None,
                )),
            },
            _ => Err(error::resp("Unexpected value", resp)),
        }
    }
}

impl<A, B> FromResp for (A, B)
where
    A: FromResp,
    B: FromResp,
{
    fn from_resp_int(resp: RespValue) -> Result<(A, B), Error> {
        match resp {
            RespValue::Array(ary) => {
                if ary.len() == 2 {
                    let mut ary_iter = ary.into_iter();
                    Ok((
                        A::from_resp(ary_iter.next().expect("No value"))?,
                        B::from_resp(ary_iter.next().expect("No value"))?,
                    ))
                } else {
                    Err(Error::RESP(
                        format!("Array needs to be 2 elements, is: {}", ary.len()),
                        None,
                    ))
                }
            }
            _ => Err(error::resp("Unexpected value", resp)),
        }
    }
}

impl<A, B, C> FromResp for (A, B, C)
where
    A: FromResp,
    B: FromResp,
    C: FromResp,
{
    fn from_resp_int(resp: RespValue) -> Result<(A, B, C), Error> {
        match resp {
            RespValue::Array(ary) => {
                if ary.len() == 3 {
                    let mut ary_iter = ary.into_iter();
                    Ok((
                        A::from_resp(ary_iter.next().expect("No value"))?,
                        B::from_resp(ary_iter.next().expect("No value"))?,
                        C::from_resp(ary_iter.next().expect("No value"))?,
                    ))
                } else {
                    Err(Error::RESP(
                        format!("Array needs to be 3 elements, is: {}", ary.len()),
                        None,
                    ))
                }
            }
            _ => Err(error::resp("Unexpected value", resp)),
        }
    }
}

/// Macro to create a RESP array, useful for preparing commands to send.  Elements can be any type, or a mixture
/// of types, that satisfy `Into<RespValue>`.
///
/// As a general rule, if a value is moved, the data can be deconstructed (if appropriate, e.g. String) and the raw
/// data moved into the corresponding `RespValue`.  If a reference is provided, the data will be copied instead.
///
/// # Examples
///
/// ```
/// #[macro_use]
/// extern crate redis_async;
///
/// fn main() {
///     let value = format!("something_{}", 123);
///     resp_array!["SET", "key_name", value];
/// }
/// ```
///
/// For variable length Redis commands:
///
/// ```
/// #[macro_use]
/// extern crate redis_async;
///
/// fn main() {
///     let data = vec!["data", "from", "somewhere", "else"];
///     let command = resp_array!["RPUSH", "mykey"].append(data);
/// }
/// ```
#[macro_export]
macro_rules! resp_array {
    ($($e:expr),*) => {
        {
            $crate::resp::RespValue::Array(vec![
                $(
                    $e.into(),
                )*
            ])
        }
    }
}

macro_rules! into_resp {
    ($t:ty, $f:ident) => {
        impl<'a> From<$t> for RespValue {
            fn from(from: $t) -> RespValue {
                from.$f()
            }
        }
    };
}

/// A specific trait to convert into a `RespValue::BulkString`
pub trait ToRespString {
    fn to_resp_string(self) -> RespValue;
}

macro_rules! string_into_resp {
    ($t:ty) => {
        into_resp!($t, to_resp_string);
    };
}

impl ToRespString for String {
    fn to_resp_string(self) -> RespValue {
        RespValue::BulkString(self.into_bytes())
    }
}
string_into_resp!(String);

impl<'a> ToRespString for &'a String {
    fn to_resp_string(self) -> RespValue {
        RespValue::BulkString(self.as_bytes().into())
    }
}
string_into_resp!(&'a String);

impl<'a> ToRespString for &'a str {
    fn to_resp_string(self) -> RespValue {
        RespValue::BulkString(self.as_bytes().into())
    }
}
string_into_resp!(&'a str);

impl<'a> ToRespString for &'a [u8] {
    fn to_resp_string(self) -> RespValue {
        RespValue::BulkString(self.to_vec())
    }
}
string_into_resp!(&'a [u8]);

impl ToRespString for Vec<u8> {
    fn to_resp_string(self) -> RespValue {
        RespValue::BulkString(self)
    }
}
string_into_resp!(Vec<u8>);

pub trait ToRespInteger {
    fn to_resp_integer(self) -> RespValue;
}

impl<T: ToRespInteger> From<T> for RespValue {
    fn from(value: T) -> Self {
        value.to_resp_integer()
    }
}

macro_rules! to_resp_integer_impls {
    ($($int:ty,)+) => {
        $(
            impl ToRespInteger for $int {
                fn to_resp_integer(self) -> RespValue {
                    RespValue::Integer(self as i64)
                }
            }
        )+
    }
}

to_resp_integer_impls! {
    i8, i16, i32, i64, isize,
    u8, u16, u32, u64, usize,
    i128,
    u128,
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

fn write_header(symb: u8, len: i64, buf: &mut BytesMut) {
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

impl Encoder<RespValue> for RespCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: RespValue, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match msg {
            RespValue::Nil => {
                write_header(b'$', -1, buf);
            }
            RespValue::Array(ary) => {
                write_header(b'*', ary.len() as i64, buf);
                for v in ary {
                    self.encode(v, buf)?;
                }
            }
            RespValue::BulkString(bstr) => {
                let len = bstr.len();
                write_header(b'$', len as i64, buf);
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

type DecodeResult = Result<Option<(usize, RespValue)>, Error>;

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
                buf.advance(pos);
                Ok(Some(item))
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::BytesMut;

    use tokio_util::codec::{Decoder, Encoder};

    use super::{Error, FromResp, RespCodec, RespValue};

    fn obj_to_bytes(obj: RespValue) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        let mut codec = RespCodec;
        codec.encode(obj, &mut bytes).unwrap();
        bytes.to_vec()
    }

    #[test]
    fn test_resp_array_macro() {
        let resp_object = resp_array!["SET", "x"];
        let bytes = obj_to_bytes(resp_object);
        assert_eq!(b"*2\r\n$3\r\nSET\r\n$1\r\nx\r\n", bytes.as_slice());

        let resp_object = resp_array!["RPUSH", "wyz"].append(vec!["a", "b"]);
        let bytes = obj_to_bytes(resp_object);
        assert_eq!(
            &b"*4\r\n$5\r\nRPUSH\r\n$3\r\nwyz\r\n$1\r\na\r\n$1\r\nb\r\n"[..],
            bytes.as_slice()
        );

        let vals = vec![String::from("a"), String::from("b")];
        let resp_object = resp_array!["RPUSH", "xyz"].append(&vals);
        let bytes = obj_to_bytes(resp_object);
        assert_eq!(
            &b"*4\r\n$5\r\nRPUSH\r\n$3\r\nxyz\r\n$1\r\na\r\n$1\r\nb\r\n"[..],
            bytes.as_slice()
        );
    }

    #[test]
    fn test_bulk_string() {
        let resp_object = RespValue::BulkString(b"THISISATEST".to_vec());
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
        assert_eq!(
            b"*2\r\n$5\r\nTEST1\r\n$5\r\nTEST2\r\n".to_vec(),
            bytes.to_vec()
        );

        let deserialized = codec.decode(&mut bytes).unwrap().unwrap();
        assert_eq!(deserialized, resp_object);
    }

    #[test]
    fn test_nil_string() {
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&b"$-1\r\n"[..]);

        let mut codec = RespCodec;
        let deserialized = codec.decode(&mut bytes).unwrap().unwrap();
        assert_eq!(deserialized, RespValue::Nil);
    }

    #[test]
    fn test_integer_overflow() {
        let resp_object = RespValue::Integer(i64::max_value());
        let res = i32::from_resp(resp_object);
        assert!(res.is_err());
    }

    #[test]
    fn test_integer_underflow() {
        let resp_object = RespValue::Integer(-2);
        let res = u64::from_resp(resp_object);
        assert!(res.is_err());
    }

    #[test]
    fn test_integer_convesion() {
        let resp_object = RespValue::Integer(50);
        assert_eq!(u32::from_resp(resp_object).unwrap(), 50);
    }

    #[test]
    fn test_hashmap_conversion() {
        let mut expected = HashMap::new();
        expected.insert("KEY1".to_string(), "VALUE1".to_string());
        expected.insert("KEY2".to_string(), "VALUE2".to_string());

        let resp_object = RespValue::Array(vec![
            "KEY1".into(),
            "VALUE1".into(),
            "KEY2".into(),
            "VALUE2".into(),
        ]);
        assert_eq!(
            HashMap::<String, String>::from_resp(resp_object).unwrap(),
            expected
        );
    }

    #[test]
    fn test_hashmap_conversion_fails_with_odd_length_array() {
        let resp_object = RespValue::Array(vec![
            "KEY1".into(),
            "VALUE1".into(),
            "KEY2".into(),
            "VALUE2".into(),
            "KEY3".into(),
        ]);
        let res = HashMap::<String, String>::from_resp(resp_object);

        match res {
            Err(Error::RESP(_, _)) => {}
            _ => panic!("Should not be able to convert an odd number of elements to a hashmap"),
        }
    }
}
