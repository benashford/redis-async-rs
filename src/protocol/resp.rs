/*
 * Copyright 2017-2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! An implementation of the RESP protocol

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::str;
use std::sync::Arc;

use crate::error::{self, Error};

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

    /// Push item to Resp array
    ///
    /// This will panic if called for anything other than arrays
    pub fn push<T: Into<RespValue>>(&mut self, item: T) {
        match self {
            RespValue::Array(ref mut vals) => {
                vals.push(item.into());
            }
            _ => panic!("Can only push to arrays"),
        }
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
    ($($e:expr),* $(,)?) => {
        {
            $crate::protocol::RespValue::Array(vec![
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

impl ToRespString for Arc<str> {
    fn to_resp_string(self) -> RespValue {
        RespValue::BulkString(self.as_bytes().into())
    }
}
string_into_resp!(Arc<str>);

pub trait ToRespInteger {
    fn to_resp_integer(self) -> RespValue;
}

macro_rules! integer_into_resp {
    ($t:ty) => {
        into_resp!($t, to_resp_integer);
    };
}

impl ToRespInteger for i64 {
    fn to_resp_integer(self) -> RespValue {
        RespValue::Integer(self)
    }
}
integer_into_resp!(i64);

macro_rules! impl_toresp_integers {
    ($($int_ty:ident),* $(,)*) => {
        $(
            impl ToRespInteger for $int_ty {
                fn to_resp_integer(self) -> RespValue {
                    let new_self = self as i64;
                    new_self.to_resp_integer()
                }
            }
            integer_into_resp!($int_ty);
        )*
    };
}

impl_toresp_integers!(isize, i32, u32);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::error::Error;

    use super::{FromResp, RespValue};

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
