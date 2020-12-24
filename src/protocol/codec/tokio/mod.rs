/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

mod decode;
mod encode;

/// Codec to read frames
pub struct RespCodec;

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use tokio_util::codec::{Decoder, Encoder};

    use crate::protocol::resp::RespValue;

    use super::RespCodec;

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
}
