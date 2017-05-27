// TODO - this stuff - all options
#[derive(Debug)]
pub enum RespValue {
    Array(Vec<RespValue>),
    BulkString(Vec<u8>)
}

impl<'a> From<&'a str> for RespValue {
    fn from(a: &'a str) -> RespValue {
        RespValue::BulkString(a.as_bytes().to_vec())
    }
}

impl<'a> From<(&'a str, &'a str)> for RespValue {
    fn from((a, b): (&'a str, &'a str)) -> RespValue {
        RespValue::Array(vec![a.into(), b.into()])
    }
}

impl<'a> From<(&'a str, &'a str, &'a str)> for RespValue {
    fn from((a, b, c): (&'a str, &'a str, &'a str)) -> RespValue {
        RespValue::Array(vec![a.into(), b.into(), c.into()])
    }
}