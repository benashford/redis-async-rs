/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

#[macro_use]
pub(crate) mod resp;

pub(crate) mod codec;

#[cfg(feature = "tokio_codec")]
pub(crate) use codec::tokio::RespCodec;

pub use resp::{FromResp, RespValue};
