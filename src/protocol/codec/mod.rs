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

#[cfg(feature = "tokio_codec")]
pub(crate) mod tokio;

#[cfg(feature = "with_async_std")]
pub(crate) use encode::encode;

#[cfg(feature = "with_async_std")]
pub(crate) use decode::decode;
