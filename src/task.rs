/*
 * Copyright 2020 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::future::Future;

#[cfg(feature = "with_tokio")]
pub(crate) fn spawn<F>(f: F)
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    tokio::spawn(f);
}

#[cfg(feature = "with_async_std")]
pub(crate) fn spawn<F>(f: F)
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    async_global_executor::spawn(f).detach()
}
