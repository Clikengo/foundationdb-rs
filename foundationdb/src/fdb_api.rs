// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the Fdb API versioning C API
//!
//! https://apple.github.io/foundationdb/api-c.html#api-versioning

use error::{self, Result};
use foundationdb_sys as fdb_sys;
use network::NetworkBuilder;

/// Returns the max api version of the underlying Fdb C API Client
pub fn get_max_api_version() -> i32 {
    unsafe { fdb_sys::fdb_get_max_api_version() as i32 }
}

/// A type which represents the successful innitialization of an Fdb API Version
pub struct FdbApi(private::PrivateFdbApi);

// forces the FdnApi construction to be private to this module
mod private {
    pub(super) struct PrivateFdbApi;
}

impl FdbApi {
    /// Returns a NetworkBuilder for configuring the Fdb Network options
    pub fn network(self) -> NetworkBuilder {
        NetworkBuilder::from(self)
    }
}

/// A Builder with which different versions of the Fdb C API can be initialized
pub struct FdbApiBuilder {
    runtime_version: i32,
    header_version: i32,
}

impl FdbApiBuilder {
    /// The version of run-time behavior the API is requested to provide.
    ///
    /// Must be less than or equal to header_version, `foundationdb_sys::FDB_API_VERSION`, and should almost always be equal.
    /// Language bindings which themselves expose API versioning will usually pass the version requested by the application.
    pub fn set_runtime_version(mut self, version: i32) -> Self {
        self.runtime_version = version;
        self
    }

    /// The version of the ABI (application binary interface) that the calling code expects to find in the shared library.
    ///
    /// If you are using an FFI, this must correspond to the version of the API you are using as a reference (currently 510). For example, the number of arguments that a function takes may be affected by this value, and an incorrect value is unlikely to yield success.
    ///
    /// TODO: it is likely that this should never be changed, and be pinned to the sys crates versions... may be removed.
    pub fn set_header_version(mut self, version: i32) -> Self {
        self.header_version = version;
        self
    }

    /// The API version can only be initialized once in the lifetime of a process
    pub fn build(self) -> Result<FdbApi> {
        unsafe {
            error::eval(fdb_sys::fdb_select_api_version_impl(
                self.runtime_version,
                self.header_version,
            ))?;
        }

        Ok(FdbApi(private::PrivateFdbApi))
    }
}

impl Default for FdbApiBuilder {
    fn default() -> Self {
        FdbApiBuilder {
            runtime_version: fdb_sys::FDB_API_VERSION as i32,
            header_version: fdb_sys::FDB_API_VERSION as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_api() {
        assert!(get_max_api_version() > 0);
    }
}
