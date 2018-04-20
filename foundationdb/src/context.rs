use std::os::raw::c_int;

use failure::Error;
use foundationdb_sys as fdb;

struct Context {}

impl Context {
    /// Defaults to current API version in the sys crate
    fn new() -> Result<Self, Error> {
        Self::init(fdb::FDB_API_VERSION as i32, fdb::FDB_API_VERSION as i32)
    }

    fn init(runtime_version: i32, header_version: i32) -> Result<Self, Error> {
        unsafe {
            let error_code = fdb::fdb_select_api_version_impl(runtime_version, header_version);
            if error_code != 0 {
                Err(format_err!(
                    "invalid runtime_version: {} or header_version: {} code: {}",
                    runtime_version,
                    header_version,
                    error_code
                ))
            } else {
                Ok(Context {})
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_init_context() {
        Context::new().expect("failed to initialize context");
    }
}
