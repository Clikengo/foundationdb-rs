// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Error types for the Fdb crate

use std;
use std::ffi::CStr;
use std::fmt::{self, Display};

use failure::{self, Backtrace, Context, Fail};

use foundationdb_sys as fdb_sys;
use options;

pub(crate) fn eval(error_code: fdb_sys::fdb_error_t) -> Result<()> {
    let rust_code = error_code as i32;
    if rust_code == 0 {
        Ok(())
    } else {
        Err(Error::from(error_code))
    }
}

/// The Standard Error type of FoundationDB
#[derive(Debug)]
pub struct Error {
    kind: Context<ErrorKind>,
    // Not all retryable error should be retried. For example, error_code=1020 transaction conflict
    // error is always retryable (so `Error::is_retryable` always returns `true`), while it
    // should not be retried if `TransactionOption::RetryLimit` is exceeded. So we should keep
    // track whether the error should be retried or not.
    should_retry: bool,
}

/// An error from Fdb with associated code and message
#[derive(Debug, Fail)]
pub enum ErrorKind {
    /// Errors that originate from the FoundationDB layers
    #[fail(display = "FoundationDB error({}): {}", error_code, error_str)]
    Fdb {
        /// The FoundationDB error code
        error_code: i32,
        /// The error string as defined by FoundationDB
        error_str: &'static str,
    },
    /// Other errors not directly attributed to the FoundationDB layers
    #[fail(display = "FoundationDB-RS error: {}", error)]
    Other {
        /// The cause of this error
        error: failure::Error,
    },
}

/// An Fdb Result type
pub type Result<T> = std::result::Result<T, Error>;

impl Fail for Error {
    fn cause(&self) -> Option<&Fail> {
        self.kind.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.kind.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.kind, f)
    }
}

impl From<failure::Error> for Error {
    fn from(error: failure::Error) -> Self {
        Error::from_other(error)
    }
}

impl Error {
    /// Converts from the raw Fdb error code into an `Error`
    pub fn from(error_code: fdb_sys::fdb_error_t) -> Self {
        let error_str = unsafe { CStr::from_ptr(fdb_sys::fdb_get_error(error_code)) };

        Error {
            kind: Context::new(ErrorKind::Fdb {
                error_code: error_code as i32,
                error_str: error_str
                    .to_str()
                    .expect("bad error string from FoundationDB"),
            }),
            should_retry: false,
        }
    }

    /// Creates an error from another error or failure
    pub fn from_other(error: failure::Error) -> Self {
        Error {
            kind: Context::new(ErrorKind::Other { error }),
            should_retry: false,
        }
    }

    /// Indicates the transaction may have succeeded, though not in a way the system can verify.
    pub fn is_maybe_committed(&self) -> bool {
        match *self.kind.get_context() {
            ErrorKind::Fdb { error_code, .. } => {
                let check = unsafe {
                    fdb_sys::fdb_error_predicate(
                        options::ErrorPredicate::MaybeCommitted.code() as i32,
                        error_code,
                    ) as i32
                };

                check != 0
            }
            _ => false,
        }
    }

    /// Indicates the operations in the transactions should be retried because of transient error.
    pub fn is_retryable(&self) -> bool {
        match *self.kind.get_context() {
            ErrorKind::Fdb { error_code, .. } => {
                let check = unsafe {
                    fdb_sys::fdb_error_predicate(
                        options::ErrorPredicate::Retryable.code() as i32,
                        error_code,
                    ) as i32
                };

                check != 0
            }
            _ => false,
        }
    }

    /// Indicates the transaction has not committed, though in a way that can be retried.
    pub fn is_retryable_not_committed(&self) -> bool {
        match *self.kind.get_context() {
            ErrorKind::Fdb { error_code, .. } => {
                let check = unsafe {
                    fdb_sys::fdb_error_predicate(
                        options::ErrorPredicate::RetryableNotCommitted.code() as i32,
                        error_code,
                    ) as i32
                };

                check != 0
            }
            _ => false,
        }
    }

    /// Error code
    pub fn code(&self) -> i32 {
        match *self.kind.get_context() {
            ErrorKind::Fdb { error_code, .. } => error_code,
            _ => 4000,
        }
    }

    pub(crate) fn set_should_retry(&mut self, should_retry: bool) {
        self.should_retry = should_retry;
    }

    pub(crate) fn should_retry(&self) -> bool {
        self.should_retry
    }
}
