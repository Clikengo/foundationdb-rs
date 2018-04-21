use std::ffi::CStr;

use foundationdb_sys as fdb;

#[derive(Debug, Fail)]
#[fail(display = "FoundationDB error({}): {}", error_code, error_str)]
pub struct FdbError {
    error_code: i32,
    error_str: &'static str,
}

impl FdbError {
    pub fn from(error_code: fdb::fdb_error_t) -> Self {
        let error_str = unsafe { CStr::from_ptr(fdb::fdb_get_error(error_code)) };

        FdbError {
            error_code: error_code as i32,
            error_str: error_str
                .to_str()
                .expect("bad error string from FoundationDB"),
        }
    }

    pub fn is_maybe_committed(&self) -> bool {
        let check = unsafe {
            fdb::fdb_error_predicate(
                FdbErrorPredicate::MaybeCommitted.into_fdb_predicate() as i32,
                self.error_code,
            ) as i32
        };

        check != 0
    }

    pub fn is_retryable(&self) -> bool {
        let check = unsafe {
            fdb::fdb_error_predicate(
                FdbErrorPredicate::Retryable.into_fdb_predicate() as i32,
                self.error_code,
            ) as i32
        };

        check != 0
    }

    pub fn is_retryable_not_committed(&self) -> bool {
        let check = unsafe {
            fdb::fdb_error_predicate(
                FdbErrorPredicate::RetryableNotCommitted.into_fdb_predicate() as i32,
                self.error_code,
            ) as i32
        };

        check != 0
    }
}

enum FdbErrorPredicate {
    MaybeCommitted,
    Retryable,
    RetryableNotCommitted,
}

impl FdbErrorPredicate {
    fn into_fdb_predicate(&self) -> fdb::FDBErrorPredicate {
        match *self {
            FdbErrorPredicate::MaybeCommitted => {
                fdb::FDBErrorPredicate_FDB_ERROR_PREDICATE_MAYBE_COMMITTED
            }
            FdbErrorPredicate::Retryable => fdb::FDBErrorPredicate_FDB_ERROR_PREDICATE_RETRYABLE,
            FdbErrorPredicate::RetryableNotCommitted => {
                fdb::FDBErrorPredicate_FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED
            }
        }
    }
}
