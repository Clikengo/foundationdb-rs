use error;
use foundationdb_sys as fdb;
use std;

pub enum NetworkOption {
    /// IP:PORT
    ///
    /// Deprecated
    LocalAddress(String),
    /// path to cluster file
    ///
    /// Deprecated
    ClusterFile(String),
    /// path to output directory (or NULL for current working directory)
    ///
    /// Enables trace output to a file in a directory of the clients choosing
    TraceEnable(String),
    /// max size of a single trace output file
    ///
    /// Sets the maximum size in bytes of a single trace output file. This value should be in the range ``[0, INT64_MAX]``. If the value is set to 0, there is no limit on individual file size. The default is a maximum size of 10,485,760 bytes.
    TraceRollSize(u32),
    /// max total size of trace files
    ///
    /// Sets the maximum size of all the trace output files put together. This value should be in the range ``[0, INT64_MAX]``. If the value is set to 0, there is no limit on the total size of the files. The default is a maximum size of 104,857,600 bytes. If the default roll size is used, this means that a maximum of 10 trace files will be written at a time.
    TraceMaxLogsSize(u32),
    /// value of the logGroup attribute
    ///
    /// Sets the 'logGroup' attribute with the specified value for all events in the trace output files. The default log group is 'default'.
    TraceLogGroup(String),
    /// knob_name=knob_value
    ///
    /// Set internal tuning or debugging knobs
    Knob(String),
    /// file path or linker-resolved name
    ///
    /// Set the TLS plugin to load. This option, if used, must be set before any other TLS options
    TlsPlugin(String),
    /// certificates
    ///
    /// Set the certificate chain
    TlsCertByte(Vec<u8>),
    /// file path
    ///
    /// Set the file from which to load the certificate chain
    TlsCertPath(String),
    /// key
    ///
    /// Set the private key corresponding to your own certificate
    TlsKeyByte(Vec<u8>),
    /// file path
    ///
    /// Set the file from which to load the private key corresponding to your own certificate
    TlsKeyPath(String),
    /// verification pattern
    ///
    /// Set the peer certificate field verification criteria
    TlsVerifyPeer(Vec<u8>),
    BuggifyEnable,
    BuggifyDisable,
    /// probability expressed as a percentage between 0 and 100
    ///
    /// Set the probability of a BUGGIFY section being active for the current execution.  Only applies to code paths first traversed AFTER this option is changed.
    BuggifySectionActivatedProbability(u32),
    /// probability expressed as a percentage between 0 and 100
    ///
    /// Set the probability of an active BUGGIFY section being fired
    BuggifySectionFiredProbability(u32),
    /// Disables the multi-version client API and instead uses the local client directly. Must be set before setting up the network.
    DisableMultiVersionClientApi,
    /// If set, callbacks from external client libraries can be called from threads created by the FoundationDB client library. Otherwise, callbacks will be called from either the thread used to add the callback or the network thread. Setting this option can improve performance when connected using an external client, but may not be safe to use in all environments. Must be set before setting up the network. WARNING: This feature is considered experimental at this time.
    CallbacksOnExternalThread,
    /// path to client library
    ///
    /// Adds an external client library for use by the multi-version client API. Must be set before setting up the network.
    ExternalClientLibrary(String),
    /// path to directory containing client libraries
    ///
    /// Searches the specified path for dynamic libraries and adds them to the list of client libraries for use by the multi-version client API. Must be set before setting up the network.
    ExternalClientDirectory(String),
    /// Prevents connections through the local client, allowing only connections through externally loaded client libraries. Intended primarily for testing.
    DisableLocalClient,
    /// Disables logging of client statistics, such as sampled transaction activity.
    DisableClientStatisticsLogging,
    /// Enables debugging feature to perform slow task profiling. Requires trace logging to be enabled. WARNING: this feature is not recommended for use in production.
    EnableSlowTaskProfiling,
    /// [release version],[source version],[protocol version];...
    ///
    /// This option is set automatically to communicate the list of supported clients to the active client.
    SupportedClientVersion(String),
    /// This option is set automatically on all clients loaded externally using the multi-version API.
    ExternalClient,
    /// Transport ID for the child connection
    ///
    /// This option tells a child on a multiversion client what transport ID to use.
    ExternalClientTransportId(u32),
}

impl NetworkOption {
    pub fn code(&self) -> fdb::FDBNetworkOption {
        match *self {
            NetworkOption::LocalAddress(ref _v) => 10,
            NetworkOption::ClusterFile(ref _v) => 20,
            NetworkOption::TraceEnable(ref _v) => 30,
            NetworkOption::TraceRollSize(ref _v) => 31,
            NetworkOption::TraceMaxLogsSize(ref _v) => 32,
            NetworkOption::TraceLogGroup(ref _v) => 33,
            NetworkOption::Knob(ref _v) => 40,
            NetworkOption::TlsPlugin(ref _v) => 41,
            NetworkOption::TlsCertByte(ref _v) => 42,
            NetworkOption::TlsCertPath(ref _v) => 43,
            NetworkOption::TlsKeyByte(ref _v) => 45,
            NetworkOption::TlsKeyPath(ref _v) => 46,
            NetworkOption::TlsVerifyPeer(ref _v) => 47,
            NetworkOption::BuggifyEnable => 48,
            NetworkOption::BuggifyDisable => 49,
            NetworkOption::BuggifySectionActivatedProbability(ref _v) => 50,
            NetworkOption::BuggifySectionFiredProbability(ref _v) => 51,
            NetworkOption::DisableMultiVersionClientApi => 60,
            NetworkOption::CallbacksOnExternalThread => 61,
            NetworkOption::ExternalClientLibrary(ref _v) => 62,
            NetworkOption::ExternalClientDirectory(ref _v) => 63,
            NetworkOption::DisableLocalClient => 64,
            NetworkOption::DisableClientStatisticsLogging => 70,
            NetworkOption::EnableSlowTaskProfiling => 71,
            NetworkOption::SupportedClientVersion(ref _v) => 1000,
            NetworkOption::ExternalClient => 1001,
            NetworkOption::ExternalClientTransportId(ref _v) => 1002,
        }
    }
    pub unsafe fn apply(&self) -> std::result::Result<(), error::FdbError> {
        let code = self.code();
        let err = match *self {
            NetworkOption::LocalAddress(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::ClusterFile(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TraceEnable(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TraceRollSize(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_network_set_option(code, data.as_ptr() as *const u8, 8)
            }
            NetworkOption::TraceMaxLogsSize(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_network_set_option(code, data.as_ptr() as *const u8, 8)
            }
            NetworkOption::TraceLogGroup(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::Knob(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TlsPlugin(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TlsCertByte(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TlsCertPath(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TlsKeyByte(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TlsKeyPath(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::TlsVerifyPeer(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::BuggifyEnable => fdb::fdb_network_set_option(code, std::ptr::null(), 0),
            NetworkOption::BuggifyDisable => fdb::fdb_network_set_option(code, std::ptr::null(), 0),
            NetworkOption::BuggifySectionActivatedProbability(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_network_set_option(code, data.as_ptr() as *const u8, 8)
            }
            NetworkOption::BuggifySectionFiredProbability(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_network_set_option(code, data.as_ptr() as *const u8, 8)
            }
            NetworkOption::DisableMultiVersionClientApi => {
                fdb::fdb_network_set_option(code, std::ptr::null(), 0)
            }
            NetworkOption::CallbacksOnExternalThread => {
                fdb::fdb_network_set_option(code, std::ptr::null(), 0)
            }
            NetworkOption::ExternalClientLibrary(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::ExternalClientDirectory(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::DisableLocalClient => {
                fdb::fdb_network_set_option(code, std::ptr::null(), 0)
            }
            NetworkOption::DisableClientStatisticsLogging => {
                fdb::fdb_network_set_option(code, std::ptr::null(), 0)
            }
            NetworkOption::EnableSlowTaskProfiling => {
                fdb::fdb_network_set_option(code, std::ptr::null(), 0)
            }
            NetworkOption::SupportedClientVersion(ref v) => {
                fdb::fdb_network_set_option(code, v.as_ptr() as *const u8, v.len() as i32)
            }
            NetworkOption::ExternalClient => fdb::fdb_network_set_option(code, std::ptr::null(), 0),
            NetworkOption::ExternalClientTransportId(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_network_set_option(code, data.as_ptr() as *const u8, 8)
            }
        };
        if err != 0 {
            Err(error::FdbError::from(err))
        } else {
            Ok(())
        }
    }
}

pub enum DatabaseOption {
    /// Max location cache entries
    ///
    /// Set the size of the client location cache. Raising this value can boost performance in very large databases where clients access data in a near-random pattern. Defaults to 100000.
    LocationCacheSize(u32),
    /// Max outstanding watches
    ///
    /// Set the maximum number of watches allowed to be outstanding on a database connection. Increasing this number could result in increased resource usage. Reducing this number will not cancel any outstanding watches. Defaults to 10000 and cannot be larger than 1000000.
    MaxWatch(u32),
    /// Hexadecimal ID
    ///
    /// Specify the machine ID that was passed to fdbserver processes running on the same machine as this client, for better location-aware load balancing.
    MachineId(String),
    /// Hexadecimal ID
    ///
    /// Specify the datacenter ID that was passed to fdbserver processes running in the same datacenter as this client, for better location-aware load balancing.
    DatacenterId(String),
}

impl DatabaseOption {
    pub fn code(&self) -> fdb::FDBDatabaseOption {
        match *self {
            DatabaseOption::LocationCacheSize(ref _v) => 10,
            DatabaseOption::MaxWatch(ref _v) => 20,
            DatabaseOption::MachineId(ref _v) => 21,
            DatabaseOption::DatacenterId(ref _v) => 22,
        }
    }
    pub unsafe fn apply(
        &self,
        target: *mut fdb::FDBDatabase,
    ) -> std::result::Result<(), error::FdbError> {
        let code = self.code();
        let err = match *self {
            DatabaseOption::LocationCacheSize(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_database_set_option(target, code, data.as_ptr() as *const u8, 8)
            }
            DatabaseOption::MaxWatch(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_database_set_option(target, code, data.as_ptr() as *const u8, 8)
            }
            DatabaseOption::MachineId(ref v) => {
                fdb::fdb_database_set_option(target, code, v.as_ptr() as *const u8, v.len() as i32)
            }
            DatabaseOption::DatacenterId(ref v) => {
                fdb::fdb_database_set_option(target, code, v.as_ptr() as *const u8, v.len() as i32)
            }
        };
        if err != 0 {
            Err(error::FdbError::from(err))
        } else {
            Ok(())
        }
    }
}

pub enum TransactionOption {
    /// The transaction, if not self-conflicting, may be committed a second time after commit succeeds, in the event of a fault
    CausalWriteRisky,
    /// The read version will be committed, and usually will be the latest committed, but might not be the latest committed in the event of a fault or partition
    CausalReadRisky,
    CausalReadDisable,
    /// The next write performed on this transaction will not generate a write conflict range. As a result, other transactions which read the key(s) being modified by the next write will not conflict with this transaction. Care needs to be taken when using this option on a transaction that is shared between multiple threads. When setting this option, write conflict ranges will be disabled on the next write operation, regardless of what thread it is on.
    NextWriteNoWriteConflictRange,
    /// Committing this transaction will bypass the normal load balancing across proxies and go directly to the specifically nominated 'first proxy'.
    CommitOnFirstProxy,
    CheckWritesEnable,
    /// Reads performed by a transaction will not see any prior mutations that occured in that transaction, instead seeing the value which was in the database at the transaction's read version. This option may provide a small performance benefit for the client, but also disables a number of client-side optimizations which are beneficial for transactions which tend to read and write the same keys within a single transaction.
    ReadYourWritesDisable,
    /// Disables read-ahead caching for range reads. Under normal operation, a transaction will read extra rows from the database into cache if range reads are used to page through a series of data one row at a time (i.e. if a range read with a one row limit is followed by another one row range read starting immediately after the result of the first).
    ReadAheadDisable,
    DurabilityDatacenter,
    DurabilityRisky,
    DurabilityDevNullIsWebScale,
    /// Specifies that this transaction should be treated as highest priority and that lower priority transactions should block behind this one. Use is discouraged outside of low-level tools
    PrioritySystemImmediate,
    /// Specifies that this transaction should be treated as low priority and that default priority transactions should be processed first. Useful for doing batch work simultaneously with latency-sensitive work
    PriorityBatch,
    /// This is a write-only transaction which sets the initial configuration. This option is designed for use by database system tools only.
    InitializeNewDatabase,
    /// Allows this transaction to read and modify system keys (those that start with the byte 0xFF)
    AccessSystemKey,
    /// Allows this transaction to read system keys (those that start with the byte 0xFF)
    ReadSystemKey,
    DebugDump,
    /// Optional transaction name
    ///
    DebugRetryLogging(String),
    /// String identifier to be used in the logs when tracing this transaction. The identifier must not exceed 100 characters.
    ///
    /// Enables tracing for this transaction and logs results to the client trace logs. Client trace logging must be enabled to get log output.
    TransactionLoggingEnable(String),
    /// value in milliseconds of timeout
    ///
    /// Set a timeout in milliseconds which, when elapsed, will cause the transaction automatically to be cancelled. Valid parameter values are ``[0, INT_MAX]``. If set to 0, will disable all timeouts. All pending and any future uses of the transaction will throw an exception. The transaction can be used again after it is reset. Like all transaction options, a timeout must be reset after a call to onError. This behavior allows the user to make the timeout dynamic.
    Timeout(u32),
    /// number of times to retry
    ///
    /// Set a maximum number of retries after which additional calls to onError will throw the most recently seen error code. Valid parameter values are ``[-1, INT_MAX]``. If set to -1, will disable the retry limit. Like all transaction options, the retry limit must be reset after a call to onError. This behavior allows the user to make the retry limit dynamic.
    RetryLimit(u32),
    /// value in milliseconds of maximum delay
    ///
    /// Set the maximum amount of backoff delay incurred in the call to onError if the error is retryable. Defaults to 1000 ms. Valid parameter values are ``[0, INT_MAX]``. Like all transaction options, the maximum retry delay must be reset after a call to onError. If the maximum retry delay is less than the current retry delay of the transaction, then the current retry delay will be clamped to the maximum retry delay.
    MaxRetryDelay(u32),
    /// Snapshot read operations will see the results of writes done in the same transaction.
    SnapshotRywEnable,
    /// Snapshot read operations will not see the results of writes done in the same transaction.
    SnapshotRywDisable,
    /// The transaction can read and write to locked databases, and is resposible for checking that it took the lock.
    LockAware,
    /// By default, operations that are performed on a transaction while it is being committed will not only fail themselves, but they will attempt to fail other in-flight operations (such as the commit) as well. This behavior is intended to help developers discover situations where operations could be unintentionally executed after the transaction has been reset. Setting this option removes that protection, causing only the offending operation to fail.
    UsedDuringCommitProtectionDisable,
    /// The transaction can read from locked databases.
    ReadLockAware,
}

impl TransactionOption {
    pub fn code(&self) -> fdb::FDBTransactionOption {
        match *self {
            TransactionOption::CausalWriteRisky => 10,
            TransactionOption::CausalReadRisky => 20,
            TransactionOption::CausalReadDisable => 21,
            TransactionOption::NextWriteNoWriteConflictRange => 30,
            TransactionOption::CommitOnFirstProxy => 40,
            TransactionOption::CheckWritesEnable => 50,
            TransactionOption::ReadYourWritesDisable => 51,
            TransactionOption::ReadAheadDisable => 52,
            TransactionOption::DurabilityDatacenter => 110,
            TransactionOption::DurabilityRisky => 120,
            TransactionOption::DurabilityDevNullIsWebScale => 130,
            TransactionOption::PrioritySystemImmediate => 200,
            TransactionOption::PriorityBatch => 201,
            TransactionOption::InitializeNewDatabase => 300,
            TransactionOption::AccessSystemKey => 301,
            TransactionOption::ReadSystemKey => 302,
            TransactionOption::DebugDump => 400,
            TransactionOption::DebugRetryLogging(ref _v) => 401,
            TransactionOption::TransactionLoggingEnable(ref _v) => 402,
            TransactionOption::Timeout(ref _v) => 500,
            TransactionOption::RetryLimit(ref _v) => 501,
            TransactionOption::MaxRetryDelay(ref _v) => 502,
            TransactionOption::SnapshotRywEnable => 600,
            TransactionOption::SnapshotRywDisable => 601,
            TransactionOption::LockAware => 700,
            TransactionOption::UsedDuringCommitProtectionDisable => 701,
            TransactionOption::ReadLockAware => 702,
        }
    }
    pub unsafe fn apply(
        &self,
        target: *mut fdb::FDBTransaction,
    ) -> std::result::Result<(), error::FdbError> {
        let code = self.code();
        let err = match *self {
            TransactionOption::CausalWriteRisky => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::CausalReadRisky => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::CausalReadDisable => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::NextWriteNoWriteConflictRange => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::CommitOnFirstProxy => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::CheckWritesEnable => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::ReadYourWritesDisable => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::ReadAheadDisable => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::DurabilityDatacenter => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::DurabilityRisky => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::DurabilityDevNullIsWebScale => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::PrioritySystemImmediate => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::PriorityBatch => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::InitializeNewDatabase => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::AccessSystemKey => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::ReadSystemKey => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::DebugDump => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::DebugRetryLogging(ref v) => fdb::fdb_transaction_set_option(
                target,
                code,
                v.as_ptr() as *const u8,
                v.len() as i32,
            ),
            TransactionOption::TransactionLoggingEnable(ref v) => fdb::fdb_transaction_set_option(
                target,
                code,
                v.as_ptr() as *const u8,
                v.len() as i32,
            ),
            TransactionOption::Timeout(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_transaction_set_option(target, code, data.as_ptr() as *const u8, 8)
            }
            TransactionOption::RetryLimit(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_transaction_set_option(target, code, data.as_ptr() as *const u8, 8)
            }
            TransactionOption::MaxRetryDelay(v) => {
                let data: [u8; 8] = std::mem::transmute(v as i64);
                fdb::fdb_transaction_set_option(target, code, data.as_ptr() as *const u8, 8)
            }
            TransactionOption::SnapshotRywEnable => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::SnapshotRywDisable => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::LockAware => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::UsedDuringCommitProtectionDisable => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
            TransactionOption::ReadLockAware => {
                fdb::fdb_transaction_set_option(target, code, std::ptr::null(), 0)
            }
        };
        if err != 0 {
            Err(error::FdbError::from(err))
        } else {
            Ok(())
        }
    }
}

pub enum StreamingMode {
    /// Client intends to consume the entire range and would like it all transferred as early as possible.
    WantAll,
    /// The default. The client doesn't know how much of the range it is likely to used and wants different performance concerns to be balanced. Only a small portion of data is transferred to the client initially (in order to minimize costs if the client doesn't read the entire range), and as the caller iterates over more items in the range larger batches will be transferred in order to minimize latency.
    Iterator,
    /// Infrequently used. The client has passed a specific row limit and wants that many rows delivered in a single batch. Because of iterator operation in client drivers make request batches transparent to the user, consider ``WANT_ALL`` StreamingMode instead. A row limit must be specified if this mode is used.
    Exact,
    /// Infrequently used. Transfer data in batches small enough to not be much more expensive than reading individual rows, to minimize cost if iteration stops early.
    Small,
    /// Infrequently used. Transfer data in batches sized in between small and large.
    Medium,
    /// Infrequently used. Transfer data in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible. If the client stops iteration early, some disk and network bandwidth may be wasted. The batch size may still be too small to allow a single client to get high throughput from the database, so if that is what you need consider the SERIAL StreamingMode.
    Large,
    /// Transfer data in batches large enough that an individual client can get reasonable read bandwidth from the database. If the client stops iteration early, considerable disk and network bandwidth may be wasted.
    Serial,
}

impl StreamingMode {
    pub fn code(&self) -> fdb::FDBStreamingMode {
        match *self {
            StreamingMode::WantAll => -2,
            StreamingMode::Iterator => -1,
            StreamingMode::Exact => 0,
            StreamingMode::Small => 1,
            StreamingMode::Medium => 2,
            StreamingMode::Large => 3,
            StreamingMode::Serial => 4,
        }
    }
}

pub enum MutationType {
    /// addend
    ///
    /// Performs an addition of little-endian integers. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``. The integers to be added must be stored in a little-endian representation.  They can be signed in two's complement representation or unsigned. You can add to an integer at a known offset in the value by prepending the appropriate number of zero bytes to ``param`` and padding with zero bytes to match the length of the value. However, this offset technique requires that you know the addition will not cause the integer field within the value to overflow.
    Add(Vec<u8>),
    /// value with which to perform bitwise and
    ///
    /// Deprecated
    And(Vec<u8>),
    /// value with which to perform bitwise and
    ///
    /// Performs a bitwise ``and`` operation.  If the existing value in the database is not present, then ``param`` is stored in the database. If the existing value in the database is shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.
    BitAnd(Vec<u8>),
    /// value with which to perform bitwise or
    ///
    /// Deprecated
    Or(Vec<u8>),
    /// value with which to perform bitwise or
    ///
    /// Performs a bitwise ``or`` operation.  If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.
    BitOr(Vec<u8>),
    /// value with which to perform bitwise xor
    ///
    /// Deprecated
    Xor(Vec<u8>),
    /// value with which to perform bitwise xor
    ///
    /// Performs a bitwise ``xor`` operation.  If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``.
    BitXor(Vec<u8>),
    /// value to check against database value
    ///
    /// Performs a little-endian comparison of byte strings. If the existing value in the database is not present or shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``. The larger of the two values is then stored in the database.
    Max(Vec<u8>),
    /// value to check against database value
    ///
    /// Performs a little-endian comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored in the database. If the existing value in the database is shorter than ``param``, it is first extended to the length of ``param`` with zero bytes.  If ``param`` is shorter than the existing value in the database, the existing value is truncated to match the length of ``param``. The smaller of the two values is then stored in the database.
    Min(Vec<u8>),
    /// value to which to set the transformed key
    ///
    /// Transforms ``key`` using a versionstamp for the transaction. Sets the transformed key in the database to ``param``. A versionstamp is a 10 byte, unique, monotonically (but not sequentially) increasing value for each committed transaction. The first 8 bytes are the committed version of the database. The last 2 bytes are monotonic in the serialization order for transactions. WARNING: At this time versionstamps are compatible with the Tuple layer only in the Java and Python bindings. Note that this implies versionstamped keys may not be used with the Subspace and Directory layers except in those languages.
    SetVersionstampedKey(Vec<u8>),
    /// value to versionstamp and set
    ///
    /// Transforms ``param`` using a versionstamp for the transaction. Sets ``key`` in the database to the transformed parameter. A versionstamp is a 10 byte, unique, monotonically (but not sequentially) increasing value for each committed transaction. The first 8 bytes are the committed version of the database. The last 2 bytes are monotonic in the serialization order for transactions. WARNING: At this time versionstamped values are not compatible with the Tuple layer.
    SetVersionstampedValue(Vec<u8>),
    /// value to check against database value
    ///
    /// Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored. Otherwise the smaller of the two values is then stored in the database.
    ByteMin(Vec<u8>),
    /// value to check against database value
    ///
    /// Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then ``param`` is stored. Otherwise the larger of the two values is then stored in the database.
    ByteMax(Vec<u8>),
}

impl MutationType {
    pub fn code(&self) -> fdb::FDBMutationType {
        match *self {
            MutationType::Add(ref _v) => 2,
            MutationType::And(ref _v) => 6,
            MutationType::BitAnd(ref _v) => 6,
            MutationType::Or(ref _v) => 7,
            MutationType::BitOr(ref _v) => 7,
            MutationType::Xor(ref _v) => 8,
            MutationType::BitXor(ref _v) => 8,
            MutationType::Max(ref _v) => 12,
            MutationType::Min(ref _v) => 13,
            MutationType::SetVersionstampedKey(ref _v) => 14,
            MutationType::SetVersionstampedValue(ref _v) => 15,
            MutationType::ByteMin(ref _v) => 16,
            MutationType::ByteMax(ref _v) => 17,
        }
    }
}

pub enum ConflictRangeType {
    /// Used to add a read conflict range
    Read,
    /// Used to add a write conflict range
    Write,
}

impl ConflictRangeType {
    pub fn code(&self) -> fdb::FDBConflictRangeType {
        match *self {
            ConflictRangeType::Read => 0,
            ConflictRangeType::Write => 1,
        }
    }
}

pub enum ErrorPredicate {
    /// Returns ``true`` if the error indicates the operations in the transactions should be retried because of transient error.
    Retryable,
    /// Returns ``true`` if the error indicates the transaction may have succeeded, though not in a way the system can verify.
    MaybeCommitted,
    /// Returns ``true`` if the error indicates the transaction has not committed, though in a way that can be retried.
    RetryableNotCommitted,
}

impl ErrorPredicate {
    pub fn code(&self) -> fdb::FDBErrorPredicate {
        match *self {
            ErrorPredicate::Retryable => 50000,
            ErrorPredicate::MaybeCommitted => 50001,
            ErrorPredicate::RetryableNotCommitted => 50002,
        }
    }
}
