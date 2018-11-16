# Change Log: foundationdb-rs

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## 0.3.0

### Changed

- `GetKeyResult` and `GetAddressResult` return value no longer unwrap to Result #94 (@yjh0502)

### Added

- Win64 support #92 (@Speedy37)

## 0.2.0

### Added

- Database::transact (#34, @yjh0502)
- RangeOptionBuilder::from_tuple (#81, @rushmorem)
- Subspace (#54 #56 #57 #76 #78, @yjh0502 @rushmorem)
- Transaction::watch (#25 #59, @yjh0502)
- Transaction::atomic_op (#26, @yjh0502)
- Transaction::get_range (#28, @yjh0502)
- Transaction::{get, set}_read_version (#38, @yjh0502)
- Transaction::add_conflict_range (#50, @yjh0502)
- Tuple interfaces (#40 #41 #42 #46 #47 #51 #60 #62 #64 #67 #74 #80 #83, @yjh0502 @rushmorem)
- Additional tests for Transactions (#33, @yjh0502)
- Class Scheduling Tutorial in examples (#65)
- FoundationDB bindingtester support (#39 #43 #45, @yjh0502)
- FoundationDB benchmarking test suite support (#70 #73, @yjh0502)
- Support to scripts for installing on Centos/RHEL

### Changed

- Added TupleError to foundationdb::Error (#77)
- API names more inline with Rust style guidelinse (#84 @rushmorem)

## 0.1.0

### Added

- first release
- C api Bindings
- Cluster API
- Database API
- Network API
- Transaction API
- Options generation
- FdbFuture abstraction over Futures 0.1 API
- fdb_api initialization
- FdbError conversion