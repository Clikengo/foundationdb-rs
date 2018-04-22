extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate foundationdb_sys;
#[macro_use]
extern crate lazy_static;
extern crate futures;

pub mod cluster;
pub mod context;
pub mod database;
pub mod error;
pub mod future;
pub mod network;
pub mod options;
pub mod transaction;

//move to prelude?
pub use cluster::Cluster;
pub use database::Database;
pub use transaction::Transaction;
