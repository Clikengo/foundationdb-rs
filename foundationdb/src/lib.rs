#[macro_use]
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate foundationdb_sys;
extern crate futures;

pub mod cluster;
pub mod database;
pub mod error;
pub mod fdb_api;
pub mod future;
pub mod network;
pub mod options;
pub mod transaction;

//move to prelude?
pub use cluster::Cluster;
pub use database::Database;
pub use transaction::Transaction;

pub fn init() -> error::Result<network::Network> {
    fdb_api::FdbApiBuilder::default().build()?.network().build()
}

pub fn default_api() -> error::Result<network::NetworkBuilder> {
    Ok(fdb_api::FdbApiBuilder::default().build()?.network())
}

pub fn builder() -> fdb_api::FdbApiBuilder {
    fdb_api::FdbApiBuilder::default()
}
