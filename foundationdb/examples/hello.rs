extern crate foundationdb;
extern crate foundationdb_sys;
extern crate futures;

use foundationdb::*;
use foundationdb_sys as fdb_sys;

use futures::future::*;

use error::FdbError;

//TODO: impl Future
fn example_set_get() -> Box<Future<Item = (), Error = FdbError>> {
    let fut = Cluster::new("/etc/foundationdb/fdb.cluster")
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            trx.set(b"hello", b"world");
            trx.commit()
        })
        .and_then(|trx| result(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello"))
        .and_then(|res| {
            let val = res.value();
            eprintln!("value: {:?}", val);

            let trx = res.transaction();
            trx.clear(b"hello");
            trx.commit()
        })
        .and_then(|trx| result(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello"))
        .and_then(|res| {
            eprintln!("value: {:?}", res.value());
            Ok(())
        });

    Box::new(fut)
}

#[cfg(target_os = "linux")]
fn default_config_path() -> &'static str {
    "/etc/foundationdb/fdb.cluster"
}
#[cfg(target_os = "macos")]
fn default_config_path() -> &'static str {
    "/usr/local/etc/foundationdb/fdb.cluster"
}

fn example_get_multi() -> Box<Future<Item = (), Error = FdbError>> {
    let fut = Cluster::new(default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            let keys: &[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];

            let futs = keys.iter().map(|k| trx.get(k)).collect::<Vec<_>>();
            join_all(futs)
        })
        .and_then(|results| {
            for (i, res) in results.into_iter().enumerate() {
                eprintln!("res[{}]: {:?}", i, res.value());
            }
            Ok(())
        });

    Box::new(fut)
}

fn main() {
    let handle = unsafe {
        //TODO: switch to safe API
        let version = fdb_sys::fdb_get_max_api_version();
        let err = fdb_sys::fdb_select_api_version_impl(version, version);
        if err != 0 {
            panic!("fdb_select_api_version: {:?}", FdbError::from(err));
        }

        let err = fdb_sys::fdb_setup_network();
        if err != 0 {
            panic!("fdb_setup_network: {:?}", FdbError::from(err));
        }

        std::thread::spawn(|| {
            let err = fdb_sys::fdb_run_network();
            if err != 0 {
                panic!("fdb_run_network: {:?}", FdbError::from(err));
            }
        })
    };

    example_set_get().wait().expect("failed to run");
    example_get_multi().wait().expect("failed to run");

    unsafe {
        //TODO: switch to safe API
        fdb_sys::fdb_stop_network();
    }
    handle.join().expect("failed to join fdb thread");
}
