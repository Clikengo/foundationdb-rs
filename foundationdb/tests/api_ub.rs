use std::panic;

#[test]
#[ignore]
fn test_run() {
    let old = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let mut db = None;
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        // Run the foundationdb client API
        let _drop_me = unsafe { foundationdb::boot() };
        db = Some(futures::executor::block_on(foundationdb::Database::new_compat(None)).unwrap());
        // Try to escape via unwind
        panic!("UNWIND!")
    }));
    assert!(result.is_err());
    let trx = db.unwrap().create_trx().unwrap();
    let _err = futures::executor::block_on(trx.get_read_version()).unwrap_err();
    panic::set_hook(old);
}
