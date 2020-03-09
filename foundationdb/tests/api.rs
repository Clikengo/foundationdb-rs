use foundationdb as fdb;
use foundationdb::{api::FdbApiBuilder, Database};
use std::thread;

#[test]
#[should_panic(expected = "the fdb select api version can only be run once per process")]
fn test_run() {
    let (runner, cond) = FdbApiBuilder::default()
        .build()
        .expect("could not initialize api")
        .build()
        .expect("could not initialize network");

    let net_thread = thread::spawn(move || {
        unsafe { runner.run() }.expect("failed to run");
    });
    let stopper = cond.wait();

    // network thread is running

    #[cfg(not(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0")))]
    {
        assert!(Database::from_path("test".to_string().as_str()).is_err());
        assert!(Database::from_path(fdb::default_config_path()).is_ok());
    }
    assert!(
        futures::executor::block_on(Database::new_compat(Some("test".to_string().as_str())))
            .is_err()
    );
    assert!(
        futures::executor::block_on(Database::new_compat(Some(fdb::default_config_path()))).is_ok()
    );

    stopper.stop().expect("failed to stop");
    net_thread.join().expect("failed to join net thread");
    println!("stopped!");

    // this should fail:
    let _ = FdbApiBuilder::default().build();
    panic!("previous line should have panicked!");
}
