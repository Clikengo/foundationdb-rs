use lazy_static::lazy_static;

/// generate random string. Foundationdb watch only fires when value changed, so updating with same
/// value twice will not fire watches. To make examples work over multiple run, we use random
/// string as a value.
#[allow(unused)]
pub fn random_str(len: usize) -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    ::std::iter::repeat(())
        .take(len)
        .map(|()| rng.sample(Alphanumeric))
        .collect::<String>()
}

lazy_static! {
    static ref ENV: foundationdb::fdb_api::NetworkAutoStop =
        foundationdb::boot().expect("fdb boot failed");
}

#[allow(unused)]
pub fn boot() {
    let _end = &*ENV;
}
