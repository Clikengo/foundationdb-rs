use foundationdb as fdb;

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

#[allow(unused)]
pub async fn database() -> fdb::FdbResult<fdb::Database> {
    fdb::Database::new_compat(None).await
}
