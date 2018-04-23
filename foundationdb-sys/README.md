
# Building 

Follow the FoundationDB installation instructions: https://apple.github.io/foundationdb/api-general.html#installing-client-binaries

The bindgen output file `bindings.rs` should be checked in with updates. It is generated as part of the foundation-sys build, but off by default.

## Generate new bindings

Run:

```console
$> BINDGEN=true cargo build
```

And format the file with `cargo fmt`, and submit a new PR to the `foundationdb-sys` repo.