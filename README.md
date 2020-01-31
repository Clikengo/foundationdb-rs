[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/Clikengo/foundationdb-rs/CI)](https://github.com/Clikengo/foundationdb-rs/actions)
[![Codecov](https://img.shields.io/codecov/c/github/Clikengo/foundationdb-rs)](https://codecov.io/gh/Clikengo/foundationdb-rs)
![Rustc 1.39+](https://img.shields.io/badge/rustc-1.39+-lightgrey)
[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=Clikengo/foundationdb-rs)](https://dependabot.com)

# FoundationDB Rust Client

The repo consists of multiple crates

| Library | Status | Description |
|---------|--------|-------------|
| [**foundationdb**](foundationdb/README.md) | [![Crates.io](https://img.shields.io/crates/v/foundationdb)](https://crates.io/crates/foundationdb) [![foundationdb](https://docs.rs/foundationdb/badge.svg)](https://docs.rs/foundationdb) | High level FoundationDB client API |
| [**foundationdb-sys**](foundationdb-sys/README.md) | [![Crates.io](https://img.shields.io/crates/v/foundationdb-sys)](https://crates.io/crates/foundationdb-sys) [![foundationdb-sys](https://docs.rs/foundationdb-sys/badge.svg)](https://docs.rs/foundationdb-sys) | C API bindings for FoundationDB |
| **foundationdb-gen** | n/a | Code generator for common options and types of FoundationDB |

The current version requires rustc 1.39+ to work (async/await feature).
The previous version (0.3) is still maintained and is available within the 0.3 branch.

You can access the `master` branch documentation [here](https://clikengo.github.io/foundationdb-rs/foundationdb/index.html).

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
