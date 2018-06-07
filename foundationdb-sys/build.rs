extern crate bindgen;

use std::env;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
const INCLUDE_PATH: &str = "-I/usr/include/foundationdb/";

#[cfg(target_os = "macos")]
const INCLUDE_PATH: &str = "-I/usr/local/include/foundationdb/";


#[cfg(target_os = "windows")]
const INCLUDE_PATH: &str = "-IC:/Program Files/foundationdb/include/foundationdb";

fn main() {
    // Link against fdb_c
    println!("cargo:rustc-link-lib=fdb_c");

    #[cfg(target_os = "windows")]
    println!("cargo:rustc-link-search=C:/Program Files/foundationdb/lib/foundationdb");

    if env::var_os("BINDGEN").is_some() {
        // The bindgen::Builder is the main entry point
        // to bindgen, and lets you build up options for
        // the resulting bindings.
        let bindings = bindgen::Builder::default()
        // TODO: there must be a way to get foundationdb from pkg-config...
        .clang_arg(INCLUDE_PATH)
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        .generate_comments(true)
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate FoundationDB bindings");

        // Write the bindings to the $OUT_DIR/bindings.rs file.
        // let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        let out_path = PathBuf::from("src");
        bindings
            .write_to_file(out_path.join("bindings.rs"))
            .expect("Couldn't write bindings!");
    }
}
