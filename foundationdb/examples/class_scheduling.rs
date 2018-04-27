// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

extern crate foundationdb;

use std::thread;

const levels: &[&str] = &[
    "intro",
    "for dummies",
    "remedial",
    "101",
    "201",
    "301",
    "mastery",
    "lab",
    "seminar",
];

const types: &[&str] = &[
    "chem", "bio", "cs", "geometry", "calc", "alg", "film", "music", "art", "dance",
];

const times: &[&str] = &[
    "2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00", "12:00",
    "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00",
];

fn init_class_names() -> Vec<String> {
    let mut class_names = Vec::with_capacity(levels.len() * types.len() * times.len());
    for level in levels {
        for tipe in types {
            for time in times {
                class_names.push(format!("{} {} {}", time, tipe, level));
            }
        }
    }

    return class_names;
}

fn main() {
    let network = foundationdb::init().expect("failed to initialize FoundationDB");

    let handle = thread::spawn(move || {
        let error = network.run();

        if let Err(error) = error {
            panic!("fdb_run_network: {}", error);
        }
    });

    network.wait();

    let class_names = init_class_names();

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
