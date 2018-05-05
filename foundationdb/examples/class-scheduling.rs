// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#[macro_use]
extern crate lazy_static;
extern crate foundationdb;
extern crate futures;
extern crate rand;

use std::borrow::Cow;
use std::thread;

use self::rand::{Rng, ThreadRng};
use futures::future::{self, Future};

use foundationdb as fdb;
use foundationdb::transaction::RangeOptionBuilder;
use foundationdb::tuple::{Decode, Encode};
use foundationdb::{Cluster, Database, Transaction};

// Data model:
// ("attends", student, class) = ""
// ("class", class_name) = seatsLeft

// Generate 1,620 classes like '9:00 chem for dummies'
const LEVELS: &[&str] = &[
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

const TYPES: &[&str] = &[
    "chem", "bio", "cs", "geometry", "calc", "alg", "film", "music", "art", "dance",
];

const TIMES: &[&str] = &[
    "2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00", "12:00",
    "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00",
];

lazy_static! {
    static ref ALL_CLASSES: Vec<String> = all_classes();
}

// TODO: make these tuples?
fn all_classes() -> Vec<String> {
    let mut class_names: Vec<String> = Vec::new();
    for level in LEVELS {
        for _type in TYPES {
            for time in TIMES {
                class_names.push(format!("{} {} {}", time, _type, level));
            }
        }
    }

    class_names
}

fn init_classes(trx: &Transaction, all_classes: &[String]) {
    for class in all_classes {
        trx.set(&class.encode_to_vec(), &100_i64.encode_to_vec());
    }
}

fn init(db: &Database, all_classes: &[String]) {
    let trx = db.create_trx().expect("could not create transaction");
    trx.clear_subspace_range(&("attends",));
    trx.clear_subspace_range(&("class",));
    init_classes(&trx, all_classes);

    trx.commit().wait().expect("failed to initialize data");
}

fn get_available_classes(db: &Database) -> Vec<String> {
    let trx = db.create_trx().expect("could not create transaction");

    let range = RangeOptionBuilder::from_tuple(&"class");

    trx.get_range(range.build(), 1_024)
        .and_then(|got_range| {
            let mut available_classes = Vec::<String>::new();

            // TODO: change keyvalues to key_values
            for key_value in got_range.keyvalues().as_ref() {
                let count = i64::decode_full(key_value.value()).expect("failed to decode count");

                if count > 0 {
                    let class =
                        String::decode_full(key_value.key()).expect("failed to decode class");
                    available_classes.push(class);
                }
            }

            future::ok(available_classes)
        })
        .wait()
        .expect("failed to get classes")
}

fn ditch_trx(trx: &Transaction, student: &str, class: &str) {
    let attends_key = ("attends", student, class).encode_to_vec();

    // TODO: should get take an &Encode? current impl does encourage &[u8] reuse...
    if trx.get(&attends_key, true)
        .wait()
        .expect("get failed")
        .value()
        .expect("value failed")
        .is_none()
    {
        return;
    }

    let class_key = ("class", class).encode_to_vec();
    let available_seats: i64 = i64::decode_full(
        trx.get(&class_key, true)
            .wait()
            .expect("get failed")
            .value()
            .expect("value failed")
            .expect("class seats were not initialized"),
    ).expect("failed to decode i64") + 1;

    println!("{} ditching class: {}", student, class);
    trx.set(&class_key, &available_seats.encode_to_vec());
    trx.clear(&attends_key);
}

fn ditch(db: &Database, student: &str, class: &str) -> Result<(), String> {
    let trx = db.create_trx().expect("could not create transaction");

    ditch_trx(&trx, student, class);
    println!("{} ditch commit: {}", student, class);
    trx.commit().wait().map_err(|e| format!("error: {}", e))?;

    Ok(())
}

fn signup_trx(trx: &Transaction, student: &str, class: &str) -> Result<(), String> {
    let attends_key = ("attends", student, class).encode_to_vec();
    if trx.get(&attends_key, true)
        .wait()
        .expect("get failed")
        .value()
        .expect("value failed")
        .is_none()
    {
        return Ok(());
    }

    let class_key = ("class".to_string(), class.to_string()).encode_to_vec();
    let available_seats: i64 = i64::decode_full(
        trx.get(&class_key, true)
            .wait()
            .expect("get failed")
            .value()
            .expect("value failed")
            .expect("class seats were not initialized"),
    ).expect("failed to decode i64");

    if available_seats <= 0 {
        return Err(format!("No remaining seats"));
    }

    // TODO: impl Deref for [KeyValue] on KeyValues
    let attends_range = RangeOptionBuilder::from_tuple(&("attends", student)).build();
    if trx.get_range(attends_range, 1_024)
        .wait()
        .expect("get_range failed")
        .keyvalues()
        .len() >= 5
    {
        return Err(format!("Too many classes"));
    }

    println!("{} taking class: {}", student, class);
    trx.set(&class_key, &(available_seats - 1).encode_to_vec());
    trx.set(&attends_key, &"".encode_to_vec());
    Ok(())
}

fn signup(db: &Database, student: &str, class: &str) -> Result<(), String> {
    let trx = db.create_trx().expect("could not create transaction");

    signup_trx(&trx, student, class)?;

    println!("{} signup commit: {}", student, class);
    trx.commit().wait().map_err(|e| format!("error: {}", e))?;

    Ok(())
}

fn switch_classes(
    db: &Database,
    student_id: &str,
    old_class: &str,
    new_class: &str,
) -> Result<(), String> {
    let trx = db.create_trx().expect("could not create transaction");

    ditch_trx(&trx, student_id, old_class);
    signup_trx(&trx, student_id, new_class)?;

    println!("{} switch commit: {}, {}", student_id, old_class, new_class);
    trx.commit().wait().map_err(|e| format!("error: {}", e))?;

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mood {
    Add,
    Ditch,
    Switch,
}

fn perform_op(
    db: &Database,
    rng: &mut ThreadRng,
    mood: Mood,
    student_id: &str,
    all_classes: &[String],
    my_classes: &mut Vec<String>,
) -> Result<(), String> {
    println!("{} performing op: {:?}", student_id, mood);

    match mood {
        Mood::Add => {
            let class = rng.choose(all_classes).unwrap();
            signup(&db, &student_id, class)?;
            my_classes.push(class.to_string());
        }
        Mood::Ditch => {
            let class = rng.choose(all_classes).unwrap();
            ditch(&db, student_id, class)?;
            my_classes.retain(|s| s != class);
        }
        Mood::Switch => {
            let old_class = rng.choose(my_classes).unwrap().to_string();
            let new_class = rng.choose(all_classes).unwrap();
            switch_classes(&db, student_id, &old_class, new_class)?;
            my_classes.retain(|s| s != &old_class);
            my_classes.push(new_class.to_string());
        }
    }
    Ok(())
}

fn simulate_students(student_id: usize, num_ops: usize) {
    Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| {
            let student_id = format!("s{}", student_id);
            let mut rng = rand::thread_rng();

            let mut available_classes = Cow::Borrowed(&*ALL_CLASSES);
            let mut my_classes = Vec::<String>::new();

            for _ in 0..num_ops {
                let mut moods = Vec::<Mood>::new();

                if my_classes.len() > 0 {
                    moods.push(Mood::Ditch);
                    moods.push(Mood::Switch);
                }

                if my_classes.len() < 5 {
                    moods.push(Mood::Add);
                }

                let mood = rng.choose(&moods).map(|mood| *mood).unwrap();

                // on errors we recheck for available classes
                if perform_op(
                    &db,
                    &mut rng,
                    mood,
                    &student_id,
                    &available_classes,
                    &mut my_classes,
                ).is_err()
                {
                    println!("getting more classes");
                    available_classes = Cow::Owned(get_available_classes(&db));
                }
            }

            future::ok(())
        })
        .wait()
        .expect("got error in simulation");
}

fn run_sim(_db: &Database, students: usize, ops_per_student: usize) {
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(students);
    for i in 0..students {
        // TODO: ClusterInner has a mutable pointer reference, if thread-safe, mark that trait as Sync, then we can clone DB here...
        threads.push(thread::spawn(move || {
            simulate_students(i, ops_per_student);
        }));
    }

    // explicitly join...
    for thread in threads {
        thread.join().expect("failed to join thread");
    }

    println!("Ran {} transactions", students * ops_per_student);
}

fn main() {
    let network = fdb::init().expect("failed to initialize FoundationDB");

    let handle = thread::spawn(move || {
        let error = network.run();

        if let Err(error) = error {
            panic!("fdb_run_network: {}", error);
        }
    });
    network.wait();

    // run scheduling
    Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| {
            init(&db, &*ALL_CLASSES);
            println!("Initialized");
            run_sim(&db, 10, 10);

            future::ok(())
        })
        .wait()
        .expect("failed to create cluster");

    // shutdown
    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
