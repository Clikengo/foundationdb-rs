// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate failure;
extern crate foundationdb;
extern crate futures;
extern crate rand;

use futures::executor::block_on;

use std::borrow::Cow;
use std::thread;

use self::rand::{rngs::ThreadRng, seq::SliceRandom};
use futures::future::{self, TryFutureExt};

use foundationdb as fdb;
use foundationdb::transaction::RangeOptionBuilder;
use foundationdb::tuple::{Decode, Encode};
use foundationdb::{Database, Subspace, Transaction};

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
    let class_subspace = Subspace::from("class");
    for class in all_classes {
        trx.set(&class_subspace.pack(class), &100_i64.to_vec());
    }
}

async fn init(db: &Database, all_classes: &[String]) {
    db.transact(|trx| {
        async move {
            trx.clear_subspace_range("attends");
            trx.clear_subspace_range("class");
            init_classes(&trx, all_classes);

            Ok(())
        }
    })
    .await
    .unwrap();
}

async fn get_available_classes(db: &Database) -> Vec<String> {
    let trx = db.create_trx().expect("could not create transaction");

    let range = RangeOptionBuilder::from("class");

    trx.get_range(range.build(), 1_024)
        .and_then(|got_range| {
            let mut available_classes = Vec::<String>::new();

            for key_value in got_range.key_values().as_ref() {
                let count = i64::try_from(key_value.value()).expect("failed to decode count");

                if count > 0 {
                    let class = String::try_from(key_value.key()).expect("failed to decode class");
                    available_classes.push(class);
                }
            }

            future::ok(available_classes)
        })
        .await
        .expect("failed to get classes")
}

async fn ditch_trx(trx: &Transaction, student: &str, class: &str) {
    let attends_key = ("attends", student, class).to_vec();

    // TODO: should get take an &Encode? current impl does encourage &[u8] reuse...
    if trx
        .get(&attends_key, true)
        .await
        .expect("get failed")
        .value()
        .is_none()
    {
        return;
    }

    let class_key = ("class", class).to_vec();
    let available_seats: i64 = i64::try_from(
        trx.get(&class_key, true)
            .await
            .expect("get failed")
            .value()
            .expect("class seats were not initialized"),
    )
    .expect("failed to decode i64")
        + 1;

    //println!("{} ditching class: {}", student, class);
    trx.set(&class_key, &available_seats.to_vec());
    trx.clear(&attends_key);
}

async fn ditch(db: &Database, student: String, class: String) -> Result<(), failure::Error> {
    db.transact(|trx| {
        let student = student.clone();
        let class = class.clone();

        async move {
            ditch_trx(&trx, &student, &class).await;

            Ok(())
        }
    })
    .await
    .map_err(|e| format_err!("error in signup: {}", e))
}

async fn signup_trx(trx: &Transaction, student: &str, class: &str) -> Result<(), failure::Error> {
    let attends_key = ("attends", student, class).to_vec();
    if trx
        .get(&attends_key, true)
        .await
        .expect("get failed")
        .value()
        .is_some()
    {
        //println!("{} already taking class: {}", student, class);
        return Ok(());
    }

    let class_key = ("class", class).to_vec();
    let available_seats: i64 = i64::try_from(
        trx.get(&class_key, true)
            .await
            .expect("get failed")
            .value()
            .expect("class seats were not initialized"),
    )
    .expect("failed to decode i64");

    if available_seats <= 0 {
        bail!("No remaining seats");
    }

    let attends_range = RangeOptionBuilder::from(("attends", student)).build();
    if trx
        .get_range(attends_range, 1_024)
        .await
        .expect("get_range failed")
        .key_values()
        .len()
        >= 5
    {
        bail!("Too many classes");
    }

    //println!("{} taking class: {}", student, class);
    trx.set(&class_key, &(available_seats - 1).to_vec());
    trx.set(&attends_key, &"".to_vec());
    Ok(())
}

async fn signup(db: &Database, student: String, class: String) -> Result<(), failure::Error> {
    db.transact(|trx| {
        let student = student.clone();
        let class = class.clone();
        async move { signup_trx(&trx, &student, &class).await }
    })
    .await
    .map_err(|e| format_err!("error in signup: {}", e))
}

async fn switch_classes(
    db: &Database,
    student_id: String,
    old_class: String,
    new_class: String,
) -> Result<(), failure::Error> {
    db.transact(|trx| {
        let student_id = student_id.clone();
        let old_class = old_class.clone();
        let new_class = new_class.clone();

        async move {
            ditch_trx(&trx, &student_id, &old_class).await;
            signup_trx(&trx, &student_id, &new_class).await.unwrap();

            Ok(())
        }
    })
    .await
    .map_err(|e| format_err!("error in switch: {}", e))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mood {
    Add,
    Ditch,
    Switch,
}

async fn perform_op(
    db: &Database,
    rng: &mut ThreadRng,
    mood: Mood,
    student_id: &str,
    all_classes: &[String],
    my_classes: &mut Vec<String>,
) -> Result<(), failure::Error> {
    match mood {
        Mood::Add => {
            let class = all_classes.choose(rng).unwrap();
            signup(&db, student_id.to_string(), class.to_string()).await?;
            my_classes.push(class.to_string());
        }
        Mood::Ditch => {
            let class = all_classes.choose(rng).unwrap();
            ditch(&db, student_id.to_string(), class.to_string()).await?;
            my_classes.retain(|s| s != class);
        }
        Mood::Switch => {
            let old_class = my_classes.choose(rng).unwrap().to_string();
            let new_class = all_classes.choose(rng).unwrap();
            switch_classes(
                &db,
                student_id.to_string(),
                old_class.to_string(),
                new_class.to_string(),
            )
            .await?;
            my_classes.retain(|s| s != &old_class);
            my_classes.push(new_class.to_string());
        }
    }
    Ok(())
}

fn simulate_students(student_id: usize, num_ops: usize) {
    let db = Database::new(foundationdb::default_config_path()).unwrap();

    block_on(async {
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

            let mood = moods.choose(&mut rng).map(|mood| *mood).unwrap();

            // on errors we recheck for available classes
            if perform_op(
                &db,
                &mut rng,
                mood,
                &student_id,
                &available_classes,
                &mut my_classes,
            )
            .await
            .is_err()
            {
                println!("getting available classes");
                available_classes = Cow::Owned(get_available_classes(&db).await);
            }
        }

        Ok::<(), failure::Error>(())
    })
    .expect("got error in simulation");
}

async fn run_sim(db: &Database, students: usize, ops_per_student: usize) {
    let mut threads: Vec<(usize, thread::JoinHandle<()>)> = Vec::with_capacity(students);
    for i in 0..students {
        // TODO: ClusterInner has a mutable pointer reference, if thread-safe, mark that trait as Sync, then we can clone DB here...
        threads.push((
            i,
            thread::spawn(move || {
                simulate_students(i, ops_per_student);
            }),
        ));
    }

    // explicitly join...
    for (id, thread) in threads {
        thread.join().expect("failed to join thread");

        let student_id = format!("s{}", id);
        let attends_range = RangeOptionBuilder::from(("attends", &student_id)).build();

        for key_value in db
            .create_trx()
            .unwrap()
            .get_range(attends_range, 1_024)
            .await
            .expect("get_range failed")
            .key_values()
            .into_iter()
        {
            let (_, s, class) = <(String, String, String)>::try_from(key_value.key()).unwrap();
            assert_eq!(student_id, s);

            println!("{} is taking: {}", student_id, class);
        }
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
    let db = Database::new(foundationdb::default_config_path()).unwrap();

    block_on(async {
        init(&db, &*ALL_CLASSES).await;

        println!("Initialized");
        run_sim(&db, 10, 10).await;

        Ok::<(), failure::Error>(())
    })
    .expect("failed to run");

    // shutdown
    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
