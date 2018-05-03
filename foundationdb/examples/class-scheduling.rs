// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

extern crate foundationdb;
extern crate futures;

use std::thread;

use futures::future::{self, Future};

use foundationdb as fdb;
use foundationdb::transaction::RangeOptionBuilder;
use foundationdb::tuple::Single;
use foundationdb::{Cluster, Database, Transaction, Tuple};

// Data model:
// ("attends", student, class) = ""
// ("class", class_name) = seatsLeft

// Generate 1,620 classes like '9:00 chem for dummies'
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

fn init_classes(trx: &Transaction) {
    let mut class_names: Vec<String> = Vec::new();
    for level in levels {
        for _type in types {
            for time in times {
                // TODO: cleanup encoding?
                trx.set(
                    &format!("{} {} {}", time, _type, level).encode_to_vec(),
                    &100_i64.encode_to_vec(),
                );
            }
        }
    }
}

fn init(db: &Database) {
    let trx = db.create_trx().expect("could not create transaction");
    // TODO: we can probably clean up range, probably add a Tuple.range()
    trx.clear_range(
        &("attends".to_string(), vec![0x00_u8]).encode_to_vec(),
        &("attends".to_string(), vec![0xFF_u8]).encode_to_vec(),
    );
    trx.clear_range(
        &("class".to_string(), vec![0x00_u8]).encode_to_vec(),
        &("class".to_string(), vec![0xFF_u8]).encode_to_vec(),
    );
    init_classes(&trx);

    trx.commit().wait().expect("failed to initialize data");
}

fn available_classes(db: Database) -> Vec<String> {
    let trx = db.create_trx().expect("could not create transaction");

    // TODO: can this be subspace?
    let range = RangeOptionBuilder::from_tuple(&("class".to_string(),));

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

//   private static List<String> availableClasses(TransactionContext db) {
//     return db.run((Transaction tr) -> {
//       List<String> classNames = new ArrayList<String>();
//       for(KeyValue kv: tr.getRange(Tuple.from("class").range())) {
//         if (decodeInt(kv.getValue()) > 0)
//           classNames.add(Tuple.fromBytes(kv.getKey()).getString(1));
//       }
//       return classNames;
//     });
//   }

//   private static void drop(TransactionContext db, final String s, final String c) {
//     db.run((Transaction tr) -> {
//       byte[] rec = Tuple.from("attends", s, c).pack();
//       if (tr.get(rec).join() == null)
//         return null; // not taking this class
//       byte[] classKey = Tuple.from("class", c).pack();
//       tr.set(classKey, encodeInt(decodeInt(tr.get(classKey).join()) + 1));
//       tr.clear(rec);
//       return null;
//     });
//   }

//   private static void signup(TransactionContext db, final String s, final String c) {
//     db.run((Transaction tr) -> {
//       byte[] rec = Tuple.from("attends", s, c).pack();
//       if (tr.get(rec).join() != null)
//         return null; // already signed up

//       int seatsLeft = decodeInt(tr.get(Tuple.from("class", c).pack()).join());
//       if (seatsLeft == 0)
//         throw new IllegalStateException("No remaining seats");

//       List<KeyValue> classes = tr.getRange(Tuple.from("attends", s).range()).asList().join();
//       if (classes.size() == 5)
//         throw new IllegalStateException("Too many classes");

//       tr.set(Tuple.from("class", c).pack(), encodeInt(seatsLeft - 1));
//       tr.set(rec, Tuple.from("").pack());
//       return null;
//     });
//   }

//   private static void switchClasses(TransactionContext db, final String s, final String oldC, final String newC) {
//     db.run((Transaction tr) -> {
//       drop(tr, s, oldC);
//       signup(tr, s, newC);
//       return null;
//     });
//   }

//   //
//   // Testing
//   //

//   private static void simulateStudents(int i, int ops) {

//     String studentID = "s" + Integer.toString(i);
//     List<String> allClasses = classNames;
//     List<String> myClasses = new ArrayList<String>();

//     String c;
//     String oldC;
//     String newC;
//     Random rand = new Random();

//     for (int j=0; j<ops; j++) {
//       int classCount = myClasses.size();
//       List<String> moods = new ArrayList<String>();
//       if (classCount > 0) {
//         moods.add("drop");
//         moods.add("switch");
//       }
//       if (classCount < 5)
//         moods.add("add");
//       String mood = moods.get(rand.nextInt(moods.size()));

//       try {
//         if (allClasses.isEmpty())
//           allClasses = availableClasses(db);
//         if (mood.equals("add")) {
//           c = allClasses.get(rand.nextInt(allClasses.size()));
//           signup(db, studentID, c);
//           myClasses.add(c);
//         } else if (mood.equals("drop")) {
//           c = myClasses.get(rand.nextInt(myClasses.size()));
//           drop(db, studentID, c);
//           myClasses.remove(c);
//         } else if (mood.equals("switch")) {
//           oldC = myClasses.get(rand.nextInt(myClasses.size()));
//           newC = allClasses.get(rand.nextInt(allClasses.size()));
//           switchClasses(db, studentID, oldC, newC);
//           myClasses.remove(oldC);
//           myClasses.add(newC);
//         }
//       } catch (Exception e) {
//         System.out.println(e.getMessage() +  "Need to recheck available classes.");
//         allClasses.clear();
//       }

//     }

//   }

fn run_sim(students: usize, ops_per_student: usize) {}

//   private static void runSim(int students, final int ops_per_student) throws InterruptedException {
//     List<Thread> threads = new ArrayList<Thread>(students);//Thread[students];
//     for (int i = 0; i < students; i++) {
//       final int j = i;
//       threads.add(new Thread(() -> simulateStudents(j, ops_per_student)) );
//     }
//     for (Thread thread: threads)
//       thread.start();
//     for (Thread thread: threads)
//       thread.join();
//     System.out.format("Ran %d transactions%n", students * ops_per_student);
//   }

//   public static void main(String[] args) throws InterruptedException {
//     init(db);
//     System.out.println("Initialized");
//     runSim(10,10);
//   }

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
            init(&db);
            println!("Initialized");
            run_sim(10, 10);

            future::ok(())
        })
        .wait()
        .expect("failed to create cluster");

    // shutdown
    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
