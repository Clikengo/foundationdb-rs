extern crate foundationdb as fdb;
extern crate futures;
extern crate rand;
extern crate stopwatch;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate structopt;

use std::sync::atomic::*;
use std::sync::Arc;

use futures::future::*;
use stopwatch::Stopwatch;
use structopt::StructOpt;

use fdb::error::*;
use fdb::*;

#[derive(Clone)]
struct Counter {
    size: usize,
    inner: Arc<AtomicUsize>,
}
impl Counter {
    fn new(size: usize) -> Self {
        Self {
            size,
            inner: Default::default(),
        }
    }

    fn decr(&self) -> bool {
        let val = self.inner.fetch_add(1, Ordering::SeqCst);
        val < self.size
    }
}

struct BenchRunner {
    #[allow(unused)]
    db: Database,
    counter: Counter,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,

    rng: rand::XorShiftRng,
    trx: Option<Transaction>,
}

impl BenchRunner {
    fn new(db: Database, counter: Counter, opt: &Opt) -> Self {
        let mut key_buf = Vec::with_capacity(opt.key_len);
        key_buf.resize(opt.key_len, 0u8);

        let mut val_buf = Vec::with_capacity(opt.val_len);
        val_buf.resize(opt.val_len, 0u8);

        let rng = rand::weak_rng();
        let trx = db.create_trx().expect("failed to create trx");

        Self {
            db,
            counter,
            key_buf,
            val_buf,

            rng,
            trx: Some(trx),
        }
    }

    //TODO: impl future
    fn run(self) -> Box<Future<Item = (), Error = FdbError>> {
        Box::new(loop_fn(self, Self::step))
    }

    //TODO: impl future
    fn step(mut self) -> Box<Future<Item = Loop<(), Self>, Error = FdbError>> {
        use rand::Rng;

        self.rng.fill_bytes(&mut self.key_buf);
        self.rng.fill_bytes(&mut self.val_buf);

        self.key_buf[0] = 0x01;

        let trx = self.trx.take().unwrap();

        trx.set(&self.key_buf, &self.val_buf);
        let f = trx.commit().map(move |trx| {
            trx.reset();
            self.trx = Some(trx);

            if self.counter.decr() {
                Loop::Continue(self)
            } else {
                Loop::Break(())
            }
        });
        Box::new(f)
    }
}

struct Bench {
    db: Database,
    opt: Opt,
}

impl Bench {
    fn run(self) {
        let counter = Counter::new(self.opt.count);

        let runners = (0..self.opt.threads)
            .into_iter()
            .map(|_n| BenchRunner::new(self.db.clone(), counter.clone(), &self.opt).run())
            .collect::<Vec<_>>();

        let sw = Stopwatch::start_new();
        join_all(runners).wait().expect("failed to run bench");
        let elapsed = sw.elapsed_ms() as usize;

        info!(
            "bench took: {:?} ms, {:?} tps",
            elapsed,
            1000 * self.opt.count / elapsed
        );
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "fdb-bench")]
struct Opt {
    #[structopt(short = "t", long = "threads", default_value = "1000")]
    threads: usize,

    #[structopt(short = "c", long = "count", default_value = "300000")]
    count: usize,

    #[structopt(long = "key-len", default_value = "10")]
    key_len: usize,
    #[structopt(long = "val-len", default_value = "100")]
    val_len: usize,
}

fn main() {
    env_logger::init();
    let opt = Opt::from_args();

    let network = fdb_api::FdbApiBuilder::default()
        .build()
        .expect("failed to init api")
        .network()
        .build()
        .expect("failed to init network");

    let handle = std::thread::spawn(move || {
        let error = network.run();

        if let Err(error) = error {
            panic!("fdb_run_network: {}", error);
        }
    });

    network.wait();

    let cluster_path = fdb::default_config_path();
    let cluster = Cluster::new(cluster_path)
        .wait()
        .expect("failed to create cluster");

    let db = cluster
        .create_database()
        .wait()
        .expect("failed to get database");

    let bench = Bench { db, opt };
    bench.run();

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
