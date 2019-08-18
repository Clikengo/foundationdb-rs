#![feature(async_await)]

extern crate foundationdb as fdb;
extern crate futures;
extern crate rand;
extern crate stopwatch;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate structopt;

use std::sync::atomic::*;
use std::sync::Arc;

use futures::future::*;
use futures::executor::block_on;
use rand::prelude::*;
use rand::rngs::mock::StepRng;
use stopwatch::Stopwatch;
use structopt::StructOpt;

use crate::fdb::error::*;
use crate::fdb::*;

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

    fn decr(&self, n: usize) -> bool {
        let val = self.inner.fetch_add(n, Ordering::SeqCst);
        val < self.size
    }
}

struct BenchRunner {
    #[allow(unused)]
    db: Database,
    counter: Counter,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    rng: StepRng,
    trx_batch_size: usize,
}

impl BenchRunner {
    fn new(db: Database, rng: StepRng, counter: Counter, opt: &Opt) -> Self {
        let mut key_buf = Vec::with_capacity(opt.key_len);
        key_buf.resize(opt.key_len, 0u8);

        let mut val_buf = Vec::with_capacity(opt.val_len);
        val_buf.resize(opt.val_len, 0u8);

        Self {
            db,
            counter,
            key_buf,
            val_buf,

            rng,
            trx_batch_size: opt.trx_batch_size,
        }
    }

    //TODO: impl future
    async fn run(mut self) -> Result<()> {
        let trx = self.db.create_trx().expect("failed to create trx");

        loop {
            let trx = trx.clone();
            for _ in 0..self.trx_batch_size {
                self.rng.fill_bytes(&mut self.key_buf);
                self.rng.fill_bytes(&mut self.val_buf);
                self.key_buf[0] = 0x01;
                trx.set(&self.key_buf, &self.val_buf);
            }

            let trx2 = trx.clone();
            trx.commit().await?;
            trx2.reset();

            if self.counter.decr(self.trx_batch_size) {
                continue;
            } else {
                break;
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
struct Bench {
    db: Database,
    opt: Opt,
}

impl Bench {
    fn run(self) {
        let opt = &self.opt;
        let counter = Counter::new(opt.count);

        let mut handles = Vec::new();

        let sw = Stopwatch::start_new();

        let step = (opt.queue_depth + opt.threads - 1) / opt.threads;
        let mut start = 0;
        for _ in 0..opt.threads {
            let end = std::cmp::min(start + step, opt.queue_depth);

            let range = start..end;
            let counter = counter.clone();
            let b = self.clone();
            let handle = std::thread::spawn(move || block_on(b.run_range(range, counter)));
            handles.push(handle);

            start = end;
        }

        for handle in handles {
            handle
                .join()
                .expect("failed to join")
                .expect("failed to run bench");
        }

        let elapsed = sw.elapsed_ms() as usize;

        info!(
            "bench took: {:?} ms, {:?} tps",
            elapsed,
            1000 * opt.count / elapsed
        );
    }

    async fn run_range(
        &self,
        r: std::ops::Range<usize>,
        counter: Counter,
    ) -> Result<()> {
        let runners = r
            .into_iter()
            .map(|n| {
                // With deterministic Rng, benchmark with same parameters will overwrite same set
                // of keys again, which makes benchmark result stable.
                let rng = StepRng::new(n as u64, 1);
                BenchRunner::new(self.db.clone(), rng, counter.clone(), &self.opt).run()
            })
            .collect::<Vec<_>>();

        for r in join_all(runners).await {
            r.expect("didn't expect error in transaction");
        }

        Ok(())
    }
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "fdb-bench")]
struct Opt {
    #[structopt(short = "t", long = "threads", default_value = "1")]
    threads: usize,

    #[structopt(short = "q", long = "queue-depth", default_value = "1000")]
    queue_depth: usize,

    #[structopt(short = "c", long = "count", default_value = "300000")]
    count: usize,

    #[structopt(long = "trx-batch-size", default_value = "10")]
    trx_batch_size: usize,

    #[structopt(long = "key-len", default_value = "10")]
    key_len: usize,
    #[structopt(long = "val-len", default_value = "100")]
    val_len: usize,
}

fn main() {
    env_logger::init();
    let opt = Opt::from_args();

    info!("opt: {:?}", opt);

    let network = fdb::init().expect("failed to init network");

    let handle = std::thread::spawn(move || {
        let error = network.run();

        if let Err(error) = error {
            panic!("fdb_run_network: {}", error);
        }
    });

    network.wait();

    let db = Database::new(foundationdb::default_config_path()).expect("db connect failed");

    let bench = Bench { db, opt };
    bench.run();

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
