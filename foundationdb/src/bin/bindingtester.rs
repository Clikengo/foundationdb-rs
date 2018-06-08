extern crate foundationdb as fdb;
extern crate foundationdb_sys as fdb_sys;
extern crate futures;
#[macro_use]
extern crate log;

use std::collections::HashMap;

use fdb::error::Error;
use fdb::keyselector::KeySelector;
use fdb::tuple::*;
use fdb::*;
use futures::future::*;
use futures::prelude::*;

use fdb::options::{MutationType, StreamingMode};
fn mutation_from_str(s: &str) -> MutationType {
    match s {
        "ADD" => MutationType::Add,
        "AND" => MutationType::And,
        "BIT_AND" => MutationType::BitAnd,
        "OR" => MutationType::Or,
        "BIT_OR" => MutationType::BitOr,
        "XOR" => MutationType::Xor,
        "BIT_XOR" => MutationType::BitXor,
        "MAX" => MutationType::Max,
        "MIN" => MutationType::Min,
        "SET_VERSIONSTAMPED_KEY" => MutationType::SetVersionstampedKey,
        "SET_VERSIONSTAMPED_VALUE" => MutationType::SetVersionstampedValue,
        "BYTE_MIN" => MutationType::ByteMin,
        "BYTE_MAX" => MutationType::ByteMax,
        _ => unimplemented!(),
    }
}

pub fn streaming_from_value(val: i32) -> StreamingMode {
    match val {
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_WANT_ALL => StreamingMode::WantAll,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR => StreamingMode::Iterator,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_EXACT => StreamingMode::Exact,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_SMALL => StreamingMode::Small,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_MEDIUM => StreamingMode::Medium,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_LARGE => StreamingMode::Large,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_SERIAL => StreamingMode::Serial,
        _ => unimplemented!(),
    }
}

struct Instr {
    code: InstrCode,
    database: bool,
    snapshot: bool,
    starts_with: bool,
    selector: bool,
}

impl std::fmt::Debug for Instr {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "[{:?}", self.code)?;
        if self.database {
            write!(fmt, " db")?;
        }
        if self.snapshot {
            write!(fmt, " snapshot")?;
        }
        if self.starts_with {
            write!(fmt, " starts_with")?;
        }
        if self.selector {
            write!(fmt, " selector")?;
        }
        write!(fmt, "]")
    }
}

impl Instr {
    fn pop_database(&mut self) -> bool {
        if self.database {
            self.database = false;
            true
        } else {
            false
        }
    }
    fn pop_snapshot(&mut self) -> bool {
        if self.snapshot {
            self.snapshot = false;
            true
        } else {
            false
        }
    }
    fn pop_starts_with(&mut self) -> bool {
        if self.starts_with {
            self.starts_with = false;
            true
        } else {
            false
        }
    }
    fn pop_selector(&mut self) -> bool {
        if self.selector {
            self.selector = false;
            true
        } else {
            false
        }
    }

    fn has_flags(&self) -> bool {
        self.database || self.snapshot || self.starts_with || self.selector
    }
}

#[derive(Debug)]
enum InstrCode {
    // data operations
    Push(Vec<u8>),
    Dup,
    EmptyStack,
    Swap,
    Pop,
    Sub,
    Concat,
    LogStack,

    // foundationdb operations
    NewTransaction,
    UseTransacton,
    OnError,
    Get,
    GetKey,
    GetRange,
    GetReadVersion,
    GetVersionstamp,
    Set,
    SetReadVersion,
    Clear,
    ClearRange,
    AtomicOp,
    ReadConflictRange,
    WriteConflictRange,
    ReadConflictKey,
    WriteConflictKey,
    DisableWriteConflict,
    Commit,
    Reset,
    Cancel,
    GetCommittedVersion,
    WaitFuture,

    // TODO: tuple operations
    TuplePack,
    TuplePackWithVersionstamp,
    TupleUnpack,
    TupleRange,
    TupleSort,
    EncodeFloat,
    EncodeDouble,
    DecodeFloat,
    DecodeDouble,
    // TODO: thread operations
    // misc
    UnitTests,
}

fn has_opt<'a>(cmd: &'a str, opt: &'static str) -> (&'a str, bool) {
    if cmd.ends_with(opt) {
        (&cmd[0..(cmd.len() - opt.len())], true)
    } else {
        (cmd, false)
    }
}

impl Instr {
    fn from(data: &[u8]) -> Self {
        use InstrCode::*;

        let tup: Tuple = Decode::try_from(data).unwrap();
        let cmd = match tup[0] {
            Element::String(ref s) => s.clone(),
            _ => panic!("unexpected instr"),
        };

        let cmd = cmd.as_str();

        let (cmd, database) = has_opt(cmd, "_DATABASE");
        let (cmd, snapshot) = has_opt(cmd, "_SNAPSHOT");
        let (cmd, starts_with) = has_opt(cmd, "_STARTS_WITH");
        let (cmd, selector) = has_opt(cmd, "_SELECTOR");

        let code = match cmd {
            "PUSH" => {
                let data = tup[1].to_vec();
                Push(data)
            }
            "DUP" => Dup,
            "EMPTY_STACK" => EmptyStack,
            "SWAP" => Swap,
            "POP" => Pop,
            "SUB" => Sub,
            "CONCAT" => Concat,
            "LOG_STACK" => LogStack,

            "NEW_TRANSACTION" => NewTransaction,
            "USE_TRANSACTION" => UseTransacton,
            "ON_ERROR" => OnError,
            "GET" => Get,
            "GET_KEY" => GetKey,
            "GET_RANGE" => GetRange,
            "GET_READ_VERSION" => GetReadVersion,
            "GET_VERSIONSTAMP" => GetVersionstamp,

            "SET" => Set,
            "SET_READ_VERSION" => SetReadVersion,
            "CLEAR" => Clear,
            "CLEAR_RANGE" => ClearRange,
            "ATOMIC_OP" => AtomicOp,
            "READ_CONFLICT_RANGE" => ReadConflictRange,
            "WRITE_CONFLICT_RANGE" => WriteConflictRange,
            "READ_CONFLICT_KEY" => ReadConflictKey,
            "WRITE_CONFLICT_KEY" => WriteConflictKey,
            "DISABLE_WRITE_CONFLICT" => DisableWriteConflict,
            "COMMIT" => Commit,
            "RESET" => Reset,
            "CANCEL" => Cancel,
            "GET_COMMITTED_VERSION" => GetCommittedVersion,
            "WAIT_FUTURE" => WaitFuture,

            "TUPLE_PACK" => TuplePack,
            "TUPKE_PACK_WITH_VERSONSTAMP" => TuplePackWithVersionstamp,
            "TUPLE_UNPACK" => TupleUnpack,
            "TUPLE_RANGE" => TupleRange,
            "TUPLE_SORT" => TupleSort,
            "ENCODE_FLOAT" => EncodeFloat,
            "ENCODE_DOUBLE" => EncodeDouble,
            "DECODE_FLOAT" => DecodeFloat,
            "DECODE_DOUBLE" => DecodeDouble,

            "UNIT_TESTS" => UnitTests,

            name => unimplemented!("inimplemented instr: {}", name),
        };
        Instr {
            code,
            database,
            snapshot,
            starts_with,
            selector,
        }
    }
}

type StackFuture = Box<Future<Item = (Transaction, Vec<u8>), Error = Error>>;
struct StackItem {
    number: usize,
    // TODO: enum
    data: Option<Vec<u8>>,
    fut: Option<StackFuture>,
}

impl Clone for StackItem {
    fn clone(&self) -> Self {
        if self.fut.is_some() {
            panic!("cannot clone future stack item");
        }
        Self {
            number: self.number,
            data: self.data.clone(),
            fut: None,
        }
    }
}

impl StackItem {
    fn data(self) -> Vec<u8> {
        if let Some(data) = self.data {
            return data;
        }

        //TODO: wait
        match self.fut.unwrap().wait() {
            Ok((_trx, data)) => data.to_vec(),
            Err(e) => {
                let code = format!("{}", e.code());
                let tup = (b"ERROR".to_vec(), code.into_bytes());
                debug!("ERROR: {:?}", e);
                let bytes = tup.to_vec();
                bytes.to_vec()
            }
        }
    }
}

impl std::fmt::Debug for StackItem {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "[item num={}, data={:?}]", self.number, self.data)
    }
}

struct StackMachine {
    db: Database,
    prefix: Vec<u8>,

    // A global transaction map from byte string to Transactions. This map is shared by all tester
    // 'threads'.
    transactions: HashMap<Vec<u8>, Transaction>,

    // A stack of data items of mixed types and their associated metadata. At a minimum, each item
    // should be stored with the 0-based instruction number which resulted in it being put onto the
    // stack. Your stack must support push and pop operations. It may be helpful if it supports
    // random access, clear and a peek operation. The stack is initialized to be empty.
    stack: Vec<StackItem>,

    // A current FDB transaction name (stored as a byte string). The transaction name should be
    // initialized to the prefix that instructions are being read from.
    cur_transaction: Vec<u8>,

    // A last seen FDB version, which is a 64-bit integer.
    last_version: i64,
}

fn strinc(key: &mut [u8]) {
    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            return;
        }

        if i == 0 {
            panic!("failed to strinc");
        }
    }
}

impl StackMachine {
    fn new(db: Database, prefix: String) -> Self {
        let cur_transaction = prefix.clone().into_bytes();
        let mut transactions = HashMap::new();
        transactions.insert(cur_transaction.clone(), db.create_trx().unwrap());

        Self {
            db,
            prefix: prefix.into_bytes(),

            transactions,
            stack: Vec::new(),
            cur_transaction,
            last_version: 0,
        }
    }

    fn fetch_instr(&self) -> Box<Future<Item = Vec<Instr>, Error = Error>> {
        let db = self.db.clone();

        let prefix = self.prefix.clone();
        let f = db.transact(move |trx| {
            let opt = transaction::RangeOptionBuilder::from(&prefix).build();
            let instrs = Vec::new();
            let f = trx.get_ranges(opt)
                .map_err(|(_opt, e)| e)
                .fold(instrs, |mut instrs, res| {
                    let kvs = res.key_values();

                    for kv in kvs.as_ref() {
                        let instr = Instr::from(kv.value());
                        instrs.push(instr);
                    }
                    Ok::<_, Error>(instrs)
                });
            f
        });
        Box::new(f)
    }

    fn pop(&mut self) -> StackItem {
        self.stack.pop().expect("stack empty")
    }

    fn pop_item<S>(&mut self) -> S
    where
        S: Decode,
    {
        let data = self.pop().data();
        match Decode::try_from(&data) {
            Ok(v) => v,
            Err(e) => {
                panic!("failed to decode item {:?}: {:?}", data, e);
            }
        }
    }

    fn pop_data(&mut self) -> Vec<u8> {
        self.pop().data()
    }

    fn pop_selector(&mut self) -> KeySelector {
        let key: Vec<u8> = self.pop_item();
        let or_equal: i64 = self.pop_item();
        let offset: i64 = self.pop_item();

        KeySelector::new(key, or_equal != 0, offset as usize)
    }

    fn push_item<S>(&mut self, number: usize, s: &S)
    where
        S: Encode,
    {
        let data = s.to_vec();
        self.push(number, data);
    }

    fn push(&mut self, number: usize, data: Vec<u8>) {
        self.stack.push(StackItem {
            number,
            data: Some(data),
            fut: None,
        });
    }

    fn push_fut<F>(&mut self, number: usize, fut: F)
    where
        F: Future<Item = (Transaction, Vec<u8>), Error = Error> + 'static,
    {
        let item = StackItem {
            number,
            data: None,
            fut: Some(Box::new(fut)),
        };
        self.stack.push(item);
    }

    fn run_step(&mut self, number: usize, mut instr: Instr) {
        use InstrCode::*;

        let is_db = instr.pop_database();
        let mut mutation = false;
        let trx = if is_db {
            self.db.create_trx().unwrap()
        } else {
            self.transactions
                .get(&self.cur_transaction)
                .cloned()
                .expect("failed to find trx")
        };

        match instr.code {
            Push(ref data) => self.push(number, data.clone()),
            Dup => {
                let top = self.pop();
                self.stack.push(top.clone());
                self.stack.push(top.clone());
            }
            EmptyStack => self.stack.clear(),
            Swap => {
                let idx: i64 = self.pop_item();
                {
                    let len = self.stack.len();
                    let idx1 = len - 1;
                    //XXX
                    let idx2 = len - 1 - (idx as usize);

                    let tmp = self.stack[idx1].clone();
                    self.stack[idx1] = self.stack[idx2].clone();
                    self.stack[idx2] = tmp;
                }
            }
            Pop => {
                self.pop();
            }
            Sub => {
                let a: i64 = self.pop_item();
                let b: i64 = self.pop_item();

                self.push_item(number, &(a - b));
            }
            Concat => {
                let mut a: Vec<u8> = self.pop_item();
                let mut b: Vec<u8> = self.pop_item();
                a.append(&mut b);
                self.push_item(number, &a);
            }
            // LogStack,
            NewTransaction => {
                let name = self.cur_transaction.clone();
                let trx = self.db.create_trx().expect("failed to create trx");

                self.transactions.insert(name, trx);
            }
            UseTransacton => {
                let name: Vec<u8> = self.pop_item();
                self.cur_transaction = name;
            }
            OnError => {
                let code: i64 = self.pop_item();
                let trx0 = trx.clone();
                let f = trx.on_error(Error::from(code as i32))
                    .map(move |_| (trx0, b"RESULT_NOT_PRESENT".to_vec()));
                self.push_fut(number, f);
            }
            Get => {
                let key: Vec<u8> = self.pop_item();
                let trx0 = trx.clone();
                let f = trx.get(&key, instr.pop_snapshot()).map(move |res| {
                    let val = res.value();
                    let val = match val {
                        Some(v) => v.to_vec(),
                        None => b"RESULT_NOT_PRESENT".to_vec(),
                    };

                    debug!("get  : key={:?}, value={:?}", key, val);
                    (trx0, val)
                });

                self.push_fut(number, f);
            }

            GetKey => {
                let selector = self.pop_selector();
                let mut prefix: Vec<u8> = self.pop_item();

                //TODO: wait
                let key = trx.get_key(selector, instr.pop_snapshot())
                    .map(move |res| res.value().to_vec())
                    .wait()
                    .unwrap();

                if key.starts_with(&prefix) {
                    self.push_item(number, &key);
                } else if key < prefix {
                    self.push_item(number, &prefix);
                } else {
                    strinc(&mut prefix);
                    self.push_item(number, &prefix);
                }
            }

            GetRange => {
                let selector = instr.pop_selector();

                let (begin, end) = if instr.pop_starts_with() {
                    let begin: Vec<u8> = self.pop_item();
                    let mut end = begin.clone();
                    strinc(&mut end);
                    (
                        KeySelector::first_greater_or_equal(&begin),
                        KeySelector::first_greater_or_equal(&end),
                    )
                } else if selector {
                    let begin = self.pop_selector();
                    let end = self.pop_selector();
                    (begin, end)
                } else {
                    let begin: Vec<u8> = self.pop_item();
                    let end: Vec<u8> = self.pop_item();
                    (
                        KeySelector::first_greater_or_equal(&begin),
                        KeySelector::first_greater_or_equal(&end),
                    )
                };

                let limit: i64 = self.pop_item();
                let reverse: i64 = self.pop_item();
                let streaming_mode: i64 = self.pop_item();
                let mode = streaming_from_value(streaming_mode as i32);

                debug!(
                    "range: begin={:?}, end={:?}, limit={:?}, rev={:?}, mode={:?}",
                    begin, end, limit, reverse, mode
                );

                let prefix: Option<Vec<u8>> = if selector {
                    Some(self.pop_item())
                } else {
                    None
                };

                let opt = transaction::RangeOptionBuilder::new(begin, end)
                    .mode(mode)
                    .limit(limit as usize)
                    .reverse(reverse != 0)
                    .snapshot(instr.pop_snapshot())
                    .build();

                let mut out = Vec::new();
                let trx0 = trx.clone();
                let f = trx.get_ranges(opt)
                    .map_err(|(_, e)| e)
                    .fold(out, move |mut out, res| {
                        let kvs = res.key_values();

                        debug!("range: len={:?}", kvs.as_ref().len());

                        for kv in kvs.as_ref() {
                            let key = kv.key();
                            let value = kv.value();
                            debug!("key: {:?}, value: {:?}", key, value);
                            if let Some(ref prefix) = prefix {
                                if !key.starts_with(prefix) {
                                    continue;
                                }
                            }
                            key.to_vec().encode_to(&mut out).expect("failed to encode");
                            value
                                .to_vec()
                                .encode_to(&mut out)
                                .expect("failed to encode");
                        }
                        Ok::<_, Error>(out)
                    })
                    .map(|out| (trx0, out));

                //TODO: wait
                self.push_fut(number, f);

                let item = self.pop();
                let number = item.number;
                self.push(number, item.data());
            }

            GetReadVersion => {
                //TODO: wait
                let version = trx.get_read_version()
                    .wait()
                    .expect("failed to get read version");

                //TODO
                instr.pop_snapshot();

                self.last_version = version;
                self.push_item(number, &b"GOT_READ_VERSION".to_vec());
            }

            GetVersionstamp => {
                let trx0 = trx.clone();
                let f = trx.clone()
                    .get_versionstamp()
                    .map(move |v| (trx0, v.versionstamp().to_vec()));
                self.push_fut(number, f);
            }

            Set => {
                let key: Vec<u8> = self.pop_item();
                let value: Vec<u8> = self.pop_item();

                debug!("set  : key={:?}, value={:?}", key, value);
                trx.set(&key, &value);
                mutation = true;
            }

            SetReadVersion => {
                trx.set_read_version(self.last_version);
            }

            Clear => {
                let key: Vec<u8> = self.pop_item();
                trx.clear(&key);

                debug!("clear: key={:?}", key);
                mutation = true;
            }

            ClearRange => {
                let begin: Vec<u8> = self.pop_item();
                let end = if instr.pop_starts_with() {
                    let mut end = begin.clone();
                    strinc(&mut end);
                    end
                } else {
                    let end: Vec<u8> = self.pop_item();
                    end
                };
                trx.clear_range(&begin, &end);
                mutation = true;
            }

            AtomicOp => {
                let optype: String = self.pop_item();
                let key: Vec<u8> = self.pop_item();
                let value: Vec<u8> = self.pop_item();

                let op = mutation_from_str(&optype);
                trx.atomic_op(&key, &value, op);
                mutation = true;
            }

            Reset => {
                trx.clone().reset();
            }

            Commit => {
                let f = trx.clone()
                    .commit()
                    .map(|trx| (trx, b"RESULT_NOT_PRESENT".to_vec()));
                self.push_fut(number, f);
            }

            GetCommittedVersion => {
                let last_version = trx.committed_version()
                    .expect("failed to get committed version");
                self.last_version = last_version;
                self.push_item(number, &b"GOT_COMMITTED_VERSION".to_vec());
            }

            WaitFuture => {
                //TODO
                let item = self.pop();
                let number = item.number;
                self.push(number, item.data());
            }

            TuplePack => {
                let n: i64 = self.pop_item();

                let mut buf = Vec::new();
                for _ in 0..n {
                    let mut data = self.pop_data();
                    buf.append(&mut data);
                }
                self.push_item(number, &buf);
            }

            TupleUnpack => {
                let data: Vec<u8> = self.pop_item();
                let mut data = data.as_slice();

                while !data.is_empty() {
                    let (val, offset): (Element, _) = Decode::decode_from(data).unwrap();
                    let bytes = val.to_vec();
                    self.push_item(number, &bytes);
                    data = &data[offset..];
                }
            }

            TupleRange => {
                let n: i64 = self.pop_item();

                let mut tup = Vec::new();
                for _ in 0..n {
                    let mut data = self.pop_data();
                    tup.append(&mut data);
                }

                //TODO
                {
                    let mut data = tup.clone();
                    data.push(0x00);
                    self.push_item(number, &data);
                }
                {
                    let mut data = tup.clone();
                    data.push(0xff);
                    self.push_item(number, &data);
                }
            }

            UnitTests => {
                //TODO
            }

            instr => {
                unimplemented!("instr: {:?}", instr);
            }
        }

        if is_db && mutation {
            //TODO
            trx.commit().wait().expect("failed to commit");
            self.push_item(number, &b"RESULT_NOT_PRESENT".to_vec());
        }

        if instr.has_flags() {
            panic!("flag not handled for instr: {:?}", instr);
        }
    }

    fn run(&mut self) {
        let instrs = self.fetch_instr()
            .wait()
            .expect("failed to read instructions");

        for (i, instr) in instrs.into_iter().enumerate() {
            debug!("{}/{}, {:?}", i, self.stack.len(), instr);
            self.run_step(i, instr);

            /*
            if i == 135 {
                break;
            }
            */
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let prefix = &args[1];

    let mut cluster_path = fdb::default_config_path();
    if args.len() > 3 {
        cluster_path = &args[3];
    }

    let api_version = args[2].parse::<i32>().expect("failed to parse api version");

    let network = fdb_api::FdbApiBuilder::default()
        .set_runtime_version(api_version)
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

    let cluster = Cluster::new(cluster_path)
        .wait()
        .expect("failed to create cluster");

    let db = cluster
        .create_database()
        .wait()
        .expect("failed to get database");

    let mut sm = StackMachine::new(db, prefix.to_owned());

    sm.run();

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
