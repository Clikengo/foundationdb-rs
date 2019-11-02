#[macro_use]
extern crate log;

use foundationdb as fdb;
use foundationdb_sys as fdb_sys;

use std::borrow::Cow;
use std::collections::HashMap;
use std::pin::Pin;

use fdb::error::Error;
use fdb::keyselector::KeySelector;
use fdb::options::{ConflictRangeType, DatabaseOption, TransactionOption};
use fdb::tuple::{de::from_bytes, ser::into_bytes, ser::to_bytes, Bytes, Element, Subspace};
use fdb::*;
use futures::future;
use futures::prelude::*;

static RESULT_NOT_PRESENT: Bytes = Bytes(Cow::Borrowed(b"RESULT_NOT_PRESENT"));

use crate::fdb::options::{MutationType, StreamingMode};
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
    UseTransaction,
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

    TuplePack,
    TuplePackWithVersionstamp,
    TupleUnpack,
    TupleRange,
    TupleSort,
    EncodeFloat,
    EncodeDouble,
    DecodeFloat,
    DecodeDouble,

    // Thread Operations
    StartThread,
    WaitEmpty,

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
        use crate::InstrCode::*;

        let data = Bytes::from(data);
        let tup: Vec<Element> = from_bytes(&data).unwrap();
        let cmd = tup[0].as_str().unwrap();

        let (cmd, database) = has_opt(cmd, "_DATABASE");
        let (cmd, snapshot) = has_opt(cmd, "_SNAPSHOT");
        let (cmd, starts_with) = has_opt(cmd, "_STARTS_WITH");
        let (cmd, selector) = has_opt(cmd, "_SELECTOR");

        let code = match cmd {
            "PUSH" => Push(to_bytes(&tup[1]).unwrap()),
            "DUP" => Dup,
            "EMPTY_STACK" => EmptyStack,
            "SWAP" => Swap,
            "POP" => Pop,
            "SUB" => Sub,
            "CONCAT" => Concat,
            "LOG_STACK" => LogStack,

            "NEW_TRANSACTION" => NewTransaction,
            "USE_TRANSACTION" => UseTransaction,
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

            "START_THREAD" => StartThread,
            "WAIT_EMPTY" => WaitEmpty,

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

struct StackFutResult {
    state: Option<(Bytes<'static>, TransactionState)>,
    data: Vec<u8>,
}
impl From<Vec<u8>> for StackFutResult {
    fn from(data: Vec<u8>) -> Self {
        StackFutResult { state: None, data }
    }
}

type StackFuture = Pin<Box<dyn Future<Output = FdbResult<StackFutResult>>>>;
struct StackItem {
    number: usize,
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

impl std::fmt::Debug for StackItem {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "[item num={}, data={:?}]", self.number, self.data)
    }
}

fn range(bytes: Bytes) -> (Bytes<'static>, Bytes<'static>) {
    let mut begin = bytes.into_owned();
    let mut end = begin.clone();

    begin.push(0x00);
    end.push(0xff);

    (begin.into(), end.into())
}

enum TransactionState {
    Transaction(Transaction),
    TransactionCommitted(TransactionCommitted),
    TransactionCommitError(TransactionCommitError),
    Pending,
}
impl std::fmt::Debug for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use TransactionState as S;

        match self {
            S::Transaction(..) => "Transaction",
            S::TransactionCommitted(..) => "TransactionCommitted",
            S::TransactionCommitError(..) => "TransactionCommitError",
            S::Pending => "Pending",
        }
        .fmt(f)
    }
}

impl TransactionState {
    fn reset(&mut self) {
        use TransactionState as S;
        *self = match std::mem::replace(self, TransactionState::Pending) {
            S::TransactionCommitted(c) => S::Transaction(c.reset()),
            S::TransactionCommitError(c) => S::Transaction(c.reset()),
            c => c,
        }
    }

    fn as_mut(&mut self) -> &mut Transaction {
        use TransactionState as S;

        self.reset();
        match *self {
            S::Transaction(ref mut tr) => tr,
            _ => panic!("transaction is owned by a future that is still not done"),
        }
    }

    fn take(&mut self) -> Transaction {
        use TransactionState as S;

        self.reset();
        match std::mem::replace(self, TransactionState::Pending) {
            S::Transaction(tr) => {
                *self = S::Pending;
                tr
            }
            _ => panic!("transaction is owned by a future that is still not done"),
        }
    }
}

struct StackMachine {
    prefix: Bytes<'static>,

    // A global transaction map from byte string to Transactions. This map is shared by all tester
    // 'threads'.
    transactions: HashMap<Bytes<'static>, TransactionState>,

    // A stack of data items of mixed types and their associated metadata. At a minimum, each item
    // should be stored with the 0-based instruction number which resulted in it being put onto the
    // stack. Your stack must support push and pop operations. It may be helpful if it supports
    // random access, clear and a peek operation. The stack is initialized to be empty.
    stack: Vec<StackItem>,

    // A current FDB transaction name (stored as a byte string). The transaction name should be
    // initialized to the prefix that instructions are being read from.
    cur_transaction: Bytes<'static>,

    // A last seen FDB version, which is a 64-bit integer.
    last_version: i64,
}

fn strinc(key: Bytes) -> Bytes {
    let mut key = key.into_owned();
    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            return Bytes::from(key);
        }
    }
    panic!("failed to strinc");
}

impl StackMachine {
    fn new(db: &Database, prefix: String) -> Self {
        let prefix = Bytes::from(prefix.clone().into_bytes());
        let cur_transaction = prefix.clone();
        let mut transactions = HashMap::new();
        transactions.insert(
            cur_transaction.clone(),
            TransactionState::Transaction(db.create_trx().unwrap()),
        );

        Self {
            prefix,
            transactions,
            stack: Vec::new(),
            cur_transaction,
            last_version: 0,
        }
    }

    async fn fetch_instr(&self, trx: &Transaction) -> Result<Vec<Instr>, Error> {
        let opt = RangeOptionBuilder::from(&Subspace::from(&self.prefix)).build();
        debug!("opt = {:?}", opt);
        let instrs = Vec::new();
        trx.get_ranges(opt, false)
            .try_fold(instrs, |mut instrs, res| {
                for kv in res.iter() {
                    let instr = Instr::from(kv.value());
                    instrs.push(instr);
                }
                future::ok(instrs)
            })
            .await
    }

    async fn pop(&mut self) -> StackItem {
        let mut item = self.stack.pop().expect("stack empty");
        if let Some(fut) = item.fut.take() {
            let data = fut
                .await
                .and_then(|r| {
                    if let Some((name, state)) = r.state {
                        trace!("{:?} = {:?}", name, state);
                        match state {
                            TransactionState::TransactionCommitError(e) => {
                                let err = FdbError::from_error_code(e.code());
                                self.transactions
                                    .insert(name, TransactionState::TransactionCommitError(e));
                                return Err(err);
                            }
                            state => {
                                self.transactions.insert(name, state);
                            }
                        }
                    }
                    Ok(r.data)
                })
                .unwrap_or_else(|err| {
                    trace!("ERROR {:?}", err);
                    let packed = to_bytes(&(
                        Bytes::from(b"ERROR".as_ref()),
                        Bytes::from(format!("{}", err.code()).into_bytes()),
                    ))
                    .unwrap();
                    to_bytes(&Bytes::from(packed)).unwrap()
                });
            item.data = Some(data);
        }
        item
    }

    async fn pop_item<S>(&mut self) -> S
    where
        S: for<'de> serde::Deserialize<'de>,
    {
        let data = self.pop_data().await;
        match from_bytes(&data) {
            Ok(v) => v,
            Err(e) => {
                panic!("failed to decode item {:?}: {:?}", Bytes::from(data), e);
            }
        }
    }

    async fn pop_bytes(&mut self) -> Bytes<'static> {
        let data = self.pop_data().await;
        match from_bytes::<Bytes>(&data) {
            Ok(v) => Bytes::from(v.into_owned()),
            Err(e) => {
                panic!("failed to decode bytes {:?}: {:?}", Bytes::from(data), e);
            }
        }
    }

    async fn pop_element(&mut self) -> Element<'static> {
        let data = self.pop_data().await;
        match from_bytes::<Element>(&data) {
            Ok(v) => v.into_owned(),
            Err(e) => {
                panic!("failed to decode bytes {:?}: {:?}", Bytes::from(data), e);
            }
        }
    }

    async fn pop_data(&mut self) -> Vec<u8> {
        let item = self.pop().await;
        if let Some(data) = item.data {
            return data;
        }
        panic!("no data");
    }

    async fn pop_selector(&mut self) -> KeySelector<'static> {
        let key: Bytes = self.pop_bytes().await;
        let or_equal: i32 = self.pop_item().await;
        let offset: i32 = self.pop_item().await;

        KeySelector::new(key.0, or_equal != 0, offset)
    }

    fn push_item<S>(&mut self, number: usize, s: &S)
    where
        S: serde::Serialize,
    {
        let data = to_bytes(s).unwrap();
        self.push(number, data);
    }

    fn push(&mut self, number: usize, data: Vec<u8>) {
        self.stack.push(StackItem {
            number,
            data: Some(data),
            fut: None,
        });
    }

    fn push_fut(&mut self, number: usize, fut: StackFuture) {
        let item = StackItem {
            number,
            data: None,
            fut: Some(fut),
        };
        self.stack.push(item);
    }

    async fn run_step(&mut self, db: &Database, number: usize, mut instr: Instr) -> FdbResult<()> {
        use crate::InstrCode::*;

        let is_db = instr.pop_database();
        let mut mutation = false;
        let mut pending = false;
        let (mut trx, trx_name) = if is_db {
            (TransactionState::Transaction(db.create_trx()?), None)
        } else {
            (
                self.transactions
                    .remove(&self.cur_transaction) // some instr requires transaction ownership
                    .expect("failed to find trx"),
                Some(&self.cur_transaction),
            )
        };

        match instr.code {
            // Pushes the provided item onto the stack.
            Push(ref data) => {
                debug!("push {:?}", Bytes::from(data.as_slice()));
                self.push(number, data.clone())
            }
            // Duplicates the top item on the stack. The instruction number for
            // the duplicate item should be the same as the original.
            Dup => {
                let top = self.pop().await;
                debug!(
                    "dup {:?}",
                    Bytes::from(top.data.as_ref().unwrap().as_slice())
                );
                self.stack.push(top.clone());
                self.stack.push(top);
            }
            // Discards all items in the stack.
            EmptyStack => {
                debug!("empty_stack");
                self.stack.clear()
            }
            // Pops the top item off of the stack as INDEX.
            // Swaps the items in the stack at depth 0 and depth INDEX.
            // Does not modify the instruction numbers of the swapped items.
            Swap => {
                let depth: usize = self.pop_item().await;
                let depth_0 = self.stack.len() - 1;
                let depth = depth_0 - depth;
                debug!("swap {} {}", depth_0, depth);
                self.stack.swap(depth_0, depth);
            }
            // Pops and discards the top item on the stack.
            Pop => {
                debug!("pop");
                self.pop().await;
            }
            // Pops the top two items off of the stack as A and B and then
            // pushes the difference (A-B) onto the stack.
            // A and B may be assumed to be integers.
            Sub => {
                let a: i64 = self.pop_item().await;
                let b: i64 = self.pop_item().await;
                debug!("sub {:?} - {:?}", a, b);
                self.push_item(number, &(a - b));
            }
            // Pops the top two items off the stack as A and B and then pushes
            // the concatenation of A and B onto the stack. A and B can be
            // assumed to be of the same type and will be either byte strings or
            // unicode strings.
            Concat => {
                let a = self.pop_data().await;
                let b = self.pop_data().await;
                debug!(
                    "concat {:?} {:?}",
                    Bytes::from(a.as_slice()),
                    Bytes::from(b.as_slice())
                );
                if let (Ok(a), Ok(b)) = (from_bytes::<Bytes>(&a), from_bytes::<Bytes>(&b)) {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(&a);
                    bytes.extend_from_slice(&b);
                    self.push_item(number, &Bytes::from(bytes));
                } else if let (Ok(a), Ok(b)) = (from_bytes::<String>(&a), from_bytes::<String>(&b))
                {
                    self.push_item(number, &format!("{}{}", a, b));
                } else {
                    panic!("failed to decode item {:?} {:?}", a, b);
                }
            }
            // Pops the top item off the stack as PREFIX. Using a new
            // transaction with normal retry logic, inserts a key-value pair
            // into the database for each item in the stack of the form:
            //
            // PREFIX + tuple.pack((stackIndex, instructionNumber)) = tuple.pack((item,))
            //
            // where stackIndex is the current index of the item in the stack.
            // The oldest item in the stack should have stackIndex 0.
            //
            // If the byte string created by tuple packing the item exceeds 40000 bytes,
            // then the value should be truncated to the first 40000 bytes of the packed
            // tuple.
            //
            // When finished, the stack should be empty. Note that because the stack may be
            // large, it may be necessary to commit the transaction every so often (e.g.
            // after every 100 sets) to avoid past_version errors.
            LogStack => {
                let _prefix: Bytes = self.pop_bytes().await;
                // TODO
            }

            // Creates a new transaction and stores it in the global transaction map
            // under the currently used transaction name.
            NewTransaction => {
                let name = self.cur_transaction.clone();
                debug!("create_trx {:?}", name);
                self.transactions
                    .insert(name, TransactionState::Transaction(db.create_trx()?));
            }

            // Pop the top item off of the stack as TRANSACTION_NAME. Begin using the
            // transaction stored at TRANSACTION_NAME in the transaction map for future
            // operations. If no entry exists in the map for the given name, a new
            // transaction should be inserted.
            UseTransaction => {
                let name: Bytes = self.pop_bytes().await;
                debug!("use_transaction {:?}", name);
                if !self.transactions.contains_key(&name) {
                    self.transactions.insert(
                        name.clone(),
                        TransactionState::Transaction(db.create_trx()?),
                    );
                }
                self.cur_transaction = name;
            }
            // Pops the top item off of the stack as ERROR_CODE. Passes ERROR_CODE in a
            // language-appropriate way to the on_error method of current transaction
            // object and blocks on the future. If on_error re-raises the error, bubbles
            // the error out as indicated above. May optionally push a future onto the
            // stack.
            OnError => {
                let trx_name = trx_name.cloned();
                let error_code: i32 = self.pop_item().await;
                let error = Error::from_error_code(error_code);
                debug!("on_error {:?}", error);
                let f = trx
                    .take()
                    .on_error(&error)
                    .map_ok(|trx| StackFutResult {
                        state: trx_name.map(|n| (n, TransactionState::Transaction(trx))),
                        data: to_bytes(&RESULT_NOT_PRESENT).unwrap(),
                    })
                    .boxed_local();
                self.push_fut(number, f);
                pending = true;
            }

            // Pops the top item off of the stack as KEY and then looks up KEY in the
            // database using the get() method. May optionally push a future onto the
            // stack.
            Get => {
                let key: Bytes = self.pop_bytes().await;
                debug!("get {:?}", key);
                let f = trx
                    .as_mut()
                    .get(&key, instr.pop_snapshot())
                    .map_ok(|v| {
                        match v {
                            Some(v) => to_bytes(&Bytes::from(v.as_ref())).unwrap(),
                            None => to_bytes(&RESULT_NOT_PRESENT).unwrap(),
                        }
                        .into()
                    })
                    .boxed_local();
                self.push_fut(number, f);
                pending = true;
            }

            // Pops the top four items off of the stack as KEY, OR_EQUAL, OFFSET, PREFIX
            // and then constructs a key selector. This key selector is then resolved
            // using the get_key() method to yield RESULT. If RESULT starts with PREFIX,
            // then RESULT is pushed onto the stack. Otherwise, if RESULT < PREFIX, PREFIX
            // is pushed onto the stack. If RESULT > PREFIX, then strinc(PREFIX) is pushed
            // onto the stack. May optionally push a future onto the stack.
            GetKey => {
                let selector = self.pop_selector().await;
                let prefix: Bytes = self.pop_bytes().await;
                debug!("get_key {:?}, prefix = {:?}", selector, prefix);

                let f = trx
                    .as_mut()
                    .get_key(&selector, instr.pop_snapshot())
                    .map_ok(|key| {
                        {
                            let key = Bytes::from(key.as_ref());
                            if key.starts_with(&prefix) {
                                to_bytes(&key).unwrap()
                            } else if key < prefix {
                                to_bytes(&prefix).unwrap()
                            } else {
                                assert!(key > prefix);
                                to_bytes(&strinc(prefix)).unwrap()
                            }
                        }
                        .into()
                    })
                    .boxed_local();
                self.push_fut(number, f);
                pending = true;
            }

            // Pops the top five items off of the stack as BEGIN_KEY, END_KEY, LIMIT,
            // REVERSE and STREAMING_MODE. Performs a range read in a language-appropriate
            // way using these parameters. The resulting range of n key-value pairs are
            // packed into a tuple as [k1,v1,k2,v2,...,kn,vn], and this single packed value
            // is pushed onto the stack.
            GetRange => {
                let selector = instr.pop_selector();
                let starts_with = instr.pop_starts_with();
                let snapshot = instr.pop_snapshot();

                let trx_name = trx_name.cloned();
                let (begin, end) = if starts_with {
                    let begin: Bytes = self.pop_bytes().await;
                    let end = strinc(begin.clone());
                    (
                        KeySelector::first_greater_or_equal(begin.0),
                        KeySelector::first_greater_or_equal(end.0),
                    )
                } else if selector {
                    let begin = self.pop_selector().await;
                    let end = self.pop_selector().await;
                    (begin, end)
                } else {
                    let begin: Bytes = self.pop_bytes().await;
                    let end: Bytes = self.pop_bytes().await;
                    (
                        KeySelector::first_greater_or_equal(begin.0),
                        KeySelector::first_greater_or_equal(end.0),
                    )
                };

                let limit: i64 = self.pop_item().await;
                let reverse: i64 = self.pop_item().await;
                let streaming_mode: i32 = self.pop_item().await;
                let mode = streaming_from_value(streaming_mode);
                debug!(
                    "get_range begin={:?}, end={:?}, limit={:?}, rev={:?}, mode={:?}",
                    begin, end, limit, reverse, mode
                );

                let prefix: Option<Bytes> = if selector {
                    Some(self.pop_bytes().await)
                } else {
                    None
                };
                let opt = transaction::RangeOptionBuilder::new(begin, end)
                    .mode(mode)
                    .limit(limit as usize)
                    .reverse(reverse != 0)
                    .build();
                async fn get_range(
                    trx: Transaction,
                    trx_name: Option<Bytes<'static>>,
                    prefix: Option<Bytes<'static>>,
                    opt: transaction::RangeOption<'static>,
                    snapshot: bool,
                ) -> FdbResult<StackFutResult> {
                    let data: Vec<u8> = trx
                        .get_ranges(opt, snapshot)
                        .try_fold(Vec::new(), move |mut out, kvs| {
                            for kv in kvs.iter() {
                                let key = kv.key();
                                let value = kv.value();
                                debug!(" - {:?} {:?}", Bytes::from(key), Bytes::from(value));
                                if let Some(ref prefix) = prefix {
                                    if !key.starts_with(prefix) {
                                        continue;
                                    }
                                }
                                into_bytes(&Bytes::from(key), &mut out).expect("failed to encode");
                                into_bytes(&Bytes::from(value), &mut out)
                                    .expect("failed to encode");
                            }
                            future::ok(out)
                        })
                        .await?;
                    Ok(StackFutResult {
                        state: trx_name.map(|n| (n, TransactionState::Transaction(trx))),
                        data: to_bytes(&Bytes::from(data)).unwrap(),
                    })
                }
                let f = get_range(trx.take(), trx_name, prefix, opt, snapshot).boxed_local();
                self.push_fut(number, f);
                pending = true;
            }

            //  TODO #### GET_RANGE_STARTS_WITH (_SNAPSHOT, _DATABASE)

            // Pops the top four items off of the stack as PREFIX, LIMIT, REVERSE and
            // STREAMING_MODE. Performs a prefix range read in a language-appropriate way
            // using these parameters. Output is pushed onto the stack as with GET_RANGE.

            // #### GET_RANGE_SELECTOR (_SNAPSHOT, _DATABASE)

            // Pops the top ten items off of the stack as BEGIN_KEY, BEGIN_OR_EQUAL,
            // BEGIN_OFFSET, END_KEY, END_OR_EQUAL, END_OFFSET, LIMIT, REVERSE,
            // STREAMING_MODE, and PREFIX. Constructs key selectors BEGIN and END from
            // the first six parameters, and then performs a range read in a language-
            // appropriate way using BEGIN, END, LIMIT, REVERSE and STREAMING_MODE. Output
            // is pushed onto the stack as with GET_RANGE, excluding any keys that do not
            // begin with PREFIX.

            // Gets the current read version and stores it in the internal stack machine
            // state as the last seen version. Pushed the string "GOT_READ_VERSION" onto
            // the stack.
            GetReadVersion => {
                let _snapshot = instr.pop_snapshot();
                let version = trx
                    .as_mut()
                    .get_read_version()
                    .await
                    .expect("failed to get read version");

                self.last_version = version;
                self.push_item(number, &Bytes::from(b"GOT_READ_VERSION".as_ref()));
            }

            // Calls get_versionstamp and pushes the resulting future onto the stack.
            GetVersionstamp => {
                let f = trx
                    .as_mut()
                    .get_versionstamp()
                    .map_ok(|v| to_bytes(&Bytes::from(v.as_ref())).unwrap().into())
                    .boxed_local();
                self.push_fut(number, f);
                pending = true;
            }

            // Pops the top two items off of the stack as KEY and VALUE. Sets KEY to have
            // the value VALUE. A SET_DATABASE call may optionally push a future onto the
            // stack.
            Set => {
                let key: Bytes = self.pop_bytes().await;
                let value: Bytes = self.pop_bytes().await;
                debug!("set {:?} {:?}", key, value);
                trx.as_mut().set(&key, &value);
                mutation = true;
            }

            // Sets the current transaction read version to the internal state machine last
            // seen version.
            SetReadVersion => {
                debug!("set_read_version {:?}", self.last_version);
                trx.as_mut().set_read_version(self.last_version);
            }

            // Pops the top item off of the stack as KEY and then clears KEY from the
            // database. A CLEAR_DATABASE call may optionally push a future onto the stack.
            Clear => {
                let key: Bytes = self.pop_bytes().await;
                debug!("clear {:?}", key);
                trx.as_mut().clear(&key);
                mutation = true;
            }

            // CLEAR_RANGE
            // Pops the top two items off of the stack as BEGIN_KEY and END_KEY. Clears the
            // range of keys from BEGIN_KEY to END_KEY in the database. A
            // CLEAR_RANGE_DATABASE call may optionally push a future onto the stack.
            //
            // CLEAR_RANGE_STARTS_WITH
            // Pops the top item off of the stack as PREFIX and then clears all keys from
            // the database that begin with PREFIX. A CLEAR_RANGE_STARTS_WITH_DATABASE call
            // may optionally push a future onto the stack.
            ClearRange => {
                let starts_with = instr.pop_starts_with();
                let (begin, end) = if starts_with {
                    let prefix = self.pop_bytes().await;
                    range(prefix)
                } else {
                    let begin: Bytes = self.pop_bytes().await;
                    let end: Bytes = self.pop_bytes().await;
                    (begin, end)
                };
                debug!("clear_range {:?} {:?}", begin, end);
                trx.as_mut().clear_range(&begin, &end);
                mutation = true;
            }

            // Pops the top three items off of the stack as OPTYPE, KEY, and VALUE.
            // Performs the atomic operation described by OPTYPE upon KEY with VALUE. An
            // ATOMIC_OP_DATABASE call may optionally push a future onto the stack.
            AtomicOp => {
                let optype: String = self.pop_item().await;
                let key: Bytes = self.pop_bytes().await;
                let value: Bytes = self.pop_bytes().await;
                debug!("atomic_op {:?} {:?} {:?}", key, value, optype);

                let op = mutation_from_str(&optype);
                trx.as_mut().atomic_op(&key, &value, op);
                mutation = true;
            }

            // Pops the top two items off of the stack as BEGIN_KEY and END_KEY. Adds a
            // read conflict range or write conflict range from BEGIN_KEY to END_KEY.
            // Pushes the byte string "SET_CONFLICT_RANGE" onto the stack.
            ReadConflictRange => {
                let begin: Bytes = self.pop_bytes().await;
                let end: Bytes = self.pop_bytes().await;
                debug!("read_conflict_range {:?} {:?}", begin, end);
                trx.as_mut()
                    .add_conflict_range(&begin, &end, ConflictRangeType::Read)?;
            }
            WriteConflictRange => {
                let begin: Bytes = self.pop_bytes().await;
                let end: Bytes = self.pop_bytes().await;
                debug!("write_conflict_range {:?} {:?}", begin, end);
                trx.as_mut()
                    .add_conflict_range(&begin, &end, ConflictRangeType::Write)?;
            }
            // Pops the top item off of the stack as KEY. Adds KEY as a read conflict key
            // or write conflict key. Pushes the byte string "SET_CONFLICT_KEY" onto the
            // stack.
            ReadConflictKey => {
                let begin: Bytes = self.pop_bytes().await;
                let mut end = begin.clone().into_owned();
                end.push(0);
                debug!("read_conflict_key {:?} {:?}", begin, end);
                trx.as_mut()
                    .add_conflict_range(&begin, &end, ConflictRangeType::Read)?;
            }
            WriteConflictKey => {
                let begin: Bytes = self.pop_bytes().await;
                let mut end = begin.clone().into_owned();
                end.push(0);
                debug!("write_conflict_key {:?} {:?}", begin, end);
                trx.as_mut()
                    .add_conflict_range(&begin, &end, ConflictRangeType::Write)?;
            }
            // Sets the NEXT_WRITE_NO_WRITE_CONFLICT_RANGE transaction option on the
            // current transaction. Does not modify the stack.
            DisableWriteConflict => {
                debug!("disable_write_conflict");
                trx.as_mut()
                    .set_option(TransactionOption::NextWriteNoWriteConflictRange)?
            }
            // Commits the current transaction (with no retry behavior). May optionally
            // push a future onto the stack.
            Commit => {
                debug!("commit");
                let trx_name = trx_name.cloned();
                let f = trx
                    .take()
                    .commit()
                    .map(|r| {
                        Ok(match r {
                            Ok(c) => StackFutResult {
                                state: trx_name
                                    .map(|n| (n, TransactionState::TransactionCommitted(c))),
                                data: to_bytes(&RESULT_NOT_PRESENT).unwrap(),
                            },
                            Err(c) => StackFutResult {
                                state: trx_name
                                    .map(|n| (n, TransactionState::TransactionCommitError(c))),
                                data: Vec::new(),
                            },
                        })
                    })
                    .boxed_local();
                self.push_fut(number, f);
                pending = true;
            }
            // Resets the current transaction.
            Reset => {
                debug!("reset");
                trx.as_mut().reset();
            }
            // Cancels the current transaction.
            Cancel => {
                debug!("cancel");
                let cancelled = trx.take().cancel();
                trx = TransactionState::Transaction(cancelled.reset());
            }

            // Gets the committed version from the current transaction and stores it in the
            // internal stack machine state as the last seen version. Pushes the byte
            // string "GOT_COMMITTED_VERSION" onto the stack.
            GetCommittedVersion => {
                debug!("committed_version");
                if let TransactionState::TransactionCommitted(t) = &trx {
                    let last_version = t
                        .committed_version()
                        .expect("failed to get committed version");
                    self.last_version = last_version;
                    self.push_item(number, &Bytes::from(b"GOT_COMMITTED_VERSION".as_ref()));
                } else {
                    panic!("committed_version() called on a non commited transaction");
                }
            }

            // Pops the top item off the stack and pushes it back on. If the top item on
            // the stack is a future, this will have the side effect of waiting on the
            // result of the future and pushing the result on the stack. Does not change
            // the instruction number of the item.
            WaitFuture => {
                debug!("wait_future");
                let item = self.pop().await;
                self.stack.push(item);
            }
            // Pops the top item off of the stack as N. Pops the next N items off of the
            // stack and packs them as the tuple [item0,item1,...,itemN], and then pushes
            // this single packed value onto the stack.
            TuplePack => {
                let n: usize = self.pop_item().await;
                debug!("tuple_pack {}", n);
                let mut buf = Vec::new();
                for _ in 0..n {
                    let element: Element = self.pop_element().await;
                    debug!(" - {:?}", element);
                    buf.push(element);
                }
                let packed = to_bytes(&buf).unwrap();
                self.push_item(number, &Bytes::from(packed));
            }

            // Pops the top item off of the stack as a byte string prefix. Pops the next item
            // off of the stack as N. Pops the next N items off of the stack and packs them
            // as the tuple [item0,item1,...,itemN], with the provided prefix and tries to
            // append the position of the first incomplete versionstamp as if the byte
            // string were to be used as a key in a SET_VERSIONSTAMP_KEY atomic op. If there
            // are no incomplete versionstamp instances, then this pushes the literal byte
            // string 'ERROR: NONE' to the stack. If there is more than one, then this pushes
            // the literal byte string 'ERROR: MULTIPLE'. If there is exactly one, then it pushes
            // the literal byte string 'OK' and then pushes the packed tuple. (Languages that
            // do not contain a 'Versionstamp' tuple-type do not have to implement this
            // operation.)
            TuplePackWithVersionstamp => {
                let prefix = self.pop_bytes().await;
                let n: usize = self.pop_item().await;
                let mut buf = Vec::new();
                for _ in 0..n {
                    let mut data = self.pop_data().await;
                    buf.append(&mut data);
                }
                unimplemented!()
            }

            // Pops the top item off of the stack as PACKED, and then unpacks PACKED into a
            // tuple. For each element of the tuple, packs it as a new tuple and pushes it
            // onto the stack.
            TupleUnpack => {
                let data = self.pop_bytes().await;
                debug!("tuple_unpack {:?}", data);
                let data: Vec<Element> = from_bytes(&data).unwrap();
                for element in data {
                    debug!(" - {:?}", element);
                    self.push_item(number, &Bytes::from(to_bytes(&element).unwrap()));
                }
            }
            // Pops the top item off of the stack as N. Pops the next N items off of the
            // stack, and passes these items as a tuple (or array, or language-appropriate
            // structure) to the tuple range method. Pushes the begin and end elements of
            // the returned range onto the stack.
            TupleRange => {
                let n: usize = self.pop_item().await;
                debug!("tuple_range {:?}", n);
                let mut tup = Vec::new();
                for _ in 0..n {
                    let mut data = self.pop_data().await;
                    tup.append(&mut data);
                }

                {
                    let mut data = tup.clone();
                    data.push(0x00);
                    self.push_item(number, &Bytes::from(data));
                }
                {
                    let mut data = tup;
                    data.push(0xff);
                    self.push_item(number, &Bytes::from(data));
                }
            }

            // Pops the top item off of the stack as PREFIX. Creates a new stack machine
            // instance operating on the same database as the current stack machine, but
            // operating on PREFIX. The new stack machine should have independent internal
            // state. The new stack machine should begin executing instructions concurrent
            // with the current stack machine through a language-appropriate mechanism.
            StartThread => {
                let prefix = self.pop_bytes();
                unimplemented!()
            }
            WaitEmpty => {}

            UnitTests => {
                db.set_option(DatabaseOption::LocationCacheSize(100001))?;
                db.set_option(DatabaseOption::MaxWatches(100001))?;
                db.set_option(DatabaseOption::DatacenterId("dc_id".to_string()))?;
                db.set_option(DatabaseOption::MachineId("machine_id".to_string()))?;
                db.set_option(DatabaseOption::TransactionTimeout(100000))?;
                db.set_option(DatabaseOption::TransactionTimeout(0))?;
                db.set_option(DatabaseOption::TransactionTimeout(0))?;
                db.set_option(DatabaseOption::TransactionMaxRetryDelay(100))?;
                db.set_option(DatabaseOption::TransactionRetryLimit(10))?;
                db.set_option(DatabaseOption::TransactionRetryLimit(-1))?;
                db.set_option(DatabaseOption::SnapshotRywEnable)?;
                db.set_option(DatabaseOption::SnapshotRywDisable)?;

                let tr = trx.as_mut();
                tr.set_option(TransactionOption::PrioritySystemImmediate)?;
                tr.set_option(TransactionOption::PriorityBatch)?;
                tr.set_option(TransactionOption::CausalReadRisky)?;
                tr.set_option(TransactionOption::CausalWriteRisky)?;
                tr.set_option(TransactionOption::ReadYourWritesDisable)?;
                tr.set_option(TransactionOption::ReadSystemKeys)?;
                tr.set_option(TransactionOption::AccessSystemKeys)?;
                tr.set_option(TransactionOption::Timeout(60 * 1000))?;
                tr.set_option(TransactionOption::RetryLimit(50))?;
                tr.set_option(TransactionOption::MaxRetryDelay(100))?;
                tr.set_option(TransactionOption::UsedDuringCommitProtectionDisable)?;
                tr.set_option(TransactionOption::DebugTransactionIdentifier(
                    "my_transaction".to_string(),
                ))?;
                tr.set_option(TransactionOption::LogTransaction)?;
                tr.set_option(TransactionOption::ReadLockAware)?;
                tr.set_option(TransactionOption::LockAware)?;

                tr.get(b"\xff", false).await?;

                // TODO
                // test_cancellation(db)
                // test_retry_limits(db)
                // test_db_retry_limits(db)
                // test_timeouts(db)
                // test_db_timeouts(db)
                // test_combinations(db)
                // test_locality(db)
                // test_predicates()
            }
            instr => {
                unimplemented!("instr: {:?}", instr);
            }
        }

        if is_db && pending {
            let item = self.pop().await;
            self.stack.push(item);
        }

        if is_db && mutation {
            trx.take().commit().await?;
            self.push_item(number, &RESULT_NOT_PRESENT);
        } else if !self.transactions.contains_key(&self.cur_transaction) {
            self.transactions.insert(self.cur_transaction.clone(), trx);
        }

        if instr.has_flags() {
            panic!("flag not handled for instr: {:?}", instr);
        }

        Ok(())
    }

    async fn run(&mut self, db: Database) -> FdbResult<()> {
        info!("Fetching instructions...");
        let instrs = self.fetch_instr(&db.create_trx()?).await?;
        info!("{} instructions found", instrs.len());

        for (i, instr) in instrs.into_iter().enumerate() {
            info!("{}/{}, {:?}", i, self.stack.len(), instr);
            self.run_step(&db, i, instr).await?;

            /*
            if i == 135 {
                break;
            }
            */
        }

        Ok(())
    }
}

fn main() {
    env_logger::init();

    let args = std::env::args().collect::<Vec<_>>();
    let prefix = &args[1];

    let mut cluster_path = None;
    if args.len() > 3 {
        cluster_path = Some(&args[3]);
    }

    let api_version = args[2].parse::<i32>().expect("failed to parse api version");

    let network = api::FdbApiBuilder::default()
        .set_runtime_version(api_version)
        .build()
        .expect("failed to initialize FoundationDB API")
        .boot()
        .expect("failed to initialize FoundationDB network thread");

    let db = if let Some(cluster_path) = cluster_path {
        Database::from_path(cluster_path)
    } else {
        Database::default()
    }
    .expect("failed to get database");
    let mut sm = StackMachine::new(&db, prefix.to_owned());
    futures::executor::block_on(sm.run(db)).unwrap();

    drop(network);
}
