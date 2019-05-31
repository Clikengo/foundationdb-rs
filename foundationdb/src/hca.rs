// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Docs!
//!

use std::sync::Mutex;

use byteorder::ByteOrder;
use futures::future::Future;
use futures::stream::Stream;
use rand::Rng;

use error::Error;
use keyselector::KeySelector;
use options::{ConflictRangeType, MutationType, TransactionOption};
use subspace::Subspace;
use transaction::{RangeOptionBuilder, Transaction};
use tuple::Element;

lazy_static! {
    static ref LOCK: Mutex<i32> = Mutex::new(0);
}

const ONE_BYTES: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

/// High Contention Allocator
///
#[derive(Debug, Clone)]
pub struct HighContentionAllocator {
    counters: Subspace,
    recent: Subspace,
}

impl HighContentionAllocator {
    /// New HCA
    pub fn new(subspace: Subspace) -> HighContentionAllocator {
        HighContentionAllocator {
            counters: subspace.subspace(0),
            recent: subspace.subspace(1),
        }
    }

    /// Returns a byte string that
    ///   1) has never and will never be returned by another call to this
    ///      method on the same subspace
    ///   2) is nearly as short as possible given the above
    pub fn allocate(&self, transaction: &mut Transaction) -> Result<i64, Error> {
        let (begin, end) = self.counters.range();

        loop {
            let counters_begin = KeySelector::first_greater_or_equal(&begin);
            let counters_end = KeySelector::first_greater_than(&end);
            let range_option = RangeOptionBuilder::new(counters_begin, counters_end)
                .reverse(true)
                .limit(1)
                .snapshot(true)
                .build();

            let kvs: Vec<i64> = transaction
                .get_ranges(range_option)
                .map_err(|(_, e)| e)
                .fold(Vec::new(), move |mut out, range_result| {
                    let kvs = range_result.key_values();

                    for kv in kvs.as_ref() {
                        if let Element::I64(counter) = self.counters.unpack(kv.key())? {
                            out.push(counter);
                        }
                    }

                    Ok::<_, Error>(out)
                })
                .wait()?;

            let mut start: i64 = 0;
            let mut window: i64;

            if kvs.len() == 1 {
                start = kvs[0];
            }

            let mut window_advanced = false;

            loop {
                let mutex_guard = LOCK.lock().unwrap();

                if window_advanced {
                    transaction
                        .clear_range(self.counters.bytes(), self.counters.subspace(start).bytes());
                    transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                    transaction
                        .clear_range(self.recent.bytes(), self.recent.subspace(start).bytes());
                }

                let counters_subspace_with_start = self.counters.subspace(start);

                // Increment the allocation count for the current window
                transaction.atomic_op(
                    counters_subspace_with_start.bytes(),
                    ONE_BYTES,
                    MutationType::Add,
                );

                let subspace_start_trx = transaction
                    .get(counters_subspace_with_start.bytes(), true)
                    .wait()?;
                let count = byteorder::LittleEndian::read_i64(subspace_start_trx.value().unwrap());

                drop(mutex_guard);

                window = HighContentionAllocator::window_size(start);

                if count * 2 < window {
                    break;
                }

                start += window;
                window_advanced = true;
            }

            loop {
                // As of the snapshot being read from, the window is less than half
                // full, so this should be expected to take 2 tries.  Under high
                // contention (and when the window advances), there is an additional
                // subsequent risk of conflict for this transaction.
                let mut rng = rand::thread_rng();

                let candidate: i64 = rng.gen_range(0, window) + start;
                let recent_subspace_for_candidate = self.recent.subspace(candidate);
                let candidate_subspace = recent_subspace_for_candidate.bytes();

                let mutex_guard = LOCK.lock().unwrap();

                let counters_begin = KeySelector::first_greater_or_equal(&begin);
                let counters_end = KeySelector::first_greater_than(&end);
                let range_option = RangeOptionBuilder::new(counters_begin, counters_end)
                    .reverse(true)
                    .limit(1)
                    .snapshot(true)
                    .build();

                let kvs: Vec<i64> = transaction
                    .get_ranges(range_option)
                    .map_err(|(_, e)| e)
                    .fold(Vec::new(), move |mut out, range_result| {
                        let kvs = range_result.key_values();

                        for kv in kvs.as_ref() {
                            if let Element::I64(counter) = self.counters.unpack(kv.key())? {
                                out.push(counter);
                            }
                        }

                        Ok::<_, Error>(out)
                    })
                    .wait()?;

                let candidate_value_trx = transaction.get(candidate_subspace, false);

                transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                transaction.set(candidate_subspace, &[]);

                drop(mutex_guard);

                if !kvs.is_empty() {
                    let current_start = kvs[0];

                    if current_start > start {
                        break;
                    }
                }

                let candidate_value = candidate_value_trx.wait()?;

                match candidate_value.value() {
                    Some(_) => (),
                    None => {
                        transaction.add_conflict_range(
                            candidate_subspace,
                            candidate_subspace,
                            ConflictRangeType::Write,
                        )?;
                        return Ok::<_, Error>(candidate);
                    }
                };
            }
        }
    }

    fn window_size(start: i64) -> i64 {
        // Larger window sizes are better for high contention, smaller sizes for
        // keeping the keys small.  But if there are many allocations, the keys
        // can't be too small.  So start small and scale up.  We don't want this to
        // ever get *too* big because we have to store about window_size/2 recent
        // items.
        match start {
            _ if start < 255 => 64,
            _ if start < 65535 => 1024,
            _ => 8192,
        }
    }
}
