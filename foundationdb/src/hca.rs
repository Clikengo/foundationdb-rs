// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! The directory layer offers subspace indirection, where logical application subspaces are mapped to short, auto-generated key prefixes. This prefix assignment is done by the High Contention Allocator, which allows many clients to allocate short directory prefixes efficiently.
//!
//! The allocation process works over candidate value windows. It uses two subspaces to operate, the "counters" subspace and "recents" subspace (derived from the subspace used to create the HCA).
//!
//! "counters" contains a single key : "counters : window_start", whose value is the number of allocations in the current window. "window_start" is an integer that marks the lower bound of values that can be assigned from the current window.
//! "recents" can contain many keys : "recents : <candidate>", where each "candidate" is an integer that has been assigned to some client
//!
//! Assignment has two stages that are executed in a loop until they both succeed.
//!
//! 1. Find the current window. The client scans "counters : *" to get the current "window_start" and how many allocations have been made in the current window.
//!      If the window is more than half-full (using pre-defined window sizes), the window is advanced: "counters : *" and "recents : *" are both cleared, and a new "counters : window_start + window_size" key is created with a value of 0. (1) is retried
//!      If the window still has space, it moves to (2).
//!
//! 2. Find a candidate value inside that window. The client picks a candidate number between "[window_start, window_start + window_size)" and tries to set the key "recents : <candidate>".
//!      If the write succeeds, the candidate is returned as the allocated value. Success!
//!      If the write fails because the window has been advanced, it repeats (1).
//!      If the write fails because the value was already set, it repeats (2).

use std::sync::Mutex;

use byteorder::ByteOrder;
use futures::{StreamExt,TryStreamExt};
use rand::Rng;

use crate::error::Error;
use crate::keyselector::KeySelector;
use crate::options::{ConflictRangeType, MutationType, TransactionOption};
use crate::subspace::Subspace;
use crate::transaction::{RangeOptionBuilder, Transaction};
use crate::tuple::Element;
use futures::future::ready;

const ONE_BYTES: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

/// Represents a High Contention Allocator for a given subspace
#[derive(Debug)]
pub struct HighContentionAllocator {
    counters: Subspace,
    recent: Subspace,
    allocation_mutex: Mutex<()>,
}

impl HighContentionAllocator {
    /// Constructs an allocator that will use the input subspace for assigning values.
    /// The given subspace should not be used by anything other than the allocator
    pub fn new(subspace: Subspace) -> HighContentionAllocator {
        HighContentionAllocator {
            counters: subspace.subspace(0),
            recent: subspace.subspace(1),
            allocation_mutex: Mutex::new(()),
        }
    }

    /// Returns a byte string that
    ///   1) has never and will never be returned by another call to this method on the same subspace
    ///   2) is nearly as short as possible given the above
    pub async fn allocate(&self, transaction: Transaction) -> Result<i64, Error> {
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
                .fold(Ok(Vec::new()), move |out, range_result| {
                    let mut out = out.unwrap();
                    let range_result = range_result.unwrap();

                    let kvs = range_result.key_values();

                    for kv in kvs.as_ref() {
                        // fixme: unwrap
                        if let Element::I64(counter) = self.counters.unpack(kv.key()).unwrap() {
                            out.push(counter);
                        }
                    }

                    ready(Ok::<_, Error>(out))
                })
                .await?;

            let mut start: i64 = 0;
            let mut window: i64;

            if kvs.len() == 1 {
                start = kvs[0];
            }

            let mut window_advanced = false;

            loop {
                let mutex_guard = self.allocation_mutex.lock().unwrap();

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
                    .await?;
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

                let mutex_guard = self.allocation_mutex.lock().unwrap();

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
                    .fold(Ok(Vec::new()), move |out, range_result| {
                        let mut out = out.unwrap();
                        let range_result = range_result.unwrap();

                        let kvs = range_result.key_values();

                        for kv in kvs.as_ref() {
                            // fixme: unwrap
                            if let Element::I64(counter) = self.counters.unpack(kv.key()).unwrap() {
                                out.push(counter);
                            }
                        }

                        ready(Ok::<_, Error>(out))
                    })
                    .await?;

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

                let candidate_value = candidate_value_trx.await?;

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
