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
    fn new(subspace: Subspace) -> HighContentionAllocator {
        HighContentionAllocator {
            counters: subspace.subspace(0),
            recent: subspace.subspace(1),
        }
    }

    fn allocate(
        &self,
        transaction: &mut Transaction,
        subspace: &Subspace,
    ) -> Result<Subspace, Error> {
        loop {
            let range_option = RangeOptionBuilder::from(self.counters.range())
                .reverse(true)
                .limit(1)
                .snapshot(true)
                .build();

            let kvs: Vec<i64> = transaction.get_ranges(range_option)
                .map_err(|(_, e)| e)
                .filter_map(|range_result| {
                    for kv in range_result.key_values().as_ref() {
                        if let Element::I64(counter) = self.counters.unpack(kv.key()).expect("unable to unpack counter key") {
                            return Some(counter);
                        }
                    }

                    return None;
                })
                .collect()
                .wait()?;

            let mut start: i64 = 0;
            let mut window: i64 = 0;

            if kvs.len() == 1 {
                start = kvs[0];
            }

            let mut window_advanced = false;

            loop {
                let mutex_guard = LOCK.lock().unwrap();

                if window_advanced {
                    transaction.clear_range(self.counters.bytes(), self.counters.subspace(start).bytes());
                    transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                    transaction.clear_range(self.recent.bytes(), self.recent.subspace(start).bytes());
                }

                let counters_subspace_with_start = self.counters.subspace(start);

                // Increment the allocation count for the current window
                transaction.atomic_op(counters_subspace_with_start.bytes(), ONE_BYTES, MutationType::Add);

                let subspace_start_trx = transaction.get(counters_subspace_with_start.bytes(), true).wait()?;
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

                let candidate = self.recent.subspace(rng.gen::<i64>() + start);
                let candidate_subspace = candidate.bytes();

                let mutex_guard = LOCK.lock().unwrap();

                let range_option = RangeOptionBuilder::from(self.counters.range())
                    .reverse(true)
                    .limit(1)
                    .snapshot(true)
                    .build();

                let kvs: Vec<i64> = transaction.get_ranges(range_option)
                    .map_err(|(_, e)| e)
                    .filter_map(|range_result| {
                        for kv in range_result.key_values().as_ref() {
                            if let Element::I64(counter) = self.counters.unpack(kv.key()).expect("unable to unpack counter key") {
                                return Some(counter);
                            }
                        }
                        return None;
                    })
                    .collect()
                    .wait()?;

                let candidate_value_trx = transaction.get(candidate_subspace, false);

                transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                transaction.set(candidate_subspace, &[]);

                drop(mutex_guard);

                if kvs.len() > 0 {
                    let current_start = kvs[0];

                    if current_start > start {
                        break;
                    }
                }

                let candidate_value = candidate_value_trx.wait()?;

                match candidate_value.value() {
                    Some(x) => {
                        if x.len() == 0 {
                            transaction.add_conflict_range(candidate_subspace, candidate_subspace, ConflictRangeType::Write)?;
                            return Ok(self.counters.subspace(candidate_subspace));
                        }
                    }
                    None => {
                        continue;
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
        if start < 255 {
            return 64;
        }
        if start < 65535 {
            return 1024;
        }
        return 8192;
    }
}
