Rust FoundationDB bindingtester
===============================

This exe implements the official FoundationDB bindingtester protocol.

By running `./bindingtester.py ${this_executable}`, you can test how the rust foundationdb bindings behave.

The following configurations are tested and should pass without any issue:

```
./bindingtester.py --test-name scripted
./bindingtester.py --num-ops 1000 --test-name api --api-version 610
./bindingtester.py --num-ops 1000 --concurrency 5 --test-name api --api-version 610
./bindingtester.py --num-ops 10000 --concurrency 1 --test-name directory --api-version 610 --no-directory-snapshot-ops
```