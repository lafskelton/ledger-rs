# ledger-rs
A simple Rust library for reading and writing a contiguous table of rows to and from the Linux filesystem. It provides extremely efficient methods for reading and writing rows virtually in-place.

This library is a small part of a larger project and is actively evolving. PRs welcome.

### Under the hood

ledger-rs uses memmap2 and rkyv for file IO and zero-copy serialization.

The term "virtually in-place" refers to how the library leverages the Linux kernel's highly optimized demand-paging and page-cache logic. When a table file is loaded, the kernel does not load the entire file into RAM. It only loads pages as they are accessed.

Note: Relying on memmap assumes that the underlying file is not modified by the OS or other external programs while mapped. Doing so will result in undefined behavior.

### Current Status & Limitations
- Provides high-speed checked and unchecked (unsafe) access methods.
- Supports primitive types, arrays of primitives, and capped-length Strings.
- Does not currently support indexing beyond the auto-incrementing row_id returned upon insertion.
- Deleted rows must be managed manually by the user.
- Error handling is currently a work in progress.
- Concurrent operations (like apply, map, insert_many, and read_range) via Tokio tasks are planned for a future update.

### Usage

To create a ledger file, define a struct to act as the row schema using the #[ledger] macro. Most primitive datatypes are supported. String types require a #[max_len(N)] attribute.

```
use ledger_rs::{page::PageSchema, utils::DatastoreError};
use ledger_rs_macros::ledger;
use std::path::Path;

#[ledger]
pub struct FileManifest {
    pub id: u32,
    #[max_len(32)]
    pub title: String,
    #[max_len(32)]
    pub location: String,
}

fn main() -> Result<(), DatastoreError> {
    let path = Path::new("./");
    let mut ledger = FileManifest::create_ledger(path, "Documents", "My documents")?;

    // Writing rows
    for n in 0..123456 {
        let title = format!("document #{}", n);
        let loc = format!("/home/ubuntu/Documents/document_{}.txt", n);
        let row = FileManifest::new(n, &title, &loc);

        let _row_id = ledger.insert(&row)?;
    }

    // Reading rows
    for n in 0..123456 {
        let Some(row) = ledger.access_row(n)? else {
            panic!("Row not found!");
        };

        if row.id() != n {
            panic!("Row ID mismatch");
        }
    }

    Ok(())
}

```
The library exposes both checked (safe) and unchecked (unsafe) methods for interacting with the ledger. Unchecked methods bypass bounds and validation checks for maximum throughput.
```
// Writing to a ledger
let row_id = ledger.insert(&row)?;

unsafe {
    let row_id = ledger.insert_unchecked(&row);
}

// Reading from a ledger
let row = ledger.access_row(id)?.unwrap();

unsafe {
    let row = ledger.access_row_unchecked(id);
}

// Mutating a row
let mut_row = ledger.access_row_mut(id)?.unwrap();

unsafe {
    let mut_row = ledger.access_row_unchecked_mut(id);
}

```

### Bench

The repository includes an AI slop benchmark script to test the *checked* API throughput. This library is a small part of a bigger library, so I haven't put much effort into benchmarking since this lib exceeds my needs.

Run the benchmark:
```
cargo run --bin slop_bench --release
```

```
Starting sequential insertion of 1234567 records...

Starting sequential insertion of 1234567 records...
💾 Disk sync cost: 31.877302ms
📊 INSERTION STATS
  📦 Record Size: 72 bytes
  💽 Est. Ledger: ~89 MB

  ⚡ Throughput: 1144688.18 ops/sec
  ⏱️  Total Time:      1.08s
  📈 Latencies:  p50:    701ns | p90:    751ns | p99:    911ns | p99.9:  2.685µs | Max: 309.074µs

🎲 Generating cryptographic-grade chaos entropy (shuffling)... DONE

Starting chaotic random access of 1234567 records...
📊 RANDOM READ STATS
  📦 Record Size: 72 bytes
  💽 Est. Ledger: ~89 MB

  ⚡ Throughput: 5233678.55 ops/sec
  ⏱️  Total Time:   235.89ms
  📈 Latencies:  p50:    140ns | p90:    230ns | p99:    361ns | p99.9:  1.432µs | Max: 21.761µs

Starting chaotic random mutations of 1234567 records...
💾 Disk sync cost (post-mutation): 27.668456ms
📊 RANDOM MUTATION STATS
  📦 Record Size: 72 bytes
  💽 Est. Ledger: ~89 MB

  ⚡ Throughput: 5107741.16 ops/sec
  ⏱️  Total Time:   241.71ms
  📈 Latencies:  p50:    100ns | p90:    190ns | p99:  1.423µs | p99.9:  2.174µs | Max: 23.505µs


```
Hardware: AMD Ryzen 7 mobile + DDR5 + 980 PRO nvme (luks)


