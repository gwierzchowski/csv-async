# csv-async
[![crates.io](https://img.shields.io/crates/v/csv-async)](https://crates.io/crates/csv-async)
[![](https://img.shields.io/crates/d/csv-async.svg)](https://crates.io/crates/csv-async)
[![](https://img.shields.io/crates/dv/csv-async.svg)](https://crates.io/crates/csv-async)
[![Documentation](https://docs.rs/csv-async/badge.svg)](https://docs.rs/csv-async)
[![Version](https://img.shields.io/badge/rustc-1.56+-ab6000.svg)](https://blog.rust-lang.org/2021/10/21/Rust-1.56.0.html)

[![build status](https://github.com/gwierzchowski/csv-async/workflows/Linux/badge.svg?branch=master&event=push)](https://github.com/gwierzchowski/csv-async/actions?query=workflow%3ALinux)
[![build status](https://github.com/gwierzchowski/csv-async/workflows/Windows/badge.svg?branch=master&event=push)](https://github.com/gwierzchowski/csv-async/actions?query=workflow%3AWindows)
[![build status](https://github.com/gwierzchowski/csv-async/workflows/MacOS/badge.svg?branch=master&event=push)](https://github.com/gwierzchowski/csv-async/actions?query=workflow%3AMacOS)
[![codecov](https://codecov.io/gh/gwierzchowski/csv-async/branch/master/graph/badge.svg)](https://codecov.io/gh/gwierzchowski/csv-async)

This is CSV library to use in asynchronous environment.
Implemented API is similar to existing [csv](https://github.com/BurntSushi/rust-csv) crate with few exceptions like builder's `create_` functions instead of `from_` as in `csv`.

Some code is borrowed from `csv` crate (synchronized with version 1.1.6 - Mar 2021).
This package shares CSV parsing routines with `csv` by means of using `csv-core` crate.
Major version of this crate will be kept in sync with major version of `csv` with which it is API compatible.

CSV files are being read or written by objects of types `AsyncReader` / `AsyncWriter` to / from generic 
text-based structures or by `AsyncDeserializer` / `AsyncSerializer` to / from data specific structures with generated `serde` interfaces.

Library does not contain synchronous reader/writer. If you need it - please use `csv` crate.

## Cargo Features
Features which can be enabled / disabled during library build.

| Feature      | Default | Description |
|--------------|---------|-------------|
| `with_serde` | on      | Enables crate to use [serde](https://serde.rs) derive macros |
| `tokio`      | off     | Enables crate to be used with [tokio](https://tokio.rs) runtime and libraries |

Enabling `tokio` feature allows user to use `tokio::fs::File` and makes `AsyncReader` (`AsyncWriter`) 
to be based on `tokio::io::AsyncRead` (`tokio::io::AsyncWrite`). Currently this crate depends on tokio version 0.2.

Without `tokio` feature, this crate depends only on `futures` crate and reader (writer) are based on traits `futures::io::AsyncRead` (`futures::io::AsyncWrite`), what allows user to use `async_std::fs::File`.

## Example usage:  
Sample input file:
```csv
city,region,country,population
Southborough,MA,United States,9686
Northbridge,MA,United States,14061
Marlborough,MA,United States,38334
Springfield,MA,United States,152227
Springfield,MO,United States,150443
Springfield,NJ,United States,14976
Concord,NH,United States,42605
```

```rust
use std::error::Error;
use std::process;
use futures::stream::StreamExt;
use async_std::fs::File;

async fn filter_by_region(region:&str, file_in:&str, file_out:&str) -> Result<(), Box<dyn Error>> {
    // Function reads CSV file that has column named "region" at second position (index = 1).
    // It writes to new file only rows with region equal to passed argument
    // and removes region column.
    let mut rdr = csv_async::AsyncReader::from_reader(
        File::open(file_in).await?
    );
    let mut wri = csv_async::AsyncWriter::from_writer(
        File::create(file_out).await?
    );
    wri.write_record(rdr
        .headers()
        .await?.into_iter()
        .filter(|h| *h != "region")
    ).await?;
    let mut records = rdr.records();
    while let Some(record) = records.next().await {
        let record = record?;
        match record.get(1) {
            Some(reg) if reg == region => 
                wri.write_record(record
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != 1)
                    .map(|(_, s)| s)
                ).await?,
            _ => {},
        }
    }
    Ok(())
}

fn main() {
    async_std::task::block_on(async {
        if let Err(err) = filter_by_region(
            "MA",
            "/tmp/all_regions.csv",
            "/tmp/MA_only.csv"
        ).await {
            eprintln!("error running filter_by_region: {}", err);
            process::exit(1);
        }
    });
}
```

For serde example please see documentation [root](https://docs.rs/csv-async) page.

## Plans
Some ideas for future development:

- Create benchmarks, maybe some performance improvements.
- Things marked as TODO in the code.
- Support for `smol` asynchronous runtime.
- Create more examples and tutorial.
