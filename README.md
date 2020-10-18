# csv-async
This is CSV library to be used in asynchronous environment.
Implemented API is similar to existing [csv](https://github.com/BurntSushi/rust-csv) crate except that this crate does not support `serde`.

Some code is borrowed from `csv` crate (synchronized with version 1.1.3 - May 2020).
This package shares CSV parsing routines with `csv` crate by means of using `csv-core` crate.

The package contains preliminary versions of `AsyncReader` and `AsyncWriter`.
It does not contain synchronous reader/writer. If you need it - please use `csv` crate.

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
    // Function reads CSV file that has column named "region"
    // at second position (index = 1).
    // It writes to new file only rows with region equal to passed argument
    // and remove region column.
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
            println!("error running filter_by_region: {}", err);
            process::exit(1);
        }
    });
}
```
## Plans
Some ideas for future development:

- Create benchmarks, maybe some performance improvements.
- Create more examples and tutorial.
- Investigate possibility to add support for `serde`.

