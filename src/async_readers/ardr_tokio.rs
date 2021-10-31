use tokio::io;

use crate::AsyncReaderBuilder;
use crate::byte_record::{ByteRecord, Position};
use crate::error::Result;
use crate::string_record::StringRecord;
use super::{
    AsyncReaderImpl,
    StringRecordsStream, StringRecordsIntoStream,
    ByteRecordsStream, ByteRecordsIntoStream,
};

impl AsyncReaderBuilder {
    /// Build a CSV reader from this configuration that reads data from `rdr`.
    ///
    /// Note that the CSV reader is buffered automatically, so you should not
    /// wrap `rdr` in a buffered reader.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReaderBuilder;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new().create_reader(data.as_bytes());
    ///     let mut records = rdr.into_records();
    ///     while let Some(record) = records.next().await {
    ///         println!("{:?}", record?);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn create_reader<R: io::AsyncRead + Unpin + Send + Sync>(&self, rdr: R) -> AsyncReader<R> {
        AsyncReader::new(self, rdr)
    }
}

/// A already configured CSV reader for `tokio` runtime.
///
/// A CSV reader takes as input CSV data and transforms that into standard Rust
/// values. The reader reads CSV data is as a sequence of records,
/// where a record is a sequence of fields and each field is a string.
///
/// # Configuration
///
/// A CSV reader has convenient constructor method `create_reader`.
/// However, if you want to configure the CSV reader to use
/// a different delimiter or quote character (among many other things), then
/// you should use a [`AsyncReaderBuilder`](struct.AsyncReaderBuilder.html) to construct
/// a `AsyncReader`. For example, to change the field delimiter:
///
/// ```
/// use std::error::Error;
/// use csv_async::AsyncReaderBuilder;
/// use tokio_stream::StreamExt;
///
/// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
/// async fn example() -> Result<(), Box<dyn Error>> {
///     let data = "\
/// city;country;pop
/// Boston;United States;4628910
/// ";
///     let mut rdr = AsyncReaderBuilder::new()
///         .delimiter(b';')
///         .create_reader(data.as_bytes());
///
///     let mut records = rdr.records();
///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
///     Ok(())
/// }
/// ```
///
/// # Error handling
///
/// In general, CSV *parsing* does not ever return an error. That is, there is
/// no such thing as malformed CSV data. Instead, this reader will prioritize
/// finding a parse over rejecting CSV data that it does not understand. This
/// choice was inspired by other popular CSV parsers, but also because it is
/// pragmatic. CSV data varies wildly, so even if the CSV data is malformed,
/// it might still be possible to work with the data. In the land of CSV, there
/// is no "right" or "wrong," only "right" and "less right."
///
/// With that said, a number of errors can occur while reading CSV data:
///
/// * By default, all records in CSV data must have the same number of fields.
///   If a record is found with a different number of fields than a prior
///   record, then an error is returned. This behavior can be disabled by
///   enabling flexible parsing via the `flexible` method on
///   [`AsyncReaderBuilder`](struct.AsyncReaderBuilder.html).
/// * When reading CSV data from a resource (like a file), it is possible for
///   reading from the underlying resource to fail. This will return an error.
///   For subsequent calls to the reader after encountering a such error
///   (unless `seek` is used), it will behave as if end of file had been
///   reached, in order to avoid running into infinite loops when still
///   attempting to read the next record when one has errored.
/// * When reading CSV data into `String` or `&str` fields (e.g., via a
///   [`StringRecord`](struct.StringRecord.html)), UTF-8 is strictly
///   enforced. If CSV data is invalid UTF-8, then an error is returned. If
///   you want to read invalid UTF-8, then you should use the byte oriented
///   APIs such as [`ByteRecord`](struct.ByteRecord.html). If you need explicit
///   support for another encoding entirely, then you'll need to use another
///   crate to transcode your CSV data to UTF-8 before parsing it.
/// * When using Serde to deserialize CSV data into Rust types, it is possible
///   for a number of additional errors to occur. For example, deserializing
///   a field `xyz` into an `i32` field will result in an error.
///
/// For more details on the precise semantics of errors, see the
/// [`Error`](enum.Error.html) type.
#[derive(Debug)]
pub struct AsyncReader<R>(AsyncReaderImpl<R>);

impl<'r, R> AsyncReader<R>
where
    R: io::AsyncRead + Unpin + Send + Sync + 'r,
{
    /// Create a new CSV reader given a builder and a source of underlying
    /// bytes.
    fn new(builder: &AsyncReaderBuilder, rdr: R) -> AsyncReader<R> {
        AsyncReader(AsyncReaderImpl::new(builder, rdr))
    }

    /// Create a new CSV parser with a default configuration for the given
    /// reader.
    ///
    /// To customize CSV parsing, use a `ReaderBuilder`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReader;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///     let mut records = rdr.into_records();
    ///     while let Some(record) = records.next().await {
    ///         println!("{:?}", record?);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn from_reader(rdr: R) -> AsyncReader<R> {
        AsyncReaderBuilder::new().create_reader(rdr)
    }

    /// Returns a borrowed iterator over all records as strings.
    ///
    /// Each item yielded by this iterator is a `Result<StringRecord, Error>`.
    /// Therefore, in order to access the record, callers must handle the
    /// possibility of error (typically with `try!` or `?`).
    ///
    /// If `has_headers` was enabled via a `ReaderBuilder` (which is the
    /// default), then this does not include the first record.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReader;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///     let mut records = rdr.records();
    ///     while let Some(record) = records.next().await {
    ///         println!("{:?}", record?);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn records(&mut self) -> StringRecordsStream<R> {
        StringRecordsStream::new(&mut self.0)
    }

    /// Returns an owned iterator over all records as strings.
    ///
    /// Each item yielded by this iterator is a `Result<StringRecord, Error>`.
    /// Therefore, in order to access the record, callers must handle the
    /// possibility of error (typically with `try!` or `?`).
    ///
    /// This is mostly useful when you want to return a CSV iterator or store
    /// it somewhere.
    ///
    /// If `has_headers` was enabled via a `ReaderBuilder` (which is the
    /// default), then this does not include the first record.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReader;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let rdr = AsyncReader::from_reader(data.as_bytes());
    ///     let mut records = rdr.into_records();
    ///     while let Some(record) = records.next().await {
    ///         println!("{:?}", record?);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn into_records(self) -> StringRecordsIntoStream<'r, R> {
        StringRecordsIntoStream::new(self.0)
    }

    /// Returns a borrowed iterator over all records as raw bytes.
    ///
    /// Each item yielded by this iterator is a `Result<ByteRecord, Error>`.
    /// Therefore, in order to access the record, callers must handle the
    /// possibility of error (typically with `try!` or `?`).
    ///
    /// If `has_headers` was enabled via a `ReaderBuilder` (which is the
    /// default), then this does not include the first record.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReader;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///     let mut iter = rdr.byte_records();
    ///     assert_eq!(iter.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(iter.next().await.is_none());
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn byte_records(&mut self) -> ByteRecordsStream<R> {
        ByteRecordsStream::new(&mut self.0)
    }

    /// Returns an owned iterator over all records as raw bytes.
    ///
    /// Each item yielded by this iterator is a `Result<ByteRecord, Error>`.
    /// Therefore, in order to access the record, callers must handle the
    /// possibility of error (typically with `try!` or `?`).
    ///
    /// This is mostly useful when you want to return a CSV iterator or store
    /// it somewhere.
    ///
    /// If `has_headers` was enabled via a `ReaderBuilder` (which is the
    /// default), then this does not include the first record.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReader;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let rdr = AsyncReader::from_reader(data.as_bytes());
    ///     let mut iter = rdr.into_byte_records();
    ///     assert_eq!(iter.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(iter.next().await.is_none());
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn into_byte_records(self) -> ByteRecordsIntoStream<'r, R> {
        ByteRecordsIntoStream::new(self.0)
    }

    /// Returns a reference to the first row read by this parser.
    ///
    /// If no row has been read yet, then this will force parsing of the first
    /// row.
    ///
    /// If there was a problem parsing the row or if it wasn't valid UTF-8,
    /// then this returns an error.
    ///
    /// If the underlying reader emits EOF before any data, then this returns
    /// an empty record.
    ///
    /// Note that this method may be used regardless of whether `has_headers`
    /// was enabled (but it is enabled by default).
    ///
    /// # Example
    ///
    /// This example shows how to get the header row of CSV data. Notice that
    /// the header row does not appear as a record in the iterator!
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReader;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///
    ///     // We can read the headers before iterating.
    ///     {
    ///     // `headers` borrows from the reader, so we put this in its
    ///     // own scope. That way, the borrow ends before we try iterating
    ///     // below. Alternatively, we could clone the headers.
    ///     let headers = rdr.headers().await?;
    ///     assert_eq!(headers, vec!["city", "country", "pop"]);
    ///     }
    ///
    ///     {
    ///     let mut records = rdr.records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(records.next().await.is_none());
    ///     }
    ///
    ///     // We can also read the headers after iterating.
    ///     let headers = rdr.headers().await?;
    ///     assert_eq!(headers, vec!["city", "country", "pop"]);
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn headers(&mut self) -> Result<&StringRecord> {
        self.0.headers().await
    }

    /// Returns a reference to the first row read by this parser as raw bytes.
    ///
    /// If no row has been read yet, then this will force parsing of the first
    /// row.
    ///
    /// If there was a problem parsing the row then this returns an error.
    ///
    /// If the underlying reader emits EOF before any data, then this returns
    /// an empty record.
    ///
    /// Note that this method may be used regardless of whether `has_headers`
    /// was enabled (but it is enabled by default).
    ///
    /// # Example
    ///
    /// This example shows how to get the header row of CSV data. Notice that
    /// the header row does not appear as a record in the iterator!
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncReader;
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///
    ///     // We can read the headers before iterating.
    ///     {
    ///     // `headers` borrows from the reader, so we put this in its
    ///     // own scope. That way, the borrow ends before we try iterating
    ///     // below. Alternatively, we could clone the headers.
    ///     let headers = rdr.byte_headers().await?;
    ///     assert_eq!(headers, vec!["city", "country", "pop"]);
    ///     }
    ///
    ///     {
    ///     let mut records = rdr.byte_records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(records.next().await.is_none());
    ///     }
    ///
    ///     // We can also read the headers after iterating.
    ///     let headers = rdr.byte_headers().await?;
    ///     assert_eq!(headers, vec!["city", "country", "pop"]);
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn byte_headers(&mut self) -> Result<&ByteRecord> {
        self.0.byte_headers().await
    }

    /// Set the headers of this CSV parser manually.
    ///
    /// This overrides any other setting (including `set_byte_headers`). Any
    /// automatic detection of headers is disabled. This may be called at any
    /// time.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{AsyncReader, StringRecord};
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///
    ///     assert_eq!(rdr.headers().await?, vec!["city", "country", "pop"]);
    ///     rdr.set_headers(StringRecord::from(vec!["a", "b", "c"]));
    ///     assert_eq!(rdr.headers().await?, vec!["a", "b", "c"]);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn set_headers(&mut self, headers: StringRecord) {
        self.0.set_headers(headers);
    }

    /// Set the headers of this CSV parser manually as raw bytes.
    ///
    /// This overrides any other setting (including `set_headers`). Any
    /// automatic detection of headers is disabled. This may be called at any
    /// time.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{AsyncReader, ByteRecord};
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///
    ///     assert_eq!(rdr.byte_headers().await?, vec!["city", "country", "pop"]);
    ///     rdr.set_byte_headers(ByteRecord::from(vec!["a", "b", "c"]));
    ///     assert_eq!(rdr.byte_headers().await?, vec!["a", "b", "c"]);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn set_byte_headers(&mut self, headers: ByteRecord) {
        self.0.set_byte_headers(headers);
    }

    /// Read a single row into the given record. Returns false when no more
    /// records could be read.
    ///
    /// If `has_headers` was enabled via a `ReaderBuilder` (which is the
    /// default), then this will treat initial row as headers and read the first data record.
    ///
    /// This method is useful when you want to read records as fast as
    /// as possible. It's less ergonomic than an iterator, but it permits the
    /// caller to reuse the `StringRecord` allocation, which usually results
    /// in higher throughput.
    ///
    /// Records read via this method are guaranteed to have a position set
    /// on them, even if the reader is at EOF or if an error is returned.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{AsyncReader, StringRecord};
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///     let mut record = StringRecord::new();
    ///
    ///     if rdr.read_record(&mut record).await? {
    ///         assert_eq!(record, vec!["Boston", "United States", "4628910"]);
    ///         Ok(())
    ///     } else {
    ///         Err(From::from("expected at least one record but got none"))
    ///     }
    /// }
    /// ```
    #[inline]
    pub async fn read_record(&mut self, record: &mut StringRecord) -> Result<bool> {
        self.0.read_record(record).await
    }

    /// Read a single row into the given byte record. Returns false when no
    /// more records could be read.
    ///
    /// If `has_headers` was enabled via a `ReaderBuilder` (which is the
    /// default), then this will treat initial row as headers and read the first data record.
    ///
    /// This method is useful when you want to read records as fast as
    /// as possible. It's less ergonomic than an iterator, but it permits the
    /// caller to reuse the `ByteRecord` allocation, which usually results
    /// in higher throughput.
    ///
    /// Records read via this method are guaranteed to have a position set
    /// on them, even if the reader is at EOF or if an error is returned.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{ByteRecord, AsyncReader};
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///     let mut record = ByteRecord::new();
    ///
    ///     if rdr.read_byte_record(&mut record).await? {
    ///         assert_eq!(record, vec!["Boston", "United States", "4628910"]);
    ///         Ok(())
    ///     } else {
    ///         Err(From::from("expected at least one record but got none"))
    ///     }
    /// }
    /// ```
    #[inline]
    pub async fn read_byte_record(&mut self, record: &mut ByteRecord) -> Result<bool> {
        self.0.read_byte_record(record).await
    }

    /// Return the current position of this CSV reader.
    ///
    /// The byte offset in the position returned can be used to `seek` this
    /// reader. In particular, seeking to a position returned here on the same
    /// data will result in parsing the same subsequent record.
    ///
    /// # Example: reading the position
    ///
    /// ```
    /// use std::error::Error;
    /// use std::io;
    /// use csv_async::{AsyncReader, Position};
    /// use tokio_stream::StreamExt;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,popcount
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ";
    ///     let rdr = AsyncReader::from_reader(io::Cursor::new(data));
    ///     let mut iter = rdr.into_records();
    ///     let mut pos = Position::new();
    ///     loop {
    ///         let next = iter.next().await;
    ///         if let Some(next) = next {
    ///             pos = next?.position().expect("Cursor should be at some valid position").clone();
    ///         } else {
    ///             break;
    ///         }
    ///     }
    ///
    ///     // `pos` should now be the position immediately before the last
    ///     // record.
    ///     assert_eq!(pos.byte(), 51);
    ///     assert_eq!(pos.line(), 3);
    ///     assert_eq!(pos.record(), 2);
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn position(&self) -> &Position {
        self.0.position()
    }

    /// Returns true if and only if this reader has been exhausted.
    ///
    /// When this returns true, no more records can be read from this reader.
    ///
    /// # Example
    ///
    /// ```
    /// # use tokio1 as tokio;
    /// use std::error::Error;
    /// use tokio::io;
    /// use tokio_stream::StreamExt;
    /// use csv_async::{AsyncReader, Position};
    ///
    /// # fn main() { tokio::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,popcount
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(data.as_bytes());
    ///     assert!(!rdr.is_done());
    ///     {
    ///         let mut records = rdr.records();
    ///         while let Some(record) = records.next().await {
    ///             let _ = record?;
    ///         }
    ///     }
    ///     assert!(rdr.is_done());
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn is_done(&self) -> bool {
        self.0.is_done()
    }

    /// Returns true if and only if this reader has been configured to
    /// interpret the first record as a header record.
    #[inline]
    pub fn has_headers(&self) -> bool {
        self.0.has_headers()
    }

    /// Returns a reference to the underlying reader.
    #[inline]
    pub fn get_ref(&self) -> &R {
        self.0.get_ref()
    }

    /// Returns a mutable reference to the underlying reader.
    #[inline]
    pub fn get_mut(&mut self) -> &mut R {
        self.0.get_mut()
    }

    /// Unwraps this CSV reader, returning the underlying reader.
    ///
    /// Note that any leftover data inside this reader's internal buffer is
    /// lost.
    #[inline]
    pub fn into_inner(self) -> R {
        self.0.into_inner()
    }
}

impl<R: io::AsyncRead + io::AsyncSeek + std::marker::Unpin> AsyncReader<R> {
    /// Seeks the underlying reader to the position given.
    ///
    /// This comes with a few caveats:
    ///
    /// * Any internal buffer associated with this reader is cleared.
    /// * If the given position does not correspond to a position immediately
    ///   before the start of a record, then the behavior of this reader is
    ///   unspecified.
    /// * Any special logic that skips the first record in the CSV reader
    ///   when reading or iterating over records is disabled.
    ///
    /// If the given position has a byte offset equivalent to the current
    /// position, then no seeking is performed.
    ///
    /// If the header row has not already been read, then this will attempt
    /// to read the header row before seeking. Therefore, it is possible that
    /// this returns an error associated with reading CSV data.
    ///
    /// Note that seeking is performed based only on the byte offset in the
    /// given position. Namely, the record or line numbers in the position may
    /// be incorrect, but this will cause any future position generated by
    /// this CSV reader to be similarly incorrect.
    ///
    /// # Example: seek to parse a record twice
    ///
    /// ```
    /// # use tokio1 as tokio;
    /// use std::error::Error;
    /// use tokio::io;
    /// use tokio_stream::StreamExt;
    /// use csv_async::{AsyncReader, Position};
    ///
    /// # fn main() { tokio::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,popcount
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(std::io::Cursor::new(data));
    ///     let mut pos = Position::new();
    ///     {
    ///     let mut records = rdr.records();
    ///     loop {
    ///         let next = records.next().await;
    ///         if let Some(next) = next {
    ///             pos = next?.position().expect("Cursor should be at some valid position").clone();
    ///         } else {
    ///             break;
    ///         }
    ///     }
    ///     }
    ///
    ///     {
    ///     // Now seek the reader back to `pos`. This will let us read the
    ///     // last record again.
    ///     rdr.seek(pos).await?;
    ///     let mut records = rdr.into_records();
    ///     if let Some(result) = records.next().await {
    ///         let record = result?;
    ///         assert_eq!(record, vec!["Concord", "United States", "42695"]);
    ///         Ok(())
    ///     } else {
    ///         Err(From::from("expected at least one record but got none"))
    ///     }
    ///     }
    /// }
    /// ```
    #[inline]
    pub async fn seek(&mut self, pos: Position) -> Result<()> {
        self.0.seek(pos).await
    }

    /// This is like `seek`, but provides direct control over how the seeking
    /// operation is performed via `io::SeekFrom`.
    ///
    /// The `pos` position given *should* correspond the position indicated
    /// by `seek_from`, but there is no requirement. If the `pos` position
    /// given is incorrect, then the position information returned by this
    /// reader will be similarly incorrect.
    ///
    /// If the header row has not already been read, then this will attempt
    /// to read the header row before seeking. Therefore, it is possible that
    /// this returns an error associated with reading CSV data.
    ///
    /// Unlike `seek`, this will always cause an actual seek to be performed.
    #[inline]
    pub async fn seek_raw(
        &mut self,
        seek_from: io::SeekFrom,
        pos: Position,
    ) -> Result<()> {
        self.0.seek_raw(seek_from, pos).await
    }

    /// Rewinds the underlying reader to first data record.
    ///
    /// Function is aware of header presence.
    /// After `rewind` record iterators will return first data record (skipping header if present), while
    /// after `seek(0)` they will return header row (even if `has_header` is set).
    /// 
    /// # Example: Reads the same data multiply times
    ///
    /// ```
    /// # use tokio1 as tokio;
    /// use std::error::Error;
    /// use tokio::io;
    /// use tokio_stream::StreamExt;
    /// use csv_async::AsyncReader;
    ///
    /// # fn main() { tokio::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,popcount
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ";
    ///     let mut rdr = AsyncReader::from_reader(std::io::Cursor::new(data));
    ///     let mut output = Vec::new();
    ///     loop {
    ///         let mut records = rdr.records();
    ///         while let Some(rec) = records.next().await {
    ///             output.push(rec?);
    ///         }
    ///         if output.len() >= 6 {
    ///             break;
    ///         } else {
    ///             drop(records);
    ///             rdr.rewind().await?;
    ///         }
    ///     }
    ///     assert_eq!(output, 
    ///         vec![
    ///             vec!["Boston", "United States", "4628910"],
    ///             vec!["Concord", "United States", "42695"],
    ///             vec!["Boston", "United States", "4628910"],
    ///             vec!["Concord", "United States", "42695"],
    ///             vec!["Boston", "United States", "4628910"],
    ///             vec!["Concord", "United States", "42695"],
    ///         ]);
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn rewind(&mut self) -> Result<()> {
        self.0.rewind().await
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io;
    use tokio_stream::StreamExt;
    use tokio::runtime::Runtime;

    use crate::byte_record::ByteRecord;
    use crate::error::ErrorKind;
    use crate::string_record::StringRecord;
    use crate::Trim;

    use super::{Position, AsyncReaderBuilder, AsyncReader};

    fn b(s: &str) -> &[u8] {
        s.as_bytes()
    }
    fn s(b: &[u8]) -> &str {
        ::std::str::from_utf8(b).unwrap()
    }

    fn newpos(byte: u64, line: u64, record: u64) -> Position {
        let mut p = Position::new();
        p.set_byte(byte).set_line(line).set_record(record);
        p
    }

    async fn count(stream: impl StreamExt) -> usize {
        stream.fold(0, |acc, _| acc + 1 ).await
    }

    #[test]
    fn read_byte_record() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,\"b,ar\",baz\nabc,mno,xyz");
            let mut rdr =
                AsyncReaderBuilder::new().has_headers(false).create_reader(data);
            let mut rec = ByteRecord::new();

            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("foo", s(&rec[0]));
            assert_eq!("b,ar", s(&rec[1]));
            assert_eq!("baz", s(&rec[2]));

            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("abc", s(&rec[0]));
            assert_eq!("mno", s(&rec[1]));
            assert_eq!("xyz", s(&rec[2]));

            assert!(!rdr.read_byte_record(&mut rec).await.unwrap());
        });
    }

    #[test]
    fn read_trimmed_records_and_headers() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,  bar,\tbaz\n  1,  2,  3\n1\t,\t,3\t\t");
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::All)
                .create_reader(data);
            let mut rec = ByteRecord::new();
            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!("1", s(&rec[0]));
            assert_eq!("2", s(&rec[1]));
            assert_eq!("3", s(&rec[2]));
            let mut rec = StringRecord::new();
            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!("1", &rec[0]);
            assert_eq!("", &rec[1]);
            assert_eq!("3", &rec[2]);
            {
                let headers = rdr.headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!("foo", &headers[0]);
                assert_eq!("bar", &headers[1]);
                assert_eq!("baz", &headers[2]);
            }
        });
    }

    #[test]
    fn read_trimmed_header() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,  bar,\tbaz\n  1,  2,  3\n1\t,\t,3\t\t");
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::Headers)
                .create_reader(data);
            let mut rec = ByteRecord::new();
            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!("  1", s(&rec[0]));
            assert_eq!("  2", s(&rec[1]));
            assert_eq!("  3", s(&rec[2]));
            {
                let headers = rdr.headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!("foo", &headers[0]);
                assert_eq!("bar", &headers[1]);
                assert_eq!("baz", &headers[2]);
            }
        });
    }

    #[test]
    fn read_trimed_header_invalid_utf8() {
        Runtime::new().unwrap().block_on(async {
            let data = &b"foo,  b\xFFar,\tbaz\na,b,c\nd,e,f"[..];
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::Headers)
                .create_reader(data);
            let mut rec = StringRecord::new();

            // force the headers to be read
            let _ = rdr.read_record(&mut rec).await;
            // Check the byte headers are trimmed
            {
                let headers = rdr.byte_headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!(b"foo", &headers[0]);
                assert_eq!(b"b\xFFar", &headers[1]);
                assert_eq!(b"baz", &headers[2]);
            }
            match *rdr.headers().await.unwrap_err().kind() {
                ErrorKind::Utf8 { pos: Some(ref pos), ref err } => {
                    assert_eq!(pos, &newpos(0, 1, 0));
                    assert_eq!(err.field(), 1);
                    assert_eq!(err.valid_up_to(), 3);
                }
                ref err => panic!("match failed, got {:?}", err),
            }
        });
    }

    #[test]
    fn read_trimmed_records() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,  bar,\tbaz\n  1,  2,  3\n1\t,\t,3\t\t");
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::Fields)
                .create_reader(data);
            let mut rec = ByteRecord::new();
            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!("1", s(&rec[0]));
            assert_eq!("2", s(&rec[1]));
            assert_eq!("3", s(&rec[2]));
            {
                let headers = rdr.headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!("foo", &headers[0]);
                assert_eq!("  bar", &headers[1]);
                assert_eq!("\tbaz", &headers[2]);
            }
        });
    }

    #[test]
    fn read_record_unequal_fails() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo\nbar,baz");
            let mut rdr =
                AsyncReaderBuilder::new().has_headers(false).create_reader(data);
            let mut rec = ByteRecord::new();

            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!(1, rec.len());
            assert_eq!("foo", s(&rec[0]));

            match rdr.read_byte_record(&mut rec).await {
                Err(err) => match *err.kind() {
                    ErrorKind::UnequalLengths {
                        expected_len: 1,
                        ref pos,
                        len: 2,
                    } => {
                        assert_eq!(pos, &Some(newpos(4, 2, 1)));
                    }
                    ref wrong => panic!("match failed, got {:?}", wrong),
                },
                wrong => panic!("match failed, got {:?}", wrong),
            }
        });
    }

    #[test]
    fn read_record_unequal_ok() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo\nbar,baz");
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .create_reader(data);
            let mut rec = ByteRecord::new();

            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!(1, rec.len());
            assert_eq!("foo", s(&rec[0]));

            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!(2, rec.len());
            assert_eq!("bar", s(&rec[0]));
            assert_eq!("baz", s(&rec[1]));

            assert!(!rdr.read_byte_record(&mut rec).await.unwrap());
        });
    }

    // This tests that even if we get a CSV error, we can continue reading
    // if we want.
    #[test]
    fn read_record_unequal_continue() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo\nbar,baz\nquux");
            let mut rdr =
                AsyncReaderBuilder::new().has_headers(false).create_reader(data);
            let mut rec = ByteRecord::new();

            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!(1, rec.len());
            assert_eq!("foo", s(&rec[0]));

            match rdr.read_byte_record(&mut rec).await {
                Err(err) => match err.kind() {
                    &ErrorKind::UnequalLengths {
                        expected_len: 1,
                        ref pos,
                        len: 2,
                    } => {
                        assert_eq!(pos, &Some(newpos(4, 2, 1)));
                    }
                    wrong => panic!("match failed, got {:?}", wrong),
                },
                wrong => panic!("match failed, got {:?}", wrong),
            }

            assert!(rdr.read_byte_record(&mut rec).await.unwrap());
            assert_eq!(1, rec.len());
            assert_eq!("quux", s(&rec[0]));

            assert!(!rdr.read_byte_record(&mut rec).await.unwrap());
        });
    }

    #[test]
    fn read_record_headers() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f");
            let mut rdr = AsyncReaderBuilder::new().has_headers(true).create_reader(data);
            let mut rec = StringRecord::new();

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("a", &rec[0]);

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("d", &rec[0]);

            assert!(!rdr.read_record(&mut rec).await.unwrap());

            {
                let headers = rdr.byte_headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!(b"foo", &headers[0]);
                assert_eq!(b"bar", &headers[1]);
                assert_eq!(b"baz", &headers[2]);
            }
            {
                let headers = rdr.headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!("foo", &headers[0]);
                assert_eq!("bar", &headers[1]);
                assert_eq!("baz", &headers[2]);
            }
        });
    }

    #[test]
    fn read_record_headers_invalid_utf8() {
        Runtime::new().unwrap().block_on(async {
            let data = &b"foo,b\xFFar,baz\na,b,c\nd,e,f"[..];
            let mut rdr = AsyncReaderBuilder::new().has_headers(true).create_reader(data);
            let mut rec = StringRecord::new();

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("a", &rec[0]);

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("d", &rec[0]);

            assert!(!rdr.read_record(&mut rec).await.unwrap());

            // Check that we can read the headers as raw bytes, but that
            // if we read them as strings, we get an appropriate UTF-8 error.
            {
                let headers = rdr.byte_headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!(b"foo", &headers[0]);
                assert_eq!(b"b\xFFar", &headers[1]);
                assert_eq!(b"baz", &headers[2]);
            }
            match *rdr.headers().await.unwrap_err().kind() {
                ErrorKind::Utf8 { pos: Some(ref pos), ref err } => {
                    assert_eq!(pos, &newpos(0, 1, 0));
                    assert_eq!(err.field(), 1);
                    assert_eq!(err.valid_up_to(), 1);
                }
                ref err => panic!("match failed, got {:?}", err),
            }
        });
    }

    #[test]
    fn read_record_no_headers_before() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f");
            let mut rdr =
                AsyncReaderBuilder::new().has_headers(false).create_reader(data);
            let mut rec = StringRecord::new();

            {
                let headers = rdr.headers().await.unwrap();
                assert_eq!(3, headers.len());
                assert_eq!("foo", &headers[0]);
                assert_eq!("bar", &headers[1]);
                assert_eq!("baz", &headers[2]);
            }

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("foo", &rec[0]);

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("a", &rec[0]);

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("d", &rec[0]);

            assert!(!rdr.read_record(&mut rec).await.unwrap());
        });
    }

    #[test]
    fn read_record_no_headers_after() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f");
            let mut rdr =
                AsyncReaderBuilder::new().has_headers(false).create_reader(data);
            let mut rec = StringRecord::new();

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("foo", &rec[0]);

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("a", &rec[0]);

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("d", &rec[0]);

            assert!(!rdr.read_record(&mut rec).await.unwrap());

            let headers = rdr.headers().await.unwrap();
            assert_eq!(3, headers.len());
            assert_eq!("foo", &headers[0]);
            assert_eq!("bar", &headers[1]);
            assert_eq!("baz", &headers[2]);
        });
    }

    #[test]
    fn seek() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f\ng,h,i");
            let mut rdr = AsyncReaderBuilder::new().create_reader(std::io::Cursor::new(data));
            rdr.seek(newpos(18, 3, 2)).await.unwrap();

            let mut rec = StringRecord::new();

            assert_eq!(18, rdr.position().byte());
            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("d", &rec[0]);

            assert_eq!(24, rdr.position().byte());
            assert_eq!(4, rdr.position().line());
            assert_eq!(3, rdr.position().record());
            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("g", &rec[0]);

            assert!(!rdr.read_record(&mut rec).await.unwrap());
        });
    }

    // Test that we can read headers after seeking even if the headers weren't
    // explicit read before seeking.
    #[test]
    fn seek_headers_after() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f\ng,h,i");
            let mut rdr = AsyncReaderBuilder::new().create_reader(std::io::Cursor::new(data));
            rdr.seek(newpos(18, 3, 2)).await.unwrap();
            assert_eq!(rdr.headers().await.unwrap(), vec!["foo", "bar", "baz"]);
        });
    }

    // Test that we can read headers after seeking if the headers were read
    // before seeking.
    #[test]
    fn seek_headers_before_after() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f\ng,h,i");
            let mut rdr = AsyncReaderBuilder::new().create_reader(std::io::Cursor::new(data));
            let headers = rdr.headers().await.unwrap().clone();
            rdr.seek(newpos(18, 3, 2)).await.unwrap();
            assert_eq!(&headers, rdr.headers().await.unwrap());
        });
    }

    // Test that even if we didn't read headers before seeking, if we seek to
    // the current byte offset, then no seeking is done and therefore we can
    // still read headers after seeking.
    #[test]
    fn seek_headers_no_actual_seek() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f\ng,h,i");
            let mut rdr = AsyncReaderBuilder::new().create_reader(std::io::Cursor::new(data));
            rdr.seek(Position::new()).await.unwrap();
            assert_eq!("foo", &rdr.headers().await.unwrap()[0]);
        });
    }

    #[test]
    fn rewind() {
        Runtime::new().unwrap().block_on(async {
            let data = b("foo,bar,baz\na,b,c\nd,e,f\ng,h,i");
            let mut rdr = AsyncReaderBuilder::new().create_reader(std::io::Cursor::new(data));

            let mut rec = StringRecord::new();
            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("a", &rec[0]);

            // assert_eq!(18, rdr.position().byte());
            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("d", &rec[0]);

            rdr.rewind().await.unwrap();

            assert!(rdr.read_record(&mut rec).await.unwrap());
            assert_eq!(3, rec.len());
            assert_eq!("a", &rec[0]);
        });
    }

    // Test that position info is reported correctly in absence of headers.
    #[test]
    fn positions_no_headers() {
        Runtime::new().unwrap().block_on(async {
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(false)
                .create_reader("a,b,c\nx,y,z".as_bytes())
                .into_records();

            let pos = rdr.next().await.unwrap().unwrap().position().unwrap().clone();
            assert_eq!(pos.byte(), 0);
            assert_eq!(pos.line(), 1);
            assert_eq!(pos.record(), 0);

            let pos = rdr.next().await.unwrap().unwrap().position().unwrap().clone();
            assert_eq!(pos.byte(), 6);
            assert_eq!(pos.line(), 2);
            assert_eq!(pos.record(), 1);
        });
    }

    // Test that position info is reported correctly with headers.
    #[test]
    fn positions_headers() {
        Runtime::new().unwrap().block_on(async {
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(true)
                .create_reader("a,b,c\nx,y,z".as_bytes())
                .into_records();

            let pos = rdr.next().await.unwrap().unwrap().position().unwrap().clone();
            assert_eq!(pos.byte(), 6);
            assert_eq!(pos.line(), 2);
            assert_eq!(pos.record(), 1);
        });
    }

    // Test that reading headers on empty data yields an empty record.
    #[test]
    fn headers_on_empty_data() {
        Runtime::new().unwrap().block_on(async {
            let mut rdr = AsyncReaderBuilder::new().create_reader("".as_bytes());
            let r = rdr.byte_headers().await.unwrap();
            assert_eq!(r.len(), 0);
        });
    }

    // Test that reading the first record on empty data works.
    #[test]
    fn no_headers_on_empty_data() {
        Runtime::new().unwrap().block_on(async {
            let mut rdr =
            AsyncReaderBuilder::new().has_headers(false).create_reader("".as_bytes());
            assert_eq!(count(rdr.records()).await, 0);
        });
    }

    // Test that reading the first record on empty data works, even if
    // we've tried to read headers before hand.
    #[test]
    fn no_headers_on_empty_data_after_headers() {
        Runtime::new().unwrap().block_on(async {
            let mut rdr =
                AsyncReaderBuilder::new().has_headers(false).create_reader("".as_bytes());
            assert_eq!(rdr.headers().await.unwrap().len(), 0);
            assert_eq!(count(rdr.records()).await, 0);
        });
    }

    #[test]
    fn no_infinite_loop_on_io_errors() {
        struct FailingRead;
        impl io::AsyncRead for FailingRead {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context,
                _buf: &mut tokio::io::ReadBuf
            ) -> Poll<Result<(), io::Error>> {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "Broken reader")))
            }
        }
        impl Unpin for FailingRead {}
    
        Runtime::new().unwrap().block_on(async {
            let mut record_results = AsyncReader::from_reader(FailingRead).into_records();
            let first_result = record_results.next().await;
            assert!(
                matches!(&first_result, Some(Err(e)) if matches!(e.kind(), crate::ErrorKind::Io(_)))
            );
            assert!(record_results.next().await.is_none());
        });
    }
}
