use std::future::Future;
use std::pin::Pin;
use std::result;
use std::task::{Context, Poll};

cfg_if::cfg_if! {
if #[cfg(feature = "tokio")] {
    use std::io::SeekFrom;
    use tokio::io::{self, AsyncBufRead};
    use tokio_stream::Stream;
} else {
    use futures::io::{self, AsyncBufRead, AsyncSeekExt};
    use futures::stream::Stream;
}}
    
use csv_core::{ReaderBuilder as CoreReaderBuilder};
use csv_core::{Reader as CoreReader};
#[cfg(feature = "with_serde")]
use serde::de::DeserializeOwned;

use crate::{Terminator, Trim};
use crate::byte_record::{ByteRecord, Position};
use crate::error::{Error, ErrorKind, Result, Utf8Error};
use crate::string_record::StringRecord;

cfg_if::cfg_if! {
if #[cfg(feature = "tokio")] {
    pub mod ardr_tokio;
} else {
    pub mod ardr_futures;
}}
    
#[cfg(all(feature = "with_serde", not(feature = "tokio")))]
pub mod ades_futures;
    
#[cfg(all(feature = "with_serde", feature = "tokio"))]
pub mod ades_tokio;

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-// Builder
//-//////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a CSV reader with various configuration knobs.
///
/// This builder can be used to tweak the field delimiter, record terminator
/// and more. Once a CSV reader / deserializer is built, its configuration cannot be
/// changed.
#[derive(Debug)]
pub struct AsyncReaderBuilder {
    capacity: usize,
    flexible: bool,
    has_headers: bool,
    trim: Trim,
    end_on_io_error: bool,
    /// The underlying CSV parser builder.
    ///
    /// We explicitly put this on the heap because CoreReaderBuilder embeds an
    /// entire DFA transition table, which along with other things, tallies up
    /// to almost 500 bytes on the stack.
    builder: Box<CoreReaderBuilder>,
}

impl Default for AsyncReaderBuilder {
    fn default() -> AsyncReaderBuilder {
        AsyncReaderBuilder {
            capacity: 8 * (1 << 10),
            flexible: false,
            has_headers: true,
            trim: Trim::default(),
            end_on_io_error: true,
            builder: Box::new(CoreReaderBuilder::default()),
        }
    }
}

impl AsyncReaderBuilder {
    /// Create a new builder for configuring CSV parsing.
    ///
    /// To convert a builder into a reader, call one of the methods starting
    /// with `from_`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::{AsyncReaderBuilder, StringRecord};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new().from_reader(data.as_bytes());
    ///
    ///     let records = rdr
    ///         .records()
    ///         .map(Result::unwrap)
    ///         .collect::<Vec<StringRecord>>().await;
    ///     assert_eq!(records, vec![
    ///         vec!["Boston", "United States", "4628910"],
    ///         vec!["Concord", "United States", "42695"],
    ///     ]);
    ///     Ok(())
    /// }
    /// ```
    pub fn new() -> AsyncReaderBuilder {
        AsyncReaderBuilder::default()
    }
    
    /// Returns csv_core Builder reference.
    #[deprecated(
        since = "1.0.1",
        note = "This getter is no longer needed and will be removed"
    )]
    pub fn get_core_builder_ref(&self) -> &Box<CoreReaderBuilder> {
        &self.builder
    }

    /// The field delimiter to use when parsing CSV.
    ///
    /// The default is `b','`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::{AsyncReaderBuilder, StringRecord};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await}); }
    /// async fn example() {
    ///     let data = "\
    /// city;country;pop
    /// Boston;United States;4628910
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .delimiter(b';')
    ///         .from_reader(data.as_bytes());
    ///
    ///     let records = rdr
    ///         .records()
    ///         .map(Result::unwrap)
    ///         .collect::<Vec<StringRecord>>().await;
    ///     assert_eq!(records, vec![
    ///         vec!["Boston", "United States", "4628910"],
    ///     ]);
     /// }
    /// ```
    pub fn delimiter(&mut self, delimiter: u8) -> &mut AsyncReaderBuilder {
        self.builder.delimiter(delimiter);
        self
    }
    
    /// Returns information if read file has headers.
    #[deprecated(
        since = "1.0.1",
        note = "This getter is no longer needed and will be removed"
    )]
    pub fn get_headers_presence(&self) -> bool {
        self.has_headers
    }

    /// Whether to treat the first row as a special header row.
    ///
    /// By default, the first row is treated as a special header row, which
    /// means the header is never returned by any of the record reading methods
    /// or iterators. When this is disabled (`yes` set to `false`), the first
    /// row is not treated specially.
    ///
    /// Note that the `headers` and `byte_headers` methods are unaffected by
    /// whether this is set. Those methods always return the first record.
    ///
    /// # Example
    ///
    /// This example shows what happens when `has_headers` is disabled.
    /// Namely, the first row is treated just like any other row.
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .has_headers(false)
    ///         .from_reader(data.as_bytes());
    ///     let mut iter = rdr.records();
    ///
    ///     // Read the first record.
    ///     assert_eq!(iter.next().await.unwrap()?, vec!["city", "country", "pop"]);
    ///
    ///     // Read the second record.
    ///     assert_eq!(iter.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    /// 
    ///     assert!(iter.next().await.is_none());
    ///     Ok(())
    /// }
    /// ```
    pub fn has_headers(&mut self, yes: bool) -> &mut AsyncReaderBuilder {
        self.has_headers = yes;
        self
    }
    
    /// Returns information if read file has headers.
    #[deprecated(
        since = "1.0.1",
        note = "This getter is no longer needed and will be removed"
    )]
    pub fn is_flexible(&self) -> bool {
        self.flexible
    }

    /// Whether the number of fields in records is allowed to change or not.
    ///
    /// When disabled (which is the default), parsing CSV data will return an
    /// error if a record is found with a number of fields different from the
    /// number of fields in a previous record.
    ///
    /// When enabled, this error checking is turned off.
    ///
    /// # Example: flexible records enabled
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     // Notice that the first row is missing the population count.
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .flexible(true)
    ///         .from_reader(data.as_bytes());
    ///     let mut records = rdr.records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "United States"]);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Example: flexible records disabled
    ///
    /// This shows the error that appears when records of unequal length
    /// are found and flexible records have been disabled (which is the
    /// default).
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::{ErrorKind, AsyncReaderBuilder};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     // Notice that the first row is missing the population count.
    ///     let data = "\
    /// city,country,pop
    /// Boston,United States
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .flexible(false)
    ///         .from_reader(data.as_bytes());
    ///
    ///     let mut records = rdr.records();
    ///     match records.next().await {
    ///         Some(Err(err)) => match *err.kind() {
    ///             ErrorKind::UnequalLengths { expected_len, len, .. } => {
    ///                 // The header row has 3 fields...
    ///                 assert_eq!(expected_len, 3);
    ///                 // ... but the first row has only 2 fields.
    ///                 assert_eq!(len, 2);
    ///                 Ok(())
    ///             }
    ///             ref wrong => {
    ///                 Err(From::from(format!(
    ///                     "expected UnequalLengths error but got {:?}",
    ///                     wrong)))
    ///             }
    ///         }
    ///         Some(Ok(rec)) =>
    ///             Err(From::from(format!(
    ///                 "expected one errored record but got good record {:?}",
    ///                  rec))),
    ///         None =>
    ///            Err(From::from(
    ///                "expected one errored record but got none"))
    ///     }
    /// }
    /// ```
    pub fn flexible(&mut self, yes: bool) -> &mut AsyncReaderBuilder {
        self.flexible = yes;
        self
    }
    
    /// Returns information if read file has headers.
    #[deprecated(
        since = "1.0.1",
        note = "This getter is no longer needed and will be removed"
    )]
    pub fn get_trim_option(&self) -> Trim {
        self.trim
    }
    
    /// If set, CSV records' stream will end when first i/o error happens. 
    /// Otherwise it will continue trying to read from underlying reader.
    /// By default this option is set.
    pub fn end_on_io_error(&mut self, yes: bool) -> &mut AsyncReaderBuilder {
        self.end_on_io_error = yes;
        self
    }

    /// Whether fields are trimmed of leading and trailing whitespace or not.
    ///
    /// By default, no trimming is performed. This method permits one to
    /// override that behavior and choose one of the following options:
    ///
    /// 1. `Trim::Headers` trims only header values.
    /// 2. `Trim::Fields` trims only non-header or "field" values.
    /// 3. `Trim::All` trims both header and non-header values.
    ///
    /// A value is only interpreted as a header value if this CSV reader is
    /// configured to read a header record (which is the default).
    ///
    /// When reading string records, characters meeting the definition of
    /// Unicode whitespace are trimmed. When reading byte records, characters
    /// meeting the definition of ASCII whitespace are trimmed. ASCII
    /// whitespace characters correspond to the set `[\t\n\v\f\r ]`.
    ///
    /// # Example
    ///
    /// This example shows what happens when all values are trimmed.
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::{AsyncReaderBuilder, StringRecord, Trim};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city ,   country ,  pop
    /// Boston,\"
    ///    United States\",4628910
    /// Concord,   United States   ,42695
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .trim(Trim::All)
    ///         .from_reader(data.as_bytes());
    ///     let records = rdr
    ///         .records()
    ///         .map(Result::unwrap)
    ///         .collect::<Vec<StringRecord>>().await;
    ///     assert_eq!(records, vec![
    ///         vec!["Boston", "United States", "4628910"],
    ///         vec!["Concord", "United States", "42695"],
    ///     ]);
    ///     Ok(())
    /// }
    /// ```
    pub fn trim(&mut self, trim: Trim) -> &mut AsyncReaderBuilder {
        self.trim = trim;
        self
    }

    /// The record terminator to use when parsing CSV.
    ///
    /// A record terminator can be any single byte. The default is a special
    /// value, `Terminator::CRLF`, which treats any occurrence of `\r`, `\n`
    /// or `\r\n` as a single record terminator.
    ///
    /// # Example: `$` as a record terminator
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::{AsyncReaderBuilder, Terminator};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "city,country,pop$Boston,United States,4628910";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .terminator(Terminator::Any(b'$'))
    ///         .from_reader(data.as_bytes());
    ///     let mut iter = rdr.records();
    ///     assert_eq!(iter.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(iter.next().await.is_none());
    ///     Ok(())
    /// }
    /// ```
    pub fn terminator(&mut self, term: Terminator) -> &mut AsyncReaderBuilder {
        self.builder.terminator(term.to_core());
        self
    }

    /// The quote character to use when parsing CSV.
    ///
    /// The default is `b'"'`.
    ///
    /// # Example: single quotes instead of double quotes
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,'United States',4628910
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .quote(b'\'')
    ///         .from_reader(data.as_bytes());
    ///     let mut iter = rdr.records();
    ///     assert_eq!(iter.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(iter.next().await.is_none());
    ///     Ok(())
    /// }
    /// ```
    pub fn quote(&mut self, quote: u8) -> &mut AsyncReaderBuilder {
        self.builder.quote(quote);
        self
    }

    /// The escape character to use when parsing CSV.
    ///
    /// In some variants of CSV, quotes are escaped using a special escape
    /// character like `\` (instead of escaping quotes by doubling them).
    ///
    /// By default, recognizing these idiosyncratic escapes is disabled.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,\"The \\\"United\\\" States\",4628910
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .escape(Some(b'\\'))
    ///         .from_reader(data.as_bytes());
    ///     let mut records = rdr.records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "The \"United\" States", "4628910"]);
    ///     Ok(())
    /// }
    /// ```
    pub fn escape(&mut self, escape: Option<u8>) -> &mut AsyncReaderBuilder {
        self.builder.escape(escape);
        self
    }

    /// Enable double quote escapes.
    ///
    /// This is enabled by default, but it may be disabled. When disabled,
    /// doubled quotes are not interpreted as escapes.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,\"The \"\"United\"\" States\",4628910
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .double_quote(false)
    ///         .from_reader(data.as_bytes());
    ///     let mut records = rdr.records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "The \"United\"\" States\"", "4628910"]);
    ///     Ok(())
    /// }
    /// ```
    pub fn double_quote(&mut self, yes: bool) -> &mut AsyncReaderBuilder {
        self.builder.double_quote(yes);
        self
    }

    /// Enable or disable quoting.
    ///
    /// This is enabled by default, but it may be disabled. When disabled,
    /// quotes are not treated specially.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// Boston,\"The United States,4628910
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .quoting(false)
    ///         .from_reader(data.as_bytes());
    ///     let mut records = rdr.records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "\"The United States", "4628910"]);
    ///     Ok(())
    /// }
    /// ```
    pub fn quoting(&mut self, yes: bool) -> &mut AsyncReaderBuilder {
        self.builder.quoting(yes);
        self
    }

    /// The comment character to use when parsing CSV.
    ///
    /// If the start of a record begins with the byte given here, then that
    /// line is ignored by the CSV parser.
    ///
    /// This is disabled by default.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city,country,pop
    /// #Concord,United States,42695
    /// Boston,United States,4628910
    /// ";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .comment(Some(b'#'))
    ///         .from_reader(data.as_bytes());
    ///     let mut records = rdr.records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(records.next().await.is_none());
    ///     Ok(())
    /// }
    /// ```
    pub fn comment(&mut self, comment: Option<u8>) -> &mut AsyncReaderBuilder {
        self.builder.comment(comment);
        self
    }

    /// A convenience method for specifying a configuration to read ASCII
    /// delimited text.
    ///
    /// This sets the delimiter and record terminator to the ASCII unit
    /// separator (`\x1F`) and record separator (`\x1E`), respectively.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use futures::stream::StreamExt;
    /// use csv_async::AsyncReaderBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let data = "\
    /// city\x1Fcountry\x1Fpop\x1EBoston\x1FUnited States\x1F4628910";
    ///     let mut rdr = AsyncReaderBuilder::new()
    ///         .ascii()
    ///         .from_reader(data.as_bytes());
    ///     let mut records = rdr.byte_records();
    ///     assert_eq!(records.next().await.unwrap()?, vec!["Boston", "United States", "4628910"]);
    ///     assert!(records.next().await.is_none());
    ///     Ok(())
    /// }
    /// ```
    pub fn ascii(&mut self) -> &mut AsyncReaderBuilder {
        self.builder.ascii();
        self
    }
    
    /// Returns buffer capacity.
    #[deprecated(
        since = "1.0.1",
        note = "This getter is no longer needed and will be removed"
    )]
    pub fn get_buffer_capacity(&self) -> usize {
        self.capacity
    }

    /// Set the capacity (in bytes) of the buffer used in the CSV reader.
    /// This defaults to a reasonable setting.
    pub fn buffer_capacity(&mut self, capacity: usize) -> &mut AsyncReaderBuilder {
        self.capacity = capacity;
        self
    }

    /// Enable or disable the NFA for parsing CSV.
    ///
    /// This is intended to be a debug option. The NFA is always slower than
    /// the DFA.
    #[doc(hidden)]
    pub fn nfa(&mut self, yes: bool) -> &mut AsyncReaderBuilder {
        self.builder.nfa(yes);
        self
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-// Reader
//-//////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ReaderState {
    /// When set, this contains the first row of any parsed CSV data.
    ///
    /// This is always populated, regardless of whether `has_headers` is set.
    headers: Option<Headers>,
    /// When set, the first row of parsed CSV data is excluded from things
    /// that read records, like iterators and `read_record`.
    has_headers: bool,
    /// When set, there is no restriction on the length of records. When not
    /// set, every record must have the same number of fields, or else an error
    /// is reported.
    flexible: bool,
    trim: Trim,
    /// The number of fields in the first record parsed.
    first_field_count: Option<u64>,
    /// The current position of the parser.
    ///
    /// Note that this position is only observable by callers at the start
    /// of a record. More granular positions are not supported.
    cur_pos: Position,
    /// Whether the first record has been read or not.
    first: bool,
    /// Whether the reader has been seek or not.
    seeked: bool,
    /// If set, CSV records' stream will end when first i/o error happens. 
    /// Otherwise it will continue trying to read from underlying reader.
    end_on_io_error: bool,
    /// IO errors on the underlying reader will be considered as an EOF for
    /// subsequent read attempts, as it would be incorrect to keep on trying
    /// to read when the underlying reader has broken.
    ///
    /// For clarity, having the best `Debug` impl and in case they need to be
    /// treated differently at some point, we store whether the `EOF` is
    /// considered because an actual EOF happened, or because we encountered
    /// an IO error.
    /// This has no additional runtime cost.
    eof: ReaderEofState,
}

/// Whether EOF of the underlying reader has been reached or not.
///
/// IO errors on the underlying reader will be considered as an EOF for
/// subsequent read attempts, as it would be incorrect to keep on trying
/// to read when the underlying reader has broken.
///
/// For clarity, having the best `Debug` impl and in case they need to be
/// treated differently at some point, we store whether the `EOF` is
/// considered because an actual EOF happened, or because we encountered
/// an IO error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderEofState {
    NotEof,
    Eof,
    IOError,
}

/// Headers encapsulates any data associated with the headers of CSV data.
///
/// The headers always correspond to the first row.
#[derive(Debug)]
struct Headers {
    /// The header, as raw bytes.
    byte_record: ByteRecord,
    /// The header, as valid UTF-8 (or a UTF-8 error).
    string_record: result::Result<StringRecord, Utf8Error>,
}

impl ReaderState {
    #[inline(always)]
    fn add_record(&mut self, record: &ByteRecord) -> Result<()> {
        let i = self.cur_pos.record();
        self.cur_pos.set_record(i.checked_add(1).unwrap());
        if !self.flexible {
            match self.first_field_count {
                None => self.first_field_count = Some(record.len() as u64),
                Some(expected) => {
                    if record.len() as u64 != expected {
                        return Err(Error::new(ErrorKind::UnequalLengths {
                            pos: record.position().map(Clone::clone),
                            expected_len: expected,
                            len: record.len() as u64,
                        }));
                    }
                }
            }
        }
        Ok(())
    }
}
/// CSV async reader internal implementation used by both record reader and deserializer.
/// 
#[derive(Debug)]
pub struct AsyncReaderImpl<R> {
    /// The underlying CSV parser.
    ///
    /// We explicitly put this on the heap because CoreReader embeds an entire
    /// DFA transition table, which along with other things, tallies up to
    /// almost 500 bytes on the stack.
    core: Box<CoreReader>,
    /// The underlying reader.
    rdr: io::BufReader<R>,
    /// Various state tracking.
    ///
    /// There is more state embedded in the `CoreReader`.
    state: ReaderState,
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct FillBuf<'a, R: AsyncBufRead + ?Sized> {
    reader: &'a mut R,
}

impl<R: AsyncBufRead + ?Sized + Unpin> Unpin for FillBuf<'_, R> {}

impl<'a, R: AsyncBufRead + ?Sized + Unpin> FillBuf<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        Self { reader }
    }
}

impl<R: AsyncBufRead + ?Sized + Unpin> Future for FillBuf<'_, R> {
    type Output = io::Result<usize>;
    
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut *self.reader).poll_fill_buf(cx) {
            Poll::Ready(res) => {
                match res {
                    Ok(res) => Poll::Ready(Ok(res.len())),
                    Err(e) => Poll::Ready(Err(e))
                }
            },
            Poll::Pending => Poll::Pending
        }
    }
} 

impl<'r, R> AsyncReaderImpl<R>
where
    R: io::AsyncRead + std::marker::Unpin + 'r,
{
    /// Create a new CSV reader given a builder and a source of underlying
    /// bytes.
    fn new(builder: &AsyncReaderBuilder, rdr: R) -> AsyncReaderImpl<R> {
        AsyncReaderImpl {
            core: Box::new(builder.builder.build()),
            rdr: io::BufReader::with_capacity(builder.capacity, rdr),
            state: ReaderState {
                headers: None,
                has_headers: builder.has_headers,
                flexible: builder.flexible,
                trim: builder.trim,
                end_on_io_error: builder.end_on_io_error,
                first_field_count: None,
                cur_pos: Position::new(),
                first: false,
                seeked: false,
                eof: ReaderEofState::NotEof,
            },
        }
    }

    /// Returns a reference to the first row read by this parser.
    ///
    pub async fn headers(&mut self) -> Result<&StringRecord> {
        if self.state.headers.is_none() {
            let mut record = ByteRecord::new();
            self.read_byte_record_impl(&mut record).await?;
            self.set_headers_impl(Err(record));
        }
        let headers = self.state.headers.as_ref().unwrap();
        match headers.string_record {
            Ok(ref record) => Ok(record),
            Err(ref err) => Err(Error::new(ErrorKind::Utf8 {
                pos: headers.byte_record.position().map(Clone::clone),
                err: err.clone(),
            })),
        }
    }

    /// Returns a reference to the first row read by this parser as raw bytes.
    ///
    pub async fn byte_headers(&mut self) -> Result<&ByteRecord> {
        if self.state.headers.is_none() {
            let mut record = ByteRecord::new();
            self.read_byte_record_impl(&mut record).await?;
            self.set_headers_impl(Err(record));
        }
        Ok(&self.state.headers.as_ref().unwrap().byte_record)
    }

    /// Set the headers of this CSV parser manually.
    ///
    pub fn set_headers(&mut self, headers: StringRecord) {
        self.set_headers_impl(Ok(headers));
    }

    /// Set the headers of this CSV parser manually as raw bytes.
    ///
    pub fn set_byte_headers(&mut self, headers: ByteRecord) {
        self.set_headers_impl(Err(headers));
    }

    fn set_headers_impl(
        &mut self,
        headers: result::Result<StringRecord, ByteRecord>,
    ) {
        // If we have string headers, then get byte headers. But if we have
        // byte headers, then get the string headers (or a UTF-8 error).
        let (mut str_headers, mut byte_headers) = match headers {
            Ok(string) => {
                let bytes = string.clone().into_byte_record();
                (Ok(string), bytes)
            }
            Err(bytes) => {
                match StringRecord::from_byte_record(bytes.clone()) {
                    Ok(str_headers) => (Ok(str_headers), bytes),
                    Err(err) => (Err(err.utf8_error().clone()), bytes),
                }
            }
        };
        if self.state.trim.should_trim_headers() {
            if let Ok(ref mut str_headers) = str_headers.as_mut() {
                str_headers.trim();
            }
            byte_headers.trim();
        }
        self.state.headers = Some(Headers {
            byte_record: byte_headers,
            string_record: str_headers,
        });
    }

    /// Read a single row into the given record. Returns false when no more
    /// records could be read.
    pub async fn read_record(&mut self, record: &mut StringRecord) -> Result<bool> {
        let result = record.read(self).await;
        // We need to trim again because trimming string records includes
        // Unicode whitespace. (ByteRecord trimming only includes ASCII
        // whitespace.)
        if self.state.trim.should_trim_fields() {
            record.trim();
        }
        result
    }

    /// Read a single row into the given byte record. Returns false when no
    /// more records could be read.
    pub async fn read_byte_record(
        &mut self,
        record: &mut ByteRecord,
    ) -> Result<bool> {
        if !self.state.seeked && !self.state.has_headers && !self.state.first {
            // If the caller indicated "no headers" and we haven't yielded the
            // first record yet, then we should yield our header row if we have
            // one.
            if let Some(ref headers) = self.state.headers {
                self.state.first = true;
                record.clone_from(&headers.byte_record);
                if self.state.trim.should_trim_fields() {
                    record.trim();
                }
                return Ok(!record.is_empty());
            }
        }
        let ok = self.read_byte_record_impl(record).await?;
        self.state.first = true;
        if !self.state.seeked && self.state.headers.is_none() {
            self.set_headers_impl(Err(record.clone()));
            // If the end user indicated that we have headers, then we should
            // never return the first row. Instead, we should attempt to
            // read and return the next one.
            if self.state.has_headers {
                let result = self.read_byte_record_impl(record).await;
                if self.state.trim.should_trim_fields() {
                    record.trim();
                }
                return result;
            }
        } else if self.state.trim.should_trim_fields() {
            record.trim();
        }
        Ok(ok)
    }

    /// Read a byte record from the underlying CSV reader, without accounting
    /// for headers.
    #[inline(always)]
    async fn read_byte_record_impl(
        &mut self,
        record: &mut ByteRecord,
    ) -> Result<bool> {
        use csv_core::ReadRecordResult::*;

        record.clear();
        record.set_position(Some(self.state.cur_pos.clone()));
        match self.state.eof {
            ReaderEofState::Eof => return Ok(false),
            ReaderEofState::IOError => {
                if self.state.end_on_io_error { return Ok(false) }
            },
            ReaderEofState::NotEof => {}
        }
        let (mut outlen, mut endlen) = (0, 0);
        loop {
            let (res, nin, nout, nend) = {
                if let Err(err) = FillBuf::new(&mut self.rdr).await {
                    self.state.eof = ReaderEofState::IOError;
                    return Err(err.into());
                }
                let (fields, ends) = record.as_parts();
                self.core.read_record(
                    self.rdr.buffer(),
                    &mut fields[outlen..],
                    &mut ends[endlen..],
                )
            };
            Pin::new(&mut self.rdr).consume(nin);
            let byte = self.state.cur_pos.byte();
            self.state
                .cur_pos
                .set_byte(byte + nin as u64)
                .set_line(self.core.line());
            outlen += nout;
            endlen += nend;
            match res {
                InputEmpty => continue,
                OutputFull => {
                    record.expand_fields();
                    continue;
                }
                OutputEndsFull => {
                    record.expand_ends();
                    continue;
                }
                Record => {
                    record.set_len(endlen);
                    self.state.add_record(record)?;
                    return Ok(true);
                }
                End => {
                    self.state.eof = ReaderEofState::Eof;
                    return Ok(false);
                }
            }
        }
    }

    /// Return the current position of this CSV reader.
    ///
    #[inline]
    pub fn position(&self) -> &Position {
        &self.state.cur_pos
    }

    /// Returns true if and only if this reader has been exhausted.
    ///
    pub fn is_done(&self) -> bool {
        self.state.eof != ReaderEofState::NotEof
    }

    /// Returns true if and only if this reader has been configured to
    /// interpret the first record as a header record.
    pub fn has_headers(&self) -> bool {
        self.state.has_headers
    }

    /// Returns a reference to the underlying reader.
    pub fn get_ref(&self) -> &R {
        self.rdr.get_ref()
    }

    /// Returns a mutable reference to the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        self.rdr.get_mut()
    }

    /// Unwraps this CSV reader, returning the underlying reader.
    ///
    /// Note that any leftover data inside this reader's internal buffer is
    /// lost.
    pub fn into_inner(self) -> R {
        self.rdr.into_inner()
    }
}

#[cfg(not(feature = "tokio"))]
impl<R: io::AsyncRead + io::AsyncSeek + std::marker::Unpin> AsyncReaderImpl<R> {
    /// Seeks the underlying reader to the position given.
    ///
    pub async fn seek(&mut self, pos: Position) -> Result<()> {
        self.byte_headers().await?;
        self.state.seeked = true;
        if pos.byte() == self.state.cur_pos.byte() {
            return Ok(());
        }
        self.rdr.seek(io::SeekFrom::Start(pos.byte())).await?;
        self.core.reset();
        self.core.set_line(pos.line());
        self.state.cur_pos = pos;
        self.state.eof = ReaderEofState::NotEof;
        Ok(())
    }

    /// This is like `seek`, but provides direct control over how the seeking
    /// operation is performed via `io::SeekFrom`.
    pub async fn seek_raw(
        &mut self,
        seek_from: io::SeekFrom,
        pos: Position,
    ) -> Result<()> {
        self.byte_headers().await?;
        self.state.seeked = true;
        self.rdr.seek(seek_from).await?;
        self.core.reset();
        self.core.set_line(pos.line());
        self.state.cur_pos = pos;
        self.state.eof = ReaderEofState::NotEof;
        Ok(())
    }
}

#[cfg(feature = "tokio")]
#[allow(dead_code)]
impl<R: io::AsyncRead + io::AsyncSeek + std::marker::Unpin> AsyncReaderImpl<R> {
    /// Attempts to seek to an offset, in bytes, in a stream.
    ///
    /// If this function returns successfully, then the job has been submitted.
    /// To find out when it completes, call `poll_complete`.
    pub fn start_seek(self: Pin<&mut Self>, _cx: &mut Context, _position: SeekFrom) -> Poll<Result<()>> {
        // TODO.
        // For futures we have better luck, because there is:
        // impl<R: AsyncRead + AsyncSeek> AsyncSeek for BufReader<R>
        // not the case for tokio, for which we only have implementation for File to look at.
        unimplemented!()
    }

    /// Waits for a seek operation to complete.
    ///
    /// If the seek operation completed successfully,
    /// this method returns the new position from the start of the stream.
    /// That position can be used later with [`SeekFrom::Start`].
    ///
    /// # Errors
    ///
    /// Seeking to a negative offset is considered an error.
    ///
    /// # Panics
    ///
    /// Calling this method without calling `start_seek` first is an error.
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<u64>> {
        // TODO.
        unimplemented!()
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

async fn read_record_borrowed<'r, R>(
    rdr: &'r mut AsyncReaderImpl<R>,
    mut rec: StringRecord,
) -> (Option<Result<StringRecord>>, &'r mut AsyncReaderImpl<R>, StringRecord)
where
    R: io::AsyncRead + std::marker::Unpin
{
    let result = match rdr.read_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(Ok(rec.clone())),
        Ok(false) => None,
    };

    (result, rdr, rec)
}

/// A borrowed stream of records as strings.
///
/// The lifetime parameter `'r` refers to the lifetime of the underlying
/// CSV `Reader`.
pub struct StringRecordsStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin
{
    fut: Option<
        Pin<
            Box<
                dyn Future<
                        Output = (
                            Option<Result<StringRecord>>,
                            &'r mut AsyncReaderImpl<R>,
                            StringRecord,
                        ),
                    > + 'r,
            >,
        >,
    >,
}

impl<'r, R> StringRecordsStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin
{
    fn new(rdr: &'r mut AsyncReaderImpl<R>) -> Self {
        Self {
            fut: Some(Pin::from(Box::new(read_record_borrowed(
                rdr,
                StringRecord::new(),
            )))),
        }
    }
}

impl<'r, R> Stream for StringRecordsStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin
{
    type Item = Result<StringRecord>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        match self.fut.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready((result, rdr, rec)) => {
                if result.is_some() {
                    self.fut = Some(Pin::from(Box::new(
                        read_record_borrowed(rdr, rec),
                    )));
                } else {
                    self.fut = None;
                }

                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

async fn read_record<R>(
    mut rdr: AsyncReaderImpl<R>,
    mut rec: StringRecord,
) -> (Option<Result<StringRecord>>, AsyncReaderImpl<R>, StringRecord)
where
    R: io::AsyncRead + std::marker::Unpin
{
    let result = match rdr.read_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(Ok(rec.clone())),
        Ok(false) => None,
    };

    (result, rdr, rec)
}

/// An owned stream of records as strings.
pub struct StringRecordsIntoStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin
{
    fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Option<Result<StringRecord>>,
                        AsyncReaderImpl<R>,
                        StringRecord,
                    ),
                > + 'r,
            >,
        >,
    >,
}

impl<'r, R> StringRecordsIntoStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    fn new(rdr: AsyncReaderImpl<R>) -> Self {
        Self {
            fut: Some(Pin::from(Box::new(read_record(
                rdr,
                StringRecord::new(),
            )))),
        }
    }
}

impl<'r, R> Stream for StringRecordsIntoStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    type Item = Result<StringRecord>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        match self.fut.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready((result, rdr, rec)) => {
                if result.is_some() {
                    self.fut =
                        Some(Pin::from(Box::new(read_record(rdr, rec))));
                } else {
                    self.fut = None;
                }

                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

async fn read_byte_record_borrowed<'r, R>(
    rdr: &'r mut AsyncReaderImpl<R>,
    mut rec: ByteRecord,
) -> (Option<Result<ByteRecord>>, &'r mut AsyncReaderImpl<R>, ByteRecord)
where
    R: io::AsyncRead + std::marker::Unpin,
{
    let result = match rdr.read_byte_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(Ok(rec.clone())),
        Ok(false) => None,
    };

    (result, rdr, rec)
}

/// A borrowed stream of records as raw bytes.
///
/// The lifetime parameter `'r` refers to the lifetime of the underlying
/// CSV `Reader`.
pub struct ByteRecordsStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin,
{
    fut: Option<
        Pin<
            Box<
                dyn Future<
                        Output = (
                            Option<Result<ByteRecord>>,
                            &'r mut AsyncReaderImpl<R>,
                            ByteRecord,
                        ),
                    > + 'r,
            >,
        >,
    >,
}

impl<'r, R> ByteRecordsStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin + 'r,
{
    fn new(rdr: &'r mut AsyncReaderImpl<R>) -> Self {
        Self {
            fut: Some(Pin::from(Box::new(read_byte_record_borrowed(
                rdr,
                ByteRecord::new(),
            )))),
        }
    }
}

impl<'r, R> Stream for ByteRecordsStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin,
{
    type Item = Result<ByteRecord>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        match self.fut.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready((result, rdr, rec)) => {
                if result.is_some() {
                    self.fut = Some(Pin::from(Box::new(
                        read_byte_record_borrowed(rdr, rec),
                    )));
                } else {
                    self.fut = None;
                }

                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

async fn read_byte_record<R>(
    mut rdr: AsyncReaderImpl<R>,
    mut rec: ByteRecord,
) -> (Option<Result<ByteRecord>>, AsyncReaderImpl<R>, ByteRecord)
where
    R: io::AsyncRead + std::marker::Unpin
{
    let result = match rdr.read_byte_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(Ok(rec.clone())),
        Ok(false) => None,
    };

    (result, rdr, rec)
}

/// An owned stream of records as raw bytes.
pub struct ByteRecordsIntoStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin
{
    fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Option<Result<ByteRecord>>,
                        AsyncReaderImpl<R>,
                        ByteRecord,
                    ),
                > + 'r,
            >,
        >,
    >,
}

impl<'r, R> ByteRecordsIntoStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    fn new(rdr: AsyncReaderImpl<R>) -> Self {
        Self {
            fut: Some(Pin::from(Box::new(read_byte_record(
                rdr,
                ByteRecord::new(),
            )))),
        }
    }
}

impl<'r, R> Stream for ByteRecordsIntoStream<'r, R>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    type Item = Result<ByteRecord>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        match self.fut.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready((result, rdr, rec)) => {
                if result.is_some() {
                    self.fut =
                        Some(Pin::from(Box::new(read_byte_record(rdr, rec))));
                } else {
                    self.fut = None;
                }

                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

cfg_if::cfg_if! {
if #[cfg(feature = "with_serde")] {
    
async fn deserialize_record_borrowed<'r, R, D: DeserializeOwned>(
    rdr: &'r mut AsyncReaderImpl<R>,
    headers: Option<StringRecord>,
    mut rec: StringRecord,
) -> (Option<Result<D>>, &'r mut AsyncReaderImpl<R>, Option<StringRecord>, StringRecord)
where
    R: io::AsyncRead + std::marker::Unpin
{
    let result = match rdr.read_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(rec.deserialize(headers.as_ref())),
        Ok(false) => None,
    };

    (result, rdr, headers, rec)
}

/// A borrowed stream of deserialized records.
///
/// The lifetime parameter `'r` refers to the lifetime of the underlying CSV `Reader`.
/// type, and `D` refers to the type that this stream will deserialize a record into.
pub struct DeserializeRecordsStream<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    header_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Result<StringRecord>,
                        &'r mut AsyncReaderImpl<R>,
                    )
                > + 'r,
            >,
        >,
    >,
    rec_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Option<Result<D>>,
                        &'r mut AsyncReaderImpl<R>,
                        Option<StringRecord>,
                        StringRecord,
                    )
                > + 'r,
            >,
        >,
    >,
}

impl<'r, R, D: DeserializeOwned + 'r> DeserializeRecordsStream<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    fn new(rdr: &'r mut AsyncReaderImpl<R>) -> Self {
        let has_headers = rdr.has_headers();
        if has_headers {
            Self {
                header_fut: Some(Pin::from(Box::new(
                    async{ (rdr.headers().await.and_then(|h| Ok(h.clone())), rdr) }
                ))),
                rec_fut: None,
            }
        } else {
            Self {
                header_fut: None,
                rec_fut: Some(Pin::from(Box::new(
                    deserialize_record_borrowed(rdr, None, StringRecord::new())
                ))),
            }
        }
    }
}

impl<'r, R, D: DeserializeOwned + 'r> Stream for DeserializeRecordsStream<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    type Item = Result<D>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        if let Some(header_fut) = &mut self.header_fut {
            match header_fut.as_mut().poll(cx) {
                Poll::Ready((Ok(headers), rdr)) => {
                    self.header_fut = None;
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record_borrowed(rdr, Some(headers), StringRecord::new()),
                    )));
                    cx.waker().clone().wake();
                    Poll::Pending
                },
                Poll::Ready((Err(err), rdr)) => {
                    self.header_fut = None;
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record_borrowed(rdr, None, StringRecord::new()),
                    )));
                    Poll::Ready(Some(Err(err)))
                },
                Poll::Pending => Poll::Pending,
            }
        } else {
            match self.rec_fut.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready((result, rdr, headers, rec)) => {
                    if result.is_some() {
                        self.rec_fut = Some(Pin::from(Box::new(
                            deserialize_record_borrowed(rdr, headers, rec),
                        )));
                    } else {
                        self.rec_fut = None;
                    }
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

async fn deserialize_record_with_pos_borrowed<'r, R, D: DeserializeOwned>(
    rdr: &'r mut AsyncReaderImpl<R>,
    headers: Option<StringRecord>,
    mut rec: StringRecord,
) -> (Option<Result<D>>, Position, &'r mut AsyncReaderImpl<R>, Option<StringRecord>, StringRecord)
where
    R: io::AsyncRead + std::marker::Unpin
{
    let pos = rdr.position().clone();
    let result = match rdr.read_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(rec.deserialize(headers.as_ref())),
        Ok(false) => None,
    };

    (result, pos, rdr, headers, rec)
}

/// A borrowed stream of pairs: deserialized records and position in stream before reading record.
///
/// The lifetime parameter `'r` refers to the lifetime of the underlying CSV `Reader`.
/// type, and `D` refers to the type that this stream will deserialize a record into.
pub struct DeserializeRecordsStreamPos<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    header_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Result<StringRecord>,
                        &'r mut AsyncReaderImpl<R>,
                    )
                > + 'r,
            >,
        >,
    >,
    rec_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Option<Result<D>>,
                        Position,
                        &'r mut AsyncReaderImpl<R>,
                        Option<StringRecord>,
                        StringRecord,
                    )
                > + 'r,
            >,
        >,
    >,
}

impl<'r, R, D: DeserializeOwned + 'r> DeserializeRecordsStreamPos<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    fn new(rdr: &'r mut AsyncReaderImpl<R>) -> Self {
        let has_headers = rdr.has_headers();
        if has_headers {
            Self {
                header_fut: Some(Pin::from(Box::new(
                    async{ (rdr.headers().await.and_then(|h| Ok(h.clone())), rdr) }
                ))),
                rec_fut: None,
            }
        } else {
            Self {
                header_fut: None,
                rec_fut: Some(Pin::from(Box::new(
                    deserialize_record_with_pos_borrowed(rdr, None, StringRecord::new())
                ))),
            }
        }
    }
}

impl<'r, R, D: DeserializeOwned + 'r> Stream for DeserializeRecordsStreamPos<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    type Item = (Result<D>, Position);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        if let Some(header_fut) = &mut self.header_fut {
            match header_fut.as_mut().poll(cx) {
                Poll::Ready((Ok(headers), rdr)) => {
                    self.header_fut = None;
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record_with_pos_borrowed(rdr, Some(headers), StringRecord::new()),
                    )));
                    cx.waker().clone().wake();
                    Poll::Pending
                },
                Poll::Ready((Err(err), rdr)) => {
                    self.header_fut = None;
                    let pos = rdr.position().clone();
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record_with_pos_borrowed(rdr, None, StringRecord::new()),
                    )));
                    Poll::Ready(Some((Err(err), pos)))
                },
                Poll::Pending => Poll::Pending,
            }
        } else {
            match self.rec_fut.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready((result, pos, rdr, headers, rec)) => {
                    if let Some(result) = result {
                        self.rec_fut = Some(Pin::from(Box::new(
                            deserialize_record_with_pos_borrowed(rdr, headers, rec),
                        )));
                        Poll::Ready(Some((result, pos)))
                    } else {
                        self.rec_fut = None;
                        Poll::Ready(None)
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
    
//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

async fn deserialize_record<R, D: DeserializeOwned>(
    mut rdr: AsyncReaderImpl<R>,
    headers: Option<StringRecord>,
    mut rec: StringRecord,
) -> (Option<Result<D>>, AsyncReaderImpl<R>, Option<StringRecord>, StringRecord)
where
    R: io::AsyncRead + std::marker::Unpin
{
    let result = match rdr.read_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(rec.deserialize(headers.as_ref())),
        Ok(false) => None,
    };

    (result, rdr, headers, rec)
}

/// A owned stream of deserialized records.
///
/// The lifetime parameter `'r` refers to the lifetime of the underlying CSV `Reader`.
/// type, and `D` refers to the type that this stream will deserialize a record into.
pub struct DeserializeRecordsIntoStream<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    header_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Result<StringRecord>,
                        AsyncReaderImpl<R>,
                    )
                > + 'r,
            >,
        >,
    >,
    rec_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Option<Result<D>>,
                        AsyncReaderImpl<R>,
                        Option<StringRecord>,
                        StringRecord,
                    )
                > + 'r,
            >,
        >,
    >,
}

impl<'r, R, D: DeserializeOwned + 'r> DeserializeRecordsIntoStream<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    fn new(mut rdr: AsyncReaderImpl<R>) -> Self {
        let has_headers = rdr.has_headers();
        if has_headers {
            Self {
                header_fut: Some(Pin::from(Box::new(
                    async{ (rdr.headers().await.and_then(|h| Ok(h.clone())), rdr) }
                ))),
                rec_fut: None,
            }
        } else {
            Self {
                header_fut: None,
                rec_fut: Some(Pin::from(Box::new(
                    deserialize_record(rdr, None, StringRecord::new())
                ))),
            }
        }
    }
}

impl<'r, R, D: DeserializeOwned + 'r> Stream for DeserializeRecordsIntoStream<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    type Item = Result<D>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        if let Some(header_fut) = &mut self.header_fut {
            match header_fut.as_mut().poll(cx) {
                Poll::Ready((Ok(headers), rdr)) => {
                    self.header_fut = None;
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record(rdr, Some(headers), StringRecord::new()),
                    )));
                    cx.waker().clone().wake();
                    Poll::Pending
                },
                Poll::Ready((Err(err), rdr)) => {
                    self.header_fut = None;
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record(rdr, None, StringRecord::new()),
                    )));
                    Poll::Ready(Some(Err(err)))
                },
                Poll::Pending => Poll::Pending,
            }
        } else {
            match self.rec_fut.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready((result, rdr, headers, rec)) => {
                    if result.is_some() {
                        self.rec_fut = Some(Pin::from(Box::new(
                            deserialize_record(rdr, headers, rec),
                        )));
                    } else {
                        self.rec_fut = None;
                    }
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

//-//////////////////////////////////////////////////////////////////////////////////////////////
//-//////////////////////////////////////////////////////////////////////////////////////////////

async fn deserialize_record_with_pos<R, D: DeserializeOwned>(
    mut rdr: AsyncReaderImpl<R>,
    headers: Option<StringRecord>,
    mut rec: StringRecord,
) -> (Option<Result<D>>, Position, AsyncReaderImpl<R>, Option<StringRecord>, StringRecord)
where
    R: io::AsyncRead + std::marker::Unpin
{
    let pos = rdr.position().clone();
    let result = match rdr.read_record(&mut rec).await {
        Err(err) => Some(Err(err)),
        Ok(true) => Some(rec.deserialize(headers.as_ref())),
        Ok(false) => None,
    };

    (result, pos, rdr, headers, rec)
}

/// A owned stream of pairs: deserialized records and position in stream before reading record.
///
/// The lifetime parameter `'r` refers to the lifetime of the underlying CSV `Reader`.
/// type, and `D` refers to the type that this stream will deserialize a record into.
pub struct DeserializeRecordsIntoStreamPos<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin
{
    header_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Result<StringRecord>,
                        AsyncReaderImpl<R>,
                    )
                > + 'r,
            >,
        >,
    >,
    rec_fut: Option<
        Pin<
            Box<
                dyn Future<
                    Output = (
                        Option<Result<D>>,
                        Position,
                        AsyncReaderImpl<R>,
                        Option<StringRecord>,
                        StringRecord,
                    )
                > + 'r,
            >,
        >,
    >,
}

impl<'r, R, D: DeserializeOwned + 'r> DeserializeRecordsIntoStreamPos<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    fn new(mut rdr: AsyncReaderImpl<R>) -> Self {
        let has_headers = rdr.has_headers();
        if has_headers {
            Self {
                header_fut: Some(Pin::from(Box::new(
                    async{ (rdr.headers().await.and_then(|h| Ok(h.clone())), rdr) }
                ))),
                rec_fut: None,
            }
        } else {
            Self {
                header_fut: None,
                rec_fut: Some(Pin::from(Box::new(
                    deserialize_record_with_pos(rdr, None, StringRecord::new())
                ))),
            }
        }
    }
}

impl<'r, R, D: DeserializeOwned + 'r> Stream for DeserializeRecordsIntoStreamPos<'r, R, D>
where
    R: io::AsyncRead + std::marker::Unpin + 'r
{
    type Item = (Result<D>, Position);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        if let Some(header_fut) = &mut self.header_fut {
            match header_fut.as_mut().poll(cx) {
                Poll::Ready((Ok(headers), rdr)) => {
                    self.header_fut = None;
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record_with_pos(rdr, Some(headers), StringRecord::new()),
                    )));
                    cx.waker().clone().wake();
                    Poll::Pending
                },
                Poll::Ready((Err(err), rdr)) => {
                    self.header_fut = None;
                    let pos = rdr.position().clone();
                    self.rec_fut = Some(Pin::from(Box::new(
                        deserialize_record_with_pos(rdr, None, StringRecord::new()),
                    )));
                    Poll::Ready(Some((Err(err), pos)))
                },
                Poll::Pending => Poll::Pending,
            }
        } else {
            match self.rec_fut.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready((result, pos, rdr, headers, rec)) => {
                    if let Some(result) = result {
                        self.rec_fut = Some(Pin::from(Box::new(
                            deserialize_record_with_pos(rdr, headers, rec),
                        )));
                        Poll::Ready(Some((result, pos)))
                    } else {
                        self.rec_fut = None;
                        Poll::Ready(None)
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

}} // fi #[cfg(feature = "with_serde")]

