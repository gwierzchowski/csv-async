use csv_core::{ReaderBuilder as CoreReaderBuilder};

use crate::{Terminator, Trim};

/// Builds a CSV reader with various configuration knobs.
///
/// This builder can be used to tweak the field delimiter, record terminator
/// and more. Once a CSV `AsyncReader` is built, its configuration cannot be
/// changed.
#[derive(Debug)]
pub struct AsyncReaderBuilder {
    capacity: usize,
    flexible: bool,
    has_headers: bool,
    trim: Trim,
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
    pub fn get_trim_option(&self) -> Trim {
        self.trim
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
