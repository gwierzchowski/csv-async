use csv_core::WriterBuilder as CoreWriterBuilder;

use crate::{QuoteStyle, Terminator};

/// Builds a CSV writer with various configuration knobs.
///
/// This builder can be used to tweak the field delimiter, record terminator
/// and more. Once a CSV `AsyncWriter` is built, its configuration cannot be
/// changed.
#[derive(Debug)]
pub struct AsyncWriterBuilder {
    builder: CoreWriterBuilder,
    capacity: usize,
    flexible: bool,
}

impl Default for AsyncWriterBuilder {
    fn default() -> AsyncWriterBuilder {
        AsyncWriterBuilder {
            builder: CoreWriterBuilder::default(),
            capacity: 8 * (1 << 10),
            flexible: false,
        }
    }
}

impl AsyncWriterBuilder {
    /// Create a new builder for configuring CSV writing.
    ///
    /// To convert a builder into a writer, call one of the methods starting
    /// with `from_`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
    ///     wtr.write_record(&["a", "b", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn new() -> AsyncWriterBuilder {
        AsyncWriterBuilder::default()
    }
    
    /// Returns csv_core Builder reference.
    pub fn get_core_builder_ref(&self) -> &CoreWriterBuilder {
        &self.builder
    }

    /// The field delimiter to use when writing CSV.
    ///
    /// The default is `b','`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .delimiter(b';')
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "b", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a;b;c\nx;y;z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn delimiter(&mut self, delimiter: u8) -> &mut AsyncWriterBuilder {
        self.builder.delimiter(delimiter);
        self
    }
    
    /// Returns information if read file has headers.
    pub fn is_flexible(&self) -> bool {
        self.flexible
    }

    /// Whether the number of fields in records is allowed to change or not.
    ///
    /// When disabled (which is the default), writing CSV data will return an
    /// error if a record is written with a number of fields different from the
    /// number of fields written in a previous record.
    ///
    /// When enabled, this error checking is turned off.
    ///
    /// # Example: writing flexible records
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .flexible(true)
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "b"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Example: error when `flexible` is disabled
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .flexible(false)
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "b"]).await?;
    ///     let err = wtr.write_record(&["x", "y", "z"]).await.unwrap_err();
    ///     match *err.kind() {
    ///         csv_async::ErrorKind::UnequalLengths { expected_len, len, .. } => {
    ///             assert_eq!(expected_len, 2);
    ///             assert_eq!(len, 3);
    ///         }
    ///         ref wrong => {
    ///             panic!("expected UnequalLengths but got {:?}", wrong);
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn flexible(&mut self, yes: bool) -> &mut AsyncWriterBuilder {
        self.flexible = yes;
        self
    }

    /// The record terminator to use when writing CSV.
    ///
    /// A record terminator can be any single byte. The default is `\n`.
    ///
    /// Note that RFC 4180 specifies that record terminators should be `\r\n`.
    /// To use `\r\n`, use the special `Terminator::CRLF` value.
    ///
    /// # Example: CRLF
    ///
    /// This shows how to use RFC 4180 compliant record terminators.
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{Terminator, AsyncWriterBuilder};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .terminator(Terminator::CRLF)
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "b", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b,c\r\nx,y,z\r\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn terminator(&mut self, term: Terminator) -> &mut AsyncWriterBuilder {
        self.builder.terminator(term.to_core());
        self
    }

    /// The quoting style to use when writing CSV.
    ///
    /// By default, this is set to `QuoteStyle::Necessary`, which will only
    /// use quotes when they are necessary to preserve the integrity of data.
    ///
    /// Note that unless the quote style is set to `Never`, an empty field is
    /// quoted if it is the only field in a record.
    ///
    /// # Example: non-numeric quoting
    ///
    /// This shows how to quote non-numeric fields only.
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{QuoteStyle, AsyncWriterBuilder};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .quote_style(QuoteStyle::NonNumeric)
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "5", "c"]).await?;
    ///     wtr.write_record(&["3.14", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "\"a\",5,\"c\"\n3.14,\"y\",\"z\"\n");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Example: never quote
    ///
    /// This shows how the CSV writer can be made to never write quotes, even
    /// if it sacrifices the integrity of the data.
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{QuoteStyle, AsyncWriterBuilder};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .quote_style(QuoteStyle::Never)
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "foo\nbar", "c"]).await?;
    ///     wtr.write_record(&["g\"h\"i", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,foo\nbar,c\ng\"h\"i,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn quote_style(&mut self, style: QuoteStyle) -> &mut AsyncWriterBuilder {
        self.builder.quote_style(style.to_core());
        self
    }

    /// The quote character to use when writing CSV.
    ///
    /// The default is `b'"'`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .quote(b'\'')
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "foo\nbar", "c"]).await?;
    ///     wtr.write_record(&["g'h'i", "y\"y\"y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,'foo\nbar',c\n'g''h''i',y\"y\"y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn quote(&mut self, quote: u8) -> &mut AsyncWriterBuilder {
        self.builder.quote(quote);
        self
    }

    /// Enable double quote escapes.
    ///
    /// This is enabled by default, but it may be disabled. When disabled,
    /// quotes in field data are escaped instead of doubled.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .double_quote(false)
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "foo\"bar", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,\"foo\\\"bar\",c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn double_quote(&mut self, yes: bool) -> &mut AsyncWriterBuilder {
        self.builder.double_quote(yes);
        self
    }

    /// The escape character to use when writing CSV.
    ///
    /// In some variants of CSV, quotes are escaped using a special escape
    /// character like `\` (instead of escaping quotes by doubling them).
    ///
    /// By default, writing these idiosyncratic escapes is disabled, and is
    /// only used when `double_quote` is disabled.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new()
    ///         .double_quote(false)
    ///         .escape(b'$')
    ///         .from_writer(vec![]);
    ///     wtr.write_record(&["a", "foo\"bar", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,\"foo$\"bar\",c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn escape(&mut self, escape: u8) -> &mut AsyncWriterBuilder {
        self.builder.escape(escape);
        self
    }
    
    /// Returns buffer capacity.
    pub fn get_buffer_capacity(&self) -> usize {
        self.capacity
    }

    /// Set the capacity (in bytes) of the internal buffer used in the CSV
    /// writer. This defaults to a reasonable setting.
    pub fn buffer_capacity(&mut self, capacity: usize) -> &mut AsyncWriterBuilder {
        self.capacity = capacity;
        self
    }
}
