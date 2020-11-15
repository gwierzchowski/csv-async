use std::result;

use futures::io::{self, AsyncWrite, AsyncWriteExt};
use serde::Serialize;

use crate::AsyncWriterBuilder;
use crate::error::{IntoInnerError, Result};
use super::mwtr_serde::MemWriter;

impl AsyncWriterBuilder {
    /// Build a CSV `serde` serializer from this configuration that writes data to `ser`.
    ///
    /// Note that the CSV serializer is buffered automatically, so you should not
    /// wrap `ser` in a buffered writer.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row<'a> {
    ///     name: &'a str,
    ///     x: u64,
    ///     y: u64,
    /// }
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut ser = AsyncWriterBuilder::new().has_headers(false).create_serializer(vec![]);
    ///     ser.serialize(Row {name: "p1", x: 1, y: 2}).await?;
    ///     ser.serialize(Row {name: "p2", x: 3, y: 4}).await?;
    ///
    ///     let data = String::from_utf8(ser.into_inner().await?)?;
    ///     assert_eq!(data, "p1,1,2\np2,3,4\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn create_serializer<W: AsyncWrite + Unpin>(&self, wtr: W) -> AsyncSerializer<W> {
        AsyncSerializer::new(self, wtr)
    }
}

/// A already configured CSV `serde` serializer.
///
/// A CSV serializer takes as input Rust structures that implement `serde::Serialize` trait
/// and writes those data in a valid CSV output.
///
/// While CSV writing is considerably easier than parsing CSV, a proper writer
/// will do a number of things for you:
///
/// 1. Quote fields when necessary.
/// 2. Check that all records have the same number of fields.
/// 3. Write records with a single empty field correctly.
/// 4. Automatically serialize normal Rust types to CSV records. When that
///    type is a struct, a header row is automatically written corresponding
///    to the fields of that struct.
/// 5. Use buffering intelligently and otherwise avoid allocation. (This means
///    that callers should not do their own buffering.)
///
/// All of the above can be configured using a
/// [`AsyncWriterBuilder`](struct.AsyncWriterBuilder.html).
/// However, a `AsyncSerializer` has convenient constructor (`from_writer`) 
/// that use the default configuration.
///
/// Note that the default configuration of a `AsyncSerializer` uses `\n` for record
/// terminators instead of `\r\n` as specified by RFC 4180. Use the
/// `terminator` method on `AsyncWriterBuilder` to set the terminator to `\r\n` if
/// it's desired.
#[derive(Debug)]
pub struct AsyncSerializer<W: AsyncWrite + Unpin> {
    ser_wtr: MemWriter,
    asy_wtr: Option<W>,
}

impl<W: AsyncWrite + Unpin> Drop for AsyncSerializer<W> {
    fn drop(&mut self) {
        // We ignore result of flush() call while dropping
        // Well known problem.
        // If you care about flush result call it explicitly 
        // before AsyncSerializer goes out of scope,
        // second flush() call should be no op.
        let _ = futures::executor::block_on(self.flush());
    }
}

impl<W: AsyncWrite + Unpin> AsyncSerializer<W> {
    fn new(builder: &AsyncWriterBuilder, wtr: W) -> Self {
        AsyncSerializer {
            ser_wtr: MemWriter::new(builder),
            asy_wtr: Some(wtr),
        }
    }

    /// Build a CSV serializer with a default configuration that writes data to
    /// `ser`.
    ///
    /// Note that the CSV serializer is buffered automatically, so you should not
    /// wrap `ser` in a buffered writer.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncSerializer;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row<'a> {
    ///     name: &'a str,
    ///     x: u64,
    ///     y: u64,
    /// }
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut ser = AsyncSerializer::from_writer(vec![]);
    ///     ser.serialize(Row {name: "p1", x: 1, y: 2}).await?;
    ///     ser.serialize(Row {name: "p2", x: 3, y: 4}).await?;
    ///
    ///     let data = String::from_utf8(ser.into_inner().await?)?;
    ///     assert_eq!(data, "name,x,y\np1,1,2\np2,3,4\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn from_writer(wtr: W) -> AsyncSerializer<W> {
        AsyncWriterBuilder::new().create_serializer(wtr)
    }

    /// Serialize a single record using Serde.
    ///
    /// # Example
    ///
    /// This shows how to serialize normal Rust structs as CSV records. The
    /// fields of the struct are used to write a header row automatically.
    /// (Writing the header row automatically can be disabled by building the
    /// CSV writer with a [`WriterBuilder`](struct.WriterBuilder.html) and
    /// calling the `has_headers` method.)
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncSerializer;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row<'a> {
    ///     city: &'a str,
    ///     country: &'a str,
    ///     // Serde allows us to name our headers exactly,
    ///     // even if they don't match our struct field names.
    ///     #[serde(rename = "popcount")]
    ///     population: u64,
    /// }
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut ser = AsyncSerializer::from_writer(vec![]);
    ///     ser.serialize(Row {
    ///         city: "Boston",
    ///         country: "United States",
    ///         population: 4628910,
    ///     }).await?;
    ///     ser.serialize(Row {
    ///         city: "Concord",
    ///         country: "United States",
    ///         population: 42695,
    ///     }).await?;
    ///
    ///     let data = String::from_utf8(ser.into_inner().await?)?;
    ///     assert_eq!(data, indoc::indoc! {"
    ///         city,country,popcount
    ///         Boston,United States,4628910
    ///         Concord,United States,42695
    ///     "});
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Rules
    ///
    /// The behavior of `serialize` is fairly simple:
    ///
    /// 1. Nested containers (tuples, `Vec`s, structs, etc.) are always
    ///    flattened (depth-first order).
    ///
    /// 2. If `has_headers` is `true` and the type contains field names, then
    ///    a header row is automatically generated.
    ///
    /// However, some container types cannot be serialized, and if
    /// `has_headers` is `true`, there are some additional restrictions on the
    /// types that can be serialized. See below for details.
    ///
    /// For the purpose of this section, Rust types can be divided into three
    /// categories: scalars, non-struct containers, and structs.
    ///
    /// ## Scalars
    ///
    /// Single values with no field names are written like the following. Note
    /// that some of the outputs may be quoted, according to the selected
    /// quoting style.
    ///
    /// | Name | Example Type | Example Value | Output |
    /// | ---- | ---- | ---- | ---- |
    /// | boolean | `bool` | `true` | `true` |
    /// | integers | `i8`, `i16`, `i32`, `i64`, `i128`, `u8`, `u16`, `u32`, `u64`, `u128` | `5` | `5` |
    /// | floats | `f32`, `f64` | `3.14` | `3.14` |
    /// | character | `char` | `'☃'` | `☃` |
    /// | string | `&str` | `"hi"` | `hi` |
    /// | bytes | `&[u8]` | `b"hi"[..]` | `hi` |
    /// | option | `Option` | `None` | *empty* |
    /// | option |          | `Some(5)` | `5` |
    /// | unit | `()` | `()` | *empty* |
    /// | unit struct | `struct Foo;` | `Foo` | `Foo` |
    /// | unit enum variant | `enum E { A, B }` | `E::A` | `A` |
    /// | newtype struct | `struct Foo(u8);` | `Foo(5)` | `5` |
    /// | newtype enum variant | `enum E { A(u8) }` | `E::A(5)` | `5` |
    ///
    /// Note that this table includes simple structs and enums. For example, to
    /// serialize a field from either an integer or a float type, one can do
    /// this:
    ///
    /// ```
    /// use std::error::Error;
    ///
    /// use csv_async::AsyncSerializer;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row {
    ///     label: String,
    ///     value: Value,
    /// }
    ///
    /// #[derive(Serialize)]
    /// enum Value {
    ///     Integer(i64),
    ///     Float(f64),
    /// }
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut ser = AsyncSerializer::from_writer(vec![]);
    ///     ser.serialize(Row {
    ///         label: "foo".to_string(),
    ///         value: Value::Integer(3),
    ///     }).await?;
    ///     ser.serialize(Row {
    ///         label: "bar".to_string(),
    ///         value: Value::Float(3.14),
    ///     }).await?;
    ///
    ///     let data = String::from_utf8(ser.into_inner().await?)?;
    ///     assert_eq!(data, indoc::indoc! {"
    ///         label,value
    ///         foo,3
    ///         bar,3.14
    ///     "});
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Non-Struct Containers
    ///
    /// Nested containers are flattened to their scalar components, with the
    /// exeption of a few types that are not allowed:
    ///
    /// | Name | Example Type | Example Value | Output |
    /// | ---- | ---- | ---- | ---- |
    /// | sequence | `Vec<u8>` | `vec![1, 2, 3]` | `1,2,3` |
    /// | tuple | `(u8, bool)` | `(5, true)` | `5,true` |
    /// | tuple struct | `Foo(u8, bool)` | `Foo(5, true)` | `5,true` |
    /// | tuple enum variant | `enum E { A(u8, bool) }` | `E::A(5, true)` | *error* |
    /// | struct enum variant | `enum E { V { a: u8, b: bool } }` | `E::V { a: 5, b: true }` | *error* |
    /// | map | `BTreeMap<K, V>` | `BTreeMap::new()` | *error* |
    ///
    /// ## Structs
    ///
    /// Like the other containers, structs are flattened to their scalar
    /// components:
    ///
    /// | Name | Example Type | Example Value | Output |
    /// | ---- | ---- | ---- | ---- |
    /// | struct | `struct Foo { a: u8, b: bool }` | `Foo { a: 5, b: true }` | `5,true` |
    ///
    /// If `has_headers` is `false`, then there are no additional restrictions;
    /// types can be nested arbitrarily. For example:
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row {
    ///     label: String,
    ///     values: Vec<f64>,
    /// }
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut ser = AsyncWriterBuilder::new()
    ///         .has_headers(false)
    ///         .create_serializer(vec![]);
    ///     ser.serialize(Row {
    ///         label: "foo".to_string(),
    ///         values: vec![1.1234, 2.5678, 3.14],
    ///     }).await?;
    ///
    ///     let data = String::from_utf8(ser.into_inner().await?)?;
    ///     assert_eq!(data, indoc::indoc! {"
    ///         foo,1.1234,2.5678,3.14
    ///     "});
    ///     Ok(())
    /// }
    /// ```
    ///
    /// However, if `has_headers` were enabled in the above example, then
    /// serialization would return an error. Specifically, when `has_headers` is
    /// `true`, there are two restrictions:
    ///
    /// 1. Named field values in structs must be scalars.
    ///
    /// 2. All scalars must be named field values in structs.
    ///
    /// Other than these two restrictions, types can be nested arbitrarily.
    /// Here are a few examples:
    ///
    /// | Value | Header | Record |
    /// | ---- | ---- | ---- |
    /// | `(Foo { x: 5, y: 6 }, Bar { z: true })` | `x,y,z` | `5,6,true` |
    /// | `vec![Foo { x: 5, y: 6 }, Foo { x: 7, y: 8 }]` | `x,y,x,y` | `5,6,7,8` |
    /// | `(Foo { x: 5, y: 6 }, vec![Bar { z: Baz(true) }])` | `x,y,z` | `5,6,true` |
    /// | `Foo { x: 5, y: (6, 7) }` | *error: restriction 1* | `5,6,7` |
    /// | `(5, Foo { x: 6, y: 7 }` | *error: restriction 2* | `5,6,7` |
    /// | `(Foo { x: 5, y: 6 }, true)` | *error: restriction 2* | `5,6,true` |
    pub async fn serialize<S: Serialize>(&mut self, record: S) -> Result<()> {
        self.ser_wtr.serialize(record)?;
        self.ser_wtr.flush()?;
        self.asy_wtr.as_mut().unwrap().write_all(self.ser_wtr.data()).await?;
        self.ser_wtr.clear();
        Ok(())
    }

    /// Flushes the underlying asynchronous writer.
    pub async fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut asy_wtr) = self.asy_wtr {
            asy_wtr.flush().await?;
        }
        Ok(())
    }

    /// Flush the contents of the internal buffer and return the underlying
    /// writer.
    pub async fn into_inner(
        mut self,
    ) -> result::Result<W, IntoInnerError<AsyncSerializer<W>>> {
        match self.flush().await {
            Ok(()) => Ok(self.asy_wtr.take().unwrap()),
            Err(err) => Err(IntoInnerError::new(self, err)),
        }
    }
}
