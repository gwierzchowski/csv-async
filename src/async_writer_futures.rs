use std::result;

use csv_core::{self, WriteResult, Writer as CoreWriter};
use futures::io::{self, AsyncWrite, AsyncWriteExt};

use crate::AsyncWriterBuilder;
use crate::byte_record::ByteRecord;
use crate::error::{Error, ErrorKind, IntoInnerError, Result};


impl AsyncWriterBuilder {
    /// Build a CSV writer from this configuration that writes data to `wtr`.
    ///
    /// Note that the CSV writer is buffered automatically, so you should not
    /// wrap `wtr` in a buffered writer like `io::BufWriter`.
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
    pub fn from_writer<W: AsyncWrite + Unpin>(&self, wtr: W) -> AsyncWriter<W> {
        AsyncWriter::new(self, wtr)
    }
}

/// A already configured CSV writer.
///
/// A CSV writer takes as input Rust values and writes those values in a valid
/// CSV format as output.
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
/// However, a `AsyncWriter` has a couple of convenience constructors (`from_path`
/// and `from_writer`) that use the default configuration.
///
/// Note that the default configuration of a `AsyncWriter` uses `\n` for record
/// terminators instead of `\r\n` as specified by RFC 4180. Use the
/// `terminator` method on `AsyncWriterBuilder` to set the terminator to `\r\n` if
/// it's desired.
#[derive(Debug)]
pub struct AsyncWriter<W: AsyncWrite + Unpin> {
    core: CoreWriter,
    wtr: Option<W>,
    buf: Buffer,
    state: WriterState,
}

#[derive(Debug)]
struct WriterState {
    /// Whether inconsistent record lengths are allowed.
    flexible: bool,
    /// The number of fields writtein in the first record. This is compared
    /// with `fields_written` on all subsequent records to check for
    /// inconsistent record lengths.
    first_field_count: Option<u64>,
    /// The number of fields written in this record. This is used to report
    /// errors for inconsistent record lengths if `flexible` is disabled.
    fields_written: u64,
    /// This is set immediately before flushing the buffer and then unset
    /// immediately after flushing the buffer. This avoids flushing the buffer
    /// twice if the inner writer panics.
    panicked: bool,
}

/// A simple internal buffer for buffering writes.
///
/// We need this because the `csv_core` APIs want to write into a `&mut [u8]`,
/// which is not available with the `std::io::BufWriter` API.
#[derive(Debug)]
struct Buffer {
    /// The contents of the buffer.
    buf: Vec<u8>,
    /// The number of bytes written to the buffer.
    len: usize,
}

impl<W: AsyncWrite + Unpin> Drop for AsyncWriter<W> {
    fn drop(&mut self) {
        if self.wtr.is_some() && !self.state.panicked {
            // We ignore result of flush() call while dropping
            // Well known problem.
            // If you care about flush result call it explicitly 
            // before AsyncWriter goes out of scope,
            // second flush() call should be no op.
            let _ = futures::executor::block_on(self.flush());
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWriter<W> {
    fn new(builder: &AsyncWriterBuilder, wtr: W) -> AsyncWriter<W> {
        AsyncWriter {
            core: builder.get_core_builder_ref().build(),
            wtr: Some(wtr),
            buf: Buffer { buf: vec![0; builder.get_buffer_capacity()], len: 0 },
            state: WriterState {
                flexible: builder.is_flexible(),
                first_field_count: None,
                fields_written: 0,
                panicked: false,
            },
        }
    }

    /// Build a CSV writer with a default configuration that writes data to
    /// `wtr`.
    ///
    /// Note that the CSV writer is buffered automatically, so you should not
    /// wrap `wtr` in a buffered writer like `io::BufWriter`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriter;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriter::from_writer(vec![]);
    ///     wtr.write_record(&["a", "b", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn from_writer(wtr: W) -> AsyncWriter<W> {
        AsyncWriterBuilder::new().from_writer(wtr)
    }

    /// Write a single record.
    ///
    /// This method accepts something that can be turned into an iterator that
    /// yields elements that can be represented by a `&[u8]`.
    ///
    /// This may be called with an empty iterator, which will cause a record
    /// terminator to be written. If no fields had been written, then a single
    /// empty field is written before the terminator.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriter;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriter::from_writer(vec![]);
    ///     wtr.write_record(&["a", "b", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub async fn write_record<I, T>(&mut self, record: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        for field in record.into_iter() {
            self.write_field_impl(field).await?;
        }
        self.write_terminator().await
    }

    /// Write a single `ByteRecord`.
    ///
    /// This method accepts a borrowed `ByteRecord` and writes its contents
    /// to the underlying writer.
    ///
    /// This is similar to `write_record` except that it specifically requires
    /// a `ByteRecord`. This permits the writer to possibly write the record
    /// more quickly than the more generic `write_record`.
    ///
    /// This may be called with an empty record, which will cause a record
    /// terminator to be written. If no fields had been written, then a single
    /// empty field is written before the terminator.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::{ByteRecord, AsyncWriter};
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriter::from_writer(vec![]);
    ///     wtr.write_byte_record(&ByteRecord::from(&["a", "b", "c"][..])).await?;
    ///     wtr.write_byte_record(&ByteRecord::from(&["x", "y", "z"][..])).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    #[inline(never)]
    pub async fn write_byte_record(&mut self, record: &ByteRecord) -> Result<()> {
        if record.as_slice().is_empty() {
            return self.write_record(record).await;
        }
        // The idea here is to find a fast path for shuffling our record into
        // our buffer as quickly as possible. We do this because the underlying
        // "core" CSV writer does a lot of book-keeping to maintain its state
        // oriented API.
        //
        // The fast path occurs when we know our record will fit in whatever
        // space we have left in our buffer. We can actually quickly compute
        // the upper bound on the space required:
        let upper_bound =
            // The data itself plus the worst case: every byte is a quote.
            (2 * record.as_slice().len())
            // The number of field delimiters.
            + (record.len().saturating_sub(1))
            // The maximum number of quotes inserted around each field.
            + (2 * record.len())
            // The maximum number of bytes for the terminator.
            + 2;
        if self.buf.writable().len() < upper_bound {
            return self.write_record(record).await;
        }
        let mut first = true;
        for field in record.iter() {
            if !first {
                self.buf.writable()[0] = self.core.get_delimiter();
                self.buf.written(1);
            }
            first = false;

            if !self.core.should_quote(field) {
                self.buf.writable()[..field.len()].copy_from_slice(field);
                self.buf.written(field.len());
            } else {
                self.buf.writable()[0] = self.core.get_quote();
                self.buf.written(1);
                let (res, nin, nout) = csv_core::quote(
                    field,
                    self.buf.writable(),
                    self.core.get_quote(),
                    self.core.get_escape(),
                    self.core.get_double_quote(),
                );
                debug_assert!(res == WriteResult::InputEmpty);
                debug_assert!(nin == field.len());
                self.buf.written(nout);
                self.buf.writable()[0] = self.core.get_quote();
                self.buf.written(1);
            }
        }
        self.state.fields_written = record.len() as u64;
        self.write_terminator_into_buffer()
    }

    /// Write a single field.
    ///
    /// One should prefer using `write_record` over this method. It is provided
    /// for cases where writing a field at a time is more convenient than
    /// writing a record at a time.
    ///
    /// Note that if this API is used, `write_record` should be called with an
    /// empty iterator to write a record terminator.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriter;
    ///
    /// # fn main() { async_std::task::block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriter::from_writer(vec![]);
    ///     wtr.write_field("a").await?;
    ///     wtr.write_field("b").await?;
    ///     wtr.write_field("c").await?;
    ///     wtr.write_record(None::<&[u8]>).await?;
    ///     wtr.write_field("x").await?;
    ///     wtr.write_field("y").await?;
    ///     wtr.write_field("z").await?;
    ///     wtr.write_record(None::<&[u8]>).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub async fn write_field<T: AsRef<[u8]>>(&mut self, field: T) -> Result<()> {
        self.write_field_impl(field).await
    }

    /// Implementation of write_field.
    ///
    /// This is a separate method so we can force the compiler to inline it
    /// into write_record.
    #[inline(always)]
    async fn write_field_impl<T: AsRef<[u8]>>(&mut self, field: T) -> Result<()> {
        if self.state.fields_written > 0 {
            self.write_delimiter().await?;
        }
        let mut field = field.as_ref();
        loop {
            let (res, nin, nout) = self.core.field(field, self.buf.writable());
            field = &field[nin..];
            self.buf.written(nout);
            match res {
                WriteResult::InputEmpty => {
                    self.state.fields_written += 1;
                    return Ok(());
                }
                WriteResult::OutputFull => self.flush_buf().await?,
            }
        }
    }

    /// Flush the contents of the internal buffer to the underlying writer.
    ///
    /// If there was a problem writing to the underlying writer, then an error
    /// is returned.
    ///
    /// Note that this also flushes the underlying writer.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.flush_buf().await?;
        self.wtr.as_mut().unwrap().flush().await?;
        Ok(())
    }

    /// Flush the contents of the internal buffer to the underlying writer,
    /// without flushing the underlying writer.
    async fn flush_buf(&mut self) -> io::Result<()> {
        self.state.panicked = true;
        let result = self.wtr.as_mut().unwrap().write_all(self.buf.readable()).await;
        self.state.panicked = false;
        result?;
        self.buf.clear();
        Ok(())
    }

    /// Flush the contents of the internal buffer and return the underlying
    /// writer.
    pub async fn into_inner(
        mut self,
    ) -> result::Result<W, IntoInnerError<AsyncWriter<W>>> {
        match self.flush().await {
            Ok(()) => Ok(self.wtr.take().unwrap()),
            Err(err) => Err(IntoInnerError::new(self, err)),
        }
    }

    /// Write a CSV delimiter.
    async fn write_delimiter(&mut self) -> Result<()> {
        loop {
            let (res, nout) = self.core.delimiter(self.buf.writable());
            self.buf.written(nout);
            match res {
                WriteResult::InputEmpty => return Ok(()),
                WriteResult::OutputFull => self.flush_buf().await?,
            }
        }
    }

    /// Write a CSV terminator.
    async fn write_terminator(&mut self) -> Result<()> {
        self.check_field_count()?;
        loop {
            let (res, nout) = self.core.terminator(self.buf.writable());
            self.buf.written(nout);
            match res {
                WriteResult::InputEmpty => {
                    self.state.fields_written = 0;
                    return Ok(());
                }
                WriteResult::OutputFull => self.flush_buf().await?,
            }
        }
    }

    /// Write a CSV terminator that is guaranteed to fit into the current
    /// buffer.
    #[inline(never)]
    fn write_terminator_into_buffer(&mut self) -> Result<()> {
        self.check_field_count()?;
        match self.core.get_terminator() {
            csv_core::Terminator::CRLF => {
                self.buf.writable()[0] = b'\r';
                self.buf.writable()[1] = b'\n';
                self.buf.written(2);
            }
            csv_core::Terminator::Any(b) => {
                self.buf.writable()[0] = b;
                self.buf.written(1);
            }
            _ => unreachable!(),
        }
        self.state.fields_written = 0;
        Ok(())
    }

    fn check_field_count(&mut self) -> Result<()> {
        if !self.state.flexible {
            match self.state.first_field_count {
                None => {
                    self.state.first_field_count =
                        Some(self.state.fields_written);
                }
                Some(expected) if expected != self.state.fields_written => {
                    return Err(Error::new(ErrorKind::UnequalLengths {
                        pos: None,
                        expected_len: expected,
                        len: self.state.fields_written,
                    }))
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

impl Buffer {
    /// Returns a slice of the buffer's current contents.
    ///
    /// The slice returned may be empty.
    #[inline]
    fn readable(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    /// Returns a mutable slice of the remaining space in this buffer.
    ///
    /// The slice returned may be empty.
    #[inline]
    fn writable(&mut self) -> &mut [u8] {
        &mut self.buf[self.len..]
    }

    /// Indicates that `n` bytes have been written to this buffer.
    #[inline]
    fn written(&mut self, n: usize) {
        self.len += n;
    }

    /// Clear the buffer.
    #[inline]
    fn clear(&mut self) {
        self.len = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    
    use futures::io;
    use async_std::task;

    use crate::byte_record::ByteRecord;
    use crate::error::ErrorKind;
    use crate::string_record::StringRecord;

    use super::{AsyncWriter, AsyncWriterBuilder};

    async fn wtr_as_string(wtr: AsyncWriter<Vec<u8>>) -> String {
        String::from_utf8(wtr.into_inner().await.unwrap()).unwrap()
    }

    #[test]
    fn one_record() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_record(&["a", "b", "c"]).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
        });
    }

    #[test]
    fn one_string_record() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_record(&StringRecord::from(vec!["a", "b", "c"])).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
        });
    }

    #[test]
    fn one_byte_record() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
        });
    }

    #[test]
    fn raw_one_byte_record() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_byte_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
        });
    }

    #[test]
    fn one_empty_record() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_record(&[""]).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "\"\"\n");
        });
    }

    #[test]
    fn raw_one_empty_record() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_byte_record(&ByteRecord::from(vec![""])).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "\"\"\n");
        });
    }

    #[test]
    fn two_empty_records() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_record(&[""]).await.unwrap();
            wtr.write_record(&[""]).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "\"\"\n\"\"\n");
        });
    }

    #[test]
    fn raw_two_empty_records() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_byte_record(&ByteRecord::from(vec![""])).await.unwrap();
            wtr.write_byte_record(&ByteRecord::from(vec![""])).await.unwrap();

            assert_eq!(wtr_as_string(wtr).await, "\"\"\n\"\"\n");
        });
    }

    #[test]
    fn unequal_records_bad() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();
            let err = wtr.write_record(&ByteRecord::from(vec!["a"])).await.unwrap_err();
            match *err.kind() {
                ErrorKind::UnequalLengths { ref pos, expected_len, len } => {
                    assert!(pos.is_none());
                    assert_eq!(expected_len, 3);
                    assert_eq!(len, 1);
                }
                ref x => {
                    panic!("expected UnequalLengths error, but got '{:?}'", x);
                }
            }
        });
    }

    #[test]
    fn raw_unequal_records_bad() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().from_writer(vec![]);
            wtr.write_byte_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();
            let err =
                wtr.write_byte_record(&ByteRecord::from(vec!["a"])).await.unwrap_err();
            match *err.kind() {
                ErrorKind::UnequalLengths { ref pos, expected_len, len } => {
                    assert!(pos.is_none());
                    assert_eq!(expected_len, 3);
                    assert_eq!(len, 1);
                }
                ref x => {
                    panic!("expected UnequalLengths error, but got '{:?}'", x);
                }
            }
        });
    }

    #[test]
    fn unequal_records_ok() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().flexible(true).from_writer(vec![]);
            wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();
            wtr.write_record(&ByteRecord::from(vec!["a"])).await.unwrap();
            assert_eq!(wtr_as_string(wtr).await, "a,b,c\na\n");
        });
    }

    #[test]
    fn raw_unequal_records_ok() {
        task::block_on(async {
            let mut wtr = AsyncWriterBuilder::new().flexible(true).from_writer(vec![]);
            wtr.write_byte_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();
            wtr.write_byte_record(&ByteRecord::from(vec!["a"])).await.unwrap();
            assert_eq!(wtr_as_string(wtr).await, "a,b,c\na\n");
        });
    }

    #[test]
    fn full_buffer_should_not_flush_underlying() {
        task::block_on(async {
            #[derive(Debug)]
            struct MarkWriteAndFlush(Vec<u8>);

            impl MarkWriteAndFlush {
                fn to_str(self) -> String {
                    String::from_utf8(self.0).unwrap()
                }
            }

            impl io::AsyncWrite for MarkWriteAndFlush {
                fn poll_write(
                    mut self: Pin<&mut Self>,
                    _: &mut Context,
                    buf: &[u8]
                ) -> Poll<Result<usize, io::Error>> {
                    use std::io::Write;
                    self.0.write(b">").unwrap();
                    let written = self.0.write(buf).unwrap();
                    assert_eq!(written, buf.len());
                    self.0.write(b"<").unwrap();
                    // AsyncWriteExt::write_all panics if write returns more than buf.len()
                    // Poll::Ready(Ok(written + 2))
                    Poll::Ready(Ok(written))
                }

                fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), io::Error>> {
                    use std::io::Write;
                    self.0.write(b"!").unwrap();
                    Poll::Ready(Ok(()))
                }

                fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
                    self.poll_flush(cx)
                }
            }

            let underlying = MarkWriteAndFlush(vec![]);
            let mut wtr =
                AsyncWriterBuilder::new().buffer_capacity(4).from_writer(underlying);

            wtr.write_byte_record(&ByteRecord::from(vec!["a", "b"])).await.unwrap();
            wtr.write_byte_record(&ByteRecord::from(vec!["c", "d"])).await.unwrap();
            wtr.flush().await.unwrap();
            wtr.write_byte_record(&ByteRecord::from(vec!["e", "f"])).await.unwrap();

            let got = wtr.into_inner().await.unwrap().to_str();

            // As the buffer size is 4 we should write each record separately, and
            // flush when explicitly called and implictly in into_inner.
            assert_eq!(got, ">a,b\n<>c,d\n<!>e,f\n<!");
        });
    }

}
