use std::result;

use tokio::io::{self, AsyncWrite};

use crate::AsyncWriterBuilder;
use crate::byte_record::ByteRecord;
use crate::error::Result;
use super::AsyncWriterImpl;

impl AsyncWriterBuilder {
    /// Build a CSV writer from this configuration that writes data to `wtr`.
    ///
    /// Note that the CSV writer is buffered automatically, so you should not
    /// wrap `wtr` in a buffered writer.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriterBuilder;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
    /// async fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = AsyncWriterBuilder::new().create_writer(vec![]);
    ///     wtr.write_record(&["a", "b", "c"]).await?;
    ///     wtr.write_record(&["x", "y", "z"]).await?;
    ///
    ///     let data = String::from_utf8(wtr.into_inner().await?)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn create_writer<W: AsyncWrite + Unpin>(&self, wtr: W) -> AsyncWriter<W> {
        AsyncWriter::new(self, wtr)
    }
}

/// An already configured CSV writer for `tokio` runtime.
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
/// 4. Use buffering intelligently and otherwise avoid allocation. (This means
///    that callers should not do their own buffering.)
///
/// All of the above can be configured using a
/// [`AsyncWriterBuilder`](struct.AsyncWriterBuilder.html).
/// However, a `AsyncWriter` has convenient constructor (from_writer`) 
/// that use the default configuration.
///
/// Note that the default configuration of a `AsyncWriter` uses `\n` for record
/// terminators instead of `\r\n` as specified by RFC 4180. Use the
/// `terminator` method on `AsyncWriterBuilder` to set the terminator to `\r\n` if
/// it's desired.
#[derive(Debug)]
pub struct AsyncWriter<W: AsyncWrite + Unpin>(AsyncWriterImpl<W>);

impl<W: AsyncWrite + Unpin> AsyncWriter<W> {
    fn new(builder: &AsyncWriterBuilder, wtr: W) -> AsyncWriter<W> {
        AsyncWriter(AsyncWriterImpl::new(builder, wtr))
    }

    /// Build a CSV writer with a default configuration that writes data to
    /// `wtr`.
    ///
    /// Note that the CSV writer is buffered automatically, so you should not
    /// wrap `wtr` in a buffered writer.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_async::AsyncWriter;
    ///
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
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
        AsyncWriterBuilder::new().create_writer(wtr)
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
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
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
    #[inline]
    pub async fn write_record<I, T>(&mut self, record: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        self.0.write_record(record).await
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
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
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
    #[inline]
    pub async fn write_byte_record(&mut self, record: &ByteRecord) -> Result<()> {
        self.0.write_byte_record(record).await
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
    /// # fn main() { tokio1::runtime::Runtime::new().unwrap().block_on(async {example().await.unwrap()}); }
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
    #[inline]
    pub async fn write_field<T: AsRef<[u8]>>(&mut self, field: T) -> Result<()> {
        self.0.write_field(field).await
    }

    /// Flush the contents of the internal buffer to the underlying writer.
    ///
    /// If there was a problem writing to the underlying writer, then an error
    /// is returned.
    ///
    /// This finction is also called by writer destructor.
    #[inline]
    pub async fn flush(&mut self) -> io::Result<()> {
        self.0.flush().await
    }

    /// Flush the contents of the internal buffer and return the underlying writer.
    /// 
    pub async fn into_inner(
        self,
    ) -> result::Result<W, io::Error> {
        match self.0.into_inner().await {
            Ok(w) => Ok(w),
            Err(err) => Err(err.into_error()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    
    use tokio::io;

    use crate::byte_record::ByteRecord;
    use crate::error::ErrorKind;
    use crate::string_record::StringRecord;

    use super::{AsyncWriter, AsyncWriterBuilder};

    async fn wtr_as_string<'w>(wtr: AsyncWriter<Vec<u8>>) -> String {
        String::from_utf8(wtr.into_inner().await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn one_record() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_record(&["a", "b", "c"]).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
    }

    #[tokio::test]
    async fn one_string_record() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_record(&StringRecord::from(vec!["a", "b", "c"])).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
    }

    #[tokio::test]
    async fn one_byte_record() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
    }

    #[tokio::test]
    async fn raw_one_byte_record() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_byte_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "a,b,c\n");
    }

    #[tokio::test]
    async fn one_empty_record() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_record(&[""]).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "\"\"\n");
    }

    #[tokio::test]
    async fn raw_one_empty_record() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_byte_record(&ByteRecord::from(vec![""])).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "\"\"\n");
    }

    #[tokio::test]
    async fn two_empty_records() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_record(&[""]).await.unwrap();
        wtr.write_record(&[""]).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "\"\"\n\"\"\n");
    }

    #[tokio::test]
    async fn raw_two_empty_records() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
        wtr.write_byte_record(&ByteRecord::from(vec![""])).await.unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec![""])).await.unwrap();

        assert_eq!(wtr_as_string(wtr).await, "\"\"\n\"\"\n");
    }

    #[tokio::test]
    async fn unequal_records_bad() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
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
    }

    #[tokio::test]
    async fn raw_unequal_records_bad() {
        let mut wtr = AsyncWriter::from_writer(vec![]);
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
    }

    #[tokio::test]
    async fn unequal_records_ok() {
        let mut wtr = AsyncWriterBuilder::new().flexible(true).create_writer(vec![]);
        wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();
        wtr.write_record(&ByteRecord::from(vec!["a"])).await.unwrap();
        assert_eq!(wtr_as_string(wtr).await, "a,b,c\na\n");
    }

    #[tokio::test]
    async fn raw_unequal_records_ok() {
        let mut wtr = AsyncWriterBuilder::new().flexible(true).create_writer(vec![]);
        wtr.write_byte_record(&ByteRecord::from(vec!["a", "b", "c"])).await.unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec!["a"])).await.unwrap();
        assert_eq!(wtr_as_string(wtr).await, "a,b,c\na\n");
    }

    #[tokio::test]
    async fn full_buffer_should_not_flush_underlying() {
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

            fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
                self.poll_flush(cx)
            }
        }

        let underlying = MarkWriteAndFlush(vec![]);
        let mut wtr =
            AsyncWriterBuilder::new().buffer_capacity(4).create_writer(underlying);

        wtr.write_byte_record(&ByteRecord::from(vec!["a", "b"])).await.unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec!["c", "d"])).await.unwrap();
        wtr.flush().await.unwrap();
        wtr.write_byte_record(&ByteRecord::from(vec!["e", "f"])).await.unwrap();

        let got = wtr.into_inner().await.unwrap().to_str();

        // As the buffer size is 4 we should write each record separately, and
        // flush when explicitly called and implictly in into_inner.
        assert_eq!(got, ">a,b\n<>c,d\n<!>e,f\n<!");
    }
}
