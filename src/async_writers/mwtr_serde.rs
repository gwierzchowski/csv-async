use std::io;
use std::io::Write;

use csv_core::{
    self, WriteResult, Writer as CoreWriter
};
use serde::Serialize;

use crate::error::{Error, ErrorKind, Result};
use crate::serializer::{serialize, serialize_header};
use crate::AsyncWriterBuilder;

/// A helper struct to synchronously perform serialization of structures to bytes stored in memory
/// according to interface provided by serde::Serialize.
/// Those bytes are being then asynchronously sent to writer.
/// 
//  TODO: The `buf` here is present to ease using csv_core interface, 
//  but is redundant, degrade performance and should be eliminated.
#[derive(Debug)]
pub struct MemWriter {
    core: CoreWriter,
    wtr: io::Cursor<Vec<u8>>,
    buf: Buffer,
    state: WriterState,
}

#[derive(Debug)]
struct WriterState {
    /// Whether the Serde serializer should attempt to write a header row.
    header: HeaderState,
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

/// HeaderState encodes a small state machine for handling header writes.
#[derive(Debug)]
enum HeaderState {
    /// Indicates that we should attempt to write a header.
    Write,
    /// Indicates that writing a header was attempt, and a header was written.
    DidWrite,
    /// Indicates that writing a header was attempted, but no headers were
    /// written or the attempt failed.
    DidNotWrite,
    /// This state is used when headers are disabled. It cannot transition
    /// to any other state.
    None,
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

impl Drop for MemWriter {
    fn drop(&mut self) {
        if !self.state.panicked {
            let _ = self.flush();
        }
    }
}

impl MemWriter {
    /// Create MemWriter using configuration stored in AsyncWriterBuilder.
    /// 
    pub fn new(builder: &AsyncWriterBuilder) -> Self {
        let header_state = if builder.has_headers {
            HeaderState::Write
        } else {
            HeaderState::None
        };
        MemWriter {
            core: builder.builder.build(),
            wtr: io::Cursor::new(Vec::new()),
            buf: Buffer { buf: vec![0; builder.capacity], len: 0 },
            state: WriterState {
                header: header_state,
                flexible: builder.flexible,
                first_field_count: None,
                fields_written: 0,
                panicked: false,
            },
        }
    }

    /// Serialize a single record using Serde.
    ///
    pub fn serialize<S: Serialize>(&mut self, record: S) -> Result<()> {
        if let HeaderState::Write = self.state.header {
            let wrote_header = serialize_header(self, &record)?;
            if wrote_header {
                self.write_terminator()?;
                self.state.header = HeaderState::DidWrite;
            } else {
                self.state.header = HeaderState::DidNotWrite;
            };
        }
        serialize(self, &record)?;
        self.write_terminator()?;
        Ok(())
    }

    /// Write a single field.
    pub fn write_field<T: AsRef<[u8]>>(&mut self, field: T) -> Result<()> {
        self.write_field_impl(field)
    }

    /// Implementation of write_field.
    ///
    /// This is a separate method so we can force the compiler to inline it
    /// into write_record.
    #[inline(always)]
    fn write_field_impl<T: AsRef<[u8]>>(&mut self, field: T) -> Result<()> {
        if self.state.fields_written > 0 {
            self.write_delimiter()?;
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
                WriteResult::OutputFull => self.flush_buf()?,
            }
        }
    }

    /// Flush the contents of the internal buffer to the underlying writer.
    ///
    /// If there was a problem writing to the underlying writer, then an error
    /// is returned.
    ///
    /// Note that this also flushes the underlying writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.wtr.flush()?;
        Ok(())
    }

    /// Flush the contents of the internal buffer to the underlying writer,
    /// without flushing the underlying writer.
    fn flush_buf(&mut self) -> io::Result<()> {
        self.state.panicked = true;
        let result = self.wtr.write_all(self.buf.readable());
        self.state.panicked = false;
        result?;
        self.buf.clear();
        Ok(())
    }

    /// Returns slice with the accumulated data.
    /// Caller is responsible for calling `flush()` before this call, 
    /// otherwise returned vector may not contain all the data.
    pub fn data(&mut self) -> &[u8] {
        self.wtr.get_mut().as_slice()
    }

    /// Clears Writer internal vector, but not buffer.
    // TODO: See note about removing double buffering
    pub fn clear(&mut self) {
        self.wtr.get_mut().clear();
        self.wtr.set_position(0);
    }

    /// Write a CSV delimiter.
    fn write_delimiter(&mut self) -> Result<()> {
        loop {
            let (res, nout) = self.core.delimiter(self.buf.writable());
            self.buf.written(nout);
            match res {
                WriteResult::InputEmpty => return Ok(()),
                WriteResult::OutputFull => self.flush_buf()?,
            }
        }
    }

    /// Write a CSV terminator.
    fn write_terminator(&mut self) -> Result<()> {
        self.check_field_count()?;
        loop {
            let (res, nout) = self.core.terminator(self.buf.writable());
            self.buf.written(nout);
            match res {
                WriteResult::InputEmpty => {
                    self.state.fields_written = 0;
                    return Ok(());
                }
                WriteResult::OutputFull => self.flush_buf()?,
            }
        }
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
    use std::error::Error;

    use serde::{serde_if_integer128, Serialize};

    use crate::byte_record::ByteRecord;
    use crate::error::{ErrorKind, IntoInnerError};
    use crate::string_record::StringRecord;

    use super::{MemWriter, AsyncWriterBuilder};

    fn wtr_as_string(wtr: MemWriter) -> String {
        String::from_utf8(wtr.into_inner().unwrap()).unwrap()
    }

    impl MemWriter {
        pub fn default() -> Self {
            Self::new(&AsyncWriterBuilder::new())
        }
        
        pub fn into_inner(
            mut self,
        ) -> Result<Vec<u8>, IntoInnerError<MemWriter>> {
            match self.flush() {
                // This is not official API, so not worth to use Option trick.
                Ok(()) => Ok(self.wtr.clone().into_inner()),
                Err(err) => Err(IntoInnerError::new(self, err)),
            }
        }
    
        pub fn write_record<I, T>(&mut self, record: I) -> crate::error::Result<()>
        where
            I: IntoIterator<Item = T>,
            T: AsRef<[u8]>,
        {
            for field in record.into_iter() {
                self.write_field_impl(field)?;
            }
            self.write_terminator()
        }
    }

    #[test]
    fn one_record() {
        let mut wtr = MemWriter::default();
        wtr.write_record(&["a", "b", "c"]).unwrap();

        assert_eq!(wtr_as_string(wtr), "a,b,c\n");
    }

    #[test]
    fn one_string_record() {
        let mut wtr = MemWriter::default();
        wtr.write_record(&StringRecord::from(vec!["a", "b", "c"])).unwrap();

        assert_eq!(wtr_as_string(wtr), "a,b,c\n");
    }

    #[test]
    fn one_byte_record() {
        let mut wtr = MemWriter::default();
        wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).unwrap();

        assert_eq!(wtr_as_string(wtr), "a,b,c\n");
    }

    #[test]
    fn one_empty_record() {
        let mut wtr = MemWriter::default();
        wtr.write_record(&[""]).unwrap();

        assert_eq!(wtr_as_string(wtr), "\"\"\n");
    }

    #[test]
    fn two_empty_records() {
        let mut wtr = MemWriter::default();
        wtr.write_record(&[""]).unwrap();
        wtr.write_record(&[""]).unwrap();

        assert_eq!(wtr_as_string(wtr), "\"\"\n\"\"\n");
    }

    #[test]
    fn unequal_records_bad() {
        let mut wtr = MemWriter::default();
        wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).unwrap();
        let err = wtr.write_record(&ByteRecord::from(vec!["a"])).unwrap_err();
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

    #[test]
    fn unequal_records_ok() {
        let mut wtr = MemWriter::new(&AsyncWriterBuilder::new().flexible(true));
        wtr.write_record(&ByteRecord::from(vec!["a", "b", "c"])).unwrap();
        wtr.write_record(&ByteRecord::from(vec!["a"])).unwrap();
        assert_eq!(wtr_as_string(wtr), "a,b,c\na\n");
    }

    #[test]
    fn write_field() -> Result<(), Box<dyn Error>> {
        let mut wtr = MemWriter::default();
        wtr.write_field("a")?;
        wtr.write_field("b")?;
        wtr.write_field("c")?;
        wtr.write_terminator()?;
        wtr.write_field("x")?;
        wtr.write_field("y")?;
        wtr.write_field("z")?;
        wtr.write_terminator()?;

        let data = String::from_utf8(wtr.into_inner()?)?;
        assert_eq!(data, "a,b,c\nx,y,z\n");
        Ok(())
    }

    #[test]
    fn serialize_with_headers() {
        #[derive(Serialize)]
        struct Row {
            foo: i32,
            bar: f64,
            baz: bool,
        }

        let mut wtr = MemWriter::default();
        wtr.serialize(Row { foo: 42, bar: 42.5, baz: true }).unwrap();
        assert_eq!(wtr_as_string(wtr), "foo,bar,baz\n42,42.5,true\n");
    }

    #[test]
    fn serialize_no_headers() {
        #[derive(Serialize)]
        struct Row {
            foo: i32,
            bar: f64,
            baz: bool,
        }

        let mut wtr = MemWriter::new(&AsyncWriterBuilder::new().has_headers(false));
        wtr.serialize(Row { foo: 42, bar: 42.5, baz: true }).unwrap();
        assert_eq!(wtr_as_string(wtr), "42,42.5,true\n");
    }

    serde_if_integer128! {
        #[test]
        fn serialize_no_headers_128() {
            #[derive(Serialize)]
            struct Row {
                foo: i128,
                bar: f64,
                baz: bool,
            }

            let mut wtr = MemWriter::new(&AsyncWriterBuilder::new().has_headers(false));
            wtr.serialize(Row {
                foo: 9_223_372_036_854_775_808,
                bar: 42.5,
                baz: true,
            }).unwrap();
            assert_eq!(wtr_as_string(wtr), "9223372036854775808,42.5,true\n");
        }
    }

    #[test]
    fn serialize_tuple() {
        let mut wtr = MemWriter::default();
        wtr.serialize((true, 1.3, "hi")).unwrap();
        assert_eq!(wtr_as_string(wtr), "true,1.3,hi\n");
    }

    #[test]
    fn serialize_struct() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize)]
        struct Row<'a> {
            city: &'a str,
            country: &'a str,
            // Serde allows us to name our headers exactly,
            // even if they don't match our struct field names.
            #[serde(rename = "popcount")]
            population: u64,
        }
        
        let mut wtr = MemWriter::default();
        wtr.serialize(Row {
            city: "Boston",
            country: "United States",
            population: 4628910,
        })?;
        wtr.serialize(Row {
            city: "Concord",
            country: "United States",
            population: 42695,
        })?;
    
        let data = String::from_utf8(wtr.into_inner()?)?;
        assert_eq!(data, "\
city,country,popcount
Boston,United States,4628910
Concord,United States,42695
");
        Ok(())
    }

    #[test]
    fn serialize_enum() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize)]
        struct Row {
            label: String,
            value: Value,
        }
        #[derive(Serialize)]
        enum Value {
            Integer(i64),
            Float(f64),
        }

        let mut wtr = MemWriter::default();
        wtr.serialize(Row {
            label: "foo".to_string(),
            value: Value::Integer(3),
        })?;
        wtr.serialize(Row {
            label: "bar".to_string(),
            value: Value::Float(3.14),
        })?;

        let data = String::from_utf8(wtr.into_inner()?)?;
        assert_eq!(data, "\
label,value
foo,3
bar,3.14
");
        Ok(())
    }
    
    #[test]
    fn serialize_vec() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize)]
        struct Row {
            label: String,
            values: Vec<f64>,
        }

        let mut wtr = MemWriter::new(
            &AsyncWriterBuilder::new()
                .has_headers(false)
            );
        wtr.serialize(Row {
            label: "foo".to_string(),
            values: vec![1.1234, 2.5678, 3.14],
        })?;

        let data = String::from_utf8(wtr.into_inner()?)?;
        assert_eq!(data, "foo,1.1234,2.5678,3.14\n");
        Ok(())
    }
}
