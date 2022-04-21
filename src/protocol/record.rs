//! The actual payload message which is also the on-disk format for Kafka.
//!
//! The format evolved twice in [KIP-32] and [KIP-98]. We only support the latest generation (message version 2).
//!
//!
//! # CRC
//! The CRC used to check payload data is `CRC-32C` / iSCSI as documented by the following sources:
//!
//! - <https://kafka.apache.org/documentation/#recordbatch>
//! - <https://docs.oracle.com/javase/9/docs/api/java/util/zip/CRC32C.html>
//! - <https://reveng.sourceforge.io/crc-catalogue/all.htm>
//!
//!
//! # References
//! - [KIP-32]
//! - [KIP-98]
//! - <https://kafka.apache.org/documentation/#messageformat>
//!
//!
//! [KIP-32]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
//! [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
use std::io::{Cursor, Read, Write};

#[cfg(test)]
use proptest::prelude::*;

use super::{
    primitives::{Int16, Int32, Int64, Int8, Varint, Varlong},
    traits::{ReadError, ReadType, WriteError, WriteType},
    vec_builder::VecBuilder,
};

/// Record Header
///
/// # References
/// - <https://kafka.apache.org/documentation/#recordheader>
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct RecordHeader {
    pub key: String,
    pub value: Vec<u8>,
}

impl<R> ReadType<R> for RecordHeader
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // key
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut buf = VecBuilder::new(len);
        buf = buf.read_exact(reader)?;
        let key = String::from_utf8(buf.into()).map_err(|e| ReadError::Malformed(Box::new(e)))?;

        // value
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut value = VecBuilder::new(len);
        value = value.read_exact(reader)?;
        let value = value.into();

        Ok(Self { key, value })
    }
}

impl<W> WriteType<W> for RecordHeader
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // key
        let l = i32::try_from(self.key.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Varint(l).write(writer)?;
        writer.write_all(self.key.as_bytes())?;

        // value
        let l = i32::try_from(self.value.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Varint(l).write(writer)?;
        writer.write_all(&self.value)?;

        Ok(())
    }
}

/// Record
///
/// # References
/// - <https://kafka.apache.org/documentation/#record>
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Record {
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordHeader>,
}

impl<R> ReadType<R> for Record
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // length
        let len = Varint::read(reader)?;
        let len = u64::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let reader = &mut reader.take(len);

        // attributes
        Int8::read(reader)?;

        // timestampDelta
        let timestamp_delta = Varlong::read(reader)?.0;

        // offsetDelta
        let offset_delta = Varint::read(reader)?.0;

        // key
        let len = Varint::read(reader)?.0;
        let key = if len == -1 {
            None
        } else {
            let len = usize::try_from(len).map_err(|e| ReadError::Malformed(Box::new(e)))?;
            let mut key = VecBuilder::new(len);
            key = key.read_exact(reader)?;
            Some(key.into())
        };

        // value
        let len = Varint::read(reader)?.0;
        let value = if len == -1 {
            None
        } else {
            let len = usize::try_from(len).map_err(|e| ReadError::Malformed(Box::new(e)))?;
            let mut value = VecBuilder::new(len);
            value = value.read_exact(reader)?;
            Some(value.into())
        };

        // headers
        // Note: This is NOT a normal array but uses a Varint instead.
        let n_headers = Varint::read(reader)?;
        let n_headers =
            usize::try_from(n_headers.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut headers = VecBuilder::new(n_headers);
        for _ in 0..n_headers {
            headers.push(RecordHeader::read(reader)?);
        }

        // check if there is any trailing data because this is likely a bug
        if reader.limit() != 0 {
            return Err(ReadError::Malformed(
                format!("Found {} trailing bytes after Record", reader.limit()).into(),
            ));
        }

        Ok(Self {
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers: headers.into(),
        })
    }
}

impl<W> WriteType<W> for Record
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // ============================================================================================
        // ======================================== inner data ========================================
        // write data to buffer because we need to prepend the length
        let mut data = vec![];

        // attributes
        Int8(0).write(&mut data)?;

        // timestampDelta
        Varlong(self.timestamp_delta).write(&mut data)?;

        // offsetDelta
        Varint(self.offset_delta).write(&mut data)?;

        // key
        match &self.key {
            Some(key) => {
                let l = i32::try_from(key.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
                Varint(l).write(&mut data)?;
                data.write_all(key)?;
            }
            None => {
                Varint(-1).write(&mut data)?;
            }
        }

        // value
        match &self.value {
            Some(value) => {
                let l =
                    i32::try_from(value.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
                Varint(l).write(&mut data)?;
                data.write_all(value)?;
            }
            None => {
                Varint(-1).write(&mut data)?;
            }
        }

        // headers
        // Note: This is NOT a normal array but uses a Varint instead.
        let l =
            i32::try_from(self.headers.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Varint(l).write(&mut data)?;
        for header in &self.headers {
            header.write(&mut data)?;
        }

        // ============================================================================================
        // ============================================================================================

        // now write accumulated data
        let l = i32::try_from(data.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Varint(l).write(writer)?;
        writer.write_all(&data)?;

        Ok(())
    }
}

/// Control Batch Record
///
/// # References
/// - <https://kafka.apache.org/documentation/#controlbatch>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ControlBatchRecord {
    Abort,
    Commit,
}

impl<R> ReadType<R> for ControlBatchRecord
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // version
        let version = Int16::read(reader)?.0;
        if version != 0 {
            return Err(ReadError::Malformed(
                format!("Unknown control batch record version: {}", version).into(),
            ));
        }

        // type
        let t = Int16::read(reader)?.0;
        match t {
            0 => Ok(Self::Abort),
            1 => Ok(Self::Commit),
            _ => Err(ReadError::Malformed(
                format!("Unknown control batch record type: {}", t).into(),
            )),
        }
    }
}

impl<W> WriteType<W> for ControlBatchRecord
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // version
        Int16(0).write(writer)?;

        // type
        let t = match self {
            Self::Abort => 0,
            Self::Commit => 1,
        };
        Int16(t).write(writer)?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ControlBatchOrRecords {
    ControlBatch(ControlBatchRecord),

    // tell proptest to only generate small vectors, otherwise tests take forever
    #[cfg_attr(
        test,
        proptest(
            strategy = "prop::collection::vec(any::<Record>(), 0..2).prop_map(ControlBatchOrRecords::Records)"
        )
    )]
    Records(Vec<Record>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum RecordBatchCompression {
    NoCompression,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Record batch timestamp type.
///
/// # References
/// - <https://kafka.apache.org/documentation/#messageset> (this is the old message format, but the flag is the same)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum RecordBatchTimestampType {
    CreateTime,
    LogAppendTime,
}

/// Record Batch
///
/// # References
/// - <https://kafka.apache.org/documentation/#recordbatch>
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: ControlBatchOrRecords,
    pub compression: RecordBatchCompression,
    pub is_transactional: bool,
    pub timestamp_type: RecordBatchTimestampType,
}

impl<R> ReadType<R> for RecordBatch
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // baseOffset
        let base_offset = Int64::read(reader)?.0;

        // batchLength
        //
        // Contains all fields AFTER the length field (so excluding `baseOffset` and `batchLength`). To determine the
        // size of the CRC-checked part we must substract all sized from this field to and including the CRC field.
        let len = Int32::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let len = len
            .checked_sub(
                4 // partitionLeaderEpoch
            + 1 // magic
            + 4, // crc
            )
            .ok_or_else(|| {
                ReadError::Malformed(format!("Record batch len too small: {}", len).into())
            })?;

        // partitionLeaderEpoch
        let partition_leader_epoch = Int32::read(reader)?.0;

        // magic
        let magic = Int8::read(reader)?.0;
        if magic != 2 {
            return Err(ReadError::Malformed(
                format!("Invalid magic number in record batch: {}", magic).into(),
            ));
        }

        // crc
        let crc = Int32::read(reader)?.0;
        let crc = u32::from_be_bytes(crc.to_be_bytes());

        // data
        let mut data = VecBuilder::new(len);
        data = data.read_exact(reader)?;
        let data: Vec<u8> = data.into();
        let actual_crc = crc32c::crc32c(&data);
        if crc != actual_crc {
            return Err(ReadError::Malformed(
                format!("CRC error, got 0x{:x}, expected 0x{:x}", actual_crc, crc).into(),
            ));
        }

        // ==========================================================================================
        // ======================================== CRC data ========================================
        let mut data = Cursor::new(data);
        let body = RecordBatchBody::read(&mut data)?;

        // check if there is any trailing data because this is likely a bug
        let bytes_read = data.position();
        let bytes_total = u64::try_from(data.into_inner().len()).map_err(ReadError::Overflow)?;
        let bytes_left = bytes_total - bytes_read;
        if bytes_left != 0 {
            return Err(ReadError::Malformed(
                format!("Found {} trailing bytes after RecordBatch", bytes_left).into(),
            ));
        }

        // ==========================================================================================
        // ==========================================================================================

        Ok(Self {
            base_offset,
            partition_leader_epoch,
            last_offset_delta: body.last_offset_delta,
            first_timestamp: body.first_timestamp,
            max_timestamp: body.max_timestamp,
            producer_id: body.producer_id,
            producer_epoch: body.producer_epoch,
            base_sequence: body.base_sequence,
            compression: body.compression,
            timestamp_type: body.timestamp_type,
            is_transactional: body.is_transactional,
            records: body.records,
        })
    }
}

impl<W> WriteType<W> for RecordBatch
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // ==========================================================================================
        // ======================================== CRC data ========================================
        // collect everything that should be part of the CRC calculation
        let mut data = vec![];
        let body_ref = RecordBatchBodyRef {
            last_offset_delta: self.last_offset_delta,
            first_timestamp: self.first_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records: &self.records,
            compression: self.compression,
            is_transactional: self.is_transactional,
            timestamp_type: self.timestamp_type,
        };
        body_ref.write(&mut data)?;

        // ==========================================================================================
        // ==========================================================================================

        // baseOffset
        Int64(self.base_offset).write(writer)?;

        // batchLength
        //
        // Contains all fields AFTER the length field (so excluding `baseOffset` and `batchLength`, but including
        // `partitionLeaderEpoch`, `magic`, and `crc).
        //
        // See
        // https://github.com/kafka-rust/kafka-rust/blob/657202832806cda77d0a1801d618dc6c382b4d79/src/protocol/produce.rs#L224-L226
        let l = i32::try_from(
            data.len()
            + 4 // partitionLeaderEpoch
            + 1 // magic
            + 4, // crc
        )
        .map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Int32(l).write(writer)?;

        // partitionLeaderEpoch
        Int32(self.partition_leader_epoch).write(writer)?;

        // magic
        Int8(2).write(writer)?;

        // crc
        // See
        // https://github.com/kafka-rust/kafka-rust/blob/a551b6231a7adc9b715552b635a69ac2856ec8a1/src/protocol/mod.rs#L161-L163
        // WARNING: the range in the code linked above is correct but the polynomial is wrong!
        let crc = crc32c::crc32c(&data);
        let crc = i32::from_be_bytes(crc.to_be_bytes());
        Int32(crc).write(writer)?;

        // the actual CRC-checked data
        writer.write_all(&data)?;

        Ok(())
    }
}

/// Inner part of a [`RecordBatch`] that is protected by a header containing its length and a CRC checksum.
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct RecordBatchBody {
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: ControlBatchOrRecords,
    pub compression: RecordBatchCompression,
    pub is_transactional: bool,
    pub timestamp_type: RecordBatchTimestampType,
}

impl RecordBatchBody {
    fn read_records<R>(
        reader: &mut R,
        is_control: bool,
        n_records: usize,
    ) -> Result<ControlBatchOrRecords, ReadError>
    where
        R: Read,
    {
        if is_control {
            if n_records != 1 {
                return Err(ReadError::Malformed(
                    format!("Expected 1 control record but got {}", n_records).into(),
                ));
            }

            let record = ControlBatchRecord::read(reader)?;
            Ok(ControlBatchOrRecords::ControlBatch(record))
        } else {
            let mut records = VecBuilder::new(n_records);
            for _ in 0..n_records {
                records.push(Record::read(reader)?);
            }
            Ok(ControlBatchOrRecords::Records(records.into()))
        }
    }
}

impl<R> ReadType<R> for RecordBatchBody
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // attributes
        let attributes = Int16::read(reader)?.0;
        let compression = match attributes & 0x7 {
            0 => RecordBatchCompression::NoCompression,
            1 => RecordBatchCompression::Gzip,
            2 => RecordBatchCompression::Snappy,
            3 => RecordBatchCompression::Lz4,
            4 => RecordBatchCompression::Zstd,
            other => {
                return Err(ReadError::Malformed(
                    format!("Invalid compression type: {}", other).into(),
                ));
            }
        };
        let timestamp_type = if ((attributes >> 3) & 0x1) == 0 {
            RecordBatchTimestampType::CreateTime
        } else {
            RecordBatchTimestampType::LogAppendTime
        };
        let is_transactional = ((attributes >> 4) & 0x1) == 1;
        let is_control = ((attributes >> 5) & 0x1) == 1;

        // lastOffsetDelta
        let last_offset_delta = Int32::read(reader)?.0;

        // firstTimestamp
        let first_timestamp = Int64::read(reader)?.0;

        // maxTimestamp
        let max_timestamp = Int64::read(reader)?.0;

        // producerId
        let producer_id = Int64::read(reader)?.0;

        // producerEpoch
        let producer_epoch = Int16::read(reader)?.0;

        // baseSequence
        let base_sequence = Int32::read(reader)?.0;

        // records
        let n_records = match Int32::read(reader)?.0 {
            -1 => 0,
            n => usize::try_from(n)?,
        };
        let records = match compression {
            RecordBatchCompression::NoCompression => {
                Self::read_records(reader, is_control, n_records)?
            }
            #[cfg(feature = "compression-gzip")]
            RecordBatchCompression::Gzip => {
                use flate2::read::GzDecoder;

                let mut decoder = GzDecoder::new(reader);
                let records = Self::read_records(&mut decoder, is_control, n_records)?;

                ensure_eof(&mut decoder, "Data left in gzip block")?;

                records
            }
            #[cfg(feature = "compression-lz4")]
            RecordBatchCompression::Lz4 => {
                use lz4::Decoder;

                let mut decoder = Decoder::new(reader)?;
                let records = Self::read_records(&mut decoder, is_control, n_records)?;

                // the lz4 decoder requires us to consume the whole inner stream until we reach EOF
                ensure_eof(&mut decoder, "Data left in LZ4 block")?;

                let (_reader, res) = decoder.finish();
                res?;

                records
            }
            #[cfg(feature = "compression-snappy")]
            RecordBatchCompression::Snappy => {
                // Construct the input for the raw decoder.
                let mut input = vec![];
                reader.read_to_end(&mut input)?;

                const JAVA_MAGIC: &[u8] = &[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

                // There are "normal" compression libs, and there is Java
                // See https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_msgset_reader.c#L307-L318
                let output = if input.starts_with(JAVA_MAGIC) {
                    let mut cursor = Cursor::new(&input[JAVA_MAGIC.len()..]);

                    let mut buf_version = [0u8; 4];
                    cursor.read_exact(&mut buf_version)?;
                    if buf_version != [0, 0, 0, 1] {
                        return Err(ReadError::Malformed(
                            format!("Detected Java-specific Snappy compression, but got unknown version: {buf_version:?}").into(),
                        ));
                    }

                    let mut buf_compatible = [0u8; 4];
                    cursor.read_exact(&mut buf_compatible)?;
                    if buf_compatible != [0, 0, 0, 1] {
                        return Err(ReadError::Malformed(
                            format!("Detected Java-specific Snappy compression, but got unknown compat flags: {buf_compatible:?}").into(),
                        ));
                    }

                    let mut output = vec![];
                    while cursor.position() < cursor.get_ref().len() as u64 {
                        let mut buf_chunk_length = [0u8; 4];
                        cursor.read_exact(&mut buf_chunk_length)?;
                        let chunk_length = u32::from_be_bytes(buf_chunk_length) as usize;

                        let mut chunk_data = vec![0u8; chunk_length];
                        cursor.read_exact(&mut chunk_data)?;

                        let mut buf = carefully_decompress_snappy(&chunk_data)?;
                        output.append(&mut buf);
                    }

                    output
                } else {
                    carefully_decompress_snappy(&input)?
                };

                // Read uncompressed records.
                let mut decoder = Cursor::new(output);
                let records = Self::read_records(&mut decoder, is_control, n_records)?;

                // Check that there's no data left within the uncompressed block.
                ensure_eof(&mut decoder, "Data left in Snappy block")?;

                records
            }
            #[cfg(feature = "compression-zstd")]
            RecordBatchCompression::Zstd => {
                use zstd::Decoder;

                let mut decoder = Decoder::new(reader)?;
                let records = Self::read_records(&mut decoder, is_control, n_records)?;

                ensure_eof(&mut decoder, "Data left in zstd block")?;

                records
            }
            #[allow(unreachable_patterns)]
            _ => {
                return Err(ReadError::Malformed(
                    format!("Unimplemented compression: {:?}", compression).into(),
                ));
            }
        };

        Ok(Self {
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            compression,
            timestamp_type,
            is_transactional,
            records,
        })
    }
}

impl<W> WriteType<W> for RecordBatchBody
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let body_ref = RecordBatchBodyRef {
            last_offset_delta: self.last_offset_delta,
            first_timestamp: self.first_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records: &self.records,
            compression: self.compression,
            is_transactional: self.is_transactional,
            timestamp_type: self.timestamp_type,
        };
        body_ref.write(writer)
    }
}

/// Same as [`RecordBatchBody`] but contains referenced data.
///
/// This only supports writing.
#[derive(Debug)]
struct RecordBatchBodyRef<'a> {
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: &'a ControlBatchOrRecords,
    pub compression: RecordBatchCompression,
    pub is_transactional: bool,
    pub timestamp_type: RecordBatchTimestampType,
}

impl<'a> RecordBatchBodyRef<'a> {
    fn write_records<W>(writer: &mut W, records: &ControlBatchOrRecords) -> Result<(), WriteError>
    where
        W: Write,
    {
        match records {
            ControlBatchOrRecords::ControlBatch(control_batch) => control_batch.write(writer),
            ControlBatchOrRecords::Records(records) => {
                for record in records {
                    record.write(writer)?;
                }
                Ok(())
            }
        }
    }
}

impl<'a, W> WriteType<W> for RecordBatchBodyRef<'a>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // attributes
        let mut attributes: i16 = match self.compression {
            RecordBatchCompression::NoCompression => 0,
            RecordBatchCompression::Gzip => 1,
            RecordBatchCompression::Snappy => 2,
            RecordBatchCompression::Lz4 => 3,
            RecordBatchCompression::Zstd => 4,
        };
        match self.timestamp_type {
            RecordBatchTimestampType::CreateTime => (),
            RecordBatchTimestampType::LogAppendTime => {
                attributes |= 1 << 3;
            }
        }
        if self.is_transactional {
            attributes |= 1 << 4;
        }
        if matches!(self.records, ControlBatchOrRecords::ControlBatch(_)) {
            attributes |= 1 << 5;
        }
        Int16(attributes).write(writer)?;

        // lastOffsetDelta
        Int32(self.last_offset_delta).write(writer)?;

        // firstTimestamp
        Int64(self.first_timestamp).write(writer)?;

        // maxTimestamp
        Int64(self.max_timestamp).write(writer)?;

        // producerId
        Int64(self.producer_id).write(writer)?;

        // producerEpoch
        Int16(self.producer_epoch).write(writer)?;

        // baseSequence
        Int32(self.base_sequence).write(writer)?;

        // records
        let n_records = match &self.records {
            ControlBatchOrRecords::ControlBatch(_) => 1,
            ControlBatchOrRecords::Records(records) => records.len(),
        };
        Int32(i32::try_from(n_records)?).write(writer)?;
        match self.compression {
            RecordBatchCompression::NoCompression => {
                Self::write_records(writer, self.records)?;
            }
            #[cfg(feature = "compression-gzip")]
            RecordBatchCompression::Gzip => {
                use flate2::{write::GzEncoder, Compression};

                let mut encoder = GzEncoder::new(writer, Compression::default());
                Self::write_records(&mut encoder, self.records)?;
                encoder.finish()?;
            }
            #[cfg(feature = "compression-lz4")]
            RecordBatchCompression::Lz4 => {
                use lz4::{liblz4::BlockMode, EncoderBuilder};

                let mut encoder = EncoderBuilder::new()
                    .block_mode(
                        // the only one supported by Kafka
                        BlockMode::Independent,
                    )
                    .build(writer)?;
                Self::write_records(&mut encoder, self.records)?;
                let (_writer, res) = encoder.finish();
                res?;
            }
            #[cfg(feature = "compression-snappy")]
            RecordBatchCompression::Snappy => {
                use snap::raw::{max_compress_len, Encoder};

                let mut input = vec![];
                Self::write_records(&mut input, self.records)?;

                let mut encoder = Encoder::new();
                let mut output = vec![0; max_compress_len(input.len())];
                let len = encoder
                    .compress(&input, &mut output)
                    .map_err(|e| WriteError::Malformed(Box::new(e)))?;

                writer.write_all(&output[..len])?;
            }
            #[cfg(feature = "compression-zstd")]
            RecordBatchCompression::Zstd => {
                use zstd::Encoder;

                let mut encoder = Encoder::new(writer, 0)?;
                Self::write_records(&mut encoder, self.records)?;
                encoder.finish()?;
            }
            #[allow(unreachable_patterns)]
            _ => {
                return Err(WriteError::Malformed(
                    format!("Unimplemented compression: {:?}", self.compression).into(),
                ));
            }
        }

        Ok(())
    }
}

/// Ensure that given reader is at EOF.
#[allow(dead_code)] // only use by some features
fn ensure_eof<R>(reader: &mut R, msg: &str) -> Result<(), ReadError>
where
    R: Read,
{
    let mut buf = [0u8; 1];
    match reader.read(&mut buf) {
        Ok(0) => Ok(()),
        Ok(_) => Err(ReadError::Malformed(msg.to_string().into())),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(()),
        Err(e) => Err(ReadError::IO(e)),
    }
}

#[cfg(feature = "compression-snappy")]
fn carefully_decompress_snappy(input: &[u8]) -> Result<Vec<u8>, ReadError> {
    use crate::protocol::vec_builder::DEFAULT_BLOCK_SIZE;
    use snap::raw::{decompress_len, Decoder};

    // The snappy compression used here is unframed aka "raw". So we first need to figure out the
    // uncompressed length. See
    //
    // - https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_msgset_reader.c#L345-L348
    // - https://github.com/edenhill/librdkafka/blob/747f77c98fbddf7dc6508f76398e0fc9ee91450f/src/snappy.c#L779
    let uncompressed_size = decompress_len(input).map_err(|e| ReadError::Malformed(Box::new(e)))?;

    // Decode snappy payload.
    // The uncompressed length is unchecked and can be up to 2^32-1 bytes. To avoid a DDoS vector we try to
    // limit it to a small size and if that fails we double that size;
    let mut max_uncompressed_size = DEFAULT_BLOCK_SIZE;

    loop {
        let try_uncompressed_size = uncompressed_size.min(max_uncompressed_size);

        let mut decoder = Decoder::new();
        let mut output = vec![0; try_uncompressed_size];
        let actual_uncompressed_size = match decoder.decompress(input, &mut output) {
            Ok(size) => size,
            Err(snap::Error::BufferTooSmall { .. })
                if max_uncompressed_size < uncompressed_size =>
            {
                // try larger buffer
                max_uncompressed_size *= 2;
                continue;
            }
            Err(e) => {
                return Err(ReadError::Malformed(Box::new(e)));
            }
        };
        if actual_uncompressed_size != uncompressed_size {
            return Err(ReadError::Malformed(
                "broken snappy data".to_string().into(),
            ));
        }

        break Ok(output);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::protocol::test_utils::test_roundtrip;

    use super::*;

    use assert_matches::assert_matches;

    test_roundtrip!(RecordHeader, test_record_header_roundtrip);

    test_roundtrip!(Record, test_record_roundtrip);

    test_roundtrip!(ControlBatchRecord, test_control_batch_record_roundtrip);

    #[test]
    fn test_control_batch_record_unknown_version() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        Int16(1).write(&mut buf).unwrap();
        Int16(0).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = ControlBatchRecord::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(
            err.to_string(),
            "Malformed data: Unknown control batch record version: 1",
        );
    }

    #[test]
    fn test_control_batch_record_unknown_type() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        Int16(0).write(&mut buf).unwrap();
        Int16(2).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = ControlBatchRecord::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(
            err.to_string(),
            "Malformed data: Unknown control batch record type: 2",
        );
    }

    test_roundtrip!(RecordBatchBody, test_record_batch_body_roundtrip);

    test_roundtrip!(RecordBatch, test_record_batch_roundtrip);

    #[test]
    fn test_decode_fixture_nocompression() {
        // This data was obtained by watching rdkafka.
        let data = [
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x4b\x00\x00\x00\x00".to_vec(),
            b"\x02\x27\x24\xfe\xcd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x61".to_vec(),
            b"\xd5\x9b\x77\x00\x00\x00\x00\x61\xd5\x9b\x77\xff\xff\xff\xff\xff".to_vec(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01\x32\x00\x00".to_vec(),
            b"\x00\x00\x16\x68\x65\x6c\x6c\x6f\x20\x6b\x61\x66\x6b\x61\x02\x06".to_vec(),
            b"\x66\x6f\x6f\x06\x62\x61\x72".to_vec(),
        ]
        .concat();

        let actual = RecordBatch::read(&mut Cursor::new(data.clone())).unwrap();
        let expected = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: 0,
            first_timestamp: 1641388919,
            max_timestamp: 1641388919,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: ControlBatchOrRecords::Records(vec![Record {
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(vec![]),
                value: Some(b"hello kafka".to_vec()),
                headers: vec![RecordHeader {
                    key: "foo".to_owned(),
                    value: b"bar".to_vec(),
                }],
            }]),
            compression: RecordBatchCompression::NoCompression,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        };
        assert_eq!(actual, expected);

        let mut data2 = vec![];
        actual.write(&mut data2).unwrap();
        assert_eq!(data, data2);
    }

    #[cfg(feature = "compression-gzip")]
    #[test]
    fn test_decode_fixture_gzip() {
        // This data was obtained by watching rdkafka.
        let data = [
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x64\x00\x00\x00\x00".to_vec(),
            b"\x02\xba\x41\x46\x65\x00\x01\x00\x00\x00\x00\x00\x00\x01\x7e\x90".to_vec(),
            b"\xb3\x34\x67\x00\x00\x01\x7e\x90\xb3\x34\x67\xff\xff\xff\xff\xff".to_vec(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01\x1f\x8b\x08".to_vec(),
            b"\x00\x00\x00\x00\x00\x00\x03\xfb\xc3\xc8\xc0\xc0\x70\x82\xb1\x82".to_vec(),
            b"\x0e\x40\x2c\x23\x35\x27\x27\x5f\x21\x3b\x31\x2d\x3b\x91\x89\x2d".to_vec(),
            b"\x2d\x3f\x9f\x2d\x29\xb1\x08\x00\xe4\xcd\xba\x1f\x80\x00\x00\x00".to_vec(),
        ]
        .concat();

        let actual = RecordBatch::read(&mut Cursor::new(data)).unwrap();
        let expected = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: 0,
            first_timestamp: 1643105170535,
            max_timestamp: 1643105170535,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: ControlBatchOrRecords::Records(vec![Record {
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(vec![b'x'; 100]),
                value: Some(b"hello kafka".to_vec()),
                headers: vec![RecordHeader {
                    key: "foo".to_owned(),
                    value: b"bar".to_vec(),
                }],
            }]),
            compression: RecordBatchCompression::Gzip,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        };
        assert_eq!(actual, expected);

        let mut data2 = vec![];
        actual.write(&mut data2).unwrap();

        // don't compare if the data is equal because compression encoder might work slightly differently, use another
        // roundtrip instead
        let actual2 = RecordBatch::read(&mut Cursor::new(data2)).unwrap();
        assert_eq!(actual2, expected);
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn test_decode_fixture_lz4() {
        // This data was obtained by watching rdkafka.
        let data = [
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x63\x00\x00\x00\x00".to_vec(),
            b"\x02\x1b\xa5\x92\x35\x00\x03\x00\x00\x00\x00\x00\x00\x01\x7e\xb1".to_vec(),
            b"\x1f\xc7\x24\x00\x00\x01\x7e\xb1\x1f\xc7\x24\xff\xff\xff\xff\xff".to_vec(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01\x04\x22\x4d".to_vec(),
            b"\x18\x60\x40\x82\x23\x00\x00\x00\x8f\xfc\x01\x00\x00\x00\xc8\x01".to_vec(),
            b"\x78\x01\x00\x50\xf0\x06\x16\x68\x65\x6c\x6c\x6f\x20\x6b\x61\x66".to_vec(),
            b"\x6b\x61\x02\x06\x66\x6f\x6f\x06\x62\x61\x72\x00\x00\x00\x00".to_vec(),
        ]
        .concat();

        let actual = RecordBatch::read(&mut Cursor::new(data)).unwrap();
        let expected = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: 0,
            first_timestamp: 1643649156900,
            max_timestamp: 1643649156900,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: ControlBatchOrRecords::Records(vec![Record {
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(vec![b'x'; 100]),
                value: Some(b"hello kafka".to_vec()),
                headers: vec![RecordHeader {
                    key: "foo".to_owned(),
                    value: b"bar".to_vec(),
                }],
            }]),
            compression: RecordBatchCompression::Lz4,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        };
        assert_eq!(actual, expected);

        let mut data2 = vec![];
        actual.write(&mut data2).unwrap();

        // don't compare if the data is equal because compression encoder might work slightly differently, use another
        // roundtrip instead
        let actual2 = RecordBatch::read(&mut Cursor::new(data2)).unwrap();
        assert_eq!(actual2, expected);
    }

    #[cfg(feature = "compression-snappy")]
    #[test]
    fn test_decode_fixture_snappy() {
        // This data was obtained by watching rdkafka.
        let data = [
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x58\x00\x00\x00\x00".to_vec(),
            b"\x02\xad\x86\xf4\xf4\x00\x02\x00\x00\x00\x00\x00\x00\x01\x7e\xb6".to_vec(),
            b"\x45\x0e\x52\x00\x00\x01\x7e\xb6\x45\x0e\x52\xff\xff\xff\xff\xff".to_vec(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01\x80\x01\x1c".to_vec(),
            b"\xfc\x01\x00\x00\x00\xc8\x01\x78\xfe\x01\x00\x8a\x01\x00\x50\x16".to_vec(),
            b"\x68\x65\x6c\x6c\x6f\x20\x6b\x61\x66\x6b\x61\x02\x06\x66\x6f\x6f".to_vec(),
            b"\x06\x62\x61\x72".to_vec(),
        ]
        .concat();

        let actual = RecordBatch::read(&mut Cursor::new(data)).unwrap();
        let expected = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: 0,
            first_timestamp: 1643735486034,
            max_timestamp: 1643735486034,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: ControlBatchOrRecords::Records(vec![Record {
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(vec![b'x'; 100]),
                value: Some(b"hello kafka".to_vec()),
                headers: vec![RecordHeader {
                    key: "foo".to_owned(),
                    value: b"bar".to_vec(),
                }],
            }]),
            compression: RecordBatchCompression::Snappy,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        };
        assert_eq!(actual, expected);

        let mut data2 = vec![];
        actual.write(&mut data2).unwrap();

        // don't compare if the data is equal because compression encoder might work slightly differently, use another
        // roundtrip instead
        let actual2 = RecordBatch::read(&mut Cursor::new(data2)).unwrap();
        assert_eq!(actual2, expected);
    }

    #[cfg(feature = "compression-snappy")]
    #[test]
    fn test_decode_fixture_snappy_java() {
        // This data was obtained by watching Kafka returning a recording to rskafka that was produced by the official
        // Java client.
        let data = [
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x8c\x00\x00\x00\x00".to_vec(),
            b"\x02\x79\x1e\x2d\xce\x00\x02\x00\x00\x00\x01\x00\x00\x01\x7f\x07".to_vec(),
            b"\x25\x7a\xb1\x00\x00\x01\x7f\x07\x25\x7a\xb1\xff\xff\xff\xff\xff".to_vec(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x02\x82\x53\x4e".to_vec(),
            b"\x41\x50\x50\x59\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00".to_vec(),
            b"\x47\xff\x01\x1c\xfc\x01\x00\x00\x00\xc8\x01\x78\xfe\x01\x00\x8a".to_vec(),
            b"\x01\x00\x64\x16\x68\x65\x6c\x6c\x6f\x20\x6b\x61\x66\x6b\x61\x02".to_vec(),
            b"\x06\x66\x6f\x6f\x06\x62\x61\x72\xfa\x01\x00\x00\x02\xfe\x80\x00".to_vec(),
            b"\x96\x80\x00\x4c\x14\x73\x6f\x6d\x65\x20\x76\x61\x6c\x75\x65\x02".to_vec(),
            b"\x06\x66\x6f\x6f\x06\x62\x61\x72".to_vec(),
        ]
        .concat();

        let actual = RecordBatch::read(&mut Cursor::new(data)).unwrap();
        let expected = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: 1,
            first_timestamp: 1645092371121,
            max_timestamp: 1645092371121,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: ControlBatchOrRecords::Records(vec![
                Record {
                    timestamp_delta: 0,
                    offset_delta: 0,
                    key: Some(vec![b'x'; 100]),
                    value: Some(b"hello kafka".to_vec()),
                    headers: vec![RecordHeader {
                        key: "foo".to_owned(),
                        value: b"bar".to_vec(),
                    }],
                },
                Record {
                    timestamp_delta: 0,
                    offset_delta: 1,
                    key: Some(vec![b'x'; 100]),
                    value: Some(b"some value".to_vec()),
                    headers: vec![RecordHeader {
                        key: "foo".to_owned(),
                        value: b"bar".to_vec(),
                    }],
                },
            ]),
            compression: RecordBatchCompression::Snappy,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        };
        assert_eq!(actual, expected);

        let mut data2 = vec![];
        actual.write(&mut data2).unwrap();

        // don't compare if the data is equal because compression encoder might work slightly differently, use another
        // roundtrip instead
        let actual2 = RecordBatch::read(&mut Cursor::new(data2)).unwrap();
        assert_eq!(actual2, expected);
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_decode_fixture_zstd() {
        // This data was obtained by watching rdkafka.
        let data = [
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x5d\x00\x00\x00\x00".to_vec(),
            b"\x02\xa1\x6e\x4e\x95\x00\x04\x00\x00\x00\x00\x00\x00\x01\x7e\xbf".to_vec(),
            b"\x78\xf3\xad\x00\x00\x01\x7e\xbf\x78\xf3\xad\xff\xff\xff\xff\xff".to_vec(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01\x28\xb5\x2f".to_vec(),
            b"\xfd\x00\x58\x1d\x01\x00\xe8\xfc\x01\x00\x00\x00\xc8\x01\x78\x16".to_vec(),
            b"\x68\x65\x6c\x6c\x6f\x20\x6b\x61\x66\x6b\x61\x02\x06\x66\x6f\x6f".to_vec(),
            b"\x06\x62\x61\x72\x01\x00\x20\x05\x5c".to_vec(),
        ]
        .concat();

        let actual = RecordBatch::read(&mut Cursor::new(data)).unwrap();
        let expected = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: 0,
            first_timestamp: 1643889882029,
            max_timestamp: 1643889882029,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: ControlBatchOrRecords::Records(vec![Record {
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(vec![b'x'; 100]),
                value: Some(b"hello kafka".to_vec()),
                headers: vec![RecordHeader {
                    key: "foo".to_owned(),
                    value: b"bar".to_vec(),
                }],
            }]),
            compression: RecordBatchCompression::Zstd,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        };
        assert_eq!(actual, expected);

        let mut data2 = vec![];
        actual.write(&mut data2).unwrap();

        // don't compare if the data is equal because compression encoder might work slightly differently, use another
        // roundtrip instead
        let actual2 = RecordBatch::read(&mut Cursor::new(data2)).unwrap();
        assert_eq!(actual2, expected);
    }

    #[test]
    fn test_decode_fixture_null_key() {
        // This data was obtained by watching rdkafka driven by IOx.
        let data = [
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x1a\x00\x00\x00\x00".to_vec(),
            b"\x02\x67\x98\xb9\x54\x00\x00\x00\x00\x00\x00\x00\x00\x01\x7e\xbe".to_vec(),
            b"\xdc\x91\xf6\x00\x00\x01\x7e\xbe\xdc\x91\xf6\xff\xff\xff\xff\xff".to_vec(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01\xce\x03\x00".to_vec(),
            b"\x00\x00\x01\xce\x01\x0a\x65\x0a\x2f\x74\x65\x73\x74\x5f\x74\x6f".to_vec(),
            b"\x70\x69\x63\x5f\x33\x37\x33\x39\x38\x66\x38\x64\x2d\x39\x35\x66".to_vec(),
            b"\x38\x2d\x34\x34\x65\x65\x2d\x38\x33\x61\x34\x2d\x34\x64\x30\x63".to_vec(),
            b"\x35\x39\x32\x62\x34\x34\x36\x64\x12\x32\x0a\x03\x75\x70\x63\x12".to_vec(),
            b"\x17\x0a\x04\x75\x73\x65\x72\x10\x03\x1a\x0a\x12\x08\x00\x00\x00".to_vec(),
            b"\x00\x00\x00\xf0\x3f\x22\x01\x00\x12\x10\x0a\x04\x74\x69\x6d\x65".to_vec(),
            b"\x10\x04\x1a\x03\x0a\x01\x64\x22\x01\x00\x18\x01\x04\x18\x63\x6f".to_vec(),
            b"\x6e\x74\x65\x6e\x74\x2d\x74\x79\x70\x65\xa4\x01\x61\x70\x70\x6c".to_vec(),
            b"\x69\x63\x61\x74\x69\x6f\x6e\x2f\x78\x2d\x70\x72\x6f\x74\x6f\x62".to_vec(),
            b"\x75\x66\x3b\x20\x73\x63\x68\x65\x6d\x61\x3d\x22\x69\x6e\x66\x6c".to_vec(),
            b"\x75\x78\x64\x61\x74\x61\x2e\x69\x6f\x78\x2e\x77\x72\x69\x74\x65".to_vec(),
            b"\x5f\x62\x75\x66\x66\x65\x72\x2e\x76\x31\x2e\x57\x72\x69\x74\x65".to_vec(),
            b"\x42\x75\x66\x66\x65\x72\x50\x61\x79\x6c\x6f\x61\x64\x22\x1a\x69".to_vec(),
            b"\x6f\x78\x2d\x6e\x61\x6d\x65\x73\x70\x61\x63\x65\x12\x6e\x61\x6d".to_vec(),
            b"\x65\x73\x70\x61\x63\x65".to_vec(),
        ]
        .concat();

        let actual = RecordBatch::read(&mut Cursor::new(data.clone())).unwrap();
        let expected = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: 0,
            first_timestamp: 1643879633398,
            max_timestamp: 1643879633398,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: ControlBatchOrRecords::Records(vec![Record {
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(vec![
                    10, 101, 10, 47, 116, 101, 115, 116, 95, 116, 111, 112, 105, 99, 95, 51, 55,
                    51, 57, 56, 102, 56, 100, 45, 57, 53, 102, 56, 45, 52, 52, 101, 101, 45, 56,
                    51, 97, 52, 45, 52, 100, 48, 99, 53, 57, 50, 98, 52, 52, 54, 100, 18, 50, 10,
                    3, 117, 112, 99, 18, 23, 10, 4, 117, 115, 101, 114, 16, 3, 26, 10, 18, 8, 0, 0,
                    0, 0, 0, 0, 240, 63, 34, 1, 0, 18, 16, 10, 4, 116, 105, 109, 101, 16, 4, 26, 3,
                    10, 1, 100, 34, 1, 0, 24, 1,
                ]),
                headers: vec![
                    RecordHeader {
                        key: "content-type".to_owned(),
                        value: br#"application/x-protobuf; schema="influxdata.iox.write_buffer.v1.WriteBufferPayload""#.to_vec(),
                    },
                    RecordHeader {
                        key: "iox-namespace".to_owned(),
                        value: b"namespace".to_vec(),
                    },
                ],
            }]),
            compression: RecordBatchCompression::NoCompression,
            is_transactional: false,
            timestamp_type: RecordBatchTimestampType::CreateTime,
        };
        assert_eq!(actual, expected);

        let mut data2 = vec![];
        actual.write(&mut data2).unwrap();
        assert_eq!(data, data2);
    }
}
