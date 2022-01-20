//! The actual payload message which is also the on-disk format for Kafka.
//!
/// The format evolved twice in [KIP-32] and [KIP-98]. We only support the latest generation (message version 2).
///
/// # References
/// - [KIP-32]
/// - [KIP-98]
/// - <https://kafka.apache.org/documentation/#messageformat>
///
/// [KIP-32]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
/// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
use std::io::{Cursor, Read, Write};

use crc::{Crc, CRC_32_ISCSI};

#[cfg(test)]
use proptest::prelude::*;

use super::{
    primitives::{Array, ArrayRef, Int16, Int32, Int64, Int8, Varint, Varlong},
    traits::{ReadError, ReadType, WriteError, WriteType},
    vec_builder::VecBuilder,
};

/// CRC used to check payload data.
///
/// # References
/// - <https://kafka.apache.org/documentation/#recordbatch>
/// - <https://docs.oracle.com/javase/9/docs/api/java/util/zip/CRC32C.html>
/// - <https://reveng.sourceforge.io/crc-catalogue/all.htm>
const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

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
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub headers: Vec<RecordHeader>,
}

impl<R> ReadType<R> for Record
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // length
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let reader = &mut reader.take(len as u64);

        // attributes
        Int8::read(reader)?;

        // timestampDelta
        let timestamp_delta = Varlong::read(reader)?.0;

        // offsetDelta
        let offset_delta = Varint::read(reader)?.0;

        // key
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut key = VecBuilder::new(len);
        key = key.read_exact(reader)?;
        let key = key.into();

        // value
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut value = VecBuilder::new(len);
        value = value.read_exact(reader)?;
        let value = value.into();

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
        let l = i32::try_from(self.key.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Varint(l).write(&mut data)?;
        data.write_all(&self.key)?;

        // value
        let l = i32::try_from(self.value.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Varint(l).write(&mut data)?;
        data.write_all(&self.value)?;

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
        let actual_crc = CASTAGNOLI.checksum(&data);
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
        let bytes_total = data.into_inner().len() as u64;
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
        let crc = CASTAGNOLI.checksum(&data);
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
        if !matches!(compression, RecordBatchCompression::NoCompression) {
            // TODO: implement compression
            return Err(ReadError::Malformed(
                format!("Unimplemented compression: {:?}", compression).into(),
            ));
        }
        let records = if is_control {
            let mut records = Array::<ControlBatchRecord>::read(reader)?
                .0
                .unwrap_or_default();
            if records.len() != 1 {
                return Err(ReadError::Malformed(
                    format!("Expected 1 control record but got {}", records.len()).into(),
                ));
            }
            ControlBatchOrRecords::ControlBatch(records.pop().expect("Just checked the size"))
        } else {
            let records = Array::<Record>::read(reader)?.0.unwrap_or_default();
            ControlBatchOrRecords::Records(records)
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
        if !matches!(self.compression, RecordBatchCompression::NoCompression) {
            // TODO: implement compression
            return Err(WriteError::Malformed(
                format!("Unimplemented compression: {:?}", self.compression).into(),
            ));
        }
        match &self.records {
            ControlBatchOrRecords::ControlBatch(control_batch) => {
                let records = vec![*control_batch];
                Array::<ControlBatchRecord>(Some(records)).write(writer)?;
            }
            ControlBatchOrRecords::Records(records) => {
                ArrayRef::<Record>(Some(records)).write(writer)?;
            }
        }

        Ok(())
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
    fn test_decode_fixture() {
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
                key: vec![],
                value: b"hello kafka".to_vec(),
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
}
