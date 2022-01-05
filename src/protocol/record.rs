use std::io::{Cursor, Read, Write};

use crc::{Crc, CRC_32_ISO_HDLC};

use super::{
    primitives::{Array, Int16, Int32, Int64, Int8, Varint, Varlong},
    traits::{ReadError, ReadType, WriteError, WriteType},
};

const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// Record Header
///
/// # References
/// - <https://kafka.apache.org/documentation/#recordheader>
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct RecordHeader {
    key: String,
    value: Vec<u8>,
}

impl<R> ReadType<R> for RecordHeader
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // key
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut buf = vec![0; len];
        reader.read_exact(&mut buf)?;
        let key = String::from_utf8(buf).map_err(|e| ReadError::Malformed(Box::new(e)))?;

        // value
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut value = vec![0; len];
        reader.read_exact(&mut value)?;

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
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Record {
    timestamp_delta: i64,
    offset_delta: i32,
    key: Vec<u8>,
    value: Vec<u8>,
    headers: Vec<RecordHeader>,
}

impl<R> ReadType<R> for Record
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // length
        Varint::read(reader)?;

        // attributes
        Int8::read(reader)?;

        // timestampDelta
        let timestamp_delta = Varlong::read(reader)?.0;

        // offsetDelta
        let offset_delta = Varint::read(reader)?.0;

        // key
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut key = vec![0; len];
        reader.read_exact(&mut key)?;

        // value
        let len = Varint::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut value = vec![0; len];
        reader.read_exact(&mut value)?;

        // headers
        let headers = Array::<RecordHeader>::read(reader)?.0.unwrap_or_default();

        Ok(Self {
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
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
        Array(Some(self.headers.clone())).write(&mut data)?;

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
    base_offset: i64,
    partition_leader_epoch: i32,
    last_offset_delta: i32,
    first_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: ControlBatchOrRecords,
    compression: RecordBatchCompression,
    is_transactional: bool,
    timestamp_type: RecordBatchTimestampType,
}

impl<R> ReadType<R> for RecordBatch
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        // baseOffset
        let base_offset = Int64::read(reader)?.0;

        // batchLength
        let len = Int32::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;

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
        let mut data = vec![0; len];
        reader.read_exact(&mut data)?;
        let actual_crc = CASTAGNOLI.checksum(&data);
        if crc != actual_crc {
            return Err(ReadError::Malformed(
                format!("CRC error, got {}, expected {}", actual_crc, crc).into(),
            ));
        }

        // ==========================================================================================
        // ======================================== CRC data ========================================
        let mut data = Cursor::new(data);

        // attributes
        let attributes = Int16::read(&mut data)?.0;
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
        let last_offset_delta = Int32::read(&mut data)?.0;

        // firstTimestamp
        let first_timestamp = Int64::read(&mut data)?.0;

        // maxTimestamp
        let max_timestamp = Int64::read(&mut data)?.0;

        // producerId
        let producer_id = Int64::read(&mut data)?.0;

        // producerEpoch
        let producer_epoch = Int16::read(&mut data)?.0;

        // baseSequence
        let base_sequence = Int32::read(&mut data)?.0;

        // records
        if !matches!(compression, RecordBatchCompression::NoCompression) {
            // TODO: implement compression
            return Err(ReadError::Malformed(
                format!("Unimplemented compression: {:?}", compression).into(),
            ));
        }
        let records = if is_control {
            let mut records = Array::<ControlBatchRecord>::read(&mut data)?
                .0
                .unwrap_or_default();
            if records.len() != 1 {
                return Err(ReadError::Malformed(
                    format!("Expected 1 control record but got {}", records.len()).into(),
                ));
            }
            ControlBatchOrRecords::ControlBatch(records.pop().expect("Just checked the size"))
        } else {
            let records = Array::<Record>::read(&mut data)?.0.unwrap_or_default();
            ControlBatchOrRecords::Records(records)
        };

        // ==========================================================================================
        // ==========================================================================================

        Ok(Self {
            base_offset,
            partition_leader_epoch,
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

impl<W> WriteType<W> for RecordBatch
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // ==========================================================================================
        // ======================================== CRC data ========================================
        // collect everything that should be part of the CRC calculation
        let mut data = vec![];

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
        Int16(attributes).write(&mut data)?;

        // lastOffsetDelta
        Int32(self.last_offset_delta).write(&mut data)?;

        // firstTimestamp
        Int64(self.first_timestamp).write(&mut data)?;

        // maxTimestamp
        Int64(self.max_timestamp).write(&mut data)?;

        // producerId
        Int64(self.producer_id).write(&mut data)?;

        // producerEpoch
        Int16(self.producer_epoch).write(&mut data)?;

        // baseSequence
        Int32(self.base_sequence).write(&mut data)?;

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
                Array::<ControlBatchRecord>(Some(records)).write(&mut data)?;
            }
            ControlBatchOrRecords::Records(records) => {
                Array::<Record>(Some(records.clone())).write(&mut data)?;
            }
        }

        // ==========================================================================================
        // ==========================================================================================

        // baseOffset
        Int64(self.base_offset).write(writer)?;

        // batchLength
        // Apparently this is all the data AFTER the CRC calculation.
        // See
        // https://github.com/kafka-rust/kafka-rust/blob/657202832806cda77d0a1801d618dc6c382b4d79/src/protocol/produce.rs#L224-L226
        let l = i32::try_from(data.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
        Int32(l).write(writer)?;

        // partitionLeaderEpoch
        Int32(self.partition_leader_epoch).write(writer)?;

        // magic
        Int8(2).write(writer)?;

        // crc
        // See https://github.com/kafka-rust/kafka-rust/blob/a551b6231a7adc9b715552b635a69ac2856ec8a1/src/protocol/mod.rs#L161-L163
        let crc = CASTAGNOLI.checksum(&data);
        let crc = i32::from_be_bytes(crc.to_be_bytes());
        Int32(crc).write(writer)?;

        // the actual CRC-checked data
        writer.write_all(&data)?;

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
        assert_eq!(err.to_string(), "Unknown control batch record version: 1");
    }

    #[test]
    fn test_control_batch_record_unknown_type() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        Int16(0).write(&mut buf).unwrap();
        Int16(2).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = ControlBatchRecord::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(err.to_string(), "Unknown control batch record type: 2");
    }

    test_roundtrip!(RecordBatch, test_record_batch_roundtrip);
}
