//! Primitive types.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_types>
//! - <https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-UnsignedVarints>

use std::io::{Cursor, Read, Write};

#[cfg(test)]
use proptest::prelude::*;

use varint_rs::{VarintReader, VarintWriter};

use super::{
    record::RecordBatch,
    traits::{ReadError, ReadType, WriteError, WriteType},
    vec_builder::VecBuilder,
};

/// Represents a boolean
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Boolean(pub bool);

impl<R> ReadType<R> for Boolean
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            0 => Ok(Self(false)),
            _ => Ok(Self(true)),
        }
    }
}

impl<W> WriteType<W> for Boolean
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self.0 {
            true => Ok(writer.write_all(&[1])?),
            false => Ok(writer.write_all(&[0])?),
        }
    }
}

/// Represents an integer between `-2^7` and `2^7-1` inclusive.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Int8(pub i8);

impl<R> ReadType<R> for Int8
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(Self(i8::from_be_bytes(buf)))
    }
}

impl<W> WriteType<W> for Int8
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

/// Represents an integer between `-2^15` and `2^15-1` inclusive.
///
/// The values are encoded using two bytes in network byte order (big-endian).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Int16(pub i16);

impl<R> ReadType<R> for Int16
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(Self(i16::from_be_bytes(buf)))
    }
}

impl<W> WriteType<W> for Int16
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

/// Represents an integer between `-2^31` and `2^31-1` inclusive.
///
/// The values are encoded using four bytes in network byte order (big-endian).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Int32(pub i32);

impl<R> ReadType<R> for Int32
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(Self(i32::from_be_bytes(buf)))
    }
}

impl<W> WriteType<W> for Int32
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

/// Represents an integer between `-2^63` and `2^63-1` inclusive.
///
/// The values are encoded using eight bytes in network byte order (big-endian).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Int64(pub i64);

impl<R> ReadType<R> for Int64
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(Self(i64::from_be_bytes(buf)))
    }
}

impl<W> WriteType<W> for Int64
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let buf = self.0.to_be_bytes();
        writer.write_all(&buf)?;
        Ok(())
    }
}

/// Represents an integer between `-2^31` and `2^31-1` inclusive.
///
/// Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Varint(pub i32);

impl<R> ReadType<R> for Varint
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        Ok(Self(reader.read_i32_varint()?))
    }
}

impl<W> WriteType<W> for Varint
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        writer.write_i32_varint(self.0)?;
        Ok(())
    }
}

/// Represents an integer between `-2^63` and `2^63-1` inclusive.
///
/// Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Varlong(pub i64);

impl<R> ReadType<R> for Varlong
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        Ok(Self(reader.read_i64_varint()?))
    }
}

impl<W> WriteType<W> for Varlong
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        writer.write_i64_varint(self.0)?;
        Ok(())
    }
}

/// The UNSIGNED_VARINT type describes an unsigned variable length integer.
///
/// To serialize a number as a variable-length integer, you break it up into groups of 7 bits. The lowest 7 bits is
/// written out first, followed by the second-lowest, and so on.  Each time a group of 7 bits is written out, the high
/// bit (bit 8) is cleared if this group is the last one, and set if it is not.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct UnsignedVarint(pub u64);

impl<R> ReadType<R> for UnsignedVarint
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let mut buf = [0u8; 1];
        let mut res: u64 = 0;
        let mut shift = 0;
        loop {
            reader.read_exact(&mut buf)?;
            let c = buf[0];

            res |= ((c as u64) & 0x7f) << shift;
            shift += 7;

            if (c & 0x80) == 0 {
                break;
            }
            if shift > 63 {
                return Err(ReadError::Malformed(
                    String::from("Overflow while reading unsigned varint").into(),
                ));
            }
        }

        Ok(Self(res))
    }
}

impl<W> WriteType<W> for UnsignedVarint
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let mut curr = self.0;
        loop {
            let mut c = (curr & 0x7f) as u8;
            curr >>= 7;
            if curr > 0 {
                c |= 0x80;
            }
            writer.write_all(&[c])?;

            if curr == 0 {
                break;
            }
        }
        Ok(())
    }
}

/// Represents a sequence of characters or null.
///
/// For non-null strings, first the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of
/// the character sequence. A null value is encoded with length of -1 and there are no following bytes.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct NullableString(pub Option<String>);

impl<R> ReadType<R> for NullableString
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = Int16::read(reader)?;
        match len.0 {
            l if l < -1 => Err(ReadError::Malformed(
                format!("Invalid negative length for nullable string: {}", l).into(),
            )),
            -1 => Ok(Self(None)),
            l => {
                let mut buf = vec![0; l as usize];
                reader.read_exact(&mut buf)?;
                let s = String::from_utf8(buf).map_err(|e| ReadError::Malformed(Box::new(e)))?;
                Ok(Self(Some(s)))
            }
        }
    }
}

impl<W> WriteType<W> for NullableString
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match &self.0 {
            Some(s) => {
                let l = i16::try_from(s.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
                Int16(l).write(writer)?;
                writer.write_all(s.as_bytes())?;
                Ok(())
            }
            None => Int16(-1).write(writer),
        }
    }
}

/// Represents a sequence of characters.
///
/// First the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character
/// sequence. Length must not be negative.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct String_(pub String);

impl<R> ReadType<R> for String_
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = Int16::read(reader)?;
        let len = usize::try_from(len.0).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        let mut buf = vec![0; len];
        reader.read_exact(&mut buf)?;
        let s = String::from_utf8(buf).map_err(|e| ReadError::Malformed(Box::new(e)))?;
        Ok(Self(s))
    }
}

impl<W> WriteType<W> for String_
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        let len = i16::try_from(self.0.len()).map_err(WriteError::Overflow)?;
        Int16(len).write(writer)?;
        writer.write_all(self.0.as_bytes())?;
        Ok(())
    }
}

/// Represents a string whose length is expressed as a variable-length integer rather than a fixed 2-byte length.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct CompactString(pub String);

impl<R> ReadType<R> for CompactString
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;
        match len.0 {
            0 => Err(ReadError::Malformed(
                "CompactString must have non-zero length".into(),
            )),
            len => {
                let len = len - 1;
                let mut buf = vec![0; len as usize];
                reader.read_exact(&mut buf)?;
                let s = String::from_utf8(buf).map_err(|e| ReadError::Malformed(Box::new(e)))?;
                Ok(Self(s))
            }
        }
    }
}

impl<W> WriteType<W> for CompactString
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        CompactStringRef(&self.0).write(writer)
    }
}

/// Same as [`CompactString`] but contains referenced data.
///
/// This only supports writing.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompactStringRef<'a>(pub &'a str);

impl<'a, W> WriteType<W> for CompactStringRef<'a>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        UnsignedVarint(self.0.len() as u64 + 1).write(writer)?;
        writer.write_all(self.0.as_bytes())?;
        Ok(())
    }
}

/// Represents a nullable string whose length is expressed as a variable-length integer rather than a fixed 2-byte length.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct CompactNullableString(pub Option<String>);

impl<R> ReadType<R> for CompactNullableString
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;
        match len.0 {
            0 => Ok(Self(None)),
            len => {
                let mut buf = vec![0; len as usize - 1];
                reader.read_exact(&mut buf)?;
                let s = String::from_utf8(buf).map_err(|e| ReadError::Malformed(Box::new(e)))?;
                Ok(Self(Some(s)))
            }
        }
    }
}

impl<W> WriteType<W> for CompactNullableString
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        CompactNullableStringRef(self.0.as_deref()).write(writer)
    }
}

/// Same as [`CompactNullableString`] but contains referenced data.
///
/// This only supports writing.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompactNullableStringRef<'a>(pub Option<&'a str>);

impl<'a, W> WriteType<W> for CompactNullableStringRef<'a>
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match &self.0 {
            Some(s) => {
                UnsignedVarint(s.len() as u64 + 1).write(writer)?;
                writer.write_all(s.as_bytes())?;
            }
            None => {
                UnsignedVarint(0).write(writer)?;
            }
        }
        Ok(())
    }
}

/// Represents a raw sequence of bytes or null.
///
/// For non-null values, first the length N is given as an INT32. Then N bytes follow. A null value is encoded with
/// length of -1 and there are no following bytes.
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct NullableBytes(pub Option<Vec<u8>>);

impl<R> ReadType<R> for NullableBytes
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = Int32::read(reader)?;
        match len.0 {
            l if l < -1 => Err(ReadError::Malformed(
                format!("Invalid negative length for nullable bytes: {}", l).into(),
            )),
            -1 => Ok(Self(None)),
            l => {
                let mut buf = vec![0; l as usize];
                reader.read_exact(&mut buf)?;
                Ok(Self(Some(buf)))
            }
        }
    }
}

impl<W> WriteType<W> for NullableBytes
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match &self.0 {
            Some(s) => {
                let l = i32::try_from(s.len()).map_err(|e| WriteError::Malformed(Box::new(e)))?;
                Int32(l).write(writer)?;
                writer.write_all(s)?;
                Ok(())
            }
            None => Int32(-1).write(writer),
        }
    }
}

/// Represents a section containing optional tagged fields.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct TaggedFields(pub Vec<(UnsignedVarint, Vec<u8>)>);

impl<R> ReadType<R> for TaggedFields
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = UnsignedVarint::read(reader)?;
        let mut res = VecBuilder::new(len.0 as usize);
        for _ in 0..len.0 {
            let tag = UnsignedVarint::read(reader)?;

            let data_len = UnsignedVarint::read(reader)?;
            let mut data_builder = VecBuilder::new(data_len.0 as usize);
            data_builder.read_exact(reader)?;

            res.push((tag, data_builder.into()));
        }
        Ok(Self(res.into()))
    }
}

impl<W> WriteType<W> for TaggedFields
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        UnsignedVarint(self.0.len() as u64).write(writer)?;

        for (tag, data) in &self.0 {
            tag.write(writer)?;
            UnsignedVarint(data.len() as u64).write(writer)?;
            writer.write_all(data)?;
        }

        Ok(())
    }
}

/// Represents a sequence of objects of a given type T.
///
/// Type T can be either a primitive type (e.g. STRING) or a structure. First, the length N is given as an INT32. Then
/// N instances of type T follow. A null array is represented with a length of -1. In protocol documentation an array
/// of T instances is referred to as `[T]`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Array<T>(pub Option<Vec<T>>);

impl<R, T> ReadType<R> for Array<T>
where
    R: Read,
    T: ReadType<R>,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let len = Int32::read(reader)?;
        if len.0 == -1 {
            Ok(Self(None))
        } else {
            let len = usize::try_from(len.0)?;
            let mut res = Vec::with_capacity(len);
            for _ in 0..len {
                res.push(T::read(reader)?);
            }
            Ok(Self(Some(res)))
        }
    }
}

impl<W, T> WriteType<W> for Array<T>
where
    W: Write,
    T: WriteType<W>,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        ArrayRef(self.0.as_deref()).write(writer)
    }
}

/// Same as [`Array`] but contains referenced data.
///
/// This only supports writing.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArrayRef<'a, T>(pub Option<&'a [T]>);

impl<'a, W, T> WriteType<W> for ArrayRef<'a, T>
where
    W: Write,
    T: WriteType<W>,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        match self.0 {
            None => Int32(-1).write(writer),
            Some(inner) => {
                let len = i32::try_from(inner.len())?;
                Int32(len).write(writer)?;

                for element in inner {
                    element.write(writer)?;
                }

                Ok(())
            }
        }
    }
}

/// Represents a sequence of Kafka records as NULLABLE_BYTES.
///
/// This primitive actually depends on the message version and evolved twice in [KIP-32] and [KIP-98]. We only support
/// the latest generation (message version 2).
///
/// It seems that during `Produce` this must contain exactly one batch, but during `Fetch` this can contain zero, one or
/// more batches -- however I could not find any documentation stating this behavior.
///
/// [KIP-32]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
/// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Records(
    // tell proptest to only generate small vectors, otherwise tests take forever
    #[cfg_attr(
        test,
        proptest(strategy = "prop::collection::vec(any::<RecordBatch>(), 0..2)")
    )]
    pub Vec<RecordBatch>,
);

impl<R> ReadType<R> for Records
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let buf = NullableBytes::read(reader)?.0.unwrap_or_default();
        let len = buf.len() as u64;
        let mut buf = Cursor::new(buf);

        let mut batches = vec![];
        while buf.position() < len {
            batches.push(RecordBatch::read(&mut buf)?);
        }

        Ok(Self(batches))
    }
}

impl<W> WriteType<W> for Records
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        // TODO: it would be nice if we could avoid the copy here by writing the records and then seeking back.
        let mut buf = vec![];
        for record in &self.0 {
            record.write(&mut buf)?;
        }
        NullableBytes(Some(buf)).write(writer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::protocol::test_utils::test_roundtrip;

    use super::*;

    use assert_matches::assert_matches;

    test_roundtrip!(Boolean, test_bool_roundtrip);

    #[test]
    fn test_boolean_decode() {
        assert!(!Boolean::read(&mut Cursor::new(vec![0])).unwrap().0);

        // When reading a boolean value, any non-zero value is considered true.
        for v in [1, 35, 255] {
            assert!(Boolean::read(&mut Cursor::new(vec![v])).unwrap().0);
        }
    }

    test_roundtrip!(Int8, test_int8_roundtrip);

    test_roundtrip!(Int16, test_int16_roundtrip);

    test_roundtrip!(Int32, test_int32_roundtrip);

    test_roundtrip!(Int64, test_int64_roundtrip);

    test_roundtrip!(Varint, test_varint_roundtrip);

    #[test]
    fn test_varint_special_values() {
        // Taken from https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
        for v in [0, -1, 1, -2, 2147483647, -2147483648] {
            let mut data = vec![];
            Varint(v).write(&mut data).unwrap();

            let restored = Varint::read(&mut Cursor::new(data)).unwrap();
            assert_eq!(restored.0, v);
        }
    }

    test_roundtrip!(Varlong, test_varlong_roundtrip);

    #[test]
    fn test_varlong_special_values() {
        // Taken from https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints + min/max
        for v in [0, -1, 1, -2, 2147483647, -2147483648, i64::MIN, i64::MAX] {
            let mut data = vec![];
            Varlong(v).write(&mut data).unwrap();

            let restored = Varlong::read(&mut Cursor::new(data)).unwrap();
            assert_eq!(restored.0, v);
        }
    }

    test_roundtrip!(UnsignedVarint, test_unsigned_varint_roundtrip);

    #[test]
    fn test_unsigned_varint_read_overflow() {
        let mut buf = Cursor::new(vec![0xffu8; 64 / 7 + 1]);

        let err = UnsignedVarint::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(err.to_string(), "Overflow while reading unsigned varint");
    }

    test_roundtrip!(String_, test_string_roundtrip);

    test_roundtrip!(NullableString, test_nullable_string_roundtrip);

    #[test]
    fn test_nullable_string_read_negative_length() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        Int16(-2).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = NullableString::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(
            err.to_string(),
            "Invalid negative length for nullable string: -2"
        );
    }

    test_roundtrip!(CompactString, test_compact_string_roundtrip);

    test_roundtrip!(
        CompactNullableString,
        test_compact_nullable_string_roundtrip
    );

    test_roundtrip!(NullableBytes, test_nullable_bytes_roundtrip);

    #[test]
    fn test_nullable_bytes_read_negative_length() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        Int32(-2).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = NullableBytes::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::Malformed(_));
        assert_eq!(
            err.to_string(),
            "Invalid negative length for nullable bytes: -2"
        );
    }

    test_roundtrip!(TaggedFields, test_tagged_fields_roundtrip);

    #[test]
    fn test_tagged_fields_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());

        // number of fields
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();

        // tag
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();

        // data length
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();

        buf.set_position(0);

        let err = TaggedFields::read(&mut buf).unwrap_err();
        assert_matches!(err, ReadError::IO(_));
    }

    test_roundtrip!(Array<Int32>, test_array_roundtrip);

    test_roundtrip!(Records, test_records_roundtrip);
}
