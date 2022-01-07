use std::collections::BTreeMap;

use time::OffsetDateTime;

/// High-level record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub headers: BTreeMap<String, Vec<u8>>,
    pub timestamp: OffsetDateTime,
}

impl Record {
    /// Returns the approximate uncompressed size of this [`Record`]
    pub fn approximate_size(&self) -> usize {
        self.key.len()
            + self.value.len()
            + self
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }
}


/// Record that has offset information attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordAndOffset {
    pub record: Record,
    pub offset: i64,
}
