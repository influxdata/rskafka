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
