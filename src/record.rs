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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approximate_size() {
        let record = Record {
            key: vec![0; 23],
            value: vec![0; 45],
            headers: vec![("a".to_string(), vec![0; 5]), ("b".to_string(), vec![0; 7])]
                .into_iter()
                .collect(),
            timestamp: OffsetDateTime::now_utc(),
        };

        assert_eq!(record.approximate_size(), 23 + 45 + 1 + 5 + 1 + 7);
    }
}
