use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

/// High-level record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: BTreeMap<String, Vec<u8>>,
    pub timestamp: DateTime<Utc>,
}

impl Record {
    /// Returns the approximate uncompressed size of this [`Record`]
    pub fn approximate_size(&self) -> usize {
        self.key.as_ref().map(|k| k.len()).unwrap_or_default()
            + self.value.as_ref().map(|v| v.len()).unwrap_or_default()
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
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_approximate_size() {
        let record = Record {
            key: Some(vec![0; 23]),
            value: Some(vec![0; 45]),
            headers: vec![("a".to_string(), vec![0; 5]), ("b".to_string(), vec![0; 7])]
                .into_iter()
                .collect(),
            timestamp: Utc.timestamp_millis(1337),
        };

        assert_eq!(record.approximate_size(), 23 + 45 + 1 + 5 + 1 + 7);
    }
}
