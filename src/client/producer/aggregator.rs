use crate::record::Record;

/// The error returned by [`Aggregator`] implementations
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A type that receives one or more input and returns a single output
pub trait Aggregator {
    type Input;

    type Output;

    /// Try to append `record` implementations should return
    ///
    /// - `Ok(None)` on success
    /// - `Ok(Some(record))` if there is insufficient capacity in the `Aggregator`
    /// - `Err(_)` if an error is encountered
    ///
    /// [`Aggregator`] must only be modified if this method returns `Ok(None)`
    ///
    fn try_push(&mut self, record: Self::Input) -> Result<Option<Self::Input>, Error>;

    /// Flush the contents of this aggregator to Kafka
    fn flush(&mut self) -> Self::Output;
}

/// a [`Aggregator`] that batches up to a certain number of bytes of [`Record`]
#[derive(Debug)]
pub struct RecordAggregator {
    max_batch_size: usize,
    batch_size: usize,
    records: Vec<Record>,
}

impl Aggregator for RecordAggregator {
    type Input = Record;
    type Output = Vec<Record>;

    fn try_push(&mut self, record: Self::Input) -> Result<Option<Self::Input>, Error> {
        let record_size: usize = record.approximate_size();

        if self.batch_size + record_size > self.max_batch_size {
            return Ok(Some(record));
        }

        self.batch_size += record_size;
        self.records.push(record);

        Ok(None)
    }

    fn flush(&mut self) -> Self::Output {
        self.batch_size = 0;
        std::mem::take(&mut self.records)
    }
}

impl RecordAggregator {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            batch_size: 0,
            records: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    #[test]
    fn test_record_aggregator() {
        let r1 = Record {
            key: vec![0; 45],
            value: vec![0; 2],
            headers: Default::default(),
            timestamp: OffsetDateTime::from_unix_timestamp(20).unwrap(),
        };

        let r2 = Record {
            value: vec![0; 34],
            ..r1.clone()
        };

        assert!(r1.approximate_size() < r2.approximate_size());
        assert!(r2.approximate_size() < r2.approximate_size() * 2);

        let mut aggregator = RecordAggregator::new(r1.approximate_size() * 2);
        assert!(aggregator.try_push(r1.clone()).unwrap().is_none());
        assert!(aggregator.try_push(r1.clone()).unwrap().is_none());

        // Cannot add more data once full
        assert!(aggregator.try_push(r1.clone()).unwrap().is_some());
        assert!(aggregator.try_push(r1.clone()).unwrap().is_some());

        assert_eq!(aggregator.flush().len(), 2);

        // Test early flush
        assert!(aggregator.try_push(r1.clone()).unwrap().is_none());
        assert_eq!(aggregator.flush().len(), 1);

        assert!(aggregator.try_push(r1.clone()).unwrap().is_none());
        assert!(aggregator.try_push(r1.clone()).unwrap().is_none());
        assert_eq!(aggregator.flush().len(), 2);

        // Test empty flush
        assert_eq!(aggregator.flush().len(), 0);

        // Test flush to make space for larger record
        assert!(aggregator.try_push(r1.clone()).unwrap().is_none());
        assert!(aggregator.try_push(r2.clone()).unwrap().is_some());
        assert_eq!(aggregator.flush().len(), 1);
        assert!(aggregator.try_push(r2.clone()).unwrap().is_none());

        // Test too large record
        let mut aggregator = RecordAggregator::new(r1.approximate_size());
        assert!(aggregator.try_push(r2).unwrap().is_some());
    }
}
