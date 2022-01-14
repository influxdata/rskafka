use std::collections::HashMap;

use crate::record::Record;

/// The error returned by [`Aggregator`] implementations
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A type that receives one or more input and returns a single output
pub trait Aggregator: Send {
    /// The unaggregated input.
    type Input: Send;

    /// The aggregated output
    type Output: Send;

    /// De-aggregates the status for successful `produce` operations.
    type StatusDeaggregator: StatusDeaggregator;

    /// Try to append `record` implementations should return
    ///
    /// - `Ok(None)` on success
    /// - `Ok(Some(record))` if there is insufficient capacity in the `Aggregator`
    /// - `Err(_)` if an error is encountered
    ///
    /// [`Aggregator`] must only be modified if this method returns `Ok(None)`
    ///
    fn try_push(&mut self, record: Self::Input, tag: u64) -> Result<Option<Self::Input>, Error>;

    /// Flush the contents of this aggregator to Kafka
    fn flush(&mut self) -> (Self::Output, Self::StatusDeaggregator);
}

/// De-aggregate status for successful `produce` operations.
pub trait StatusDeaggregator: Send + std::fmt::Debug {
    /// The aggregated input status.
    type Input;

    /// The de-aggregated output status.
    type Status;

    /// De-aggregate status.
    fn build(&self, input: Self::Input, tag: u64) -> Result<Self::Status, Error>;
}

/// Helper trait to access the status of an [`Aggregator`].
pub trait AggregatorStatus {
    type Status;
}

impl<T> AggregatorStatus for T
where
    T: Aggregator,
{
    type Status = <<Self as Aggregator>::StatusDeaggregator as StatusDeaggregator>::Status;
}

#[derive(Debug, Default)]
struct AggregatorState {
    batch_size: usize,
    records: Vec<Record>,
    reverse_mapping: HashMap<u64, usize>,
}

/// a [`Aggregator`] that batches up to a certain number of bytes of [`Record`]
#[derive(Debug)]
pub struct RecordAggregator {
    max_batch_size: usize,
    state: AggregatorState,
}

impl Aggregator for RecordAggregator {
    type Input = Record;
    type Output = Vec<Record>;
    type StatusDeaggregator = RecordAggregatorStatusBuilder;

    fn try_push(&mut self, record: Self::Input, tag: u64) -> Result<Option<Self::Input>, Error> {
        let record_size: usize = record.approximate_size();

        if self.state.batch_size + record_size > self.max_batch_size {
            return Ok(Some(record));
        }

        self.state.batch_size += record_size;
        self.state
            .reverse_mapping
            .insert(tag, self.state.records.len());
        self.state.records.push(record);

        Ok(None)
    }

    fn flush(&mut self) -> (Self::Output, Self::StatusDeaggregator) {
        let state = std::mem::take(&mut self.state);
        (
            state.records,
            RecordAggregatorStatusBuilder {
                reverse_mapping: state.reverse_mapping,
            },
        )
    }
}

impl RecordAggregator {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            state: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct RecordAggregatorStatusBuilder {
    reverse_mapping: HashMap<u64, usize>,
}

impl StatusDeaggregator for RecordAggregatorStatusBuilder {
    type Input = Vec<i64>;

    type Status = i64;

    fn build(&self, input: Self::Input, tag: u64) -> Result<Self::Status, Error> {
        let pos = self.reverse_mapping.get(&tag).expect("invalid tag");
        Ok(input[*pos])
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
        assert!(aggregator.try_push(r1.clone(), 0).unwrap().is_none());
        assert!(aggregator.try_push(r1.clone(), 1).unwrap().is_none());

        // Cannot add more data once full
        assert!(aggregator.try_push(r1.clone(), 2).unwrap().is_some());
        assert!(aggregator.try_push(r1.clone(), 3).unwrap().is_some());

        // flush two records
        let (records, reverse_mapper) = aggregator.flush();
        assert_eq!(records.len(), 2);
        assert_eq!(reverse_mapper.build(vec![10, 20], 0).unwrap(), 10);
        assert_eq!(reverse_mapper.build(vec![10, 20], 1).unwrap(), 20);

        // Test early flush
        assert!(aggregator.try_push(r1.clone(), 4).unwrap().is_none());
        let (records, reverse_mapper) = aggregator.flush();
        assert_eq!(records.len(), 1);
        assert_eq!(reverse_mapper.build(vec![10], 4).unwrap(), 10);

        // next flush has full capacity again
        assert!(aggregator.try_push(r1.clone(), 5).unwrap().is_none());
        assert!(aggregator.try_push(r1.clone(), 6).unwrap().is_none());

        let (records, reverse_mapper) = aggregator.flush();
        assert_eq!(records.len(), 2);
        assert_eq!(reverse_mapper.build(vec![10, 20], 5).unwrap(), 10);
        assert_eq!(reverse_mapper.build(vec![10, 20], 6).unwrap(), 20);

        // Test empty flush
        let (records, _reverse_mapper) = aggregator.flush();
        assert_eq!(records.len(), 0);

        // Test flush to make space for larger record
        assert!(aggregator.try_push(r1.clone(), 7).unwrap().is_none());
        assert!(aggregator.try_push(r2.clone(), 8).unwrap().is_some());
        assert_eq!(aggregator.flush().0.len(), 1);
        assert!(aggregator.try_push(r2.clone(), 9).unwrap().is_none());

        // Test too large record
        let mut aggregator = RecordAggregator::new(r1.approximate_size());
        assert!(aggregator.try_push(r2, 10).unwrap().is_some());
    }
}
