use crate::record::Record;

/// The error returned by [`Aggregator`] implementations
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Return value of [Aggregator::try_push].
#[derive(Debug)]
pub enum TryPush<I, T> {
    /// Insufficient capacity.
    ///
    /// Return [`Input`](Aggregator::Input) back to caller.
    NoCapacity(I),

    /// Aggregated input.
    ///
    /// Return tag to allow retrieval of [`Status`](StatusDeaggregator::Status) from [`StatusDeaggregator`].
    Aggregated(T),
}

impl<I, T> TryPush<I, T> {
    pub fn unwrap_input(self) -> I {
        match self {
            Self::NoCapacity(input) => input,
            Self::Aggregated(_) => panic!("Aggregated"),
        }
    }

    pub fn unwrap_tag(self) -> T {
        match self {
            Self::NoCapacity(_) => panic!("NoCapacity"),
            Self::Aggregated(tag) => tag,
        }
    }
}

/// A type that receives one or more input and returns a single output
pub trait Aggregator: Send {
    /// The unaggregated input.
    type Input: Send;

    /// Tag used to deaggregate status.
    type Tag: Send;

    /// De-aggregates the status for successful `produce` operations.
    type StatusDeaggregator: StatusDeaggregator<Tag = Self::Tag>;

    /// Try to append `record` implementations should return
    ///
    /// - `Ok(TryPush::Aggregated(_))` on success
    /// - `Ok(TryPush::NoCapacity(_))` if there is insufficient capacity in the `Aggregator`
    /// - `Err(_)` if an error is encountered
    ///
    /// [`Aggregator`] must only be modified if this method returns `Ok(None)`
    ///
    fn try_push(&mut self, record: Self::Input) -> Result<TryPush<Self::Input, Self::Tag>, Error>;

    /// Flush the contents of this aggregator to Kafka
    fn flush(&mut self) -> Result<(Vec<Record>, Self::StatusDeaggregator), Error>;
}

/// De-aggregate status for successful `produce` operations.
pub trait StatusDeaggregator: Send + Sync + std::fmt::Debug {
    /// The de-aggregated output status.
    type Status;

    /// Tag used to deaggregate status.
    type Tag: Send;

    /// De-aggregate status.
    fn deaggregate(&self, input: &[i64], tag: Self::Tag) -> Result<Self::Status, Error>;
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
}

/// a [`Aggregator`] that batches up to a certain number of bytes of [`Record`]
#[derive(Debug)]
pub struct RecordAggregator {
    max_batch_size: usize,
    state: AggregatorState,
}

impl Aggregator for RecordAggregator {
    type Input = Record;
    type Tag = usize;
    type StatusDeaggregator = RecordAggregatorStatusDeaggregator;

    fn try_push(&mut self, record: Self::Input) -> Result<TryPush<Self::Input, Self::Tag>, Error> {
        let record_size: usize = record.approximate_size();

        if self.state.batch_size + record_size > self.max_batch_size {
            return Ok(TryPush::NoCapacity(record));
        }

        let tag = self.state.records.len();
        self.state.batch_size += record_size;
        self.state.records.push(record);

        Ok(TryPush::Aggregated(tag))
    }

    fn flush(&mut self) -> Result<(Vec<Record>, Self::StatusDeaggregator), Error> {
        let state = std::mem::take(&mut self.state);
        Ok((state.records, RecordAggregatorStatusDeaggregator::default()))
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

#[derive(Debug, Default, Clone, Copy)]
pub struct RecordAggregatorStatusDeaggregator {}

impl StatusDeaggregator for RecordAggregatorStatusDeaggregator {
    type Status = i64;
    type Tag = usize;

    fn deaggregate(&self, input: &[i64], tag: Self::Tag) -> Result<Self::Status, Error> {
        Ok(input[tag])
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
        let t1 = aggregator.try_push(r1.clone()).unwrap().unwrap_tag();
        let t2 = aggregator.try_push(r1.clone()).unwrap().unwrap_tag();

        // Cannot add more data once full
        aggregator.try_push(r1.clone()).unwrap().unwrap_input();
        aggregator.try_push(r1.clone()).unwrap().unwrap_input();

        // flush two records
        let (records, deagg) = aggregator.flush().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(deagg.deaggregate(&[10, 20], t1).unwrap(), 10);
        assert_eq!(deagg.deaggregate(&[10, 20], t2).unwrap(), 20);

        // Test early flush
        let t1 = aggregator.try_push(r1.clone()).unwrap().unwrap_tag();
        let (records, deagg) = aggregator.flush().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(deagg.deaggregate(&[10], t1).unwrap(), 10);

        // next flush has full capacity again
        let t1 = aggregator.try_push(r1.clone()).unwrap().unwrap_tag();
        let t2 = aggregator.try_push(r1.clone()).unwrap().unwrap_tag();

        let (records, deagg) = aggregator.flush().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(deagg.deaggregate(&[10, 20], t1).unwrap(), 10);
        assert_eq!(deagg.deaggregate(&[10, 20], t2).unwrap(), 20);

        // Test empty flush
        let (records, _deagg) = aggregator.flush().unwrap();
        assert_eq!(records.len(), 0);

        // Test flush to make space for larger record
        aggregator.try_push(r1.clone()).unwrap().unwrap_tag();
        aggregator.try_push(r2.clone()).unwrap().unwrap_input();
        assert_eq!(aggregator.flush().unwrap().0.len(), 1);
        aggregator.try_push(r2.clone()).unwrap().unwrap_tag();

        // Test too large record
        let mut aggregator = RecordAggregator::new(r1.approximate_size());
        aggregator.try_push(r2).unwrap().unwrap_input();
    }

    #[test]
    fn test_unwrap_input_ok() {
        assert_eq!(TryPush::<i8, i8>::NoCapacity(42).unwrap_input(), 42,);
    }

    #[test]
    #[should_panic(expected = "Aggregated")]
    fn test_unwrap_input_panic() {
        TryPush::<i8, i8>::Aggregated(42).unwrap_input();
    }

    #[test]
    fn test_unwrap_tag_ok() {
        assert_eq!(TryPush::<i8, i8>::Aggregated(42).unwrap_tag(), 42,);
    }

    #[test]
    #[should_panic(expected = "NoCapacity")]
    fn test_unwrap_tag_panic() {
        TryPush::<i8, i8>::NoCapacity(42).unwrap_tag();
    }
}
