use crate::protocol::primitives::{Int32, Int8};

/// The `replica_id` to use to signify the request is being made by a normal consumer.
pub const NORMAL_CONSUMER: Int32 = Int32(-1);

/// Using `READ_UNCOMMITTED` (`isolation_level = 0`) makes all records visible. With `READ_COMMITTED`
/// (`isolation_level = 1`), non-transactional and `COMMITTED` transactional records are visible. To be more
/// concrete, `READ_COMMITTED` returns all data from offsets smaller than the current LSO (last stable offset), and
/// enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard
/// `ABORTED` transactional records.
///
/// As per [KIP-98] the default is `READ_UNCOMMITTED`.
///
/// Added in version 2.
///
/// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
#[derive(Debug, Clone, Copy)]
pub enum IsolationLevel {
    ReadCommitted,
    ReadUncommitted,
}

impl From<IsolationLevel> for Int8 {
    fn from(isolation_level: IsolationLevel) -> Self {
        match isolation_level {
            IsolationLevel::ReadCommitted => Self(1),
            IsolationLevel::ReadUncommitted => Self(0),
        }
    }
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadUncommitted
    }
}
