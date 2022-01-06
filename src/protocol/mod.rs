//! The Apache Kafka protocol.
//!
//! # References
//! - <https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_feature.c#L52-L212>
//! - <https://kafka.apache.org/protocol>
//! - <https://kafka.apache.org/documentation>
pub mod api_key;
pub mod api_version;
pub mod error;
pub mod frame;
pub mod messages;
pub mod primitives;
pub mod record;
#[cfg(test)]
pub mod test_utils;
pub mod traits;
