//! The Apache Kafka protocol.
//!
//! # References
//! - <https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_feature.c#L52-L212>
//! - <https://kafka.apache.org/protocol>
//! - <https://kafka.apache.org/documentation>
//! - <https://github.com/twmb/franz-go/tree/858592494064d5a6bef4b622a567183a39932712/generate/definitions>
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
