//! Static information that is determined at build time.

/// Default client ID that is used when the user does not specify one.
///
/// Technically we don't need to send a client_id, but newer redpanda version fail to parse the message
/// without it. See <https://github.com/influxdata/rskafka/issues/169>.
pub const DEFAULT_CLIENT_ID: &str = env!("CARGO_PKG_NAME");
