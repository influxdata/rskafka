//! Helpers to implement throttling within the Kafka protocol.

use std::time::Duration;

use tracing::warn;

use crate::{backoff::ErrorOrThrottle, protocol::primitives::Int32};

pub fn maybe_throttle<E>(throttle_time_ms: Option<Int32>) -> Result<(), ErrorOrThrottle<E>>
where
    E: std::error::Error + Send,
{
    let throttle_time_ms = throttle_time_ms.map(|t| t.0).unwrap_or_default();
    let throttle_time_ms: u64 = match throttle_time_ms.try_into() {
        Ok(t) => t,
        Err(_) => {
            warn!(throttle_time_ms, "Invalid throttle time",);
            return Ok(());
        }
    };

    if throttle_time_ms == 0 {
        return Ok(());
    }

    let duration = Duration::from_millis(throttle_time_ms);
    Err(ErrorOrThrottle::Throttle(duration))
}
