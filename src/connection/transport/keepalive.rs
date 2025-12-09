use std::time::Duration;

#[derive(Debug, Clone, Copy, Default)]
pub struct KeepaliveConfig {
    /// The amount of time after which TCP keepalive probes will be sent on
    /// idle connections.
    pub time: Option<Duration>,

    /// The time interval between TCP keepalive probes.
    pub interval: Option<Duration>,

    /// The value of the `TCP_KEEPCNT` option.
    pub retries: Option<u32>,
}
