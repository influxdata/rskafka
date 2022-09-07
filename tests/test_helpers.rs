use chrono::{TimeZone, Utc};
use parking_lot::Once;
use rskafka::record::Record;
use std::{collections::BTreeMap, time::Duration};

/// Sensible test timeout.
#[allow(dead_code)]
pub const TEST_TIMEOUT: Duration = Duration::from_secs(4);

/// Environment variable to configure if integration tests should be run.
///
/// Accepts a boolean.
pub const ENV_TEST_INTEGRATION: &str = "TEST_INTEGRATION";

/// Environment variable that contains the list of bootstrap brokers.
pub const ENV_KAFKA_CONNECT: &str = "KAFKA_CONNECT";

/// Environment variable that determines which broker implementation we use.
pub const ENV_TEST_BROKER_IMPL: &str = "TEST_BROKER_IMPL";

/// Environment variable that contains the connection string for a SOCKS5 proxy that can be used for testing.
pub const ENV_SOCKS5_PROXY: &str = "SOCKS5_PROXY";

/// Broker implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerImpl {
    Kafka,
    Redpanda,
}

impl BrokerImpl {
    #[allow(dead_code)]
    pub fn supports_deletes(&self) -> bool {
        match self {
            BrokerImpl::Kafka => true,
            // See https://github.com/redpanda-data/redpanda/issues/2648
            BrokerImpl::Redpanda => false,
        }
    }
}

/// Test config.
#[derive(Debug)]
pub struct TestConfig {
    pub bootstrap_brokers: Vec<String>,
    pub broker_impl: BrokerImpl,
    pub socks5_proxy: Option<String>,
}

impl TestConfig {
    /// Get test config from environment.
    pub fn from_env() -> Option<Self> {
        dotenvy::dotenv().ok();

        match std::env::var(ENV_TEST_INTEGRATION)
            .ok()
            .map(|s| parse_as_bool(&s))
        {
            None | Some(Ok(false)) => {
                return None;
            }
            Some(Ok(true)) => {}
            Some(Err(s)) => {
                panic!("Invalid value for {ENV_TEST_INTEGRATION}: {s}")
            }
        }

        let bootstrap_brokers = std::env::var(ENV_KAFKA_CONNECT)
            .ok()
            .unwrap_or_else(|| panic!("{ENV_KAFKA_CONNECT} not set, please read README"))
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();

        let broker_impl = std::env::var(ENV_TEST_BROKER_IMPL)
            .ok()
            .unwrap_or_else(|| panic!("{ENV_TEST_BROKER_IMPL} is required to determine the broker implementation (e.g. kafka, redpanda)"))
            .to_lowercase();
        let broker_impl = match broker_impl.as_str() {
            "kafka" => BrokerImpl::Kafka,
            "redpanda" => BrokerImpl::Redpanda,
            other => panic!("Invalid {ENV_TEST_BROKER_IMPL}: {other}"),
        };

        let socks5_proxy = std::env::var(ENV_SOCKS5_PROXY).ok();

        Some(Self {
            bootstrap_brokers,
            broker_impl,
            socks5_proxy,
        })
    }
}

/// Parse string as boolean variable.
fn parse_as_bool(s: &str) -> Result<bool, String> {
    let s_lower = s.to_lowercase();

    match s_lower.as_str() {
        "0" | "false" | "f" | "no" | "n" => Ok(false),
        "1" | "true" | "t" | "yes" | "y" => Ok(true),
        _ => Err(s.to_owned()),
    }
}

/// Get [`TestConfig`] or exit test (by returning).
///
/// Takes an optional list of capabilities that are needed to run the test. These are:
///
/// - `delete`: the broker implementation must support deletes
/// - `socks5`: a SOCKS5 proxy is available for tests
#[macro_export]
macro_rules! maybe_skip_kafka_integration {
    () => {{
        match test_helpers::TestConfig::from_env() {
            Some(cfg) => cfg,
            None => {
                eprintln!(
                    "skipping Kafka integration tests - set {} to run",
                    test_helpers::ENV_KAFKA_CONNECT
                );
                return;
            }
        }
    }};
    ($cap:ident) => {
        $crate::maybe_skip_kafka_integration!($cap,)
    };
    ($cap:ident, $($other:ident),*,) => {
        $crate::maybe_skip_kafka_integration!($cap, $($other:ident),*)
    };
    (delete, $($other:ident),*) => {{
        let cfg = $crate::maybe_skip_kafka_integration!($($other),*);
        if !cfg.broker_impl.supports_deletes() {
            eprintln!("Skipping due to missing delete support");
            return;
        }
        cfg
    }};
    (socks5, $($other:ident),*) => {{
        let cfg = $crate::maybe_skip_kafka_integration!($($other),*);
        if cfg.socks5_proxy.is_none() {
            eprintln!("skipping integration tests with Proxy - set {} to run", test_helpers::ENV_SOCKS5_PROXY);
            return;
        }
        cfg
    }};
    ($cap:ident, $($other:ident),*) => {
        compile_error!(concat!("invalid capability: ", stringify!($cap)))
    };
}

/// Generated random topic name for testing.
pub fn random_topic_name() -> String {
    format!("test_topic_{}", uuid::Uuid::new_v4())
}

pub fn record(key: &[u8]) -> Record {
    Record {
        key: Some(key.to_vec()),
        value: Some(b"hello kafka".to_vec()),
        headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
        timestamp: Utc.timestamp_millis(1337),
    }
}

static LOG_SETUP: Once = Once::new();

/// Enables debug logging if the `RUST_LOG` environment variable is
/// set. Does nothing if `RUST_LOG` is not set.
pub fn maybe_start_logging() {
    if std::env::var("RUST_LOG").is_ok() {
        start_logging()
    }
}

/// Start logging.
pub fn start_logging() {
    use tracing_log::LogTracer;
    use tracing_subscriber::{filter::EnvFilter, FmtSubscriber};

    LOG_SETUP.call_once(|| {
        LogTracer::init().unwrap();

        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}
