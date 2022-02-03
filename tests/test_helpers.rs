use parking_lot::Once;
use rskafka::record::Record;
use std::collections::BTreeMap;
use time::OffsetDateTime;
/// Get the testing Kafka connection string or return current scope.
///
/// If `TEST_INTEGRATION` and `KAFKA_CONNECT` are set, return the Kafka connection URL to the
/// caller.
///
/// If `TEST_INTEGRATION` is set but `KAFKA_CONNECT` is not set, fail the tests and provide
/// guidance for setting `KAFKA_CONNECTION`.
///
/// If `TEST_INTEGRATION` is not set, skip the calling test by returning early.
#[macro_export]
macro_rules! maybe_skip_kafka_integration {
    () => {{
        use std::env;
        dotenv::dotenv().ok();

        match (
            env::var("TEST_INTEGRATION").is_ok(),
            env::var("KAFKA_CONNECT").ok(),
        ) {
            (true, Some(kafka_connection)) => kafka_connection,
            (true, None) => {
                panic!(
                    "TEST_INTEGRATION is set which requires running integration tests, but \
                    KAFKA_CONNECT is not set. Please run Kafka or Redpanda then \
                    set KAFKA_CONNECT as directed in README.md."
                )
            }
            (false, Some(_)) => {
                eprintln!("skipping Kafka integration tests - set TEST_INTEGRATION to run");
                return;
            }
            (false, None) => {
                eprintln!(
                    "skipping Kafka integration tests - set TEST_INTEGRATION and KAFKA_CONNECT to \
                    run"
                );
                return;
            }
        }
    }};
}

/// Generated random topic name for testing.
pub fn random_topic_name() -> String {
    format!("test_topic_{}", uuid::Uuid::new_v4())
}

pub fn record() -> Record {
    Record {
        key: Some(b"".to_vec()),
        value: Some(b"hello kafka".to_vec()),
        headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
        timestamp: now(),
    }
}

/// UTC "now" w/o nanoseconds
///
/// This is required because Kafka doesn't support such fine-grained resolution.
pub fn now() -> OffsetDateTime {
    let x = OffsetDateTime::now_utc().unix_timestamp_nanos();
    OffsetDateTime::from_unix_timestamp_nanos((x / 1_000_000) * 1_000_000).unwrap()
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
