use minikafka::client::Client;

/// Get the testing Kafka connection string or return current scope.
///
/// If `TEST_INTEGRATION` and `KAFKA_CONNECT` are set, return the Kafka connection URL to the
/// caller.
///
/// If `TEST_INTEGRATION` is set but `KAFKA_CONNECT` is not set, fail the tests and provide
/// guidance for setting `KAFKA_CONNECTION`.
///
/// If `TEST_INTEGRATION` is not set, skip the calling test by returning early.
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
                    KAFKA_CONNECT is not set. Please run Kafka, perhaps by using the command \
                    `docker-compose -f docker/ci-kafka-docker-compose.yml up kafka`, then \
                    set KAFKA_CONNECT to the host and port where Kafka is accessible. If \
                    running the `docker-compose` command and the Rust tests on the host, the \
                    value for `KAFKA_CONNECT` should be `localhost:9093`. If running the Rust \
                    tests in another container in the `docker-compose` network as on CI, \
                    `KAFKA_CONNECT` should be `kafka:9092`."
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

#[tokio::test]
async fn test_connect() {
    let connection = maybe_skip_kafka_integration!();
    Client::new(vec![connection]).await;
}
