use minikafka::{
    client::{error::Error as ClientError, Client},
    record::Record,
    ProtocolError,
};
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
use time::OffsetDateTime;

mod rdkafka_helper;

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

/// Generated random topic name for testing.
fn random_topic_name() -> String {
    format!("test_topic_{}", uuid::Uuid::new_v4())
}

/// UTC "now" w/o nanoseconds
///
/// This is required because Kafka doesn't support such fine-grained resolution.
fn now() -> OffsetDateTime {
    let x = OffsetDateTime::now_utc().unix_timestamp_nanos();
    OffsetDateTime::from_unix_timestamp_nanos((x / 1_000_000) * 1_000_000).unwrap()
}

#[tokio::test]
async fn test_plain() {
    let connection = maybe_skip_kafka_integration!();
    Client::new_plain(vec![connection]).await.unwrap();
}

#[tokio::test]
async fn test_topic_crud() {
    let connection = maybe_skip_kafka_integration!();
    let client = Client::new_plain(vec![connection]).await.unwrap();
    let topics = client.list_topics().await.unwrap();

    let prefix = "test_topic_crud_";

    let mut max_id = 0;
    for topic in topics {
        if let Some(maybe_int) = topic.strip_prefix(prefix) {
            if let Ok(i) = usize::from_str(maybe_int) {
                max_id = max_id.max(i);
            }
        }
    }

    let new_topic = format!("{}{}", prefix, max_id + 1);
    client.create_topic(&new_topic, 1, 1).await.unwrap();

    let topics = client.list_topics().await.unwrap();

    assert!(
        topics.contains(&new_topic),
        "topic {} not found in {:?}",
        new_topic,
        topics
    );

    let err = client.create_topic(&new_topic, 1, 1).await.unwrap_err();
    match err {
        ClientError::ServerError(ProtocolError::TopicAlreadyExists, _) => {}
        _ => panic!("Unexpected error: {}", err),
    }
}

// Disabled as currently no TLS integration tests
#[ignore]
#[tokio::test]
async fn test_tls() {
    let mut root_store = rustls::RootCertStore::empty();

    let file = std::fs::File::open("/tmp/cluster-ca.crt").unwrap();
    let mut reader = std::io::BufReader::new(file);
    match rustls_pemfile::read_one(&mut reader).unwrap().unwrap() {
        rustls_pemfile::Item::X509Certificate(key) => {
            root_store.add(&rustls::Certificate(key)).unwrap();
        }
        _ => unreachable!(),
    }

    let file = std::fs::File::open("/tmp/ca.crt").unwrap();
    let mut reader = std::io::BufReader::new(file);
    let producer_root = match rustls_pemfile::read_one(&mut reader).unwrap().unwrap() {
        rustls_pemfile::Item::X509Certificate(key) => rustls::Certificate(key),
        _ => unreachable!(),
    };

    let file = std::fs::File::open("/tmp/ca.key").unwrap();
    let mut reader = std::io::BufReader::new(file);
    let private_key = match rustls_pemfile::read_one(&mut reader).unwrap().unwrap() {
        rustls_pemfile::Item::PKCS8Key(key) => rustls::PrivateKey(key),
        _ => unreachable!(),
    };

    let config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_single_cert(vec![producer_root], private_key)
        .unwrap();

    let connection = maybe_skip_kafka_integration!();
    Client::new_with_tls(vec![connection], Arc::new(config))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_produce_empty() {
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = Client::new_plain(vec![connection]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();

    let results = client.produce(vec![]).await.unwrap();
    let results = results.unpack().unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_produce_rdkafka_consume_rdkafka() {
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = Client::new_plain(vec![connection.clone()]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();

    let record = record();

    // produce
    rdkafka_helper::produce(&connection, vec![(topic_name.clone(), 1, record.clone())]).await;

    // consume
    let mut records = rdkafka_helper::consume(&connection, &topic_name, 1, 1).await;
    assert_eq!(records.len(), 1);
    let actual = records.pop().unwrap();
    assert_eq!(actual, record);
}

#[tokio::test]
async fn test_produce_minikafka_consume_rdkafka() {
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let record = record();

    // produce
    let client = Client::new_plain(vec![connection.clone()]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();

    let results = client
        .produce(vec![(topic_name.clone(), 1, record.clone())])
        .await
        .unwrap();
    let results = results.unpack().unwrap();
    assert_eq!(results.len(), 1);

    // consume
    let mut records = rdkafka_helper::consume(&connection, &topic_name, 1, 1).await;
    assert_eq!(records.len(), 1);
    let actual = records.pop().unwrap();
    assert_eq!(actual, record);
}

#[tokio::test]
async fn test_produce_rdkafka_consume_minikafka() {
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = Client::new_plain(vec![connection.clone()]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();

    let record = record();

    // produce
    rdkafka_helper::produce(&connection, vec![(topic_name, 1, record)]).await;

    // TODO: consume
}

#[tokio::test]
async fn test_produce_minikafka_consume_minikafka() {
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = Client::new_plain(vec![connection.clone()]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();

    let record = record();

    // produce
    let results = client
        .produce(vec![(topic_name.clone(), 1, record.clone())])
        .await
        .unwrap();
    let results = results.unpack().unwrap();
    assert_eq!(results.len(), 1);

    // TODO: consume
}

#[tokio::test]
async fn test_get_high_watermark() {
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 1;

    let client = Client::new_plain(vec![connection.clone()]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();
    let partition_client = client
        .partition_client(topic_name.clone(), 0)
        .await
        .unwrap();

    // empty partition
    // It might take a while until the partition is visible and doesn't error.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let res = partition_client.get_high_watermark().await;
            match res {
                Ok(res) => {
                    assert_eq!(res, 0);
                    return;
                }
                Err(e) => {
                    println!("Error while fetching watermark, wait a bit: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await
    .unwrap();

    // add some data
    let record = record();
    let results = client
        .produce(vec![(topic_name.clone(), 0, record.clone())])
        .await
        .unwrap();
    let results = results.unpack().unwrap();
    assert_eq!(results.len(), 1);
    let expected = results[0] + 1;

    // this might take a while to converge
    tokio::time::timeout(Duration::from_secs(10), async move {
        loop {
            let res = partition_client.get_high_watermark().await.unwrap();
            if res == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}

fn record() -> Record {
    Record {
        key: b"".to_vec(),
        value: b"hello kafka".to_vec(),
        headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
        timestamp: now(),
    }
}
