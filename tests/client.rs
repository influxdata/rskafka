use minikafka::{
    client::{error::Error as ClientError, partition::PartitionClient, Client},
    record::Record,
    ProtocolError,
};
use std::{str::FromStr, sync::Arc};

mod rdkafka_helper;

mod test_helpers;
use test_helpers::{now, random_topic_name, record};

#[tokio::test]
async fn test_plain() {
    let connection = maybe_skip_kafka_integration!();
    Client::new_plain(vec![connection]).await.unwrap();
}

#[tokio::test]
async fn test_partition_leader() {
    let connection = maybe_skip_kafka_integration!();
    let client = Client::new_plain(vec![connection]).await.unwrap();
    let topic_name = random_topic_name();

    client.create_topic(&topic_name, 2, 1).await.unwrap();
    let client = client.partition_client(&topic_name, 0).await.unwrap();
    client.get_cached_leader().await.unwrap();
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
#[should_panic(expected = "records must be non-empty")]
async fn test_produce_empty() {
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = Client::new_plain(vec![connection]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();

    let partition_client = client.partition_client(&topic_name, 1).await.unwrap();
    partition_client.produce(vec![]).await.unwrap();
}

#[tokio::test]
async fn test_produce_rdkafka_consume_rdkafka() {
    assert_produce_consume(produce_rdkafka, consume_rdkafka).await;
}

#[tokio::test]
async fn test_produce_minikafka_consume_rdkafka() {
    assert_produce_consume(produce_minikafka, consume_rdkafka).await;
}

#[tokio::test]
async fn test_produce_rdkafka_consume_minikafka() {
    assert_produce_consume(produce_rdkafka, consume_minikafka).await;
}

#[tokio::test]
async fn test_produce_minikafka_consume_minikafka() {
    assert_produce_consume(produce_minikafka, consume_minikafka).await;
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

    assert_eq!(partition_client.get_high_watermark().await.unwrap(), 0);

    // add some data
    // use out-of order timestamps to ensure our "lastest offset" logic works
    let record_early = record();
    let record_late = Record {
        timestamp: record_early.timestamp + time::Duration::SECOND,
        ..record_early.clone()
    };
    partition_client
        .produce(vec![record_late.clone()])
        .await
        .unwrap();

    let expected = partition_client
        .produce(vec![record_early.clone()])
        .await
        .unwrap()
        + 1;

    assert_eq!(
        partition_client.get_high_watermark().await.unwrap(),
        expected
    );
}

async fn assert_produce_consume<F1, G1, F2, G2>(f_produce: F1, f_consume: F2)
where
    F1: Fn(Arc<PartitionClient>, String, String, i32, Vec<Record>) -> G1,
    G1: std::future::Future<Output = ()>,
    F2: Fn(Arc<PartitionClient>, String, String, i32, usize) -> G2,
    G2: std::future::Future<Output = Vec<Record>>,
{
    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = Client::new_plain(vec![connection.clone()]).await.unwrap();
    client
        .create_topic(&topic_name, n_partitions, 1)
        .await
        .unwrap();
    let partition_client = Arc::new(
        client
            .partition_client(topic_name.clone(), 1)
            .await
            .unwrap(),
    );

    let record_1 = record();
    let record_2 = Record {
        value: b"some value".to_vec(),
        timestamp: now(),
        ..record_1.clone()
    };
    let record_3 = Record {
        value: b"more value".to_vec(),
        timestamp: now(),
        ..record_1.clone()
    };

    // produce
    f_produce(
        Arc::clone(&partition_client),
        connection.clone(),
        topic_name.clone(),
        1,
        vec![record_1.clone(), record_2.clone()],
    )
    .await;
    f_produce(
        Arc::clone(&partition_client),
        connection.clone(),
        topic_name.clone(),
        1,
        vec![record_3.clone()],
    )
    .await;

    // consume
    let records = f_consume(partition_client, connection, topic_name, 1, 3).await;
    assert_eq!(records, vec![record_1, record_2, record_3]);
}

async fn produce_rdkafka(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    records: Vec<Record>,
) {
    rdkafka_helper::produce(
        &connection,
        records
            .into_iter()
            .map(|record| (topic_name.clone(), partition_index, record))
            .collect(),
    )
    .await;
}

async fn produce_minikafka(
    partition_client: Arc<PartitionClient>,
    _connection: String,
    _topic_name: String,
    _partition_index: i32,
    records: Vec<Record>,
) {
    // TODO: remove this hack
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    partition_client.produce(records).await.unwrap();
}

async fn consume_rdkafka(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    n: usize,
) -> Vec<Record> {
    rdkafka_helper::consume(&connection, &topic_name, partition_index, n).await
}

async fn consume_minikafka(
    partition_client: Arc<PartitionClient>,
    _connection: String,
    _topic_name: String,
    _partition_index: i32,
    n: usize,
) -> Vec<Record> {
    // TODO: use a proper stream here
    let mut records = vec![];
    let mut offset = 0;
    while records.len() < n {
        let res = partition_client
            .fetch_records(offset, 0..1_000_000)
            .await
            .unwrap()
            .0;
        assert!(!res.is_empty());
        for record in res {
            offset = offset.max(record.offset);
            records.push(record.record);
        }
    }
    records.into_iter().take(n).collect()
}
