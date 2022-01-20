use rskafka::{
    client::{
        error::{Error as ClientError, ProtocolError},
        partition::PartitionClient,
        ClientBuilder,
    },
    record::{Record, RecordAndOffset},
};
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};

mod rdkafka_helper;

mod test_helpers;
use test_helpers::{now, random_topic_name, record};

use crate::test_helpers::maybe_start_logging;

#[tokio::test]
async fn test_plain() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    ClientBuilder::new(vec![connection]).build().await.unwrap();
}

#[tokio::test]
async fn test_partition_leader() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
    let controller_client = client.controller_client().await.unwrap();
    let topic_name = random_topic_name();

    controller_client
        .create_topic(&topic_name, 2, 1)
        .await
        .unwrap();
    let client = client.partition_client(&topic_name, 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async move {
        loop {
            match client.get_cached_leader().await {
                Ok(_) => break,
                Err(e) => {
                    println!("Cannot get cached leader: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_topic_crud() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
    let controller_client = client.controller_client().await.unwrap();
    let topics = client.list_topics().await.unwrap();

    let prefix = "test_topic_crud_";

    let mut max_id = 0;
    for topic in topics {
        if let Some(maybe_int) = topic.name.strip_prefix(prefix) {
            if let Ok(i) = usize::from_str(maybe_int) {
                max_id = max_id.max(i);
            }
        }
    }
    let new_topic = format!("{}{}", prefix, max_id + 1);
    controller_client
        .create_topic(&new_topic, 2, 1)
        .await
        .unwrap();

    // might take a while to converge
    tokio::time::timeout(Duration::from_millis(1_000), async {
        loop {
            let topics = client.list_topics().await.unwrap();
            let topic = topics.iter().find(|t| t.name == new_topic);
            if topic.is_some() {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let err = controller_client
        .create_topic(&new_topic, 1, 1)
        .await
        .unwrap_err();
    match err {
        ClientError::ServerError(ProtocolError::TopicAlreadyExists, _) => {}
        _ => panic!("Unexpected error: {}", err),
    }
}

// Disabled as currently no TLS integration tests
#[ignore]
#[tokio::test]
async fn test_tls() {
    maybe_start_logging();

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
    ClientBuilder::new(vec![connection])
        .tls_config(Arc::new(config))
        .build()
        .await
        .unwrap();
}

// Disabled as currently no SOCKS5 integration tests
#[cfg(feature = "transport-socks5")]
#[ignore]
#[tokio::test]
async fn test_socks5() {
    maybe_start_logging();

    let client = ClientBuilder::new(vec!["my-cluster-kafka-bootstrap:9092".to_owned()])
        .socks5_proxy("localhost:1080".to_owned())
        .build()
        .await
        .unwrap();
    let partition_client = client.partition_client("myorg_mybucket", 0).await.unwrap();
    partition_client
        .fetch_records(0, 1..10_000_001, 1_000)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_produce_empty() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
    let controller_client = client.controller_client().await.unwrap();
    controller_client
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
async fn test_produce_rskafka_consume_rdkafka() {
    assert_produce_consume(produce_rskafka, consume_rdkafka).await;
}

#[tokio::test]
async fn test_produce_rdkafka_consume_rskafka() {
    assert_produce_consume(produce_rdkafka, consume_rskafka).await;
}

#[tokio::test]
async fn test_produce_rskafka_consume_rskafka() {
    assert_produce_consume(produce_rskafka, consume_rskafka).await;
}

#[tokio::test]
async fn test_get_high_watermark() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 1;

    let client = ClientBuilder::new(vec![connection.clone()])
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().await.unwrap();
    controller_client
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

    let offsets = partition_client
        .produce(vec![record_early.clone()])
        .await
        .unwrap();
    assert_eq!(offsets.len(), 1);
    let expected = offsets[0] + 1;

    assert_eq!(
        partition_client.get_high_watermark().await.unwrap(),
        expected
    );
}

async fn assert_produce_consume<F1, G1, F2, G2>(f_produce: F1, f_consume: F2)
where
    F1: Fn(Arc<PartitionClient>, String, String, i32, Vec<Record>) -> G1,
    G1: std::future::Future<Output = Vec<i64>>,
    F2: Fn(Arc<PartitionClient>, String, String, i32, usize) -> G2,
    G2: std::future::Future<Output = Vec<RecordAndOffset>>,
{
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = ClientBuilder::new(vec![connection.clone()])
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().await.unwrap();
    controller_client
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
    let mut offsets = vec![];
    offsets.append(
        &mut f_produce(
            Arc::clone(&partition_client),
            connection.clone(),
            topic_name.clone(),
            1,
            vec![record_1.clone(), record_2.clone()],
        )
        .await,
    );
    offsets.append(
        &mut f_produce(
            Arc::clone(&partition_client),
            connection.clone(),
            topic_name.clone(),
            1,
            vec![record_3.clone()],
        )
        .await,
    );

    // consume
    let actual = f_consume(partition_client, connection, topic_name, 1, 3).await;
    let expected: Vec<_> = offsets
        .into_iter()
        .zip([record_1, record_2, record_3])
        .map(|(offset, record)| RecordAndOffset { record, offset })
        .collect();
    assert_eq!(actual, expected);
}

async fn produce_rdkafka(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    records: Vec<Record>,
) -> Vec<i64> {
    rdkafka_helper::produce(
        &connection,
        records
            .into_iter()
            .map(|record| (topic_name.clone(), partition_index, record))
            .collect(),
    )
    .await
}

async fn produce_rskafka(
    partition_client: Arc<PartitionClient>,
    _connection: String,
    _topic_name: String,
    _partition_index: i32,
    records: Vec<Record>,
) -> Vec<i64> {
    partition_client.produce(records).await.unwrap()
}

async fn consume_rdkafka(
    _partition_client: Arc<PartitionClient>,
    connection: String,
    topic_name: String,
    partition_index: i32,
    n: usize,
) -> Vec<RecordAndOffset> {
    rdkafka_helper::consume(&connection, &topic_name, partition_index, n).await
}

async fn consume_rskafka(
    partition_client: Arc<PartitionClient>,
    _connection: String,
    _topic_name: String,
    _partition_index: i32,
    n: usize,
) -> Vec<RecordAndOffset> {
    // TODO: use a proper stream here
    let mut records = vec![];
    let mut offset = 0;
    while records.len() < n {
        let res = partition_client
            .fetch_records(offset, 0..1_000_000, 1_000)
            .await
            .unwrap()
            .0;
        assert!(!res.is_empty());
        for record in res {
            offset = offset.max(record.offset);
            records.push(record);
        }
    }
    records.into_iter().take(n).collect()
}

#[tokio::test]
async fn test_produce_consume_size_cutoff() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
    let controller_client = client.controller_client().await.unwrap();
    controller_client
        .create_topic(&topic_name, 1, 1)
        .await
        .unwrap();

    let partition_client = Arc::new(client.partition_client(&topic_name, 0).await.unwrap());

    let record_1 = large_record();
    let record_2 = large_record();
    let record_3 = large_record();

    // produce in spearate request so we have three record batches
    partition_client
        .produce(vec![record_1.clone()])
        .await
        .unwrap();
    partition_client
        .produce(vec![record_2.clone()])
        .await
        .unwrap();
    partition_client
        .produce(vec![record_3.clone()])
        .await
        .unwrap();

    // `max_bytes` limits seem to work slightly differently under redpanda and Apache Kafka. It seems that when amaing
    // for an cutoff "in the middle" of a record batch, Kafka just cuts off the batch while redpanda delivers it.
    // However both deliver at least one record batch.
    let limit_minimal = 2;
    let limit_one_and_half = (record_1.approximate_size() + record_2.approximate_size() / 2) as i32;

    // set up a small test closure
    let get_with_limit = |limit: i32| {
        let partition_client = Arc::clone(&partition_client);
        let record_1 = record_1.clone();
        let record_2 = record_2.clone();

        async move {
            let (records, _high_watermark) = partition_client
                .fetch_records(0, 1..(limit as i32), 1_000)
                .await
                .unwrap();
            if records.len() == 1 {
                assert_eq!(records[0].record, record_1);
            } else {
                assert_eq!(records.len(), 2);
                assert_eq!(records[0].record, record_1);
                assert_eq!(records[1].record, record_2);
            }

            records.len()
        }
    };

    // run tests
    let n_records_minimal = get_with_limit(limit_minimal).await;
    let n_records_one_and_half = get_with_limit(limit_one_and_half).await;

    // check our assumptions
    let is_kafka = (n_records_minimal == 1) && (n_records_one_and_half == 1);
    let is_redpanda = (n_records_minimal == 1) && (n_records_one_and_half == 2);
    assert!(is_kafka ^ is_redpanda);
}

pub fn large_record() -> Record {
    Record {
        key: b"".to_vec(),
        value: vec![b'x'; 1024],
        headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
        timestamp: now(),
    }
}
