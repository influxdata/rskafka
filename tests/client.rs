use assert_matches::assert_matches;
use chrono::{TimeZone, Utc};
use rskafka::{
    client::{
        error::{Error as ClientError, ProtocolError, ServerErrorResponse},
        partition::{Compression, OffsetAt, UnknownTopicHandling},
        ClientBuilder,
    },
    record::{Record, RecordAndOffset},
    BackoffConfig,
};
use std::{collections::BTreeMap, env, str::FromStr, sync::Arc, time::Duration};

mod test_helpers;
use test_helpers::{maybe_start_logging, random_topic_name, record, BrokerImpl, TEST_TIMEOUT};

#[tokio::test]
async fn test_plain() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sasl() {
    maybe_start_logging();
    if env::var("TEST_INTEGRATION").is_err() {
        return;
    }
    if env::var("KAFKA_SASL_CONNECT").is_err() {
        eprintln!("Skipping sasl test.");
        return;
    }
    let test_cfg = maybe_skip_kafka_integration!();
    // Redpanda broker doesn't support SASL/PLAIN at this moment.
    if test_cfg.broker_impl != BrokerImpl::Kafka {
        return;
    }
    ClientBuilder::new(vec![env::var("KAFKA_SASL_CONNECT").unwrap()])
        .sasl_config(rskafka::client::SaslConfig::Plain {
            username: "admin".to_string(),
            password: "admin-secret".to_string(),
        })
        .build()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_topic_crud() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
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

    // create two topics
    let mut new_topics = vec![];
    for offset in 1..=2 {
        let new_topic = format!("{}{}", prefix, max_id + offset);
        controller_client
            .create_topic(&new_topic, 2, 1, 5_000)
            .await
            .unwrap();
        new_topics.push(new_topic);
    }

    // might take a while to converge
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let topics = client.list_topics().await.unwrap();
            if new_topics
                .iter()
                .all(|new_topic| topics.iter().any(|t| &t.name == new_topic))
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // topic already exists
    let err = controller_client
        .create_topic(&new_topics[0], 1, 1, 5_000)
        .await
        .unwrap_err();
    match err {
        ClientError::ServerError {
            protocol_error: ProtocolError::TopicAlreadyExists,
            ..
        } => {}
        _ => panic!("Unexpected error: {}", err),
    }

    // delete one topic
    controller_client
        .delete_topic(&new_topics[0], 5_000)
        .await
        .unwrap();

    // might take a while to converge
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let topics = client.list_topics().await.unwrap();
            if topics.iter().all(|t| t.name != new_topics[0])
                && topics.iter().any(|t| t.name == new_topics[1])
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_partition_client() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();

    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, 1, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(topic_name.clone(), 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    assert_eq!(partition_client.topic(), &topic_name);
    assert_eq!(partition_client.partition(), 0);
}

#[tokio::test]
async fn test_non_existing_partition() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();

    // do NOT create the topic

    // short timeout, should just check that we will never finish
    tokio::time::timeout(Duration::from_millis(100), async {
        client
            .partition_client(topic_name.clone(), 0, UnknownTopicHandling::Retry)
            .await
            .unwrap();
    })
    .await
    .unwrap_err();

    let err = client
        .partition_client(topic_name.clone(), 0, UnknownTopicHandling::Error)
        .await
        .unwrap_err();
    assert_matches!(
        err,
        ClientError::ServerError {
            protocol_error: ProtocolError::UnknownTopicOrPartition,
            ..
        }
    );
}

// Disabled as currently no TLS integration tests
#[ignore]
#[tokio::test]
#[cfg(feature = "transport-tls")]
async fn test_tls() {
    maybe_start_logging();

    let mut root_store = rustls::RootCertStore::empty();

    let file = std::fs::File::open("/tmp/cluster-ca.crt").unwrap();
    let mut reader = std::io::BufReader::new(file);
    match rustls_pemfile::read_one(&mut reader).unwrap().unwrap() {
        rustls_pemfile::Item::X509Certificate(key) => {
            root_store.add(key).unwrap();
        }
        _ => unreachable!(),
    }

    let file = std::fs::File::open("/tmp/ca.crt").unwrap();
    let mut reader = std::io::BufReader::new(file);
    let producer_root = match rustls_pemfile::read_one(&mut reader).unwrap().unwrap() {
        rustls_pemfile::Item::X509Certificate(key) => key,
        _ => unreachable!(),
    };

    let file = std::fs::File::open("/tmp/ca.key").unwrap();
    let mut reader = std::io::BufReader::new(file);
    let private_key = match rustls_pemfile::read_one(&mut reader).unwrap().unwrap() {
        rustls_pemfile::Item::Pkcs8Key(key) => rustls::pki_types::PrivateKeyDer::Pkcs8(key),
        _ => unreachable!(),
    };

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![producer_root], private_key)
        .unwrap();

    let test_cfg = maybe_skip_kafka_integration!();
    ClientBuilder::new(test_cfg.bootstrap_brokers)
        .tls_config(Arc::new(config))
        .build()
        .await
        .unwrap();
}

#[cfg(feature = "transport-socks5")]
#[tokio::test]
async fn test_socks5() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!(socks5);
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .socks5_proxy(test_cfg.socks5_proxy.unwrap())
        .build()
        .await
        .unwrap();

    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, 1, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(topic_name, 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();

    let record = record(b"");
    partition_client
        .produce(vec![record.clone()], Compression::NoCompression)
        .await
        .unwrap();

    let (mut records, _watermark) = partition_client
        .fetch_records(0, 1..10_000_001, 1_000)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let record2 = records.remove(0).record;
    assert_eq!(record, record2);
}

#[tokio::test]
async fn test_produce_empty() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, n_partitions, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(&topic_name, 1, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    partition_client
        .produce(vec![], Compression::NoCompression)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_consume_empty() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, n_partitions, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(&topic_name, 1, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    let (records, watermark) = partition_client
        .fetch_records(0, 1..10_000, 1_000)
        .await
        .unwrap();
    assert!(records.is_empty());
    assert_eq!(watermark, 0);
}

#[tokio::test]
async fn test_consume_offset_out_of_range() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 2;

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, n_partitions, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(&topic_name, 1, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    let record = record(b"");
    let offsets = partition_client
        .produce(vec![record], Compression::NoCompression)
        .await
        .unwrap();
    let offset = offsets[0];

    let err = partition_client
        .fetch_records(offset + 2, 1..10_000, 1_000)
        .await
        .unwrap_err();
    assert_matches!(
        err,
        ClientError::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            response: Some(ServerErrorResponse::PartitionFetchState { .. }),
            ..
        }
    );
}

#[tokio::test]
async fn test_get_offset() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();
    let n_partitions = 1;

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers.clone())
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, n_partitions, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(topic_name.clone(), 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();

    assert_eq!(
        partition_client
            .get_offset(OffsetAt::Earliest)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        partition_client.get_offset(OffsetAt::Latest).await.unwrap(),
        0
    );

    // add some data
    // use out-of order timestamps to ensure our "lastest offset" logic works
    let record_early = record(b"");
    let record_late = Record {
        timestamp: record_early.timestamp + chrono::Duration::seconds(1),
        ..record_early.clone()
    };
    let offsets = partition_client
        .produce(vec![record_late.clone()], Compression::NoCompression)
        .await
        .unwrap();
    assert_eq!(offsets[0], 0);

    let offsets = partition_client
        .produce(vec![record_early.clone()], Compression::NoCompression)
        .await
        .unwrap();
    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets[0], 1);

    assert_eq!(
        partition_client
            .get_offset(OffsetAt::Earliest)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        partition_client.get_offset(OffsetAt::Latest).await.unwrap(),
        2
    );
}

#[tokio::test]
async fn test_produce_consume_size_cutoff() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, 1, 1, 5_000)
        .await
        .unwrap();

    let partition_client = Arc::new(
        client
            .partition_client(&topic_name, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );

    let record_1 = large_record();
    let record_2 = large_record();
    let record_3 = large_record();

    // produce in spearate request so we have three record batches
    partition_client
        .produce(vec![record_1.clone()], Compression::NoCompression)
        .await
        .unwrap();
    partition_client
        .produce(vec![record_2.clone()], Compression::NoCompression)
        .await
        .unwrap();
    partition_client
        .produce(vec![record_3.clone()], Compression::NoCompression)
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
                .fetch_records(0, 1..limit, 1_000)
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

#[tokio::test]
async fn test_consume_midbatch() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, 1, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(&topic_name, 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();

    // produce two records into a single batch
    let record_1 = record(b"x");
    let record_2 = record(b"y");

    let offsets = partition_client
        .produce(
            vec![record_1.clone(), record_2.clone()],
            Compression::NoCompression,
        )
        .await
        .unwrap();
    let _offset_1 = offsets[0];
    let offset_2 = offsets[1];

    // when fetching from the middle of the record batch, the server will return both records but we should filter out
    // the first one on the client side
    let (records, _watermark) = partition_client
        .fetch_records(offset_2, 1..10_000, 1_000)
        .await
        .unwrap();
    assert_eq!(
        records,
        vec![RecordAndOffset {
            record: record_2,
            offset: offset_2
        },],
    );
}

#[tokio::test]
async fn test_delete_records() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!(delete);
    let topic_name = random_topic_name();

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, 1, 1, 5_000)
        .await
        .unwrap();

    let partition_client = client
        .partition_client(&topic_name, 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();

    // produce the following record batches:
    // - record_1
    // - record_2, record_3
    // - record_4
    let record_1 = record(b"");
    let record_2 = record(b"x");
    let record_3 = record(b"y");
    let record_4 = record(b"z");

    let offsets = partition_client
        .produce(vec![record_1.clone()], Compression::NoCompression)
        .await
        .unwrap();
    let offset_1 = offsets[0];

    let offsets = partition_client
        .produce(
            vec![record_2.clone(), record_3.clone()],
            Compression::NoCompression,
        )
        .await
        .unwrap();
    let offset_2 = offsets[0];
    let offset_3 = offsets[1];

    let offsets = partition_client
        .produce(vec![record_4.clone()], Compression::NoCompression)
        .await
        .unwrap();
    let offset_4 = offsets[0];

    // delete from the middle of the 2nd batch
    partition_client
        .delete_records(offset_3, 1_000)
        .await
        .unwrap();

    // fetching data before the record fails
    let err = partition_client
        .fetch_records(offset_1, 1..10_000, 1_000)
        .await
        .unwrap_err();
    assert_matches!(
        err,
        ClientError::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            response: Some(ServerErrorResponse::PartitionFetchState { .. }),
            ..
        }
    );
    let err = partition_client
        .fetch_records(offset_2, 1..10_000, 1_000)
        .await
        .unwrap_err();
    assert_matches!(
        err,
        ClientError::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            response: Some(ServerErrorResponse::PartitionFetchState { .. }),
            ..
        }
    );

    // fetching untouched records still works, however the middle record batch is NOT half-deleted and still contains
    // record_2. `fetch_records` should filter this however.
    let (records, _watermark) = partition_client
        .fetch_records(offset_3, 1..10_000, 1_000)
        .await
        .unwrap();
    assert_eq!(
        records,
        vec![
            RecordAndOffset {
                record: record_3,
                offset: offset_3
            },
            RecordAndOffset {
                record: record_4,
                offset: offset_4
            },
        ],
    );

    // offsets reflect deletion
    assert_eq!(
        partition_client
            .get_offset(OffsetAt::Earliest)
            .await
            .unwrap(),
        offset_3
    );
    assert_eq!(
        partition_client.get_offset(OffsetAt::Latest).await.unwrap(),
        offset_4 + 1
    );
}

#[tokio::test]
async fn test_client_backoff_terminates() {
    maybe_start_logging();

    let mut test_cfg = maybe_skip_kafka_integration!();

    test_cfg.bootstrap_brokers = vec!["localhost:9000".to_owned()];

    let client_builder =
        ClientBuilder::new(test_cfg.bootstrap_brokers).backoff_config(BackoffConfig {
            deadline: Some(Duration::from_millis(100)),
            ..Default::default()
        });

    match client_builder.build().await {
        Err(rskafka::client::error::Error::Connection(e)) => {
            // Error can be slightly different depending on the exact underlying error.
            assert!(
                e.to_string().starts_with(concat!(
                    "all retries failed: Retry exceeded deadline. ",
                    "Source: error connecting to broker \"localhost:9000\""
                )),
                "expected error to start with \"all retries failed...\", actual: {}",
                e
            );
        }
        _ => {
            unreachable!();
        }
    };
    println!("Some");
}

pub fn large_record() -> Record {
    Record {
        key: Some(b"".to_vec()),
        value: Some(vec![b'x'; 1024]),
        headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
        timestamp: Utc.timestamp_millis_opt(1337).unwrap(),
    }
}
