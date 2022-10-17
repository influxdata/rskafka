use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use futures::{Stream, StreamExt};
use tokio::time::timeout;

use rskafka::{
    client::{
        consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder},
        error::{Error, ProtocolError},
        partition::{Compression, UnknownTopicHandling},
        ClientBuilder,
    },
    record::RecordAndOffset,
};
use test_helpers::{maybe_start_logging, random_topic_name, record, TEST_TIMEOUT};

mod test_helpers;

#[tokio::test]
async fn test_stream_consumer_start_at_0() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record = record(b"x");

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );
    partition_client
        .produce(vec![record.clone()], Compression::NoCompression)
        .await
        .unwrap();

    let mut stream = StreamConsumerBuilder::new(Arc::clone(&partition_client), StartOffset::At(0))
        .with_max_wait_ms(50)
        .build();

    // Fetch first record
    assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);

    // No further records
    assert_stream_pending(&mut stream).await;

    partition_client
        .produce(
            vec![record.clone(), record.clone()],
            Compression::NoCompression,
        )
        .await
        .unwrap();

    // Get second record
    assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);

    // Get third record
    assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);

    // No further records
    assert_stream_pending(&mut stream).await;
}

#[tokio::test]
async fn test_stream_consumer_start_at_1() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record_1 = record(b"x");
    let record_2 = record(b"y");

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );
    partition_client
        .produce(
            vec![record_1.clone(), record_2.clone()],
            Compression::NoCompression,
        )
        .await
        .unwrap();

    let mut stream = StreamConsumerBuilder::new(Arc::clone(&partition_client), StartOffset::At(1))
        .with_max_wait_ms(50)
        .build();

    // Skips first record
    let (record_and_offset, _watermark) = assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);
    assert_eq!(record_and_offset.record, record_2);

    // No further records
    assert_stream_pending(&mut stream).await;
}

#[tokio::test]
async fn test_stream_consumer_offset_out_of_range() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );

    let mut stream = StreamConsumerBuilder::new(partition_client, StartOffset::At(1)).build();

    let error = stream.next().await.expect("stream not empty").unwrap_err();
    assert_matches!(
        error,
        Error::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            ..
        }
    );

    // stream ends
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn test_stream_consumer_start_at_earliest() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record_1 = record(b"x");
    let record_2 = record(b"y");

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );
    partition_client
        .produce(vec![record_1.clone()], Compression::NoCompression)
        .await
        .unwrap();

    let mut stream =
        StreamConsumerBuilder::new(Arc::clone(&partition_client), StartOffset::Earliest)
            .with_max_wait_ms(50)
            .build();

    // Fetch first record
    let (record_and_offset, _) = assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);
    assert_eq!(record_and_offset.record, record_1);

    // No further records
    assert_stream_pending(&mut stream).await;

    partition_client
        .produce(vec![record_2.clone()], Compression::NoCompression)
        .await
        .unwrap();

    // Get second record
    let (record_and_offset, _) = assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);
    assert_eq!(record_and_offset.record, record_2);

    // No further records
    assert_stream_pending(&mut stream).await;
}

#[tokio::test]
async fn test_stream_consumer_start_at_earliest_empty() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record = record(b"x");

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );

    let mut stream =
        StreamConsumerBuilder::new(Arc::clone(&partition_client), StartOffset::Earliest)
            .with_max_wait_ms(50)
            .build();

    // No records yet
    assert_stream_pending(&mut stream).await;

    partition_client
        .produce(vec![record.clone()], Compression::NoCompression)
        .await
        .unwrap();

    // Get second record
    let (record_and_offset, _) = assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);
    assert_eq!(record_and_offset.record, record);

    // No further records
    assert_stream_pending(&mut stream).await;
}

#[tokio::test]
async fn test_stream_consumer_start_at_earliest_after_deletion() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!(delete);
    if !test_cfg.broker_impl.supports_deletes() {
        println!("Skipping due to missing delete support");
        return;
    }

    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record_1 = record(b"x");
    let record_2 = record(b"y");

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );
    partition_client
        .produce(
            vec![record_1.clone(), record_2.clone()],
            Compression::NoCompression,
        )
        .await
        .unwrap();

    partition_client.delete_records(1, 1_000).await.unwrap();

    let mut stream =
        StreamConsumerBuilder::new(Arc::clone(&partition_client), StartOffset::Earliest)
            .with_max_wait_ms(50)
            .build();

    // First record skipped / deleted
    let (record_and_offset, _) = assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);
    assert_eq!(record_and_offset.record, record_2);

    // No further records
    assert_stream_pending(&mut stream).await;
}

#[tokio::test]
async fn test_stream_consumer_start_at_latest() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record_1 = record(b"x");
    let record_2 = record(b"y");

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );
    partition_client
        .produce(vec![record_1.clone()], Compression::NoCompression)
        .await
        .unwrap();

    let mut stream = StreamConsumerBuilder::new(Arc::clone(&partition_client), StartOffset::Latest)
        .with_max_wait_ms(50)
        .build();

    // First record skipped
    assert_stream_pending(&mut stream).await;

    partition_client
        .produce(vec![record_2.clone()], Compression::NoCompression)
        .await
        .unwrap();

    // Get second record
    let (record_and_offset, _) = assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);
    assert_eq!(record_and_offset.record, record_2);

    // No further records
    assert_stream_pending(&mut stream).await;
}

#[tokio::test]
async fn test_stream_consumer_start_at_latest_empty() {
    maybe_start_logging();

    let test_cfg = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(test_cfg.bootstrap_brokers)
        .build()
        .await
        .unwrap();
    let controller_client = client.controller_client().unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record = record(b"x");

    let partition_client = Arc::new(
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    );

    let mut stream = StreamConsumerBuilder::new(Arc::clone(&partition_client), StartOffset::Latest)
        .with_max_wait_ms(50)
        .build();

    // No records yet
    assert_stream_pending(&mut stream).await;

    partition_client
        .produce(vec![record.clone()], Compression::NoCompression)
        .await
        .unwrap();

    // Get second record
    let (record_and_offset, _) = assert_ok(timeout(TEST_TIMEOUT, stream.next()).await);
    assert_eq!(record_and_offset.record, record);

    // No further records
    assert_stream_pending(&mut stream).await;
}

fn assert_ok(
    r: Result<Option<<StreamConsumer as Stream>::Item>, tokio::time::error::Elapsed>,
) -> (RecordAndOffset, i64) {
    r.expect("no timeout")
        .expect("some records")
        .expect("no error")
}

/// Assert that given stream is pending.
///
/// This will will try to poll the stream for a bit to ensure that async IO has a chance to catch up.
async fn assert_stream_pending<S>(stream: &mut S)
where
    S: Stream + Send + Unpin,
    S::Item: std::fmt::Debug,
{
    tokio::select! {
        e = stream.next() => panic!("stream is not pending, yielded: {e:?}"),
        _ = tokio::time::sleep(Duration::from_secs(1)) => {},
    };
}
