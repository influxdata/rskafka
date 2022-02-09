use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use futures::{Stream, StreamExt};
use tokio::time::timeout;

use rskafka::client::{
    consumer::{StreamConsumer, StreamConsumerBuilder},
    error::{Error, ProtocolError},
    partition::Compression,
    ClientBuilder,
};
use test_helpers::{maybe_start_logging, random_topic_name, record};

mod test_helpers;

#[tokio::test]
async fn test_stream_consumer() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
    let controller_client = client.controller_client().await.unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let record = record();

    let partition_client = Arc::new(client.partition_client(&topic, 0).await.unwrap());
    partition_client
        .produce(vec![record.clone()], Compression::NoCompression)
        .await
        .unwrap();

    let mut stream = StreamConsumerBuilder::new(Arc::clone(&partition_client), 0).build();

    let assert_ok =
        |r: Result<Option<<StreamConsumer as Stream>::Item>, tokio::time::error::Elapsed>| {
            r.expect("no timeout")
                .expect("some records")
                .expect("no error")
        };

    // Fetch first record
    assert_ok(timeout(Duration::from_millis(100), stream.next()).await);

    // No further records
    timeout(Duration::from_millis(100), stream.next())
        .await
        .expect_err("timeout");

    partition_client
        .produce(
            vec![record.clone(), record.clone()],
            Compression::NoCompression,
        )
        .await
        .unwrap();

    // Get second record
    assert_ok(timeout(Duration::from_millis(100), stream.next()).await);

    // Get third record
    assert_ok(timeout(Duration::from_millis(100), stream.next()).await);

    // No further records
    timeout(Duration::from_millis(100), stream.next())
        .await
        .expect_err("timeout");
}

#[tokio::test]
async fn test_stream_consumer_offset_out_of_range() {
    maybe_start_logging();

    let connection = maybe_skip_kafka_integration!();
    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
    let controller_client = client.controller_client().await.unwrap();

    let topic = random_topic_name();
    controller_client
        .create_topic(&topic, 1, 1, 5_000)
        .await
        .unwrap();

    let partition_client = Arc::new(client.partition_client(&topic, 0).await.unwrap());

    let mut stream = StreamConsumerBuilder::new(partition_client, 1).build();

    let error = stream.next().await.expect("stream not empty").unwrap_err();
    assert_matches!(
        error,
        Error::ServerError(ProtocolError::OffsetOutOfRange, _)
    );

    // stream ends
    assert!(stream.next().await.is_none());
}
