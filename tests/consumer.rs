use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::time::timeout;

use minikafka::client::{
    consumer::{StreamConsumer, StreamConsumerBuilder},
    Client,
};
use test_helpers::{random_topic_name, record};

mod test_helpers;

#[tokio::test]
async fn test_stream_consumer() {
    let connection = maybe_skip_kafka_integration!();
    let client = Client::new_plain(vec![connection]).await.unwrap();

    let topic = random_topic_name();
    client.create_topic(&topic, 1, 1).await.unwrap();

    let record = record();

    let partition_client = Arc::new(client.partition_client(&topic, 0).await.unwrap());
    partition_client
        .produce(vec![record.clone()])
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
    assert_ok(timeout(Duration::from_millis(1), stream.next()).await);

    // No further records
    timeout(Duration::from_millis(1), stream.next())
        .await
        .expect_err("timeout");

    partition_client
        .produce(vec![record.clone(), record.clone()])
        .await
        .unwrap();

    // Get second record
    assert_ok(timeout(Duration::from_millis(1), stream.next()).await);

    // Get third record
    assert_ok(timeout(Duration::from_millis(1), stream.next()).await);

    // No further records
    timeout(Duration::from_millis(1), stream.next())
        .await
        .expect_err("timeout");
}
