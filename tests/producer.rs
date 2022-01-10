use futures::{future::FusedFuture, pin_mut, FutureExt};
use minikafka::client::{
    producer::{aggregator::RecordAggregator, BatchProducerBuilder},
    Client,
};
use std::time::Duration;

mod test_helpers;
use std::sync::Arc;
use test_helpers::{random_topic_name, record};

#[tokio::test]
async fn test_batch_producer() {
    let connection = maybe_skip_kafka_integration!();
    let client = Client::new_plain(vec![connection]).await.unwrap();

    let topic = random_topic_name();
    client.create_topic(&topic, 1, 1).await.unwrap();

    let record = record();

    let partition_client = Arc::new(client.partition_client(&topic, 0).await.unwrap());
    let producer = BatchProducerBuilder::new(partition_client)
        .with_linger(Duration::from_secs(2))
        .build(RecordAggregator::new(record.approximate_size() * 2 + 1));

    let a = producer.produce(record.clone()).fuse();
    pin_mut!(a);

    let b = producer.produce(record.clone()).fuse();
    pin_mut!(b);

    futures::select! {
        _ = a => panic!("a finished!"),
        _ = b => panic!("b finished!"),
        _ = tokio::time::sleep(Duration::from_millis(10)).fuse() => {}
    };

    let c = producer.produce(record).fuse();
    pin_mut!(c);

    // Publish third record, should trigger flush of first and second, but not third
    loop {
        futures::select! {
            r = a => r.unwrap(),
            r = b => r.unwrap(),
            _ = c => panic!("c finished!"),
            _ = tokio::time::sleep(Duration::from_millis(10)).fuse() => break
        };
    }

    assert!(a.is_terminated());
    assert!(b.is_terminated());

    // Third record should eventually be published
    tokio::time::timeout(Duration::from_secs(5), c)
        .await
        .expect("no timeout")
        .unwrap();
}
