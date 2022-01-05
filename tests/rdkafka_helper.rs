use futures::{StreamExt, TryStreamExt};
use minikafka::record::Record;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    message::{Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, Message, TopicPartitionList,
};
use time::OffsetDateTime;

/// Create topic
pub async fn create_topic(connection: &str, topic_name: &str, n_partitions: usize) {
    // TODO: use minikafka instead of rdkafka

    // create client
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", connection);
    cfg.set("message.timeout.ms", "5000");
    let client: AdminClient<_> = cfg.create().unwrap();

    // setup request
    let topic = NewTopic::new(topic_name, n_partitions as i32, TopicReplication::Fixed(1));

    let opts = AdminOptions::default();
    let mut results = client.create_topics([&topic], &opts).await.unwrap();
    assert_eq!(results.len(), 1, "created exactly one topic");
    let result = results.pop().expect("just checked the vector length");
    result.unwrap();
}

/// Produce.
pub async fn produce(connection: &str, records: Vec<(String, i32, Record)>) {
    // create client
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", connection);
    cfg.set("message.timeout.ms", "5000");
    let client: FutureProducer<_> = cfg.create().unwrap();

    // create record
    for (topic_name, partition_index, record) in records {
        let mut headers = OwnedHeaders::new();
        for (k, v) in record.headers {
            headers = headers.add(&k, &v);
        }

        let f_record = FutureRecord::to(&topic_name)
            .partition(partition_index)
            .key(&record.key)
            .payload(&record.value)
            .headers(headers)
            .timestamp((record.timestamp.unix_timestamp_nanos() / 1_000_000) as i64);
        client.send(f_record, Timeout::Never).await.unwrap();
    }
}

/// Consume
pub async fn consume(
    connection: &str,
    topic_name: &str,
    partition_index: i32,
    n: usize,
) -> Vec<Record> {
    // create client
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", connection);
    cfg.set("message.timeout.ms", "5000");
    cfg.set("group.id", "foo");
    cfg.set("auto.offset.reset", "smallest");
    let client: StreamConsumer<_> = cfg.create().unwrap();

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition(topic_name, partition_index);
    client.assign(&assignment).unwrap();

    client
        .stream()
        .take(n)
        .map_ok(|msg| Record {
            key: msg.key().map(|k| k.to_vec()).unwrap_or_default(),
            value: msg.payload().map(|v| v.to_vec()).unwrap_or_default(),
            headers: msg
                .headers()
                .map(|headers| {
                    (0..headers.count())
                        .map(|i| {
                            let (k, v) = headers.get(i).unwrap();
                            (k.to_owned(), v.to_vec())
                        })
                        .collect()
                })
                .unwrap_or_default(),
            timestamp: OffsetDateTime::from_unix_timestamp_nanos(
                msg.timestamp().to_millis().unwrap_or_default() as i128 * 1_000_000,
            )
            .unwrap(),
        })
        .try_collect()
        .await
        .unwrap()
}
